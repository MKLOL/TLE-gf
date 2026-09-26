"""Problemset cache self-heal — a contest whose fetch failed must not stay
missing forever (complaint #299)."""
import asyncio
from types import SimpleNamespace

from tle.util import codeforces_api as cf
from tle.util.cache_system2 import _problems
from tle.util.db.cache_db_conn import CacheDbConn


def _contest(cid, start):
    return cf.Contest(id=cid, name=f'Round {cid}', startTimeSeconds=start,
                      durationSeconds=7200, phase='FINISHED')


FINISHED = [_contest(1, 100), _contest(2, 200), _contest(3, 300), _contest(4, 400)]


class TestSelection:
    def test_newest_missing_first_bounded(self):
        picked = _problems.select_missing_problemsets(FINISHED, {2}, set(), limit=2)
        assert [c.id for c in picked] == [4, 3]

    def test_unfetchable_are_skipped(self):
        picked = _problems.select_missing_problemsets(FINISHED, {2}, {4})
        assert [c.id for c in picked] == [3, 1]

    def test_nothing_missing(self):
        assert _problems.select_missing_problemsets(FINISHED, {1, 2, 3, 4}, set()) == []


class TestDb:
    def test_contest_ids_with_any_problem(self):
        db = CacheDbConn(':memory:')
        db.conn.execute("INSERT INTO problem2 (contest_id, [index], name) VALUES (1, 'A', 'x')")
        db.conn.execute("INSERT INTO problem2 (contest_id, [index], name) VALUES (1, 'B', 'y')")
        db.conn.execute("INSERT INTO problem2 (contest_id, [index], name) VALUES (3, 'A', 'z')")
        assert db.problemset_contest_ids() == {1, 3}


def _cache(cached_ids, fetch_results, monkeypatch):
    master = SimpleNamespace(
        contest_cache=SimpleNamespace(contests_by_phase={'FINISHED': list(FINISHED)}),
        conn=SimpleNamespace(problemset_contest_ids=lambda: set(cached_ids)))
    cache = _problems.ProblemsetCache(master)
    fetched, saved = [], []

    async def fetch(contest_id):
        fetched.append(contest_id)
        return fetch_results.get(contest_id, [])
    monkeypatch.setattr(cache, '_fetch_for_contest', fetch)
    monkeypatch.setattr(cache, '_save_problems', lambda problems: saved.extend(problems))
    monkeypatch.setattr(cache, '_update_from_disk', lambda: None)
    return cache, fetched, saved


class TestBackfill:
    def test_fetches_the_missing_batch_newest_first(self, monkeypatch):
        cache, fetched, saved = _cache({2}, {4: ['p4'], 3: ['p3a', 'p3b']}, monkeypatch)
        count, remaining = asyncio.run(cache._backfill_missing(limit=2))
        assert fetched == [4, 3]
        assert saved == ['p4', 'p3a', 'p3b']
        assert (count, remaining) == (3, 1)  # contest 1 still to go

    def test_a_contest_that_will_not_fetch_is_tried_once_per_run(self, monkeypatch):
        cache, fetched, saved = _cache(set(), {}, monkeypatch)
        assert asyncio.run(cache._backfill_missing(limit=10)) == (0, 4)
        assert asyncio.run(cache._backfill_missing(limit=10)) == (0, 4)
        assert fetched == [4, 3, 2, 1]  # not eight
        assert cache._unfetchable == {1, 2, 3, 4}

    def test_manual_repair_retries_previously_failed_background_fetches(self, monkeypatch):
        results = {}
        cache, fetched, saved = _cache({1, 2, 3}, results, monkeypatch)
        assert asyncio.run(cache._backfill_missing(limit=10)) == (0, 1)
        results[4] = ['p4']  # Codeforces has recovered since the background tick.
        assert asyncio.run(cache.update_missing()) == (1, 0)
        assert fetched == [4, 4]
        assert saved == ['p4']

    def test_the_gap_is_reported_once(self, monkeypatch, caplog):
        cache, _, _ = _cache(set(), {}, monkeypatch)
        with caplog.at_level('WARNING'):
            asyncio.run(cache._backfill_missing(limit=1))
            asyncio.run(cache._backfill_missing(limit=1))
        assert sum('no problemset cached' in r.message for r in caplog.records) == 1

    def test_update_missing_does_the_whole_backlog(self, monkeypatch):
        cache, fetched, _ = _cache(set(), {i: [f'p{i}'] for i in (1, 2, 3, 4)}, monkeypatch)
        count, remaining = asyncio.run(cache.update_missing())
        assert (count, remaining) == (4, 0)
        assert sorted(fetched) == [1, 2, 3, 4]

    def test_nothing_missing_is_quiet(self, monkeypatch, caplog):
        cache, fetched, _ = _cache({1, 2, 3, 4}, {}, monkeypatch)
        with caplog.at_level('INFO'):
            assert asyncio.run(cache._backfill_missing(limit=5)) == (0, 0)
        assert fetched == []
        assert not [r for r in caplog.records if 'backfill' in r.message.lower()]

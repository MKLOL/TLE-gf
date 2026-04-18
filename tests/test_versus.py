"""Tests for the versus cog's pure computation logic."""

import collections
import pytest

# We need a lightweight RatingChange-like namedtuple for testing
RatingChange = collections.namedtuple(
    'RatingChange',
    'contestId contestName handle rank ratingUpdateTimeSeconds oldRating newRating'
)


def _make_rc(contest_id, handle, rank):
    """Helper to create a minimal RatingChange for testing."""
    return RatingChange(
        contestId=contest_id,
        contestName=f'Contest {contest_id}',
        handle=handle,
        rank=rank,
        ratingUpdateTimeSeconds=1000000 + contest_id,
        oldRating=1500,
        newRating=1500,
    )


# Import the pure functions under test
from tle.cogs.versus import (
    _compute_versus_stats,
    _is_stale,
    _get_rating_changes,
    _list_shared_contests,
)
import asyncio
import time


class TestComputeVersusStats:
    def test_basic_two_users(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(2, 'alice', 5)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(2, 'bob', 3)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 2
        # Contest 1: alice rank 10 < bob rank 20 → alice wins
        # Contest 2: bob rank 3 < alice rank 5 → bob wins
        assert wins['alice'] == 1
        assert wins['bob'] == 1

    def test_three_users_placements(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 100)],
            'b': [_make_rc(1, 'b', 50)],
            'c': [_make_rc(1, 'c', 200)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        # b=50 < a=100 < c=200 → b is 1st, a is 2nd, c is 3rd
        assert wins['b'] == 1
        assert wins['a'] == 0
        assert wins['c'] == 0
        assert placements['b'][1] == 1
        assert placements['a'][2] == 1
        assert placements['c'][3] == 1

    def test_no_shared_contests(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(2, 'bob', 20)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0
        assert wins['alice'] == 0
        assert wins['bob'] == 0

    def test_tie_no_win_awarded(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(1, 'bob', 10)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        # Same rank → tie → no one gets a win
        assert wins['alice'] == 0
        assert wins['bob'] == 0
        # Both get 1st place (competition ranking)
        assert placements['alice'][1] == 1
        assert placements['bob'][1] == 1

    def test_competition_ranking_three_way(self):
        """Two users tie for 1st, third user gets 3rd (not 2nd)."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5)],
            'b': [_make_rc(1, 'b', 5)],
            'c': [_make_rc(1, 'c', 20)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        assert wins['a'] == 0  # Tie, no win
        assert wins['b'] == 0
        assert wins['c'] == 0
        # a and b both get 1st, c gets 3rd (competition ranking skips 2nd)
        assert placements['a'][1] == 1
        assert placements['b'][1] == 1
        assert placements['c'][3] == 1
        assert placements['c'].get(2, 0) == 0  # 2nd place not assigned

    def test_partial_overlap(self):
        """Only contests where 2+ users participated are counted."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10), _make_rc(3, 'a', 1)],
            'b': [_make_rc(1, 'b', 10), _make_rc(3, 'b', 2)],
            'c': [_make_rc(2, 'c', 5)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        # Shared: contest 1 (a, b), contest 2 (a, c), contest 3 (a, b) = 3 contests
        assert total == 3
        # Contest 1: a=5 beats b=10 → a wins
        # Contest 2: c=5 beats a=10 → c wins
        # Contest 3: a=1 beats b=2 → a wins
        assert wins['a'] == 2
        assert wins['b'] == 0
        assert wins['c'] == 1

    def test_empty_changes(self):
        handles = ['a', 'b']
        all_changes = {'a': [], 'b': []}
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0

    def test_missing_handle_in_changes(self):
        handles = ['a', 'b']
        all_changes = {'a': [_make_rc(1, 'a', 5)]}  # 'b' missing entirely
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0

    def test_multiple_contests_accumulate(self):
        handles = ['x', 'y']
        all_changes = {
            'x': [_make_rc(i, 'x', 10) for i in range(1, 6)],
            'y': [_make_rc(i, 'y', 20) for i in range(1, 6)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 5
        assert wins['x'] == 5  # x always has better rank
        assert wins['y'] == 0
        assert placements['x'][1] == 5
        assert placements['y'][2] == 5


class TestListSharedContests:
    def test_basic_two_users(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(2, 'alice', 5)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(2, 'bob', 3)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert len(rows) == 2
        by_id = {r['contest_id']: r for r in rows}
        assert by_id[1]['ranks'] == {'alice': 10, 'bob': 20}
        assert by_id[2]['ranks'] == {'alice': 5, 'bob': 3}
        assert by_id[1]['name'] == 'Contest 1'

    def test_sorted_newest_first(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(3, 'alice', 5), _make_rc(2, 'alice', 7)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(3, 'bob', 3),   _make_rc(2, 'bob', 9)],
        }
        rows = _list_shared_contests(handles, all_changes)
        # Time field is 1000000 + contest_id, so newest=3, then 2, then 1
        assert [r['contest_id'] for r in rows] == [3, 2, 1]

    def test_no_shared_contests(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(2, 'bob', 20)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert rows == []

    def test_empty_inputs(self):
        assert _list_shared_contests(['a', 'b'], {}) == []
        assert _list_shared_contests(['a', 'b'], {'a': [], 'b': []}) == []

    def test_partial_overlap_only_shared_included(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10)],            # contest 1 shared with a
            'c': [_make_rc(3, 'c', 1)],             # contest 3 only c — excluded
        }
        rows = _list_shared_contests(handles, all_changes)
        assert {r['contest_id'] for r in rows} == {1}
        assert rows[0]['ranks'] == {'a': 5, 'b': 10}

    def test_strict_requires_all_handles(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 5)],
            'c': [_make_rc(2, 'c', 20)],
        }
        non_strict = _list_shared_contests(handles, all_changes, strict=False)
        assert {r['contest_id'] for r in non_strict} == {1, 2}

        strict = _list_shared_contests(handles, all_changes, strict=True)
        assert [r['contest_id'] for r in strict] == [2]
        assert strict[0]['ranks'] == {'a': 10, 'b': 5, 'c': 20}

    def test_missing_handle_in_changes_ok(self):
        handles = ['a', 'b']
        all_changes = {'a': [_make_rc(1, 'a', 5)]}  # 'b' missing entirely
        assert _list_shared_contests(handles, all_changes) == []

    def test_contest_name_preserved(self):
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(42, 'a', 1)],
            'b': [_make_rc(42, 'b', 2)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert rows[0]['name'] == 'Contest 42'
        assert rows[0]['time'] == 1000000 + 42

    def test_ranks_isolated_per_contest(self):
        """Make sure the ranks dict isn't shared between rows."""
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 7)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 12)],
        }
        rows = _list_shared_contests(handles, all_changes)
        by_id = {r['contest_id']: r for r in rows}
        # Mutating one shouldn't affect the other
        by_id[1]['ranks']['mutated'] = 99
        assert 'mutated' not in by_id[2]['ranks']


class TestStrictMode:
    def test_strict_requires_all_handles(self):
        """With strict=True, only contests where ALL handles participated count."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 5)],
            'c': [_make_rc(2, 'c', 20)],
        }
        # Non-strict: contest 1 (a,b) + contest 2 (a,b,c) = 2
        wins, placements, total = _compute_versus_stats(handles, all_changes, strict=False)
        assert total == 2

        # Strict: only contest 2 has all 3
        wins, placements, total = _compute_versus_stats(handles, all_changes, strict=True)
        assert total == 1
        # Contest 2: b=5 < a=10 < c=20 → b wins
        assert wins['b'] == 1
        assert wins['a'] == 0
        assert wins['c'] == 0

    def test_strict_no_shared_contests(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5)],
            'b': [_make_rc(1, 'b', 10)],
            'c': [_make_rc(2, 'c', 20)],
        }
        # Non-strict: contest 1 has a,b → 1
        wins, _, total = _compute_versus_stats(handles, all_changes, strict=False)
        assert total == 1

        # Strict: no contest has all 3
        wins, _, total = _compute_versus_stats(handles, all_changes, strict=True)
        assert total == 0

    def test_strict_two_users_same_as_default(self):
        """With 2 users, strict and non-strict are equivalent."""
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10)],
        }
        _, _, total_default = _compute_versus_stats(handles, all_changes, strict=False)
        _, _, total_strict = _compute_versus_stats(handles, all_changes, strict=True)
        # Contest 1 has both, contest 2 has only a → both modes give 1
        assert total_default == 1
        assert total_strict == 1


class TestIsStale:
    def test_none_is_stale(self):
        assert _is_stale(None) is True

    def test_recent_is_fresh(self):
        assert _is_stale(time.time() - 100) is False

    def test_old_is_stale(self):
        # 15 days ago, outside rename season
        assert _is_stale(time.time() - 15 * 86400) is True


class TestHandleAliasDb:
    """Test the handle_alias table in CacheDbConn."""

    def _make_db(self):
        import sqlite3
        from tle.util.db.cache_db_conn import CacheDbConn
        db = CacheDbConn.__new__(CacheDbConn)
        db.db_file = ':memory:'
        db.conn = sqlite3.connect(':memory:')
        db.create_tables()
        db._run_upgrades()
        return db

    def test_no_alias_returns_none(self):
        db = self._make_db()
        aliases, resolved_at = db.get_handle_aliases('unknown')
        assert aliases is None
        assert resolved_at is None

    def test_save_and_get_aliases(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({
            'Friedrich': 'LMeyling',
            'LMeyling': 'LMeyling',
        }, now)

        aliases, resolved_at = db.get_handle_aliases('Friedrich')
        assert aliases == {'Friedrich', 'LMeyling'}
        assert resolved_at == now

        aliases, resolved_at = db.get_handle_aliases('LMeyling')
        assert aliases == {'Friedrich', 'LMeyling'}

    def test_self_alias(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({'alice': 'alice'}, now)
        aliases, _ = db.get_handle_aliases('alice')
        assert aliases == {'alice'}

    def test_triple_chain(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({
            'A': 'C',
            'B': 'C',
            'C': 'C',
        }, now)
        aliases, _ = db.get_handle_aliases('A')
        assert aliases == {'A', 'B', 'C'}


class TestGetRatingChanges:
    """Test _get_rating_changes with mocked CF API and real in-memory DB."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _make_db(self):
        import sqlite3
        from tle.util.db.cache_db_conn import CacheDbConn
        db = CacheDbConn.__new__(CacheDbConn)
        db.db_file = ':memory:'
        db.conn = sqlite3.connect(':memory:')
        db.create_tables()
        db._run_upgrades()
        return db

    def test_api_called_and_cached(self, monkeypatch):
        """First call hits API, second uses cache."""
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
            _make_rc(3, 'LMeyling', 30),
        ]

        call_count = 0
        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                nonlocal call_count
                call_count += 1
                return api_response

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': type('E', (Exception,), {}),
        })())

        db = self._make_db()

        # First call — hits API, writes to cache
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 3
        assert call_count == 1

        # Second call — fresh cache, no API
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 3
        assert call_count == 1  # Still 1, no new API call

    def test_handle_not_found(self, monkeypatch):
        """Unknown handle returns empty and doesn't crash."""
        import tle.cogs.versus as versus_mod

        class FakeNotFound(Exception):
            pass

        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                raise FakeNotFound()

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': FakeNotFound,
        })())

        db = self._make_db()
        changes = self._run(_get_rating_changes('nobody', db))
        assert changes == []

    def test_old_handle_rows_cleaned_up(self, monkeypatch):
        """When API returns data under new handle, old handle rows are removed."""
        import tle.cogs.versus as versus_mod

        # API returns all contests under current handle "LMeyling"
        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
        ]

        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                return api_response

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': type('E', (Exception,), {}),
        })())

        db = self._make_db()

        # Pre-populate old handle rows (as if bot cached them before rename)
        db.save_rating_changes([
            _make_rc(1, 'Friedrich', 50),
            _make_rc(2, 'Friedrich', 40),
        ])
        # Verify old rows exist
        old_rows = db.get_rating_changes_for_handle('Friedrich')
        assert len(old_rows) == 2

        # Now resolve "Friedrich" — API returns data under "LMeyling"
        # Simulate: queried handle is "Friedrich", API returns "LMeyling"
        changes = self._run(_get_rating_changes('Friedrich', db))
        assert len(changes) == 2
        # All returned under current handle
        assert all(c.handle == 'LMeyling' for c in changes)

        # Old rows should be cleaned up
        old_rows = db.get_rating_changes_for_handle('Friedrich')
        assert len(old_rows) == 0

    def test_old_handle_404_uses_alias(self, monkeypatch):
        """If old handle 404s but we know its current name, use that."""
        import tle.cogs.versus as versus_mod

        class FakeNotFound(Exception):
            pass

        call_count = 0
        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                nonlocal call_count
                call_count += 1
                if handle == 'Friedrich':
                    raise FakeNotFound()
                return [_make_rc(1, 'LMeyling', 50), _make_rc(2, 'LMeyling', 40)]

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': FakeNotFound,
        })())

        db = self._make_db()

        # First, resolve "LMeyling" to populate alias table and cache
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 2
        assert call_count == 1

        # Now query "Friedrich" — it 404s, but alias table knows Friedrich→LMeyling
        # First set up the alias manually (as if a previous resolution found it)
        now = int(time.time())
        db.conn.execute(
            'INSERT OR REPLACE INTO handle_alias (handle, current_handle, resolved_at) '
            'VALUES (?, ?, ?)', ('Friedrich', 'LMeyling', now)
        )
        db.conn.commit()

        changes = self._run(_get_rating_changes('Friedrich', db))
        assert len(changes) == 2
        assert all(c.handle == 'LMeyling' for c in changes)

    def test_no_duplicate_rows_after_rename(self, monkeypatch):
        """Ensure no duplicate contest entries exist after resolving a rename."""
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
        ]

        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                return api_response

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': type('E', (Exception,), {}),
        })())

        db = self._make_db()

        # Pre-populate old handle rows
        db.save_rating_changes([_make_rc(1, 'Friedrich', 50), _make_rc(2, 'Friedrich', 40)])

        # Resolve under new handle
        self._run(_get_rating_changes('Friedrich', db))

        # Check: contest 1 should only have ONE entry (LMeyling), not two
        contest_rows = db.get_rating_changes_for_contest(1)
        handles_in_contest = [r.handle for r in contest_rows]
        assert 'Friedrich' not in handles_in_contest
        assert 'LMeyling' in handles_in_contest
        assert len(handles_in_contest) == 1

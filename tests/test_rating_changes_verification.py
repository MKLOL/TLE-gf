"""Exercise late-rating repair through monitor refreshes and failed fetches."""
import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import events, tasks
from tle.util.db.cache_db_conn import CacheDbConn


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(tasks.Waiter, 'for_event', lambda *_: None, raising=False)
    monkeypatch.setattr(events, 'ContestListRefresh', object, raising=False)
    module = importlib.import_module('tle.util.cache_system2._rating_changes')
    contest = SimpleNamespace(id=1, phase='FINISHED', end_time=module.time.time())
    db = CacheDbConn(':memory:')
    master = SimpleNamespace(conn=db, contest_cache=SimpleNamespace(
        contests_by_phase={'FINISHED': [contest]}))
    cache = module.RatingChangesCache(master)
    cache._refresh_handle_cache = AsyncMock()
    monitor = SimpleNamespace(running=True, start=Mock(), stop=AsyncMock())
    cache._monitor_task = monitor
    dispatch = Mock()
    monkeypatch.setattr(cf_common, 'event_sys', SimpleNamespace(dispatch=dispatch), raising=False)
    api = AsyncMock()
    monkeypatch.setattr(cf, 'contest', SimpleNamespace(ratingChanges=api), raising=False)
    yield module, cache, contest, db, monitor, api, dispatch
    db.conn.close()


def _change(handle, old=1000, new=1100, cid=1):
    return cf.RatingChange(cid, 'Round', handle, 1, 100, old, new)


def test_contest_refresh_keeps_verification_alive_after_first_save(env):
    module, cache, contest, db, monitor, api, dispatch = env

    async def run():
        cache.monitored_contests = [contest]
        api.return_value = [_change('alice')]
        await module.RatingChangesCache._monitor_task._func(cache, None)
        # A contest-list refresh arrives before the next ten-minute tick.
        await module.RatingChangesCache._update_task._func(cache, None)
        monitor.stop.assert_not_awaited()
        assert cache.monitored_contests == []
        assert contest.id in cache._to_verify
        api.return_value = [_change('alice'), _change('bob')]
        await module.RatingChangesCache._monitor_task._func(cache, None)

    asyncio.run(run())
    assert [change.handle for change in db.get_rating_changes_for_contest(1)] == ['alice', 'bob']
    assert [change.handle for change in dispatch.call_args.kwargs['rating_changes']] == ['bob']
    assert cache._to_verify == {}


def test_pending_verification_restarts_a_stopped_monitor(env):
    module, cache, contest, db, monitor, _, _ = env
    db.save_rating_changes([_change('alice')])
    cache._to_verify[1] = (contest, 1)
    monitor.running = False
    asyncio.run(module.RatingChangesCache._update_task._func(cache, None))
    monitor.start.assert_called_once()


@pytest.mark.parametrize('first', [cf.CodeforcesApiError('temporarily down'), []])
def test_failed_or_empty_verification_is_retried(env, first):
    _, cache, contest, db, _, api, dispatch = env
    db.save_rating_changes([_change('alice')])
    cache._to_verify[1] = (contest, 1)
    api.side_effect = [first, [_change('alice'), _change('bob')]]

    async def run():
        await cache._verify_saved()
        assert 1 in cache._to_verify
        dispatch.assert_not_called()
        await cache._verify_saved()

    asyncio.run(run())
    assert cache._to_verify == {}
    assert {change.handle for change in db.get_rating_changes_for_contest(1)} == {'alice', 'bob'}


def test_cancelled_verification_remains_pending(env):
    _, cache, contest, db, _, api, _ = env
    db.save_rating_changes([_change('alice')])
    cache._to_verify[1] = (contest, 1)
    api.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cache._verify_saved())
    assert 1 in cache._to_verify


def test_recalculation_with_new_casing_replaces_one_row_and_publishes(env):
    _, cache, contest, db, _, api, dispatch = env
    db.save_rating_changes([_change('alice'), _change('other'), _change('alice', cid=2)])
    cache._to_verify[1] = (contest, 1)
    api.return_value = [_change('Alice', new=1200)]
    asyncio.run(cache._verify_saved())
    saved = db.get_rating_changes_for_contest(1)
    assert {(change.handle, change.newRating) for change in saved} == {
        ('Alice', 1200), ('other', 1100)}
    assert len(db.get_rating_changes_for_contest(2)) == 1
    assert dispatch.call_args.kwargs['rating_changes'] == [_change('Alice', new=1200)]


@pytest.mark.parametrize('initial_fetch', [True, False])
def test_cancelling_memory_refresh_does_not_lose_saved_announcements(env, initial_fetch):
    module, cache, contest, db, _, api, dispatch = env
    api.return_value = [_change('alice'), _change('bob')]
    cache._refresh_handle_cache.side_effect = asyncio.CancelledError
    if initial_fetch:
        cache.monitored_contests = [contest]
        run = module.RatingChangesCache._monitor_task._func(cache, None)
    else:
        db.save_rating_changes([_change('alice')])
        cache._to_verify[1] = (contest, 1)
        run = cache._verify_saved()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run)
    assert {change.handle for change in db.get_rating_changes_for_contest(1)} == {'alice', 'bob'}
    assert [change.handle for change in dispatch.call_args.kwargs['rating_changes']] == (
        ['alice', 'bob'] if initial_fetch else ['bob'])
    assert (1 in cache._to_verify) is initial_fetch

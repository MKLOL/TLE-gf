"""Use production TaskSpec/EventSystem to check late-rating scheduling."""
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import events, tasks
from tle.util.db.cache_db_conn import CacheDbConn


def _load_module(name, relative_path):
    path = Path(__file__).resolve().parents[1] / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_real_monitor_survives_refresh_and_announces_late_members(monkeypatch):
    real_tasks = _load_module('_rating_test_tasks', 'tle/util/tasks.py')
    real_events = _load_module('_rating_test_events', 'tle/util/events.py')
    monkeypatch.setattr(tasks, 'task_spec', real_tasks.task_spec)
    monkeypatch.setattr(tasks, 'Waiter', real_tasks.Waiter)
    monkeypatch.setattr(events, 'ContestListRefresh', real_events.ContestListRefresh, raising=False)
    monkeypatch.setattr(events, 'RatingChangesUpdate', real_events.RatingChangesUpdate)
    module = _load_module('_rating_test_cache', 'tle/util/cache_system2/_rating_changes.py')
    db = CacheDbConn(':memory:')
    event_sys = real_events.EventSystem()
    monkeypatch.setattr(cf_common, 'event_sys', event_sys, raising=False)
    contest = SimpleNamespace(id=1, phase='FINISHED', end_time=module.time.time())
    cache = module.RatingChangesCache(SimpleNamespace(
        conn=db, contest_cache=SimpleNamespace(contests_by_phase={'FINISHED': [contest]})))
    cache._refresh_handle_cache = AsyncMock()
    alice = cf.RatingChange(1, 'Round', 'alice', 1, 100, 1000, 1100)
    bob = alice._replace(handle='bob')
    api = AsyncMock(side_effect=[[alice], [alice, bob]])
    monkeypatch.setattr(cf, 'contest', SimpleNamespace(ratingChanges=api), raising=False)

    async def run():
        ticks = asyncio.Queue()
        published = asyncio.Queue()

        async def tick():
            return await ticks.get()

        async def collect(event):
            await published.put(event)

        event_sys.add_listener(real_events.Listener('collector', real_events.RatingChangesUpdate,
                                                   collect))
        monitor = cache._monitor_task
        monitor._waiter = real_tasks.Waiter(tick)
        try:
            await cache._update_task.func(cache, None)
            first = await asyncio.wait_for(published.get(), 1)
            assert [change.handle for change in first.rating_changes] == ['alice']
            await cache._update_task.func(cache, None)
            assert monitor.running  # Contest no longer monitored, verification is pending.
            await ticks.put(None)
            late = await asyncio.wait_for(published.get(), 1)
            assert [change.handle for change in late.rating_changes] == ['bob']
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(monitor.asyncio_task, 1)
            assert not monitor.running
        finally:
            if monitor.running:
                await monitor.stop()

    try:
        asyncio.run(run())
        assert api.await_count == 2
        assert {change.handle for change in db.get_rating_changes_for_contest(1)} == {'alice', 'bob'}
    finally:
        db.conn.close()

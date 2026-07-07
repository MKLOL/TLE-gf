"""Betting thread-lock tests: a settled market's thread stays open for 12h of
post-game chat, then locks — via a per-market timer, re-armed from DB state
after a restart."""
import asyncio
import time as _t

import pytest  # noqa: F401

from tle.util import codeforces_common as cf_common
from tle import constants
from tle.cogs.betting import Betting
from tle.cogs._betting_engine import _THREAD_LOCK_DELAY
from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
    _FakeChannel, _FakeThread, _FakeGuild, _FakeBot,
)


def _run(coro):
    return asyncio.run(coro)


def _cog_with_thread(db, monkeypatch):
    """A Betting cog wired to a fake channel + thread for one settled market."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
    mid = db.bet_market_create(
        GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain', 'Cape Verde',
        _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
    db.bet_market_set_thread(mid, '333')
    channel = _FakeChannel(222)
    thread = _FakeThread(333)
    bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel, 333: thread})
    return Betting(bot), mid, thread


class TestThreadLockDeferred:
    def test_settle_defers_lock_then_timer_locks(self, db, monkeypatch):
        cog, mid, thread = _cog_with_thread(db, monkeypatch)

        async def scenario():
            await cog._do_settle(
                db.bet_market_get(mid), 'home', 2, 1, source='auto')
            # Thread left open for post-game chat; lock timer armed, not fired.
            assert thread.archived is False and thread.locked is False
            assert db.bet_market_get(mid).thread_locked == 0
            assert mid in cog._lock_timers
            # Simulate the 12h timer elapsing now.
            cog._lock_timers[mid].cancel()
            await cog._lock_timer(mid, 0)
            assert thread.archived is True and thread.locked is True
            assert db.bet_market_get(mid).thread_locked == 1

        _run(scenario())

    def test_catch_up_locks_when_window_already_elapsed(self, db, monkeypatch):
        cog, mid, thread = _cog_with_thread(db, monkeypatch)
        # Settled long enough ago that the 12h window has passed (restart case).
        db.bet_settle(GUILD, mid, 'home', 1, 0, _t.time() - _THREAD_LOCK_DELAY - 60)

        _run(cog._arm_lock_timers())

        assert thread.archived is True and thread.locked is True
        assert db.bet_market_get(mid).thread_locked == 1

    def test_catch_up_rearms_timer_within_window(self, db, monkeypatch):
        cog, mid, thread = _cog_with_thread(db, monkeypatch)
        db.bet_settle(GUILD, mid, 'home', 1, 0, _t.time())  # just settled

        async def scenario():
            await cog._arm_lock_timers()
            # Still inside the 12h grace window → re-armed, not locked yet.
            assert thread.archived is False
            assert db.bet_market_get(mid).thread_locked == 0
            assert mid in cog._lock_timers
            cog._lock_timers[mid].cancel()

        _run(scenario())

    def test_lock_survives_transient_edit_failure(self, db, monkeypatch):
        """A failed thread.edit must NOT mark the market locked, so the next
        catch-up sweep retries rather than dropping the lock."""
        cog, mid, thread = _cog_with_thread(db, monkeypatch)
        db.bet_settle(GUILD, mid, 'home', 1, 0, _t.time() - _THREAD_LOCK_DELAY - 60)

        import discord

        async def _boom(**kw):
            raise discord.HTTPException(None, 'rate limited')
        thread.edit = _boom

        _run(cog._arm_lock_timers())
        assert db.bet_market_get(mid).thread_locked == 0  # still pending

        # A later sweep with a working edit finishes the job.
        thread.edit = _FakeThread.edit.__get__(thread)
        _run(cog._arm_lock_timers())
        assert thread.locked is True
        assert db.bet_market_get(mid).thread_locked == 1

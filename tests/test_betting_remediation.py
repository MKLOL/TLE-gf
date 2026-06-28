"""Tests for the one-time no-draw → 1X2 refixture remediation.

The central safety property: it voids and re-posts ONLY a named fixture's
no-draw market, and never touches a correctly-posted 1X2 game — including a
group game already in progress.
"""
import asyncio
import time

import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, db, _FakeChannel, _FakeGuild, _FakeBot,
)
from tle.cogs._betting_remediation import _REFIXTURE_FLAG


def _run(coro):
    return asyncio.run(coro)


def _make_cog(db, monkeypatch, channel):
    from tle.util import codeforces_common as cf_common
    from tle import constants
    from tle.cogs.betting import Betting
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
    bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
    return Betting(bot)


def _stub_reopen(cog, monkeypatch):
    """Capture _open_market_auto calls and fake out fresh odds so the test
    exercises the void/scope logic without a live Odds API."""
    opened = []

    async def _fake_open(guild_id, channel_id, event):
        opened.append((guild_id, channel_id, event))
        return 'reopened-mid'  # truthy market_id == successful repost

    async def _fake_events(max_age):
        # Fresh 1X2 odds for both affected fixtures, kicking off in the future.
        now = time.time()
        return [{'event_id': 'evtAA', 'sport_key': 'soccer_fifa_world_cup',
                 'home_team': 'Algeria', 'away_team': 'Austria',
                 'commence_time': now + 7200,
                 'odds': {'home': 2.0, 'draw': 3.2, 'away': 3.8},
                 'market_type': 'result'},
                {'event_id': 'evtJA', 'sport_key': 'soccer_fifa_world_cup',
                 'home_team': 'Jordan', 'away_team': 'Argentina',
                 'commence_time': now + 7200,
                 'odds': {'home': 5.0, 'draw': 3.6, 'away': 1.7},
                 'market_type': 'result'}]

    monkeypatch.setattr(cog, '_open_market_auto', _fake_open)
    monkeypatch.setattr(cog, '_ensure_wc_events', _fake_events)
    return opened


class TestRefixtureScope:
    def test_voids_and_reopens_only_the_no_draw_target(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)

        # Affected fixture: opened with NO draw (odds_draw 0), pre-kickoff, staked.
        bad = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_market_set_thread(bad, '333')
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)  # bal 900

        _run(cog._run_draw_refixture())

        m = db.bet_market_get(bad)
        assert m.status == 'cancelled'          # voided
        assert db.bet_get_balance(GUILD, USER_A) == 1000   # stake refunded
        assert len(opened) == 1                 # reposted exactly once
        assert opened[0][2]['home_team'] == 'Algeria'
        assert db.get_guild_config(GUILD, _REFIXTURE_FLAG) == '1'

    def test_handles_jordan_argentina_fixture(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)

        bad = db.bet_market_create(
            GUILD, '222', 'evtJA', 'soccer_fifa_world_cup', 'Jordan',
            'Argentina', time.time() + 7200, 5.0, 0.0, 1.7, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'away', 100, 1.7, 1000)

        _run(cog._run_draw_refixture())

        assert db.bet_market_get(bad).status == 'cancelled'
        assert db.bet_get_balance(GUILD, USER_A) == 1000
        assert len(opened) == 1
        assert opened[0][2]['home_team'] == 'Jordan'

    def test_leaves_correctly_posted_target_1x2_untouched(self, db, monkeypatch):
        # A NAMED fixture that was opened correctly as 1X2 (draw odds > 1) must
        # be left alone — the no-draw filter, not just the name, gates the void.
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)

        good = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 2.0, 3.2, 3.8, USER_A, 0.0)
        db.bet_place(GUILD, good, USER_A, 'draw', 100, 3.2, 1000)

        _run(cog._run_draw_refixture())

        m = db.bet_market_get(good)
        assert m.status == 'open'               # not voided
        assert db.bet_get_balance(GUILD, USER_A) == 900   # not refunded
        assert opened == []                     # not reposted
        assert db.get_guild_config(GUILD, _REFIXTURE_FLAG) == '1'  # resolved

    def test_does_not_void_kicked_off_target(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)

        # Named no-draw fixture that has already kicked off — never void a game
        # in progress (it can't be reposted; deploy must precede kickoff).
        bad = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() - 600, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)

        _run(cog._run_draw_refixture())

        assert db.bet_market_get(bad).status == 'open'   # left as-is
        assert db.bet_get_balance(GUILD, USER_A) == 900
        assert opened == []

    def test_does_not_void_when_channel_gone(self, db, monkeypatch):
        # Channel pre-check: if the market's channel is gone, never void —
        # otherwise stakes would be refunded with nowhere to repost.
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)  # bot only knows channel 222
        opened = _stub_reopen(cog, monkeypatch)

        bad = db.bet_market_create(
            GUILD, '999', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)

        _run(cog._run_draw_refixture())

        m = db.bet_market_get(bad)
        assert m.status == 'open'               # not voided (channel missing)
        assert db.bet_get_balance(GUILD, USER_A) == 900
        assert opened == []
        # Not resolved, so it retries on the next restart.
        assert db.get_guild_config(GUILD, _REFIXTURE_FLAG) is None

    def test_leaves_in_progress_group_game_untouched(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)

        # A correctly-posted 1X2 group game (draw odds > 1), already kicked off.
        live = db.bet_market_create(
            GUILD, '222', 'evtLIVE', 'soccer_fifa_world_cup', 'Brazil',
            'Spain', time.time() - 600, 2.1, 3.3, 3.5, USER_B, 0.0)
        db.bet_place(GUILD, live, USER_B, 'draw', 200, 3.3, 1000)  # bal 800

        _run(cog._run_draw_refixture())

        m = db.bet_market_get(live)
        assert m.status == 'open'               # untouched
        assert db.bet_get_balance(GUILD, USER_B) == 800   # not refunded
        assert opened == []                     # nothing reposted

    def test_does_not_void_when_odds_unavailable(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)

        async def _no_events(max_age):
            raise RuntimeError('odds down')
        monkeypatch.setattr(cog, '_ensure_wc_events', _no_events)

        bad = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)

        _run(cog._run_draw_refixture())

        m = db.bet_market_get(bad)
        assert m.status == 'open'               # NOT voided — odds came back empty
        assert db.bet_get_balance(GUILD, USER_A) == 900
        # Flag NOT set, so it retries on the next restart.
        assert db.get_guild_config(GUILD, _REFIXTURE_FLAG) is None

    def test_failed_repost_refunds_but_leaves_flag_unset(self, db, monkeypatch):
        # If the repost fails AFTER the void, stakes are still refunded but the
        # flag stays unset so a later restart / the scheduler can recover.
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        calls = []

        async def _fail_open(guild_id, channel_id, event):
            calls.append(1)
            return None  # repost produced no market
        _stub_reopen(cog, monkeypatch)  # fake fresh odds…
        monkeypatch.setattr(cog, '_open_market_auto', _fail_open)  # …but fail repost

        bad = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)

        _run(cog._run_draw_refixture())

        assert db.bet_market_get(bad).status == 'cancelled'   # voided
        assert db.bet_get_balance(GUILD, USER_A) == 1000      # refunded
        assert calls == [1]
        assert db.get_guild_config(GUILD, _REFIXTURE_FLAG) is None  # not stamped

    def test_idempotent_once_flag_set(self, db, monkeypatch):
        channel = _FakeChannel(222)
        cog = _make_cog(db, monkeypatch, channel)
        opened = _stub_reopen(cog, monkeypatch)
        db.set_guild_config(GUILD, _REFIXTURE_FLAG, '1')

        bad = db.bet_market_create(
            GUILD, '222', 'evtAA', 'soccer_fifa_world_cup', 'Algeria',
            'Austria', time.time() + 7200, 1.8, 0.0, 2.0, USER_A, 0.0)
        db.bet_place(GUILD, bad, USER_A, 'home', 100, 1.8, 1000)

        _run(cog._run_draw_refixture())

        assert db.bet_market_get(bad).status == 'open'   # skipped entirely
        assert opened == []

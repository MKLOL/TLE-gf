"""Engine-level stage detection and the headline fail-safe: a football-data
outage (or unknown stage) must never strip a draw from a group market.

These exercise the real wiring `_ensure_wc_events` → `_ensure_fd_matches` →
`_event_knockout` → `normalize_event`, not just the pure helpers.
"""
import asyncio
import time

import pytest  # noqa: F401

from tests.betting_test_utils import GUILD, _FakeChannel, _FakeGuild, _FakeBot


def _run(coro):
    return asyncio.run(coro)


def _cog(monkeypatch, *, fd_token=None, fd_matches=None, fd_error=False):
    from tle.util import odds_api, football_data
    from tle import constants
    from tle.cogs.betting import Betting
    monkeypatch.setattr(constants, 'ODDS_API_KEY', 'oddskey', raising=False)
    monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', fd_token,
                        raising=False)

    async def _fake_h2h(api_key, sport_keys, **kw):
        return [{'event_id': 'evtAA', 'sport_key': 'soccer_fifa_world_cup',
                 'home_team': 'Algeria', 'away_team': 'Austria',
                 'commence_time': time.time() + 7200,
                 'odds': {'home': 2.0, 'draw': 3.2, 'away': 3.8}}]
    monkeypatch.setattr(odds_api, 'fetch_h2h', _fake_h2h)

    async def _fake_matches(token, **kw):
        if fd_error:
            raise football_data.FootballDataError('down')
        return fd_matches or []
    monkeypatch.setattr(football_data, 'fetch_wc_matches', _fake_matches)

    channel = _FakeChannel(222)
    bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
    return Betting(bot)


def _stage_match(stage):
    return [{'home': 'Austria', 'away': 'Algeria',
             'commence_time': time.time() + 7200, 'stage': stage}]


class TestStageFailSafe:
    def test_group_keeps_draw_when_football_data_unconfigured(self, monkeypatch):
        # No FOOTBALL_DATA token → no stage info → must still offer a draw.
        cog = _cog(monkeypatch, fd_token=None)
        events = _run(cog._ensure_wc_events(0))
        assert len(events) == 1
        assert events[0]['market_type'] == 'result'
        assert events[0]['odds']['draw'] > 1

    def test_group_keeps_draw_when_football_data_errors(self, monkeypatch):
        # football-data unreachable → fall back to draw-allowed, never strip it.
        cog = _cog(monkeypatch, fd_token='fdkey', fd_error=True)
        events = _run(cog._ensure_wc_events(0))
        assert events[0]['market_type'] == 'result'
        assert events[0]['odds']['draw'] > 1

    def test_new_knockout_label_treated_as_knockout(self, monkeypatch):
        # A future/unknown NON-group label (e.g. a renamed knockout round) is
        # treated as knockout with no code change — the safe fallback to a draw
        # is reserved for MISSING data (covered by the two tests above), since
        # only the group phase is reliably spelled GROUP_STAGE.
        cog = _cog(monkeypatch, fd_token='fdkey',
                   fd_matches=_stage_match('LAST_32'))
        events = _run(cog._ensure_wc_events(0))
        assert events[0]['market_type'] == 'advance'

    def test_placeholder_knockout_slot_drops_draw_via_calendar(self, monkeypatch):
        # football-data hasn't filled the bracket for this fixture (its slot is
        # a nameless null-vs-null placeholder, so the name match misses), but
        # the group stage is already over — the calendar fallback must still
        # strip the draw instead of leaving a 1X2 market up for days.
        cog = _cog(monkeypatch, fd_token='fdkey', fd_matches=[
            {'home': 'X', 'away': 'Y', 'commence_time': time.time() - 86400,
             'stage': 'GROUP_STAGE'},
            {'home': None, 'away': None, 'commence_time': time.time() + 7200,
             'stage': 'LAST_16'}])
        events = _run(cog._ensure_wc_events(0))
        assert events[0]['market_type'] == 'advance'
        assert events[0]['odds']['draw'] == 0.0

    def test_group_stage_offers_draw(self, monkeypatch):
        cog = _cog(monkeypatch, fd_token='fdkey',
                   fd_matches=_stage_match('GROUP_STAGE'))
        events = _run(cog._ensure_wc_events(0))
        assert events[0]['market_type'] == 'result'
        assert events[0]['odds']['draw'] > 1

    def test_knockout_stage_drops_draw(self, monkeypatch):
        cog = _cog(monkeypatch, fd_token='fdkey',
                   fd_matches=_stage_match('LAST_16'))
        events = _run(cog._ensure_wc_events(0))
        assert events[0]['market_type'] == 'advance'
        assert events[0]['odds']['draw'] == 0.0

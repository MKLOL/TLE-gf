"""Penalty-shootout settlement safety tests.

Covers the football-data settle path's defenses against the unreliable
beyond-regulation feed data that once mis-settled two World Cup knockouts:
  * fd_settle_outcome — trust `winner`, but reject an inconsistent shootout.
  * parse_match / find_match_result — carry & orient the `penalties` tally.
  * the auto-settler's "confirm across two polls" gate for ET/penalty games.
"""
import pytest  # noqa: F401

from tle.cogs._betting_helpers import fd_settle_outcome
from tle.util import football_data
from tests.betting_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, db, _FakeChannel, _FakeGuild, _FakeBot,
)


class TestFdSettleOutcome:
    def test_plain_winner_trusted(self):
        assert fd_settle_outcome({'winner': 'home', 'home_score': 2,
                                  'away_score': 1}) == 'home'

    def test_plain_draw_trusted(self):
        assert fd_settle_outcome({'winner': 'draw', 'home_score': 1,
                                  'away_score': 1}) == 'draw'

    def test_no_winner_regular_falls_back_to_score(self):
        assert fd_settle_outcome({'winner': None, 'home_score': 3,
                                  'away_score': 0}) == 'home'

    def test_extra_time_without_winner_holds(self):
        assert fd_settle_outcome({'winner': None, 'duration': 'EXTRA_TIME',
                                  'home_score': 1, 'away_score': 1}) is None

    def test_penalties_consistent_winner_accepted(self):
        # Level 1-1, away wins the shootout 3-5 → settle away.
        assert fd_settle_outcome({
            'winner': 'away', 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 1, 'away_score': 1,
            'penalties': {'home': 3, 'away': 5}}) == 'away'

    def test_penalties_no_winner_rejected(self):
        assert fd_settle_outcome({
            'winner': None, 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 4, 'away_score': 4,
            'penalties': {'home': 4, 'away': 4}}) is None

    def test_penalties_no_winner_clear_tally_accepted(self):
        assert fd_settle_outcome({
            'winner': None, 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 1, 'away_score': 1,
            'penalties': {'home': 2, 'away': 4}}) == 'away'

    def test_penalties_decisive_fulltime_accepted_when_regular_level(self):
        # football-data can expose shootout totals in fullTime while winner is
        # null and the penalties node is bogus/tied. Accept only with a level
        # regularTime, and the auto-settler still requires two matching polls.
        assert fd_settle_outcome({
            'winner': None, 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 3, 'away_score': 5,
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}) == 'away'

    def test_penalties_decisive_fulltime_requires_regular_level(self):
        assert fd_settle_outcome({
            'winner': None, 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 3, 'away_score': 5,
            'regular_home_score': 2, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}) is None

    def test_penalties_conflicting_evidence_rejected(self):
        assert fd_settle_outcome({
            'winner': None, 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 3, 'away_score': 5,
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 5, 'away': 3}}) is None

    def test_penalties_tied_tally_rejected(self):
        assert fd_settle_outcome({
            'winner': 'home', 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 1, 'away_score': 1,
            'penalties': {'home': 4, 'away': 4}}) is None

    def test_penalties_winner_disagrees_with_tally_rejected(self):
        # winner says home, but the pens tally has away ahead → don't trust it.
        assert fd_settle_outcome({
            'winner': 'home', 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 1, 'away_score': 1,
            'penalties': {'home': 2, 'away': 4}}) is None

    def test_penalties_missing_tally_rejected(self):
        assert fd_settle_outcome({
            'winner': 'home', 'duration': 'PENALTY_SHOOTOUT',
            'home_score': 1, 'away_score': 1}) is None


class TestParsePenalties:
    def test_parse_extracts_penalties(self):
        raw = {'status': 'FINISHED', 'utcDate': '2026-07-01T16:00:00Z',
               'homeTeam': {'name': 'Germany'}, 'awayTeam': {'name': 'Paraguay'},
               'score': {'winner': 'AWAY_TEAM', 'duration': 'PENALTY_SHOOTOUT',
                         'fullTime': {'home': 1, 'away': 1},
                         'penalties': {'home': 3, 'away': 5}}}
        p = football_data.parse_match(raw)
        assert p['penalties'] == {'home': 3, 'away': 5}

    def test_parse_no_penalties_is_none(self):
        raw = {'status': 'FINISHED', 'utcDate': '2026-07-01T16:00:00Z',
               'homeTeam': {'name': 'A'}, 'awayTeam': {'name': 'B'},
               'score': {'winner': 'HOME_TEAM', 'fullTime': {'home': 2, 'away': 0}}}
        assert football_data.parse_match(raw)['penalties'] is None

    def test_parse_current_shootout_shape(self):
        raw = {'status': 'FINISHED', 'utcDate': '2026-07-03T18:00:00Z',
               'homeTeam': {'name': 'Australia'}, 'awayTeam': {'name': 'Egypt'},
               'score': {'winner': None, 'duration': 'PENALTY_SHOOTOUT',
                         'regularTime': {'home': 1, 'away': 1},
                         'fullTime': {'home': 3, 'away': 5},
                         'penalties': {'home': 4, 'away': 4}}}
        p = football_data.parse_match(raw)
        assert (p['home_score'], p['away_score']) == (3, 5)
        assert (p['regular_home_score'], p['regular_away_score']) == (1, 1)
        assert fd_settle_outcome(p) == 'away'

    def test_find_match_result_swaps_penalties_on_flip(self):
        # Provider lists Paraguay as home; our market is Germany (home) vs Paraguay.
        fd = [{'home': 'Paraguay', 'away': 'Germany', 'home_score': 1,
               'away_score': 1, 'commence_time': 1000.0, 'finished': True,
               'winner': 'home', 'duration': 'PENALTY_SHOOTOUT',
               'penalties': {'home': 5, 'away': 3}}]
        r = football_data.find_match_result('Germany', 'Paraguay', 1000.0, fd)
        # Oriented to Germany/home: Germany lost the shootout 3-5 → away wins.
        assert r['winner'] == 'away'
        assert r['penalties'] == {'home': 3, 'away': 5}


class TestConfirmGate:
    """The auto-settler must see the SAME beyond-regulation result twice before
    settling, and must never settle an internally inconsistent shootout."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _cog_and_market(self, db, monkeypatch, *, odds_draw=0.0):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Germany',
            'Paraguay', _t.time() - 100, 1.5, odds_draw, 3.0, USER_A, 0.0)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        return Betting(bot), mid

    def _fetch(self, monkeypatch, frames):
        """Patch fetch_wc_matches to return frames[call_index] each poll."""
        import time as _t
        from tle.util import football_data as fd
        state = {'i': 0}

        async def _fake(token, **kw):
            frame = frames[min(state['i'], len(frames) - 1)]
            state['i'] += 1
            base = {'home': 'Germany', 'away': 'Paraguay',
                    'commence_time': _t.time() - 100, 'finished': True}
            base.update(frame)
            return [base]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake)

    def _odds_draw_score(self, monkeypatch):
        from tle.util import odds_api

        async def _fake(api_key, sport_key, **kw):
            return [{'event_id': 'evtWC', 'completed': True,
                     'home_score': 1, 'away_score': 1}]
        monkeypatch.setattr(odds_api, 'fetch_scores', _fake)

    def test_garbage_shootout_never_settles(self, db, monkeypatch):
        cog, mid = self._cog_and_market(db, monkeypatch)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        # winner=None + tied pens (the real broken feed shape) → never settle.
        self._fetch(monkeypatch, [{
            'home_score': 4, 'away_score': 4, 'winner': None,
            'duration': 'PENALTY_SHOOTOUT', 'penalties': {'home': 4, 'away': 4}}])
        for _ in range(3):
            self._run(cog._settle_via_football_data())
        assert db.bet_market_get(mid).status == 'open'

    def test_flip_flop_only_settles_after_two_agree(self, db, monkeypatch):
        """Replays the real incident: a transient WRONG 'home' that vanishes,
        then the true 'away' — only the result that repeats is trusted."""
        cog, mid = self._cog_and_market(db, monkeypatch)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)  # backs truth
        db.bet_place(GUILD, mid, USER_B, 'home', 100, 1.0, 1000)  # backs phantom
        self._fetch(monkeypatch, [
            # poll 1: transient (wrong) decisive home win — must only be HELD.
            {'home_score': 4, 'away_score': 2, 'winner': 'home',
             'duration': 'PENALTY_SHOOTOUT', 'penalties': {'home': 4, 'away': 2}},
            # poll 2: feed flips to garbage — confirmation breaks, nothing settles.
            {'home_score': 4, 'away_score': 4, 'winner': None,
             'duration': 'PENALTY_SHOOTOUT', 'penalties': {'home': 4, 'away': 4}},
            # poll 3 & 4: the true away win, consistent — settles on the 2nd.
            {'home_score': 1, 'away_score': 1, 'winner': 'away',
             'duration': 'PENALTY_SHOOTOUT', 'penalties': {'home': 3, 'away': 5}},
        ])
        self._run(cog._settle_via_football_data())   # poll 1: held
        assert db.bet_market_get(mid).status == 'open'
        self._run(cog._settle_via_football_data())   # poll 2: garbage, reset
        assert db.bet_market_get(mid).status == 'open'
        self._run(cog._settle_via_football_data())   # poll 3: held (away)
        assert db.bet_market_get(mid).status == 'open'
        self._run(cog._settle_via_football_data())   # poll 4: confirmed → settle
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'
        assert db.bet_get_balance(GUILD, USER_A) == 1200  # 900 + 100*3.0
        assert db.bet_get_balance(GUILD, USER_B) == 900   # phantom-home backer lost

    def test_decisive_fulltime_shootout_settles_after_restart(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Australia',
            'Egypt', _t.time() - 100, 1.5, 0.0, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        self._fetch(monkeypatch, [{
            'home': 'Australia', 'away': 'Egypt',
            'home_score': 3, 'away_score': 5, 'winner': None,
            'duration': 'PENALTY_SHOOTOUT',
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}])

        self._run(cog._settle_via_football_data())  # first poll: held
        assert db.bet_market_get(mid).status == 'open'
        restarted = Betting(bot)
        self._run(restarted._settle_via_football_data())  # restart re-holds
        assert db.bet_market_get(mid).status == 'open'
        self._run(restarted._settle_via_football_data())  # confirmed after restart
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'
        assert db.bet_get_balance(GUILD, USER_A) == 1200
        assert len(channel.sent) == 1
        assert 'final' in channel.sent[0].embed.title.lower()

    def test_shootout_confirmation_blocks_odds_draw_fallback(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'oddskey', raising=False)
        monkeypatch.setattr(constants, 'BET_SETTLE_BUFFER_SECONDS', 3 * 3600,
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Australia',
            'Egypt', _t.time() - 4 * 3600, 1.5, 5.5, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'draw', 100, 1.0, 1000)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        self._fetch(monkeypatch, [{
            'home': 'Australia', 'away': 'Egypt',
            'home_score': 3, 'away_score': 5, 'winner': None,
            'duration': 'PENALTY_SHOOTOUT',
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}])
        self._odds_draw_score(monkeypatch)

        self._run(cog._settle_pending())  # first FD poll held; odds skipped
        assert db.bet_market_get(mid).status == 'open'
        assert db.bet_get_balance(GUILD, USER_B) == 900  # draw not paid
        self._run(cog._settle_pending())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'
        assert db.bet_get_balance(GUILD, USER_A) == 1200
        assert db.bet_get_balance(GUILD, USER_B) == 900

    def test_garbage_shootout_blocks_odds_draw_fallback(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'oddskey', raising=False)
        monkeypatch.setattr(constants, 'BET_SETTLE_BUFFER_SECONDS', 3 * 3600,
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Australia',
            'Egypt', _t.time() - 4 * 3600, 1.5, 5.5, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_B, 'draw', 100, 1.0, 1000)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        self._fetch(monkeypatch, [{
            'home': 'Australia', 'away': 'Egypt',
            'home_score': 1, 'away_score': 1, 'winner': None,
            'duration': 'PENALTY_SHOOTOUT',
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}])
        self._odds_draw_score(monkeypatch)

        for _ in range(3):
            self._run(cog._settle_pending())
        assert db.bet_market_get(mid).status == 'open'
        assert db.bet_get_balance(GUILD, USER_B) == 900

    def test_restart_fd_outage_keeps_blocking_odds_draw_fallback(
            self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'oddskey', raising=False)
        monkeypatch.setattr(constants, 'BET_SETTLE_BUFFER_SECONDS', 3 * 3600,
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Australia',
            'Egypt', _t.time() - 4 * 3600, 1.5, 5.5, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_B, 'draw', 100, 1.0, 1000)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        self._fetch(monkeypatch, [{
            'home': 'Australia', 'away': 'Egypt',
            'home_score': 3, 'away_score': 5, 'winner': None,
            'duration': 'PENALTY_SHOOTOUT',
            'regular_home_score': 1, 'regular_away_score': 1,
            'penalties': {'home': 4, 'away': 4}}])
        self._run(cog._settle_via_football_data())  # persists the FD block

        async def _fd_down(token, **kw):
            raise fd.FootballDataError('down')
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fd_down)
        self._odds_draw_score(monkeypatch)

        restarted = Betting(bot)
        self._run(restarted._settle_pending())
        assert db.bet_market_get(mid).status == 'open'
        assert db.bet_get_balance(GUILD, USER_B) == 900

    def test_odds_fallback_keeps_five_minute_cadence(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import odds_api
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', None,
                            raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'oddskey', raising=False)
        monkeypatch.setattr(constants, 'BET_SETTLE_BUFFER_SECONDS', 3 * 3600,
                            raising=False)
        db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 4 * 3600, 1.5, 5.5, 3.0, USER_A, 0.0)
        calls = []

        async def _scores(api_key, sport_key, **kw):
            calls.append((api_key, sport_key, kw))
            return [{'event_id': 'evtWC', 'completed': False,
                     'home_score': None, 'away_score': None}]
        monkeypatch.setattr(odds_api, 'fetch_scores', _scores)

        cog = Betting(bot=None)
        self._run(cog._settle_pending())
        self._run(cog._settle_pending())
        assert len(calls) == 1

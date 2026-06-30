"""Betting auto-settle tests (football-data + odds-api result polling)."""
import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
    _FakeChannel, _FakeThread, _FakeGuild, _FakeBot,
)


class TestAutoSettleFootballData:
    """The settle poller reads results from football-data.org (free) and
    settles any market past kickoff whose game has finished."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_settles_from_football_data(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900

        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        async def _fake_fetch(token, **kw):
            return [{'home': 'Spain', 'away': 'Cape Verde',
                     'commence_time': _t.time() - 100, 'finished': True,
                     'home_score': 3, 'away_score': 1}]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake_fetch)

        self._run(cog._settle_via_football_data())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'home'
        assert m.result_home == 3 and m.result_away == 1
        # payout = round(100 * 1.25) = 125 → 900 + 125
        assert db.bet_get_balance(GUILD, USER_A) == 1025

    def test_settlement_posts_result_only_to_parent_channel(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        db.bet_market_set_thread(mid, '333')
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)

        channel = _FakeChannel(222)
        thread = _FakeThread(333)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)],
                       {222: channel, 333: thread})
        cog = Betting(bot)

        self._run(cog._do_settle(
            db.bet_market_get(mid), 'home', 2, 1, source='auto'))

        assert len(channel.sent) == 1
        assert len(thread.sent) == 0
        assert thread.archived is True

    def test_knockout_settles_by_advancing_winner(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.5, 0.0, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)

        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        async def _fake_fetch(token, **kw):
            return [{'home': 'Spain', 'away': 'Cape Verde',
                     'commence_time': _t.time() - 100, 'finished': True,
                     'home_score': 1, 'away_score': 1, 'winner': 'away'}]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake_fetch)

        self._run(cog._settle_via_football_data())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'
        assert db.bet_get_balance(GUILD, USER_A) == 1200

    def test_penalty_shootout_on_1x2_market_pays_winner_not_draw(
            self, db, monkeypatch):
        """Regression: a knockout that went to penalties was sometimes opened as
        a draw-allowing (1X2) market. football-data reports the LEVEL fullTime
        score (1-1) plus the shootout winner; settling must pay the winner, NOT
        re-derive a draw from the 1-1 scoreline."""
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        # odds_draw > 1 → this market ALLOWS a draw (the mis-opened 1X2 case).
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.5, 5.5, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)  # backs winner
        db.bet_place(GUILD, mid, USER_B, 'draw', 100, 1.0, 1000)  # backs the draw

        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        async def _fake_fetch(token, **kw):
            return [{'home': 'Spain', 'away': 'Cape Verde',
                     'commence_time': _t.time() - 100, 'finished': True,
                     'home_score': 1, 'away_score': 1, 'winner': 'away',
                     'duration': 'PENALTY_SHOOTOUT',
                     'penalties': {'home': 3, 'away': 4}}]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake_fetch)

        # A penalty result is held one poll for confirmation, then settled.
        self._run(cog._settle_via_football_data())
        assert db.bet_market_get(mid).status == 'open'   # first poll: held
        self._run(cog._settle_via_football_data())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'  # not 'draw'
        assert db.bet_get_balance(GUILD, USER_A) == 1200  # 900 + 100*3.0
        assert db.bet_get_balance(GUILD, USER_B) == 900   # draw backer lost

    def test_no_key_no_settle(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', None,
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        cog = Betting(bot=None)
        self._run(cog._settle_via_football_data())  # no key → no-op
        assert db.bet_market_get(mid).status == 'open'

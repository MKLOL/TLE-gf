"""Betting cog tests: market resolution, withdraw, the _execute_bet path,\nstaff-permission guards, and surface UX."""
import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market, _FakeBetMessage,
)


class TestFindMarket:
    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        return Betting(bot=None)

    def _ctx(self, channel_id):
        class _Guild:
            id = GUILD

        class _Channel:
            id = channel_id

        class _Ctx:
            guild = _Guild()
            channel = _Channel()

        return _Ctx()

    def test_parent_channel_multiple_markets_requires_thread(self, db, cog):
        from tle.cogs.betting import BettingCogError
        _make_market(db, commence=1e12)
        db.bet_market_create(
            GUILD, CH, 'evt2', 'soccer_epl', 'Brazil', 'Japan', 1e12 + 1000,
            2.0, 3.0, 4.0, USER_A, 1.0)

        with pytest.raises(BettingCogError):
            cog._find_market(self._ctx(CH), require_unambiguous=True)

    def test_thread_market_is_unambiguous_even_with_parent_markets(self, db, cog):
        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        db.bet_market_create(
            GUILD, CH, 'evt2', 'soccer_epl', 'Brazil', 'Japan', 1e12 + 1000,
            2.0, 3.0, 4.0, USER_A, 1.0)

        market = cog._find_market(self._ctx(THREAD), require_unambiguous=True)

        assert market.market_id == mid


class TestWithdrawCommand:
    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        return Betting(bot=None)

    def _ctx(self, channel_id):
        class _Guild:
            id = GUILD

        class _Channel:
            id = channel_id

        class _Author:
            id = USER_A

        class _Ctx:
            guild = _Guild()
            channel = _Channel()
            author = _Author()

            def __init__(self):
                self.sent = []

            async def send(self, *args, **kwargs):
                self.sent.append((args, kwargs))

        return _Ctx()

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_withdraw_removes_only_current_thread_match(self, db, cog):
        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        other_mid = db.bet_market_create(
            GUILD, CH, 'evt2', 'soccer_epl', 'Brazil', 'Japan', 1e12 + 1000,
            2.0, 3.0, 4.0, USER_A, 0.0)
        db.bet_market_set_thread(other_mid, '444')
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_A, 'away', 200, 2.0, 1000)
        db.bet_place(GUILD, other_mid, USER_A, 'home', 300, 3.0, 1000)
        ctx = self._ctx(THREAD)

        self._run(cog._withdraw_match(ctx))

        assert db.bet_get_wagers_for_user(mid, USER_A) == []
        assert [(w.pick, w.stake) for w in db.bet_get_wagers_for_user(
            other_mid, USER_A)] == [('home', 300)]
        assert db.bet_get_balance(GUILD, USER_A) == 700
        assert len(ctx.sent) == 1

    def test_withdraw_without_bets_reports_neutral(self, db, cog):
        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        ctx = self._ctx(THREAD)

        self._run(cog._withdraw_match(ctx))

        assert len(ctx.sent) == 1


class TestExecuteBet:
    """_execute_bet wires parse_amount + escrow through the cog against a real
    DB, returning a status the command/listener render differently."""

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        return Betting(bot=None)

    def _user(self, uid):
        class _U:
            id = uid
        return _U()

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_ok(self, db, cog):
        mid = _make_market(db, commence=1e12)  # far future → open
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '300'))
        assert status == 'ok'
        assert data['stake'] == 300 and data['odds'] == 2.0
        assert data['potential'] == 600 and data['balance'] == 700

    def test_closed_after_kickoff(self, db, cog):
        mid = _make_market(db, commence=1.0)  # already kicked off
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '100'))
        assert status == 'closed'

    def test_invalid_amount(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', 'lots'))
        assert status == 'invalid'

    def test_insufficient(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '5000'))
        assert status == 'insufficient' and data['balance'] == 1000

    def test_all_in_uses_full_balance(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'away', 'all'))
        assert status == 'ok' and data['stake'] == 1000 and data['balance'] == 0

    def test_negative_pick_uses_derived_odds(self, db, cog):
        mid = _make_market(db, commence=1e12, odds=(2.0, 4.0, 4.0))
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'not_home', '100'))
        assert status == 'ok'
        assert abs(data['odds'] - 2.0) < 1e-9
        assert data['potential'] == 200

    def test_draw_rejected_on_no_draw_market(self, db, cog):
        mid = _make_market(db, commence=1e12, odds=(1.5, 0.0, 3.0))
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'draw', '100'))
        assert status == 'invalid_pick'

    def test_rebet_can_use_escrowed_stake(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', 'all'))
        assert status == 'ok' and data['balance'] == 0

        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '500'))
        assert status == 'ok'
        assert data['stake'] == 500 and data['balance'] == 500
        wager = db.bet_get_wager(mid, USER_A, 'home')
        assert wager.pick == 'home' and wager.stake == 500

    def test_different_pick_needs_free_balance(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        self._run(cog._execute_bet(
            GUILD, market, self._user(USER_A), 'home', 'all'))

        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'away', 'all'))
        assert status == 'invalid' and data is None

    def test_zero_removes_one_pick(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        self._run(cog._execute_bet(
            GUILD, market, self._user(USER_A), 'home', '300'))
        self._run(cog._execute_bet(
            GUILD, market, self._user(USER_A), 'away', '200'))

        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '0'))
        assert status == 'removed'
        assert data['stake'] == 300 and data['balance'] == 800
        assert [(w.pick, w.stake) for w in db.bet_get_wagers_for_user(mid, USER_A)] == [
            ('away', 200)]


class TestBettingStaffPermissions:
    def test_mutating_betting_commands_do_not_allow_moderator_role(self):
        from pathlib import Path
        source = Path('tle/cogs/betting.py').read_text()
        assert ('@commands.has_any_role(constants.TLE_ADMIN, '
                'constants.TLE_MODERATOR)') not in source

    def test_transfer_command_is_admin_only(self):
        from pathlib import Path
        source = Path('tle/cogs/betting.py').read_text()
        block = source[source.index("@bet.command(name='transfer'"):
                       source.index("@bet.command(name='history'")]
        assert '@commands.has_role(constants.TLE_ADMIN)' in block


class TestBettingSurfaceUX:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.util import discord_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(discord_common, '_BOT_PREFIX', ';', raising=False)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        return Betting(bot=None)

    def test_parent_market_embed_points_to_existing_thread(self, db, cog):
        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        market = db.bet_market_get(mid)
        embed = cog._market_embed(market, current_channel_id=CH)
        assert f'<#{THREAD}>' in embed.description
        assert 'thread below' not in embed.description

    def test_parent_channel_plain_bet_remains_thread_only(self, db, cog):
        mid = _make_market(db, commence=1e12)
        msg = _FakeBetMessage('Spain 100', channel_id=int(CH))

        self._run(cog.on_message(msg))

        assert msg.reactions == []
        assert db.bet_get_wager(mid, msg.author.id) is None


class TestHereHelpVisibility:
    """`;bet here` hides itself from `;help bet` once a channel is configured."""

    def _ctx(self):
        return type('Ctx', (), {'guild': type('G', (), {'id': int(GUILD)})()})()

    def test_hidden_predicate_false_when_unconfigured(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import _bet_channel_is_set
        monkeypatch.setattr(cf_common, 'user_db', db)
        assert _bet_channel_is_set(self._ctx()) is False

    def test_hidden_predicate_true_once_channel_set(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import _bet_channel_is_set
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'bet_channel', str(int(CH)))
        assert _bet_channel_is_set(self._ctx()) is True

    def test_hidden_predicate_safe_without_db(self, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import _bet_channel_is_set
        monkeypatch.setattr(cf_common, 'user_db', None)
        assert _bet_channel_is_set(self._ctx()) is False
        assert _bet_channel_is_set(None) is False

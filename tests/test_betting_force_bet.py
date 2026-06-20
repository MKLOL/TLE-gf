"""Betting cog tests for the admin ``;bet for`` command — placing (and
removing) a bet on behalf of another member."""
import pytest

from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, db, _make_market,
)


class TestForceBetForUser:
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
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        # The conftest stub returns None for these; surface the text so we can
        # assert the response names the target member.
        monkeypatch.setattr(discord_common, 'embed_success', lambda desc: desc)
        monkeypatch.setattr(discord_common, 'embed_neutral',
                            lambda desc, **kw: desc)
        return Betting(bot=None)

    def _member(self, uid, name, *, bot=False):
        return type('Member', (), {
            'id': uid, 'display_name': name, 'bot': bot})()

    def _ctx(self, admin_id='999', channel_id=THREAD):
        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': GUILD})()
                self.channel = type('C', (), {'id': channel_id})()
                self.author = type('A', (), {'id': admin_id})()
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        return _Ctx()

    def _open_market(self, db):
        mid = _make_market(db, commence=1e12)  # far future → still bettable
        db.bet_market_set_thread(mid, THREAD)
        return mid

    def test_places_bet_from_target_wallet_and_records_admin_as_actor(self, db, cog):
        mid = self._open_market(db)
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx(admin_id='999')

        self._run(cog._cmd_place_for(ctx, member, 'home 300'))

        wager = db.bet_get_wager(mid, USER_A, 'home')
        assert wager is not None and wager.stake == 300
        assert db.bet_get_balance(GUILD, USER_A) == 700  # member's own wallet paid
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'wager_stake'
        assert hist[0].actor_id == '999'  # the admin, not the bettor
        assert len(ctx.sent) == 1
        assert 'Alice' in ctx.sent[0]

    def test_resolves_team_name_pick(self, db, cog):
        mid = self._open_market(db)
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx()

        self._run(cog._cmd_place_for(ctx, member, 'Cape Verde 100'))

        wager = db.bet_get_wager(mid, USER_A, 'away')
        assert wager is not None and wager.stake == 100

    def test_zero_amount_removes_the_targets_bet(self, db, cog):
        mid = self._open_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 200, 1.0, 1000)
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx()

        self._run(cog._cmd_place_for(ctx, member, 'home 0'))

        assert db.bet_get_wager(mid, USER_A, 'home') is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000  # refunded
        assert len(ctx.sent) == 1

    def test_rejects_betting_for_a_bot(self, db, cog):
        from tle.cogs.betting import BettingCogError
        self._open_market(db)
        member = self._member(USER_A, 'Beep', bot=True)
        ctx = self._ctx()

        with pytest.raises(BettingCogError):
            self._run(cog._cmd_place_for(ctx, member, 'home 100'))

    def test_insufficient_balance_names_the_target(self, db, cog):
        from tle.cogs.betting import BettingCogError
        self._open_market(db)
        db.bet_set_balance(GUILD, USER_A, 50, 1000)
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx()

        with pytest.raises(BettingCogError) as exc:
            self._run(cog._cmd_place_for(ctx, member, 'home 500'))
        assert 'Alice' in str(exc.value)

    def test_unparseable_text_raises(self, db, cog):
        from tle.cogs.betting import BettingCogError
        self._open_market(db)
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx()

        with pytest.raises(BettingCogError):
            self._run(cog._cmd_place_for(ctx, member, 'home'))

    def test_no_open_market_raises(self, db, cog):
        from tle.cogs.betting import BettingCogError
        member = self._member(USER_A, 'Alice')
        ctx = self._ctx()

        with pytest.raises(BettingCogError):
            self._run(cog._cmd_place_for(ctx, member, 'home 100'))


class TestForceBetIsAdminOnly:
    def test_for_command_requires_admin_role(self):
        from pathlib import Path
        source = Path('tle/cogs/betting.py').read_text()
        block = source[source.index("@bet.command(name='for'"):
                       source.index('async def bet_for')]
        assert '@commands.has_role(constants.TLE_ADMIN)' in block

"""Betting cog tests for the admin coin commands: grant (give/take via a signed
amount) and grantall (raise/revert every wallet via a signed amount)."""
import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, db,
)


class TestGrantAllCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _ctx(self):
        admin = type('Member', (), {'id': '999', 'display_name': 'Admin'})()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = admin
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        return _Ctx()

    def _setup(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.util import discord_common
        from tle import constants
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(discord_common, '_BOT_PREFIX', ';', raising=False)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

    def test_grantall_pays_everyone_and_raises_seed(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        self._setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_ensure_wallet(GUILD, USER_B, 1000)
        ctx = self._ctx()
        cog = Betting(bot=None)

        self._run(Betting.grantall.__wrapped__(cog, ctx, 500))

        assert db.bet_get_balance(GUILD, USER_A) == 1500
        assert db.bet_get_balance(GUILD, USER_B) == 1500
        # A member who joins the economy later now seeds at the raised amount.
        assert cog._bet_start_balance(int(GUILD)) == 1500
        assert len(ctx.sent) == 1

    def test_negative_grantall_reverts_the_grant(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        self._setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        ctx = self._ctx()
        cog = Betting(bot=None)

        # A negative grantall reverts a prior grant (replaces ;bet ungrantall).
        self._run(Betting.grantall.__wrapped__(cog, ctx, 500))
        self._run(Betting.grantall.__wrapped__(cog, ctx, -500))

        assert db.bet_get_balance(GUILD, USER_A) == 1000
        assert cog._bet_start_balance(int(GUILD)) == 1000

    def test_grantall_rejects_zero(self, db, monkeypatch):
        from tle.cogs.betting import Betting, BettingCogError
        self._setup(db, monkeypatch)
        ctx = self._ctx()
        cog = Betting(bot=None)
        with pytest.raises(BettingCogError):
            self._run(Betting.grantall.__wrapped__(cog, ctx, 0))


class TestGrantCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _ctx(self):
        admin = type('Member', (), {'id': '999', 'display_name': 'Admin'})()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = admin
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        return _Ctx()

    def _setup(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.util import discord_common
        from tle import constants
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(discord_common, 'embed_success', lambda desc: desc)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

    def _member(self):
        return type('Member', (), {'id': USER_A, 'display_name': 'Alice'})()

    def test_positive_grant_adds_coins(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        self._setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        ctx = self._ctx()
        cog = Betting(bot=None)

        self._run(Betting.grant.__wrapped__(cog, ctx, self._member(), 300))

        assert db.bet_get_balance(GUILD, USER_A) == 1300
        assert 'Gave **300**' in ctx.sent[0]

    def test_negative_grant_takes_coins(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        self._setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        ctx = self._ctx()
        cog = Betting(bot=None)

        # ;bet grant @user -400 now takes coins (replaces ;bet take).
        self._run(Betting.grant.__wrapped__(cog, ctx, self._member(), -400))

        assert db.bet_get_balance(GUILD, USER_A) == 600
        assert 'Took **400**' in ctx.sent[0]

    def test_grant_rejects_zero(self, db, monkeypatch):
        from tle.cogs.betting import Betting, BettingCogError
        self._setup(db, monkeypatch)
        ctx = self._ctx()
        cog = Betting(bot=None)
        with pytest.raises(BettingCogError):
            self._run(Betting.grant.__wrapped__(cog, ctx, self._member(), 0))

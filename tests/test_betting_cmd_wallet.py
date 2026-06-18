"""Betting cog tests for the wallet-facing commands: check, transfer, me,\nnotify-role, and history."""
import pytest  # noqa: F401

from tle.util import odds_api
from tle.util import football_data
from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
    _FakeChannel, _FakeGuild,
)


class TestCheckCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_reports_key_health_without_secrets(self, monkeypatch):
        from tle import constants
        from tle.cogs.betting import Betting
        from tle.util import discord_common

        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'odds-secret', raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fd-secret',
                            raising=False)

        async def _sports(api_key, *, with_quota=False):
            assert api_key == 'odds-secret'
            sports = [{'key': odds_api.WORLD_CUP_SPORT_KEY,
                       'title': 'FIFA World Cup 2026'}]
            if with_quota:
                return sports, {'remaining': 412, 'used': 88, 'last': 1}
            return sports

        async def _matches(token):
            assert token == 'fd-secret'
            return [{'home': 'Spain'}, {'home': 'Brazil'}]

        monkeypatch.setattr(odds_api, 'fetch_sports', _sports)
        monkeypatch.setattr(football_data, 'fetch_wc_matches', _matches)
        monkeypatch.setattr(discord_common, 'embed_neutral', lambda desc: desc)

        class _Ctx:
            def __init__(self):
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)
        self._run(Betting.check.__wrapped__(cog, ctx))

        text = ctx.sent[0]
        assert '`ODDS_API_KEY` works' in text
        assert '`FOOTBALL_DATA_API_KEY` works' in text
        assert '2 World Cup match' in text
        assert 'quota-free' in text
        assert '412' in text and 'request(s) remaining this month' in text
        assert '88 used' in text
        assert 'odds-secret' not in text
        assert 'fd-secret' not in text


class TestTransferCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _member(self, uid, name, *, bot=False):
        return type('Member', (), {
            'id': uid,
            'display_name': name,
            'bot': bot,
        })()

    def test_transfer_command_moves_money_between_users(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        admin = self._member('999', 'Admin')
        source = self._member(USER_A, 'Alice')
        target = self._member(USER_B, 'Bob')

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = admin
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        db.bet_set_balance(GUILD, USER_A, 400, 1000)
        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.transfer.__wrapped__(
            cog, ctx, source, target, '25%'))

        assert db.bet_get_balance(GUILD, USER_A) == 300
        assert db.bet_get_balance(GUILD, USER_B) == 1100
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'transfer_out'
        assert hist[0].actor_id == str(admin.id)
        assert len(ctx.sent) == 1

    def test_transfer_command_rejects_same_source_and_destination(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        admin = self._member('999', 'Admin')
        source = self._member(USER_A, 'Alice')
        ctx = type('Ctx', (), {
            'guild': type('G', (), {'id': int(GUILD)})(),
            'author': admin,
        })()
        cog = Betting(bot=None)

        with pytest.raises(BettingCogError):
            self._run(Betting.transfer.__wrapped__(
                cog, ctx, source, source, '10'))


class TestMeCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_me_shows_balance_active_bets_and_history(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)

        author = type('Member', (), {
            'id': USER_A,
            'display_name': 'Alice',
        })()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = author
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.me.__wrapped__(cog, ctx))

        assert len(ctx.sent) == 1
        embed = ctx.sent[0]
        assert embed.title == 'Betting — Alice'
        assert 'Balance: **900**' in embed.description
        fields = {field['name']: field['value'] for field in embed.fields}
        assert 'Active bets' in fields
        assert 'Spain vs Cape Verde' in fields['Active bets']
        assert f'<#{THREAD}>' in fields['Active bets']
        assert 'Recent wallet activity' in fields


class TestNotifyCommands:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _role(self, *, permissions=0, assignable=True, mentionable=True,
              managed=False, default=False):
        return type('Role', (), {
            'id': 444,
            'name': 'notify-wc',
            'mention': '<@&444>',
            'managed': managed,
            'mentionable': mentionable,
            'permissions': type('Perms', (), {'value': permissions})(),
            'is_assignable': lambda self: assignable,
            'is_default': lambda self: default,
        })()

    def test_notifyrole_configures_role_id(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)

        role = self._role()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.notifyrole.__wrapped__(cog, ctx, role))

        assert db.get_guild_config(GUILD, 'bet_notify_role') == '444'

    def test_notifyrole_rejects_permissioned_role(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)

        role = self._role(permissions=8)

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()

            async def send(self, embed=None, **kw):
                pass

        ctx = _Ctx()
        cog = Betting(bot=None)

        with pytest.raises(BettingCogError):
            self._run(Betting.notifyrole.__wrapped__(cog, ctx, role))
        assert db.get_guild_config(GUILD, 'bet_notify_role') is None

    def test_notifyrole_rejects_unassignable_role(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)

        role = self._role(assignable=False)

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()

            async def send(self, embed=None, **kw):
                pass

        ctx = _Ctx()
        cog = Betting(bot=None)

        with pytest.raises(BettingCogError):
            self._run(Betting.notifyrole.__wrapped__(cog, ctx, role))
        assert db.get_guild_config(GUILD, 'bet_notify_role') is None

    def test_notifyrole_rejects_unpingable_role(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)

        role = self._role(mentionable=False)

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()

            async def send(self, embed=None, **kw):
                pass

        ctx = _Ctx()
        cog = Betting(bot=None)

        with pytest.raises(BettingCogError):
            self._run(Betting.notifyrole.__wrapped__(cog, ctx, role))
        assert db.get_guild_config(GUILD, 'bet_notify_role') is None

    def test_notifyrole_off_removes_config(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)

        db.set_guild_config(GUILD, 'bet_notify_role', '444')

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        # ;bet notifyrole off now clears the role (replaces ;bet clearnotifyrole).
        self._run(Betting.notifyrole.__wrapped__(cog, ctx, 'off'))

        assert db.get_guild_config(GUILD, 'bet_notify_role') is None

    def test_notify_toggles_configured_role_for_user(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)

        role = self._role()
        db.set_guild_config(GUILD, 'bet_notify_role', str(role.id))

        class _Member:
            id = USER_A
            display_name = 'Alice'

            def __init__(self):
                self.roles = []

            async def add_roles(self, role_, reason=None):
                self.roles.append(role_)

            async def remove_roles(self, role_, reason=None):
                self.roles = [r for r in self.roles if r.id != role_.id]

        author = _Member()
        guild = _FakeGuild(int(GUILD), _FakeChannel(222), roles=[role])

        class _Ctx:
            def __init__(self):
                self.guild = guild
                self.author = author
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.notify.__wrapped__(cog, ctx))
        assert [r.id for r in author.roles] == [444]

        self._run(Betting.notify.__wrapped__(cog, ctx))
        assert author.roles == []


class TestHistoryCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_any_user_can_inspect_another_wallet_history(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.bet_transfer(GUILD, USER_A, USER_B, 100, 1000, transferred_at=7.0)

        author = type('Member', (), {
            'id': USER_A,
            'display_name': 'Alice',
            'roles': [],
        })()
        target = type('Member', (), {
            'id': USER_B,
            'display_name': 'Bob',
            'roles': [],
        })()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = author
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.history.__wrapped__(cog, ctx, target))

        assert len(ctx.sent) == 1
        assert ctx.sent[0].title == 'Wallet history — Bob'

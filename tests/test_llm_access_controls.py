"""LLM user bans, disable scopes, aliasing, and quoted requests."""
import sqlite3

import pytest

from tle import constants
from tle.cogs import _llm_access as llm_access
from tle.cogs import _llm_commands as llm_commands
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tle.util.db.user_db_upgrades import upgrade_1_49_0
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeAuthor, FakeChannel, FakeCtx


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_neutral',
                        lambda desc, **kw: f'NEUTRAL: {desc}', raising=False)
    return database


def _invoke(command, *args, **kwargs):
    return run(command.__wrapped__(*args, **kwargs))


def _add_config_storage(database):
    values = {}
    database.get_guild_config = (
        lambda guild_id, key: values.get((str(guild_id), key)))
    database.set_guild_config = (
        lambda guild_id, key, value:
        values.__setitem__((str(guild_id), key), value))
    database.delete_guild_config = (
        lambda guild_id, key: values.pop((str(guild_id), key), None))
    return values


def _channel(channel_id, *, parent_id=None):
    channel = FakeChannel()
    channel.id = channel_id
    channel.parent_id = parent_id
    return channel


class TestBanStorage:
    def test_bans_are_idempotent_auditable_and_guild_scoped(self, db):
        assert db.llm_ban_user(100, 7, banned_by=3, now=20) is True
        assert db.llm_ban_user(100, 7, banned_by=9, now=30) is False
        assert db.llm_is_user_banned('100', '7') is True
        assert db.llm_is_user_banned(200, 7) is False
        row = db.llm_get_banned_users(100)[0]
        assert (row.user_id, row.banned_by, row.banned_at) == ('7', '3', 20)

    def test_unban_is_targeted_and_list_is_oldest_first(self, db):
        db.llm_ban_user(100, 8, banned_by=3, now=30)
        db.llm_ban_user(100, 7, banned_by=3, now=20)
        assert [row.user_id for row in db.llm_get_banned_users(100)] == [
            '7', '8']
        assert db.llm_unban_user(100, 7) is True
        assert db.llm_unban_user(100, 7) is False
        assert [row.user_id for row in db.llm_get_banned_users(100)] == ['8']

    def test_1_49_migration_is_idempotent_and_indexed(self):
        conn = sqlite3.connect(':memory:')
        upgrade_1_49_0(conn)
        upgrade_1_49_0(conn)
        columns = {
            row[1] for row in conn.execute(
                'PRAGMA table_info(llm_user_ban)').fetchall()
        }
        indexes = {
            row[1] for row in conn.execute(
                'PRAGMA index_list(llm_user_ban)').fetchall()
        }
        assert columns == {'guild_id', 'user_id', 'banned_by', 'banned_at'}
        assert 'llm_user_ban_guild_time' in indexes


class TestBanCommands:
    def test_moderator_can_ban_list_and_unban(self, db):
        cog = llm_cog.Llm(bot=None)
        moderator = FakeCtx(roles=(constants.TLE_MODERATOR,))
        target = FakeAuthor(7, display_name='target')
        moderator.guild.get_member = lambda user_id: target

        _invoke(llm_cog.Llm.ban, cog, moderator, '<@7>')
        assert db.llm_is_user_banned(100, 7)
        assert 'can no longer use' in moderator.text

        listing = FakeCtx(roles=(constants.TLE_MODERATOR,))
        listing.guild.get_member = lambda user_id: target
        _invoke(llm_cog.Llm.banlist, cog, listing)
        assert 'target' in listing.text and '`7`' in listing.text

        unban = FakeCtx(roles=(constants.TLE_MODERATOR,))
        _invoke(llm_cog.Llm.unban, cog, unban, '7')
        assert not db.llm_is_user_banned(100, 7)

    def test_unprivileged_user_cannot_mutate_or_view_bans(self, db):
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.ban, cog, ctx, FakeAuthor(7))
        _invoke(llm_cog.Llm.banlist, cog, ctx)
        assert not db.llm_is_user_banned(100, 7)
        assert ctx.text.count('admins or moderators') == 2

    def test_bot_owner_can_manage_access_without_a_guild_role(self, db):
        class OwnerBot:
            user = None

            async def is_owner(self, author):
                return True

        ctx = FakeCtx()
        _invoke(llm_cog.Llm.ban, llm_cog.Llm(OwnerBot()), ctx, '7')
        assert db.llm_is_user_banned(100, 7)


class TestDisableCommands:
    def test_guild_disable_and_enable_are_reversible(self, db):
        _add_config_storage(db)
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx(roles=(constants.TLE_MODERATOR,), channel=_channel(44))
        _invoke(llm_cog.Llm.disable, cog, ctx)
        assert llm_access.disabled_scope(db, 100, 44) == 'guild'
        _invoke(llm_cog.Llm.enable, cog, ctx)
        assert llm_access.disabled_scope(db, 100, 44) is None

    def test_channel_disable_isolated_and_inherited_by_threads(self, db):
        _add_config_storage(db)
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx(roles=(constants.TLE_MODERATOR,), channel=_channel(44))
        _invoke(llm_cog.Llm.disable, cog, ctx, 'here')
        assert llm_access.disabled_scope(db, 100, 44) == 'channel'
        assert llm_access.disabled_scope(db, 100, 45) is None
        thread_id = llm_access.scope_channel_id(_channel(99, parent_id=44))
        assert llm_access.disabled_scope(db, 100, thread_id) == 'channel'
        _invoke(llm_cog.Llm.enable, cog, ctx, 'here')
        assert llm_access.disabled_scope(db, 100, 44) is None

    def test_unprivileged_user_cannot_disable_requests(self, db):
        values = _add_config_storage(db)
        ctx = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.disable, llm_cog.Llm(bot=None), ctx, 'here')
        assert values == {}
        assert 'admins or moderators' in ctx.text


class TestRequestEnforcement:
    @staticmethod
    async def _forbidden(*args, **kwargs):
        raise AssertionError('blocked request reached a provider')

    def test_ban_blocks_gemini_and_grok_without_spend(self, db, monkeypatch):
        db.llm_ban_user(100, 1, banned_by=3)
        monkeypatch.setattr(gemini_api, 'complete', self._forbidden)
        monkeypatch.setattr(xai_api, 'complete', self._forbidden)
        cog = llm_cog.Llm(bot=None)

        gemini = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, gemini, question='hello')
        grok = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, grok, question='+grok hello')

        assert 'not allowed' in gemini.text
        assert 'not allowed' in grok.text
        count = db.conn.execute(
            'SELECT COUNT(*) AS count FROM llm_xai_request').fetchone().count
        assert count == 0

    def test_channel_disable_blocks_both_providers(self, db, monkeypatch):
        _add_config_storage(db)
        llm_access.set_disabled(db, 100, 44, disabled=True, scope='channel')
        monkeypatch.setattr(gemini_api, 'complete', self._forbidden)
        monkeypatch.setattr(xai_api, 'complete', self._forbidden)
        cog = llm_cog.Llm(bot=None)

        gemini = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, gemini, question='hello')
        grok = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, grok, question='+grok hello')

        assert 'disabled in this channel' in gemini.text
        assert 'disabled in this channel' in grok.text
        count = db.conn.execute(
            'SELECT COUNT(*) AS count FROM llm_xai_request').fetchone().count
        assert count == 0

    def test_at_grok_uses_the_same_ban(self, db, monkeypatch):
        db.llm_ban_user(100, 1, banned_by=3)
        monkeypatch.setattr(xai_api, 'complete', self._forbidden)
        ctx = FakeCtx()

        class Bot:
            user = None

            async def get_context(self, message):
                ctx.message = message
                return ctx

            async def can_run(self, context):
                return True

        message = FakeMessage(content='@grok hello')
        message.guild = type('Guild', (), {'id': 100})()
        message.author = type('Author', (), {
            'bot': False, 'id': 1, 'display_name': 'target'})()
        run(llm_cog.Llm(Bot()).on_message(message))
        assert 'not allowed' in ctx.text

    def test_policy_is_rechecked_before_grok_reservation(
            self, db, monkeypatch):
        _add_config_storage(db)
        db.llm_add_key('xai-ExampleKeyValue1234567890', provider='xai')
        monkeypatch.setattr(xai_api, 'complete', self._forbidden)
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx(channel=_channel(44))

        class DisableWhileQueued:
            async def run(self, provider, user_id, operation):
                llm_access.set_disabled(
                    db, 100, 44, disabled=True, scope='channel')
                return await operation()

        cog._runtime = DisableWhileQueued()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert 'disabled in this channel' in ctx.text
        count = db.conn.execute(
            'SELECT COUNT(*) AS count FROM llm_xai_request').fetchone().count
        assert count == 0


class TestCommandDispatch:
    def test_ai_alias_and_access_subcommands_are_registered(self):
        group = llm_cog.Llm.llm
        assert 'ai' in group.aliases
        assert {'ban', 'unban', 'banlist', 'disable', 'enable'} <= set(
            group.all_commands)

    @pytest.mark.parametrize('question,expected', [
        ('"disable here"', 'disable here'),
        ('“ban target”', 'ban target'),
        ("'models'", 'models'),
    ])
    def test_whole_quoted_text_is_forwarded_as_a_request(
            self, monkeypatch, question, expected):
        seen = []

        async def fake_ask(cog, ctx, text):
            seen.append(text)

        monkeypatch.setattr(llm_commands.llm_ask, 'ask', fake_ask)
        _invoke(llm_cog.Llm.llm, llm_cog.Llm(bot=None), FakeCtx(),
                question=question)
        assert seen == [expected]

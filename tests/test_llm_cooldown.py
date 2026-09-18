"""Persistent shared cooldowns for command and literal LLM requests."""

import sqlite3

import pytest

from tle import constants
from tle.cogs import _llm_access as llm_access
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tle.util.db import llm_cooldown_db
from tle.util.db.user_db_conn import UserDbConn
from tle.util.db.user_db_upgrades import (
    registry, upgrade_1_50_0, upgrade_1_51_0,
)
from tle.util.llm_keypool import Lease
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeChannel, FakeCtx


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'GEMINI_API_KEYS', '')
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_neutral',
                        lambda desc, **kw: f'NEUTRAL: {desc}', raising=False)
    return database


def _invoke(command, *args, **kwargs):
    return run(command.__wrapped__(*args, **kwargs))


def _channel(channel_id, parent_id=None):
    channel = FakeChannel()
    channel.id = channel_id
    channel.parent_id = parent_id
    return channel


class TestCooldownStorage:
    def test_claim_is_atomic_non_sliding_and_allows_exact_expiry(self, db):
        db.llm_set_cooldown(100, 60, channel_id=44)

        assert db.llm_claim_cooldowns(100, 44, now=100) is None
        denial = db.llm_claim_cooldowns(100, 44, now=110)
        assert (denial.scope, denial.retry_at) == ('channel', 160)
        assert db.llm_claim_cooldowns(100, 44, now=159).retry_at == 160
        assert db.llm_claim_cooldowns(100, 44, now=160) is None

    def test_later_global_scope_wins_and_is_guild_scoped(self, db):
        db.llm_set_cooldown(100, 120)
        db.llm_set_cooldown(100, 60, channel_id=44)
        assert db.llm_claim_cooldowns(100, 44, now=100) is None

        denial = db.llm_cooldown_retry(100, 44, now=110)
        assert (denial.scope, denial.retry_at) == ('global', 220)
        assert db.llm_cooldown_retry(100, 45, now=110).retry_at == 220
        assert db.llm_claim_cooldowns(200, 44, now=110) is None

    def test_channel_scope_isolated_and_reconfiguration_clears_timer(self, db):
        db.llm_set_cooldown(100, 60, channel_id=44)
        assert db.llm_claim_cooldowns(100, 44, now=100) is None
        assert db.llm_cooldown_retry(100, 45, now=101) is None

        db.llm_set_cooldown(100, 30, channel_id=44)
        assert db.llm_cooldown_retry(100, 44, now=101) is None
        assert db.llm_claim_cooldowns(100, 44, now=101) is None
        db.llm_set_cooldown(100, 0, channel_id=44)
        assert db.llm_get_cooldown_settings(100, 44) == {}

    def test_family_scope_is_shared_by_parent_and_threads(self, db):
        db.llm_set_cooldown(100, 60, family_id=44)
        assert db.llm_claim_cooldowns(
            100, 99, family_id=44, now=100) is None

        denial = db.llm_cooldown_retry(
            100, 98, family_id=44, now=110)
        assert (denial.scope, denial.retry_at) == ('threads', 160)
        assert db.llm_cooldown_retry(
            100, 44, family_id=44, now=110).retry_at == 160
        assert db.llm_cooldown_retry(
            100, 45, family_id=45, now=110) is None

    def test_1_50_migration_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        upgrade_1_50_0(conn)
        upgrade_1_50_0(conn)

        columns = {
            row[1] for row in conn.execute(
                'PRAGMA table_info(llm_cooldown)').fetchall()
        }
        primary = {
            row[1] for row in conn.execute(
                'PRAGMA table_info(llm_cooldown)').fetchall() if row[5]
        }
        assert columns == {
            'guild_id', 'channel_id', 'seconds', 'last_attempt_at'}
        assert primary == {'guild_id', 'channel_id'}

    def test_1_51_migrates_existing_parent_cooldown_to_family(self, db):
        db.conn.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        db.conn.execute(
            'INSERT INTO db_version (version) VALUES (?)', ('1.50.0',))
        db.conn.execute(
            'INSERT INTO llm_cooldown '
            '(guild_id, channel_id, seconds, last_attempt_at) '
            'VALUES (?, ?, ?, ?)',
            ('100', '44', 60, 100.0))
        db.conn.execute(
            'INSERT INTO llm_cooldown '
            '(guild_id, channel_id, seconds, last_attempt_at) '
            'VALUES (?, ?, ?, ?)',
            ('200', '*', 120, 50.0))
        db.conn.commit()

        registry.run(db.conn)
        upgrade_1_51_0(db.conn)

        rows = db.conn.execute(
            'SELECT guild_id, channel_id, seconds, last_attempt_at '
            'FROM llm_cooldown ORDER BY guild_id').fetchall()
        assert [tuple(row) for row in rows] == [
            ('100', 'family:44', 60, 100.0),
            ('200', '*', 120, 50.0),
        ]
        assert registry.get_current_version(db.conn) == registry.latest_version
        denial = db.llm_cooldown_retry(
            100, 99, family_id=44, now=110.0)
        assert (denial.scope, denial.retry_at) == ('threads', 160.0)
        assert db.llm_cooldown_retry(
            100, 98, family_id=44, now=110.0) == denial
        assert db.llm_cooldown_retry(
            100, 44, family_id=44, now=110.0) == denial

    def test_1_51_preserves_ambiguous_raw_row_on_scope_collision(self, db):
        db.conn.executemany(
            'INSERT INTO llm_cooldown '
            '(guild_id, channel_id, seconds, last_attempt_at) '
            'VALUES (?, ?, ?, ?)', (
                ('100', '44', 60, 100.0),
                ('100', 'family:44', 90, 200.0),
            ))
        db.conn.commit()

        upgrade_1_51_0(db.conn)
        upgrade_1_51_0(db.conn)

        rows = db.conn.execute(
            'SELECT channel_id, seconds, last_attempt_at '
            'FROM llm_cooldown ORDER BY channel_id').fetchall()
        assert [tuple(row) for row in rows] == [
            ('44', 60, 100.0),
            ('family:44', 90, 200.0),
        ]

    def test_opening_1_50_database_runs_scope_migration(self, tmp_path):
        dbfile = tmp_path / 'user.db'
        raw = sqlite3.connect(dbfile)
        upgrade_1_50_0(raw)
        raw.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        raw.execute(
            'INSERT INTO db_version (version) VALUES (?)', ('1.50.0',))
        raw.execute(
            'INSERT INTO llm_cooldown '
            '(guild_id, channel_id, seconds, last_attempt_at) '
            'VALUES (?, ?, ?, ?)',
            ('100', '44', 60, 100.0))
        raw.commit()
        raw.close()

        database = UserDbConn(str(dbfile))
        try:
            assert registry.get_current_version(
                database.conn) == registry.latest_version
            denial = database.llm_cooldown_retry(
                100, 99, family_id=44, now=110.0)
            assert (denial.scope, denial.retry_at) == ('threads', 160.0)
        finally:
            database.conn.close()


class TestCooldownCommand:
    @pytest.mark.parametrize('role', (
        constants.TLE_ADMIN, constants.TLE_MODERATOR,
    ))
    def test_privileged_member_sets_channel_and_global(self, role, db):
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx(roles=(role,), channel=_channel(44))

        _invoke(llm_cog.Llm.cooldown, cog, ctx, '60')
        assert db.llm_get_cooldown_settings(100, 44) == {'channel': 60}
        _invoke(llm_cog.Llm.cooldown, cog, ctx, '120', '+global')
        assert db.llm_get_cooldown_settings(100, 44) == {
            'channel': 60, 'global': 120}
        assert 'server-wide' in ctx.text and 'accepted prompt attempt' in ctx.text

        inspect = FakeCtx(roles=(role,), channel=_channel(44))
        _invoke(llm_cog.Llm.cooldown, cog, inspect)
        assert '60 seconds' in inspect.text and '120 seconds' in inspect.text
        assert 'Channel + threads cooldown' in inspect.text
        _invoke(llm_cog.Llm.cooldown, cog, ctx, '0', '+global')
        assert db.llm_get_cooldown_settings(100, 44) == {'channel': 60}

    def test_thread_configuration_uses_exact_thread_channel(self, db):
        ctx = FakeCtx(
            roles=(constants.TLE_MODERATOR,),
            channel=_channel(99, parent_id=44))
        _invoke(llm_cog.Llm.cooldown, llm_cog.Llm(bot=None), ctx, '60')
        assert db.llm_get_cooldown_settings(
            100, 99, family_id=44) == {'channel': 60}
        assert db.llm_get_cooldown_settings(100, 44) == {}
        assert 'this thread' in ctx.text

    def test_threads_flag_targets_parent_family_from_inside_thread(self, db):
        ctx = FakeCtx(
            roles=(constants.TLE_MODERATOR,),
            channel=_channel(99, parent_id=44))
        cog = llm_cog.Llm(bot=None)

        _invoke(llm_cog.Llm.cooldown, cog, ctx, '90', '+threads')

        assert db.llm_get_cooldown_settings(
            100, 99, family_id=44) == {'threads': 90}
        assert db.llm_get_cooldown_settings(
            100, 44, family_id=44) == {'threads': 90}
        assert db.llm_get_cooldown_settings(
            100, 45, family_id=45) == {}
        assert 'channel and all of its threads' in ctx.text

    def test_regular_user_and_invalid_values_cannot_mutate(self, db):
        cog = llm_cog.Llm(bot=None)
        regular = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.cooldown, cog, regular, '60')
        assert 'admins or moderators' in regular.text

        moderator = FakeCtx(
            roles=(constants.TLE_MODERATOR,), channel=_channel(44))
        _invoke(llm_cog.Llm.cooldown, cog, moderator, '86401')
        _invoke(llm_cog.Llm.cooldown, cog, moderator, 'nope', '+global')
        _invoke(llm_cog.Llm.cooldown, cog, moderator,
                '60', '+threads', '+global')
        assert db.llm_get_cooldown_settings(100, 44) == {}
        assert '0 to 86400' in moderator.text
        assert 'Usage:' in moderator.text

    def test_command_is_registered_on_ai_group(self):
        assert 'cooldown' in llm_cog.Llm.llm.all_commands


class TestCooldownEnforcement:
    def test_parent_channel_and_threads_have_independent_timers(
            self, db, monkeypatch):
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: 100.0)
        db.llm_set_cooldown(100, 60, channel_id=99)
        first = FakeCtx(channel=_channel(99, parent_id=44))
        llm_access.raise_if_request_blocked(db, first)

        sibling = FakeCtx(user_id=2, channel=_channel(98, parent_id=44))
        llm_access.raise_if_request_blocked(db, sibling)
        parent = FakeCtx(user_id=3, channel=_channel(44))
        llm_access.raise_if_request_blocked(db, parent)

        retry = FakeCtx(user_id=4, channel=_channel(99, parent_id=44))
        with pytest.raises(llm_access.LlmAccessDeniedError) as error:
            llm_access.raise_if_request_blocked(db, retry)
        assert '<t:160:R>' in str(error.value)

    def test_threads_cooldown_is_shared_by_parent_and_siblings(
            self, db, monkeypatch):
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: 100.0)
        db.llm_set_cooldown(100, 60, family_id=44)
        first = FakeCtx(channel=_channel(99, parent_id=44))
        llm_access.raise_if_request_blocked(db, first)

        for channel in (
                _channel(98, parent_id=44),
                _channel(44)):
            denied = FakeCtx(user_id=2, channel=channel)
            with pytest.raises(llm_access.LlmAccessDeniedError) as error:
                llm_access.raise_if_request_blocked(db, denied)
            assert 'channel and its threads' in str(error.value)
            assert '<t:160:R>' in str(error.value)

        other_channel = FakeCtx(user_id=3, channel=_channel(45))
        llm_access.raise_if_request_blocked(db, other_channel)

    def test_gemini_attempt_blocks_grok_before_spend_or_provider(
            self, db, monkeypatch):
        now = 1_700_000_000.2
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: now)
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        db.llm_add_key(
            'xai-ExampleKeyValue1234567890', provider='xai')
        db.llm_set_cooldown(100, 60)
        gemini_calls = []

        async def gemini_answer(pool, prompt, **kwargs):
            gemini_calls.append(prompt)
            return 'answer', Lease(1, 'redacted', 'test', 'model-a')

        async def forbidden_xai(*args, **kwargs):
            raise AssertionError('cooldown-denied request reached xAI')

        monkeypatch.setattr(gemini_api, 'complete', gemini_answer)
        monkeypatch.setattr(xai_api, 'complete', forbidden_xai)
        cog = llm_cog.Llm(bot=None)
        first = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, first, question='+direct hello')
        assert gemini_calls == ['hello']

        second = FakeCtx(user_id=2, channel=_channel(45))
        _invoke(llm_cog.Llm.llm, cog, second, question='+grok hello')
        assert 'shared server-wide cooldown' in second.text
        assert '<t:1700000061:R>' in second.text
        assert db.llm_xai_daily_summary(now=now).calls == 0

    def test_provider_failure_consumes_but_missing_key_does_not(
            self, db, monkeypatch):
        now = 1_700_000_000.0
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: now)
        db.llm_set_cooldown(100, 60, channel_id=44)
        cog = llm_cog.Llm(bot=None)
        missing = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, missing, question='+direct hello')
        assert 'No Gemini API keys' in missing.text
        assert db.llm_cooldown_retry(100, 44, now=now) is None

        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        cog = llm_cog.Llm(bot=None)
        calls = []

        async def failed_provider(*args, **kwargs):
            calls.append(1)
            raise gemini_api.GeminiError('provider failed')

        monkeypatch.setattr(gemini_api, 'complete', failed_provider)
        failed = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, failed, question='+direct hello')
        denied = FakeCtx(user_id=2, channel=_channel(44))
        _invoke(llm_cog.Llm.llm, cog, denied, question='+direct again')
        assert calls == [1]
        assert 'shared cooldown in this channel' in denied.text

    @pytest.mark.parametrize('trigger', ('@grok hello', '@gemini hello'))
    def test_literal_providers_obey_existing_cooldown(
            self, db, monkeypatch, trigger):
        now = 1_700_000_000.0
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: now)
        db.llm_set_cooldown(100, 60)
        assert db.llm_claim_cooldowns(100, 44, now=now) is None
        ctx = FakeCtx(user_id=2, channel=_channel(45))

        class Bot:
            user = None

            async def get_context(self, message):
                ctx.message = message
                return ctx

            async def can_run(self, context):
                return True

        async def forbidden(*args, **kwargs):
            raise AssertionError('cooldown-denied literal reached a provider')

        monkeypatch.setattr(xai_api, 'complete', forbidden)
        monkeypatch.setattr(gemini_api, 'complete', forbidden)
        message = FakeMessage(content=trigger)
        message.guild = type('Guild', (), {'id': 100})()
        message.author = type('Author', (), {
            'bot': False, 'id': 2, 'display_name': 'target'})()
        run(llm_cog.Llm(Bot()).on_message(message))
        assert 'shared server-wide cooldown' in ctx.text
        assert db.llm_xai_daily_summary(now=now).calls == 0

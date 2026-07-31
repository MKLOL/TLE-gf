"""Persistent shared cooldowns for ``;ai`` and ``@grok`` requests."""

import sqlite3

import pytest

from tle import constants
from tle.cogs import _llm_access as llm_access
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tle.util.db import llm_cooldown_db
from tle.util.db.user_db_upgrades import upgrade_1_50_0
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
        _invoke(llm_cog.Llm.cooldown, cog, ctx, '0', '+global')
        assert db.llm_get_cooldown_settings(100, 44) == {'channel': 60}

    def test_thread_configuration_uses_parent_channel(self, db):
        ctx = FakeCtx(
            roles=(constants.TLE_MODERATOR,),
            channel=_channel(99, parent_id=44))
        _invoke(llm_cog.Llm.cooldown, llm_cog.Llm(bot=None), ctx, '60')
        assert db.llm_get_cooldown_settings(100, 44) == {'channel': 60}
        assert db.llm_get_cooldown_settings(100, 99) == {}

    def test_regular_user_and_invalid_values_cannot_mutate(self, db):
        cog = llm_cog.Llm(bot=None)
        regular = FakeCtx(channel=_channel(44))
        _invoke(llm_cog.Llm.cooldown, cog, regular, '60')
        assert 'admins or moderators' in regular.text

        moderator = FakeCtx(
            roles=(constants.TLE_MODERATOR,), channel=_channel(44))
        _invoke(llm_cog.Llm.cooldown, cog, moderator, '86401')
        _invoke(llm_cog.Llm.cooldown, cog, moderator, 'nope', '+global')
        assert db.llm_get_cooldown_settings(100, 44) == {}
        assert '0 to 86400' in moderator.text
        assert 'Usage:' in moderator.text

    def test_command_is_registered_on_ai_group(self):
        assert 'cooldown' in llm_cog.Llm.llm.all_commands


class TestCooldownEnforcement:
    def test_parent_channel_and_threads_share_the_timer(self, db, monkeypatch):
        monkeypatch.setattr(llm_cooldown_db.time, 'time', lambda: 100.0)
        db.llm_set_cooldown(100, 60, channel_id=44)
        first = FakeCtx(channel=_channel(99, parent_id=44))
        llm_access.raise_if_request_blocked(db, first)

        sibling = FakeCtx(user_id=2, channel=_channel(98, parent_id=44))
        with pytest.raises(llm_access.LlmAccessDeniedError) as error:
            llm_access.raise_if_request_blocked(db, sibling)
        assert '<t:160:R>' in str(error.value)

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

    def test_literal_grok_obeys_existing_cooldown(self, db, monkeypatch):
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
            raise AssertionError('cooldown-denied @grok reached xAI')

        monkeypatch.setattr(xai_api, 'complete', forbidden)
        message = FakeMessage(content='@grok hello')
        message.guild = type('Guild', (), {'id': 100})()
        message.author = type('Author', (), {
            'bot': False, 'id': 2, 'display_name': 'target'})()
        run(llm_cog.Llm(Bot()).on_message(message))
        assert 'shared server-wide cooldown' in ctx.text
        assert db.llm_xai_daily_summary(now=now).calls == 0

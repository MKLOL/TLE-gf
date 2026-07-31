"""Owner authorization, private status, health reset, and context policy."""
import time

import pytest

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx


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


class OwnerBot:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.user = None

    async def is_owner(self, author):
        return self.allowed


class TestGlobalOwnerAuthorization:
    KEY = 'xai-OwnerOnlyExampleKey123456789'

    def test_non_owner_secret_is_deleted_but_never_stored(self, db):
        cog = llm_cog.Llm(OwnerBot(allowed=False))
        message = FakeMessage(content=f';llm grokkeys {self.KEY}')
        ctx = FakeCtx(message=message)
        _invoke(llm_cog.Llm.grokkeys, cog, ctx, self.KEY)
        assert message.deleted is True
        assert db.llm_get_keys(provider='xai') == []
        assert self.KEY not in ctx.text
        assert 'bot owner' in ctx.text

    def test_owner_can_upload_an_xai_key(self, db):
        cog = llm_cog.Llm(OwnerBot())
        ctx = FakeCtx(message=FakeMessage())
        _invoke(llm_cog.Llm.grokkeys, cog, ctx, self.KEY)
        assert len(db.llm_get_keys(provider='xai')) == 1
        assert self.KEY not in ctx.text

    def test_environment_key_erases_legacy_persisted_copy(
            self, db, monkeypatch):
        key = 'xai-LegacyEnvironmentKey123456789'
        db.llm_add_key(key, provider='xai')
        monkeypatch.setattr(constants, 'XAI_API_KEYS', key)
        pool = llm_cog.Llm(bot=None)._get_xai_pool()
        assert pool.key_count() == 1
        assert db.llm_get_keys(provider='xai') == []
        raw = db.conn.execute(
            "SELECT api_key FROM llm_api_key WHERE provider = 'xai'"
        ).fetchone()
        assert raw.api_key == ''


class TestOwnerStatus:
    KEY = 'xai-StatusExampleKey1234567890'

    def test_grok_status_is_provider_split_and_threshold_free(self, db):
        db.llm_add_key(self.KEY, provider='xai')
        day = llm_cog._today()
        db.llm_record_request(
            100, 1, 'xai', day, 'success', model='grok-test',
            router_attempts=1, answer_attempts=1, input_tokens=40,
            output_tokens=8, total_tokens=48, latency_ms=125,
            cost_microusd=70)
        reservation = db.llm_reserve_xai_request(
            1, user_limit=10, window_seconds=1800, daily_limit=100,
            reserved_microusd=500, daily_budget_microusd=1_000_000,
            return_id=True, now=time.time())
        db.llm_finalize_xai_request(
            reservation, actual_microusd=70, outcome='success')

        cog = llm_cog.Llm(OwnerBot())
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.grokstatus, cog, ctx)
        text = ctx.sent[0].description
        assert 'grok-test' in text
        assert '48 total' in text
        assert '$0.0001' in text
        assert self.KEY not in text
        assert 'daily limit' not in text.casefold()
        assert 'per 30' not in text.casefold()

    def test_non_owner_cannot_see_health_or_spend(self, db):
        db.llm_add_key(self.KEY, provider='xai')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.grokstatus,
                llm_cog.Llm(OwnerBot(allowed=False)), ctx)
        assert 'bot owner' in ctx.text
        assert 'Credit guard' not in ctx.text

    def test_owner_can_reset_reversible_xai_health(self, db):
        db.llm_add_key(self.KEY, provider='xai')
        cog = llm_cog.Llm(OwnerBot())
        pool = cog._get_xai_pool()
        lease = pool.leases()[0]
        pool.report_access(lease, message='billing pending')
        assert pool.status()[0]['state'] != 'ready'
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.healthreset, cog, ctx, 'grok')
        assert pool.status()[0]['state'] == 'ready'
        assert 'Reset' in ctx.text


class TestContextPrivacyPolicy:
    @staticmethod
    def _config_db(database):
        values = {}
        database.get_guild_config = (
            lambda guild_id, key: values.get((str(guild_id), key)))
        database.set_guild_config = (
            lambda guild_id, key, value:
            values.__setitem__((str(guild_id), key), value))
        database.delete_guild_config = (
            lambda guild_id, key: values.pop((str(guild_id), key), None))
        return values

    def test_moderator_can_set_and_inspect_channel_policy(self, db):
        self._config_db(db)
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx(roles=(constants.TLE_MODERATOR,))
        ctx.channel.id = 44
        _invoke(llm_cog.Llm.privacy, cog, ctx, 'explicit', 'channel')
        assert cog._context_policy(ctx, with_source=True) == (
            'explicit', 'channel override')
        query = FakeCtx()
        query.channel = ctx.channel
        _invoke(llm_cog.Llm.privacy, cog, query)
        assert 'explicit' in query.text

    def test_unprivileged_user_cannot_change_policy(self, db):
        values = self._config_db(db)
        cog = llm_cog.Llm(bot=None)
        ctx = FakeCtx()
        ctx.channel.id = 44
        _invoke(llm_cog.Llm.privacy, cog, ctx, 'off', 'channel')
        assert values == {}
        assert 'admins or moderators' in ctx.text

    def test_off_policy_rejects_explicit_history_without_provider_call(
            self, db, monkeypatch):
        self._config_db(db)
        db.set_guild_config(100, 'llm_context', 'off')
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

        async def forbidden(*args, **kwargs):
            raise AssertionError('privacy-off request reached provider')

        monkeypatch.setattr(gemini_api, 'complete', forbidden)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, llm_cog.Llm(bot=None), ctx,
                question='+context summarize')
        assert 'disabled here' in ctx.text


class TestLiteralTriggerChecks:
    def test_bot_global_check_can_block_at_grok(self, monkeypatch):
        ctx = FakeCtx()

        class GateBot:
            user = None

            async def get_context(self, message):
                ctx.message = message
                return ctx

            async def can_run(self, context):
                return False

        async def forbidden(*args, **kwargs):
            raise AssertionError('blocked @grok reached xAI')

        monkeypatch.setattr(xai_api, 'complete', forbidden)
        message = FakeMessage(content='@grok hello')
        message.guild = type('Guild', (), {'id': 100})()
        message.author = type('Author', (), {
            'bot': False, 'id': 1, 'display_name': 'nife'})()
        run(llm_cog.Llm(GateBot()).on_message(message))
        assert ctx.sent == []

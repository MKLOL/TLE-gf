"""Persistent credit-guard coverage for Grok Discord requests."""
import pytest

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, xai_api
from tests.llm_test_utils import FakeLlmDb, run
from tests.test_llm_cog import FakeCtx, _answers
from tests.test_llm_grok import (
    _FakeBot, _add_xai_key, _invoke, _listener_message, _xai_answers,
)


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    return database


@pytest.fixture
def cog():
    return llm_cog.Llm(bot=None)


def _event_count(database):
    return database.conn.execute(
        'SELECT COUNT(*) AS count FROM llm_xai_request'
    ).fetchone().count


class TestGrokLimitGate:
    @pytest.mark.parametrize('reason', ['user', 'daily'])
    def test_denial_is_generic_and_makes_no_provider_call(
            self, reason, cog, db, monkeypatch):
        _add_xai_key(db)
        monkeypatch.setattr(
            db, 'llm_reserve_xai_request', lambda *args, **kwargs: reason)

        async def should_not_call(*args, **kwargs):
            raise AssertionError('a rejected invocation must not call xAI')

        monkeypatch.setattr(xai_api, 'complete', should_not_call)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')

        assert ctx.text == (
            'ALERT: Grok is taking a breather right now. Try again later.')
        hidden = ('30', '300', '0.3', 'minute', 'daily', 'quota', 'limit')
        assert not any(word in ctx.text.casefold() for word in hidden)
        assert db.llm_get_usage(100, 1, llm_cog._today()) == 0

    def test_cross_guild_user_limit_is_enforced_for_moderator(
            self, cog, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)

        for index in range(constants.XAI_USER_RATE_LIMIT):
            ctx = FakeCtx(roles=('Moderator',), guild_id=100 + index % 2)
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok again')
            assert 'Grok answer' in ctx.text

        blocked = FakeCtx(roles=('Moderator',), guild_id=999)
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok again')
        assert 'taking a breather' in blocked.text
        assert len(seen) == constants.XAI_USER_RATE_LIMIT * 2
        assert _event_count(db) == constants.XAI_USER_RATE_LIMIT

    def test_success_reserves_once_despite_router_and_answer_calls(
            self, cog, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+grok hello')

        assert len(seen) == 2
        assert _event_count(db) == 1

    def test_upstream_failure_keeps_the_reserved_slot(
            self, cog, db, monkeypatch):
        _add_xai_key(db)

        async def failed(pool, prompt, **kwargs):
            kwargs['stats']['attempts'] = 1
            raise xai_api.ServiceUnavailableError('offline')

        monkeypatch.setattr(xai_api, 'complete', failed)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')

        assert 'temporarily unavailable' in ctx.text
        assert _event_count(db) == 1

    def test_missing_key_does_not_reserve(self, cog, db):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert 'No xAI API keys' in ctx.text
        assert _event_count(db) == 0

    def test_invalid_prompt_does_not_reserve(self, cog, db, monkeypatch):
        _add_xai_key(db)

        async def should_not_call(*args, **kwargs):
            raise AssertionError('an invalid prompt must not call xAI')

        monkeypatch.setattr(xai_api, 'complete', should_not_call)
        ctx = FakeCtx()
        question = '+grok ' + 'x' * (constants.LLM_MAX_PROMPT_CHARS + 1)
        _invoke(llm_cog.Llm.llm, cog, ctx, question=question)
        assert 'too long' in ctx.text.casefold()
        assert _event_count(db) == 0

    def test_literal_trigger_uses_the_same_limit_gate(
            self, db, monkeypatch):
        _add_xai_key(db)
        monkeypatch.setattr(
            db, 'llm_reserve_xai_request',
            lambda *args, **kwargs: 'daily')

        async def should_not_call(*args, **kwargs):
            raise AssertionError('a rejected @grok must not call xAI')

        monkeypatch.setattr(xai_api, 'complete', should_not_call)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message('@grok hello')))
        assert 'taking a breather' in ctx.text


def test_gemini_keeps_its_existing_output_budget(cog, db, monkeypatch):
    db.llm_add_key('AIzaSyExampleKeyValue1234567')
    seen = _answers(monkeypatch)
    _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='hello')
    assert seen['kwargs']['max_output_tokens'] == \
        constants.LLM_MAX_OUTPUT_TOKENS

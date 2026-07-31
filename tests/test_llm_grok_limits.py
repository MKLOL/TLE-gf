"""Persistent credit-guard coverage for Grok Discord requests."""
import pytest

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, xai_api
from tle.util.db.llm_db import XaiRequestDenial
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
    def test_denials_explain_retry_without_exposing_spend(
            self, cog, db, monkeypatch):
        _add_xai_key(db)

        async def should_not_call(*args, **kwargs):
            raise AssertionError('a rejected invocation must not call xAI')

        monkeypatch.setattr(xai_api, 'complete', should_not_call)
        messages = {}
        retry_at = 2_000_000_000.2
        for reason in ('user', 'daily', 'budget'):
            denial = XaiRequestDenial(reason, retry_at)
            monkeypatch.setattr(
                db, 'llm_reserve_xai_request',
                lambda *args, _denial=denial, **kwargs: _denial)
            ctx = FakeCtx()
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
            messages[reason] = ctx.text

        assert messages['user'] == (
            'ALERT: You have used all 15 Grok requests available to you in '
            'the last hour. Try again <t:2000000001:R> '
            '(<t:2000000001:F>).')
        shared = (
            "ALERT: Grok's shared daily allowance is used up. Try again "
            '<t:2000000001:R> (<t:2000000001:F>).')
        assert messages['daily'] == messages['budget'] == shared
        hidden = ('200 requests', '$0.5', 'budget', 'cost', 'spend')
        assert not any(word in shared.casefold() for word in hidden)
        assert db.llm_get_usage(100, 1, llm_cog._today()) == 0

    def test_cross_guild_user_limit_is_enforced_for_regular_user(
            self, cog, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)

        for index in range(constants.XAI_USER_RATE_LIMIT):
            ctx = FakeCtx(guild_id=100 + index % 2)
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok again')
            assert 'Grok answer' in ctx.text

        blocked = FakeCtx(guild_id=999)
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok again')
        assert 'used all 15 Grok requests' in blocked.text
        assert '<t:' in blocked.text and ':R>' in blocked.text
        assert len(seen) == constants.XAI_USER_RATE_LIMIT * 2
        assert _event_count(db) == constants.XAI_USER_RATE_LIMIT

    @pytest.mark.parametrize('role', (
        constants.TLE_ADMIN, constants.TLE_MODERATOR,
    ))
    def test_privileged_roles_bypass_personal_but_not_shared_limit(
            self, role, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'XAI_USER_RATE_LIMIT', 1)
        monkeypatch.setattr(constants, 'XAI_DAILY_REQUEST_LIMIT', 3)
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)

        for _ in range(3):
            ctx = FakeCtx(roles=(role,))
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok again')
            assert 'Grok answer' in ctx.text

        blocked = FakeCtx(roles=(role,))
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok again')
        assert 'shared daily allowance is used up' in blocked.text
        assert len(seen) == 6
        assert _event_count(db) == 3

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
            lambda *args, **kwargs: XaiRequestDenial(
                'daily', 2_000_000_000))

        async def should_not_call(*args, **kwargs):
            raise AssertionError('a rejected @grok must not call xAI')

        monkeypatch.setattr(xai_api, 'complete', should_not_call)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message('@grok hello')))
        assert 'shared daily allowance is used up' in ctx.text
        assert '<t:2000000000:R>' in ctx.text


def test_gemini_keeps_its_existing_output_budget(cog, db, monkeypatch):
    db.llm_add_key('AIzaSyExampleKeyValue1234567')
    seen = _answers(monkeypatch)
    _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='hello')
    assert seen['kwargs']['max_output_tokens'] == \
        constants.LLM_MAX_OUTPUT_TOKENS

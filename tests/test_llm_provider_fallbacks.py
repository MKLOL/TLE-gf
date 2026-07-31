"""Focused tests for provider fallback, health, and process-only keys."""
from collections import namedtuple

import pytest

from tle.util import gemini_api, xai_api
from tle.util.llm_keypool import KeyPool
from tests.llm_test_utils import FakeClock, FakeLlmDb, run, text_response

EphemeralKey = namedtuple('EphemeralKey', 'id api_key label')


def _gemini_pool(models=('strong', 'weak'), keys=2, clock=None):
    db = FakeLlmDb()
    for index in range(keys):
        db.llm_add_key(f'AIzaSy-provider-key-{index:02d}-000000')
    return KeyPool(db, models, now_fn=clock or FakeClock()), db


def _xai_pool(models=('grok-strong', 'grok-weak'), keys=1, clock=None):
    db = FakeLlmDb()
    for index in range(keys):
        db.llm_add_key(f'xai-provider-key-{index:02d}-000000', provider='xai')
    return xai_api.XaiKeyPool(
        db, models, now_fn=clock or FakeClock()), db


def _xai_response(text='ok', model='grok-answer', usage=None):
    body = {
        'model': model,
        'choices': [{
            'message': {'role': 'assistant', 'content': text},
            'finish_reason': 'stop',
        }],
    }
    if usage is not None:
        body['usage'] = usage
    return body


class TestGeminiReliability:
    def test_model_scoped_permission_denial_is_not_a_bad_key(self):
        body = {'error': {
            'status': 'PERMISSION_DENIED',
            'message': 'Permission denied for model gemini-strong',
        }}
        assert gemini_api.is_model_unavailable_error(403, body)

    def test_timeout_is_transport_failure_and_rotates(self, monkeypatch):
        pool, _ = _gemini_pool(models=('strong',), keys=2)
        calls = []

        async def generate(api_key, model, payload, session=None):
            calls.append(api_key)
            if len(calls) == 1:
                raise TimeoutError('slow')
            return 200, text_response('recovered')

        monkeypatch.setattr(gemini_api, 'generate_once', generate)
        assert run(gemini_api.complete(pool, 'hello'))[0] == 'recovered'
        assert len(calls) == 2

    def test_explicit_weaker_model_fallback_skips_other_keys(
            self, monkeypatch):
        pool, _ = _gemini_pool()
        calls = []

        async def generate(api_key, model, payload, session=None):
            calls.append((api_key, model))
            if model == 'strong':
                return 404, {'error': {'message': 'model strong not found'}}
            return 200, text_response('weaker answer')

        monkeypatch.setattr(gemini_api, 'generate_once', generate)
        answer, lease = run(gemini_api.complete(
            pool, 'hello', models=['strong', 'weak']))
        assert answer == 'weaker answer'
        assert lease.model == 'weak'
        assert [model for _, model in calls] == ['strong', 'weak']

    def test_usage_is_added_to_shared_stats(self, monkeypatch):
        pool, _ = _gemini_pool(models=('strong',), keys=1)
        body = text_response('answer')
        body['usageMetadata'] = {
            'promptTokenCount': 11,
            'candidatesTokenCount': 7,
            'totalTokenCount': 18,
        }

        async def generate(*args, **kwargs):
            return 200, body

        monkeypatch.setattr(gemini_api, 'generate_once', generate)
        stats = {'attempts': 2, 'input_tokens': 3}
        run(gemini_api.complete(pool, 'hello', stats=stats))
        assert stats == {
            'attempts': 3, 'input_tokens': 14,
            'output_tokens': 7, 'total_tokens': 18,
        }

    def test_ephemeral_keys_survive_reload_and_are_never_persisted(self):
        clock = FakeClock()
        pool = KeyPool(FakeLlmDb(), ['strong'], now_fn=clock)
        pool.set_ephemeral_keys([
            EphemeralKey(-1, 'AIzaSy-env-only-key-000000', 'environment-1')])
        assert pool.key_count() == 1
        pool.reload()
        lease = run(pool.acquire())
        assert lease.key_id == -1
        pool.report_invalid(lease)
        pool.report_invalid(lease)
        assert pool.key_count() == 1
        assert pool.status()[0]['state'] == 'invalid environment key'
        pool.reset_health(key_id=-1)
        assert run(pool.acquire()).key_id == -1


class TestXaiReliability:
    def test_unknown_model_falls_back_without_trying_it_on_every_key(
            self, monkeypatch):
        pool, _ = _xai_pool(keys=2)
        calls = []

        async def generate(api_key, payload, session=None):
            calls.append((api_key, payload['model']))
            if payload['model'] == 'grok-strong':
                return 404, {'error': 'unknown model'}, {}
            return 200, _xai_response('fallback'), {}

        monkeypatch.setattr(xai_api, 'generate_once', generate)
        answer, _ = run(xai_api.complete(pool, 'hello'))
        assert answer == 'fallback'
        assert [model for _, model in calls] == [
            'grok-strong', 'grok-weak']

    def test_model_access_denial_can_fall_back_on_the_same_key(
            self, monkeypatch):
        pool, _ = _xai_pool()
        calls = []

        async def generate(api_key, payload, session=None):
            calls.append(payload['model'])
            if payload['model'] == 'grok-strong':
                return 403, {'error': 'no access to model grok-strong'}, {}
            return 200, _xai_response('accessible fallback'), {}

        monkeypatch.setattr(xai_api, 'generate_once', generate)
        assert run(xai_api.complete(pool, 'hello'))[0] == 'accessible fallback'
        assert calls == ['grok-strong', 'grok-weak']

    def test_billing_circuit_is_reversible_and_blocks_every_model(
            self, monkeypatch):
        pool, _ = _xai_pool()

        async def denied(api_key, payload, session=None):
            return 403, {'error': 'billing credits exhausted'}, {}

        monkeypatch.setattr(xai_api, 'generate_once', denied)
        with pytest.raises(xai_api.AccessDeniedError):
            run(xai_api.complete(pool, 'hello'))
        assert {row['state'] for row in pool.status()} == {
            'access/billing cooldown'}
        pool.reset_health()
        assert {row['state'] for row in pool.status()} == {'ready'}

    def test_retry_after_is_bounded_and_resettable(self):
        pool, _ = _xai_pool(models=('grok-strong',))
        lease = pool.leases()[0]
        applied = pool.report_rate_limit(lease, retry_after=999999)
        assert applied == 900
        assert pool.status()[0]['wait'] == 900
        assert pool.reset(key_id=lease.key_id, model=lease.model) > 0
        assert pool.status()[0]['state'] == 'ready'

    def test_authentication_benches_then_retires_persisted_key(
            self, monkeypatch):
        clock = FakeClock()
        pool, db = _xai_pool(models=('grok-strong',), clock=clock)

        async def rejected(api_key, payload, session=None):
            return 401, {'error': 'bad token'}, {}

        monkeypatch.setattr(xai_api, 'generate_once', rejected)
        with pytest.raises(xai_api.AuthenticationError):
            run(xai_api.complete(pool, 'first'))
        assert pool.key_count() == 1
        clock.advance(601)
        with pytest.raises(xai_api.AuthenticationError):
            run(xai_api.complete(pool, 'second'))
        assert pool.key_count() == 0
        assert db.llm_get_keys(provider='xai') == []

    def test_ephemeral_auth_failure_never_deactivates_database_row(self):
        clock = FakeClock()
        pool = xai_api.XaiKeyPool(
            FakeLlmDb(), ['grok-strong'], now_fn=clock,
            ephemeral_keys=[EphemeralKey(
                -10, 'xai-env-only-key-000000', 'environment-1')])
        lease = pool.leases()[0]
        pool.report_invalid(lease)
        pool.report_invalid(lease)
        pool.reload()
        assert pool.key_count() == 1
        assert pool.status()[0]['state'] == 'invalid environment key'

    def test_usage_is_added_to_shared_stats(self, monkeypatch):
        pool, _ = _xai_pool(models=('grok-strong',))
        body = _xai_response(usage={
            'prompt_tokens': 13, 'completion_tokens': 5,
            'total_tokens': 18,
        })

        async def generate(*args, **kwargs):
            return 200, body, {}

        monkeypatch.setattr(xai_api, 'generate_once', generate)
        stats = {'attempts': 4, 'total_tokens': 2}
        run(xai_api.complete(pool, 'hello', stats=stats))
        assert stats == {
            'attempts': 5, 'input_tokens': 13,
            'output_tokens': 5, 'total_tokens': 20,
        }

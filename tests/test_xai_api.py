"""Tests for the native xAI Responses API client."""
import base64

import pytest

from tle.util import xai_api
from tests.llm_test_utils import FakeLlmDb, run


def _response(text='ok', model='grok-test', status='completed',
              incomplete_reason=None):
    body = {
        'model': model,
        'status': status,
        'output': [{
            'type': 'message',
            'role': 'assistant',
            'content': ([{'type': 'output_text', 'text': text}]
                        if text is not None else []),
        }],
    }
    if incomplete_reason is not None:
        body['incomplete_details'] = {'reason': incomplete_reason}
    return body


@pytest.fixture
def pool():
    database = FakeLlmDb()
    database.llm_add_key('xai-key-number-one-000000', label='one',
                         provider='xai')
    database.llm_add_key('xai-key-number-two-000000', label='two',
                         provider='xai')
    return xai_api.XaiKeyPool(database, 'grok-configured')


def _responder(monkeypatch, responses):
    """Script generate_once outcomes and record every attempted key/payload."""
    queue = list(responses)
    calls = []

    async def fake_generate_once(api_key, payload, session=None):
        calls.append({'api_key': api_key, 'payload': payload,
                      'session': session})
        outcome = queue.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        if len(outcome) == 2:
            status, body = outcome
            return status, body, {}
        return outcome

    monkeypatch.setattr(xai_api, 'generate_once', fake_generate_once)
    return calls


class TestBuildUserContent:
    def test_text_only_stays_a_plain_string(self):
        assert xai_api.build_user_content('hello') == 'hello'

    def test_supported_images_are_data_urls_before_text(self):
        content = xai_api.build_user_content('inspect these', [
            ('image/png', b'PNG'),
            ('image/jpeg; charset=binary', b'JPEG'),
            ('image/jpg', b'JPG'),
        ])

        assert [part['type'] for part in content] == [
            'input_image', 'input_image', 'input_image', 'input_text']
        first_url = content[0]['image_url']
        second_url = content[1]['image_url']
        assert first_url.startswith('data:image/png;base64,')
        assert second_url.startswith('data:image/jpeg;base64,')
        assert base64.b64decode(first_url.partition(',')[2]) == b'PNG'
        assert base64.b64decode(second_url.partition(',')[2]) == b'JPEG'
        assert content[2]['image_url'].startswith('data:image/jpeg;base64,')
        assert content[0]['detail'] == 'high'
        assert content[-1] == {
            'type': 'input_text', 'text': 'inspect these'}

    @pytest.mark.parametrize('mime', [
        'image/webp', 'image/heic', 'image/gif', None, 'text/plain',
    ])
    def test_unsupported_images_are_filtered(self, mime):
        assert xai_api.build_user_content(
            'text survives', [(mime, b'ignored')]) == 'text survives'

    def test_supported_and_unsupported_images_can_be_mixed(self):
        content = xai_api.build_user_content('question', [
            ('image/webp', b'no'), ('IMAGE/PNG', b'yes')])
        assert len(content) == 2
        assert content[0]['image_url'].startswith('data:image/png;base64,')


class TestBuildPayload:
    def test_minimal_payload(self):
        assert xai_api.build_payload('grok-a', 'hello') == {
            'model': 'grok-a',
            'input': [{'role': 'user', 'content': 'hello'}],
            'stream': False,
            'store': False,
        }

    def test_system_and_generation_options(self):
        payload = xai_api.build_payload(
            'grok-a', 'hello', system_instruction='be sharp',
            max_output_tokens=321, temperature=0.25,
            reasoning_effort='low')
        assert payload['input'][0] == {
            'role': 'system', 'content': 'be sharp'}
        assert payload['input'][1] == {
            'role': 'user', 'content': 'hello'}
        assert payload['max_output_tokens'] == 321
        assert payload['temperature'] == 0.25
        assert payload['reasoning'] == {'effort': 'low'}


class TestExtractText:
    def test_returns_text_and_actual_response_model(self):
        assert xai_api.extract_text(
            _response('answer', model='grok-actual')) == (
                'answer', 'grok-actual')

    def test_joins_typed_text_parts(self):
        body = _response()
        body['output'][0]['content'] = [
            {'type': 'output_text', 'text': 'part one'},
            {'type': 'input_image', 'text': 'ignored'},
            {'type': 'output_text', 'text': ' and two'},
        ]
        assert xai_api.extract_text(body)[0] == 'part one and two'

    def test_length_finish_marks_the_answer_truncated(self):
        answer, _ = xai_api.extract_text(
            _response('partial', status='incomplete',
                      incomplete_reason='max_output_tokens'))
        assert answer.startswith('partial')
        assert 'truncated' in answer

    def test_missing_output_raises(self):
        with pytest.raises(xai_api.XaiError, match='empty answer'):
            xai_api.extract_text({'output': [], 'status': 'completed'})

    def test_empty_content_raises(self):
        with pytest.raises(xai_api.XaiError, match='empty answer'):
            xai_api.extract_text(_response('   '))

    def test_refusal_raises_blocked_with_bounded_text(self):
        body = _response(None)
        body['output'][0]['content'] = [{
            'type': 'refusal', 'refusal': 'no ' * 1000}]
        with pytest.raises(xai_api.BlockedError) as excinfo:
            xai_api.extract_text(body)
        assert len(str(excinfo.value)) < 500

    def test_content_filter_finish_raises_blocked(self):
        with pytest.raises(xai_api.BlockedError, match='safety'):
            xai_api.extract_text(_response(
                '', status='incomplete', incomplete_reason='content_filter'))

    def test_failed_response_surfaces_bounded_provider_error(self):
        with pytest.raises(xai_api.XaiError, match='overloaded'):
            xai_api.extract_text({
                'status': 'failed', 'output': [],
                'error': {'message': 'overloaded'},
            })


class TestErrorText:
    @pytest.mark.parametrize('payload,expected', [
        ({'error': {'message': 'nested'}}, 'nested'),
        ({'error': 'plain'}, 'plain'),
        ({'message': 'top level'}, 'top level'),
        ({'detail': 'detail text'}, 'detail text'),
    ])
    def test_known_error_shapes(self, payload, expected):
        assert xai_api.error_message(payload) == expected

    def test_whitespace_is_collapsed_and_long_text_clamped(self):
        text = xai_api.truncate_error(('a\n  b ' * 1000))
        assert '\n' not in text
        assert len(text) <= xai_api._MAX_ERROR_CHARS


class TestKeyPool:
    def test_gemini_keys_never_enter_the_xai_pool(self):
        database = FakeLlmDb()
        database.llm_add_key('gemini-only-key-0000000', provider='gemini')
        pool = xai_api.XaiKeyPool(database, 'grok-a')
        assert pool.key_count() == 0

    def test_calls_start_on_successive_keys(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (200, _response()), (200, _response()),
        ])
        run(xai_api.complete(pool, 'first'))
        run(xai_api.complete(pool, 'second'))
        assert calls[0]['api_key'] != calls[1]['api_key']

    def test_one_invocation_never_retries_the_same_key(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (500, {'error': 'down'}), (500, {'error': 'down'}),
        ])
        with pytest.raises(xai_api.ServiceUnavailableError):
            run(xai_api.complete(pool, 'hello'))
        assert len({call['api_key'] for call in calls}) == 2


class TestComplete:
    def test_success_builds_payload_and_returns_lease(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [(200, _response('answer'))])
        answer, lease = run(xai_api.complete(
            pool, 'question', system_instruction='system',
            max_output_tokens=99, temperature=0.1,
            reasoning_effort='low'))
        assert answer == 'answer'
        assert lease.model == 'grok-test'
        payload = calls[0]['payload']
        assert payload['model'] == 'grok-configured'
        assert payload['input'][0]['content'] == 'system'
        assert payload['max_output_tokens'] == 99

    def test_response_model_replaces_configured_model_in_lease(
            self, pool, monkeypatch):
        _responder(monkeypatch, [
            (200, _response(model='grok-resolved-2026'))])
        _, lease = run(xai_api.complete(pool, 'hello'))
        assert lease.model == 'grok-resolved-2026'

    def test_401_rotates_then_succeeds(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (401, {'error': 'bad token'}),
            (200, _response('second answered')),
        ])
        answer, _ = run(xai_api.complete(pool, 'hello'))
        assert answer == 'second answered'
        assert calls[0]['api_key'] != calls[1]['api_key']

    def test_all_401_raises_authentication(self, pool, monkeypatch):
        _responder(monkeypatch, [
            (401, {'error': 'bad one'}), (401, {'error': 'bad two'}),
        ])
        with pytest.raises(xai_api.AuthenticationError, match='bad two'):
            run(xai_api.complete(pool, 'hello'))

    def test_bad_key_reported_as_400_rotates(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (400, {'error': 'Incorrect API key supplied'}),
            (200, _response('second answered')),
        ])
        assert run(xai_api.complete(pool, 'hello'))[0] == 'second answered'
        assert len(calls) == 2

    def test_all_403_raises_access_denied(self, pool, monkeypatch):
        _responder(monkeypatch, [
            (403, {'error': 'team blocked'}),
            (403, {'error': {'message': 'no credits'}}),
        ])
        with pytest.raises(xai_api.AccessDeniedError, match='no credits'):
            run(xai_api.complete(pool, 'hello'))

    @pytest.mark.parametrize('responses', [
        [(403, {'error': 'no credits'}), (401, {'error': 'bad key'})],
        [(429, {'error': 'slow down'}), (401, {'error': 'bad key'})],
    ])
    def test_mixed_key_failures_do_not_blame_every_key(
            self, pool, monkeypatch, responses):
        _responder(monkeypatch, responses)
        with pytest.raises(xai_api.NoCapacityError):
            run(xai_api.complete(pool, 'hello'))

    def test_all_429_raises_rate_limit_with_header_hint(
            self, pool, monkeypatch):
        _responder(monkeypatch, [
            (429, {'error': 'slow down'}, {'Retry-After': '7.5'}),
            (429, {'error': 'still limited'}, {'retry-after': '3'}),
        ])
        with pytest.raises(xai_api.RateLimitError) as excinfo:
            run(xai_api.complete(pool, 'hello'))
        assert excinfo.value.retry_after == 3

    def test_429_can_read_body_retry_hint(self, pool, monkeypatch):
        _responder(monkeypatch, [
            (429, {'error': 'limited', 'retry_after': 12}, {}),
            (429, {'error': 'limited', 'retry_after': 12}, {}),
        ])
        with pytest.raises(xai_api.RateLimitError) as excinfo:
            run(xai_api.complete(pool, 'hello'))
        assert excinfo.value.retry_after == 12

    def test_404_fails_fast_without_trying_another_key(
            self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (404, {'error': {'message': 'unknown model'}}),
        ])
        with pytest.raises(xai_api.ModelUnavailableError, match='unknown model'):
            run(xai_api.complete(pool, 'hello'))
        assert len(calls) == 1

    def test_5xx_rotates_then_succeeds(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (503, {'error': 'overloaded'}),
            (200, _response('recovered')),
        ])
        assert run(xai_api.complete(pool, 'hello'))[0] == 'recovered'
        assert len(calls) == 2

    def test_transport_error_rotates_then_succeeds(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            xai_api._AIOHTTP_CLIENT_ERROR('connection reset'),
            (200, _response('recovered')),
        ])
        assert run(xai_api.complete(pool, 'hello'))[0] == 'recovered'
        assert len(calls) == 2

    def test_non_retryable_4xx_fails_fast(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (422, {'error': {'message': 'bad payload'}}),
        ])
        with pytest.raises(xai_api.XaiError, match='bad payload'):
            run(xai_api.complete(pool, 'hello'))
        assert len(calls) == 1

    def test_no_keys_raises_even_when_gemini_has_a_key(self, monkeypatch):
        database = FakeLlmDb()
        database.llm_add_key('gemini-key-value-000000', provider='gemini')
        empty = xai_api.XaiKeyPool(database, 'grok-a')
        calls = _responder(monkeypatch, [])
        with pytest.raises(xai_api.NoKeysError):
            run(xai_api.complete(empty, 'hello'))
        assert calls == []


class TestAttemptAccounting:
    def test_success_counts_every_request_put_on_the_wire(
            self, pool, monkeypatch):
        _responder(monkeypatch, [
            (503, {'error': 'overloaded'}), (200, _response()),
        ])
        stats = {}
        run(xai_api.complete(pool, 'hello', stats=stats))
        assert stats['attempts'] == 2

    def test_failure_still_records_attempts(self, pool, monkeypatch):
        _responder(monkeypatch, [
            (403, {'error': 'denied'}), (403, {'error': 'denied'}),
        ])
        stats = {}
        with pytest.raises(xai_api.AccessDeniedError):
            run(xai_api.complete(pool, 'hello', stats=stats))
        assert stats['attempts'] == 2

    def test_max_attempts_limits_the_walk(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (503, {'error': 'overloaded'}),
        ])
        stats = {}
        with pytest.raises(xai_api.ServiceUnavailableError):
            run(xai_api.complete(pool, 'hello', max_attempts=1, stats=stats))
        assert len(calls) == 1
        assert stats['attempts'] == 1


class TestGenerateOnce:
    def test_posts_to_responses_with_bearer_auth(self):
        class Response:
            status = 200
            headers = {'x-test': 'yes'}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def json(self, content_type=None):
                return _response()

        class Session:
            def __init__(self):
                self.call = None

            def post(self, url, **kwargs):
                self.call = (url, kwargs)
                return Response()

        session = Session()
        payload = xai_api.build_payload('grok-a', 'hello')
        status, body, headers = run(xai_api.generate_once(
            'secret-value', payload, session=session))
        url, kwargs = session.call
        assert url == 'https://api.x.ai/v1/responses'
        assert kwargs['headers']['Authorization'] == 'Bearer secret-value'
        assert kwargs['headers']['Content-Type'] == 'application/json'
        assert kwargs['json'] is payload
        assert status == 200 and body['model'] == 'grok-test'
        assert headers == {'x-test': 'yes'}

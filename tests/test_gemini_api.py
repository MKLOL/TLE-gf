"""Tests for the Gemini client (``tle/util/gemini_api.py``).

Payload shaping and response parsing are exercised directly; ``complete`` is
tested against a real ``KeyPool`` with ``generate_once`` swapped out, so the
rotation-on-failure behaviour is covered without a socket.
"""
import base64

import pytest

from tle.util import gemini_api
from tle.util.llm_keypool import KeyPool
from tests.llm_test_utils import (FakeClock, FakeLlmDb, quota_error, run,
                                  text_response)


class TestBuildParts:
    def test_text_only(self):
        assert gemini_api.build_parts('hello') == [{'text': 'hello'}]

    def test_images_precede_the_text(self):
        parts = gemini_api.build_parts('what is this', [('image/png', b'RAW')])
        assert list(parts[0]) == ['inline_data']
        assert parts[1] == {'text': 'what is this'}

    def test_image_bytes_are_base64_encoded(self):
        parts = gemini_api.build_parts(None, [('image/jpeg', b'RAW')])
        inline = parts[0]['inline_data']
        assert inline['mime_type'] == 'image/jpeg'
        assert base64.b64decode(inline['data']) == b'RAW'

    def test_no_prompt_and_no_images_is_empty(self):
        assert gemini_api.build_parts(None) == []


class TestBuildPayload:
    def test_minimal_payload_has_only_contents(self):
        payload = gemini_api.build_payload([{'text': 'hi'}])
        assert payload == {'contents': [{'role': 'user', 'parts': [{'text': 'hi'}]}]}

    def test_system_instruction_and_generation_config(self):
        payload = gemini_api.build_payload(
            [{'text': 'hi'}], system_instruction='be brief',
            max_output_tokens=100, temperature=0.5)
        assert payload['systemInstruction'] == {'parts': [{'text': 'be brief'}]}
        assert payload['generationConfig'] == {'maxOutputTokens': 100,
                                               'temperature': 0.5}


class TestExtractText:
    def test_returns_the_answer(self):
        assert gemini_api.extract_text(text_response('42')) == '42'

    def test_joins_multiple_parts(self):
        payload = {'candidates': [{'content': {'parts': [{'text': 'a'},
                                                         {'text': 'b'}]}}]}
        assert gemini_api.extract_text(payload) == 'ab'

    def test_blocked_prompt_raises_blocked(self):
        with pytest.raises(gemini_api.BlockedError):
            gemini_api.extract_text({'promptFeedback': {'blockReason': 'SAFETY'}})

    def test_safety_finish_reason_raises_blocked(self):
        payload = {'candidates': [{'content': {'parts': []},
                                   'finishReason': 'SAFETY'}]}
        with pytest.raises(gemini_api.BlockedError):
            gemini_api.extract_text(payload)

    def test_no_candidates_raises(self):
        with pytest.raises(gemini_api.GeminiError):
            gemini_api.extract_text({'candidates': []})

    def test_empty_answer_raises_a_plain_error_not_blocked(self):
        payload = {'candidates': [{'content': {'parts': [{'text': '  '}]},
                                   'finishReason': 'STOP'}]}
        with pytest.raises(gemini_api.GeminiError) as excinfo:
            gemini_api.extract_text(payload)
        assert not isinstance(excinfo.value, gemini_api.BlockedError)

    def test_truncated_answer_is_marked(self):
        text = gemini_api.extract_text(text_response('partial', 'MAX_TOKENS'))
        assert text.startswith('partial')
        assert 'truncated' in text


class TestIsInvalidKeyError:
    @pytest.mark.parametrize('status,body', [
        (400, {'error': {'status': 'INVALID_ARGUMENT',
                         'message': 'API key not valid. Pass a valid API key.'}}),
        (403, {'error': {'status': 'PERMISSION_DENIED', 'message': 'denied'}}),
        (401, {'error': {'status': 'UNAUTHENTICATED', 'message': 'no creds'}}),
        (403, {'error': {'status': 'PERMISSION_DENIED',
                         'message': 'CONSUMER_SUSPENDED'}}),
    ])
    def test_key_problems_are_detected(self, status, body):
        assert gemini_api.is_invalid_key_error(status, body) is True

    @pytest.mark.parametrize('status,body', [
        (429, quota_error('GenerateRequestsPerDay')),
        (400, {'error': {'status': 'INVALID_ARGUMENT',
                         'message': 'contents is required'}}),
        (500, {'error': {'message': 'internal'}}),
    ])
    def test_other_failures_are_not_key_problems(self, status, body):
        assert gemini_api.is_invalid_key_error(status, body) is False


# ── complete(): rotation across buckets ─────────────────────────────────

@pytest.fixture
def pool():
    db = FakeLlmDb()
    db.llm_add_key('AIzaSyKeyNumberOne000000', label='proj-a')
    db.llm_add_key('AIzaSyKeyNumberTwo000000', label='proj-b')
    return KeyPool(db, ['model-a', 'model-b'], now_fn=FakeClock())


def _responder(monkeypatch, responses):
    """Swap generate_once for a scripted sequence, recording each attempt."""
    calls = []
    queue = list(responses)

    async def fake_generate_once(api_key, model, payload, session=None):
        calls.append({'api_key': api_key, 'model': model, 'payload': payload})
        return queue.pop(0) if queue else (200, text_response('fallback'))

    monkeypatch.setattr(gemini_api, 'generate_once', fake_generate_once)
    return calls


class TestComplete:
    def test_success_on_the_first_bucket(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [(200, text_response('the answer'))])
        answer, lease = run(gemini_api.complete(pool, 'question'))
        assert answer == 'the answer'
        assert lease.model == 'model-a'
        assert len(calls) == 1

    def test_prompt_and_system_instruction_reach_the_request(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [(200, text_response('ok'))])
        run(gemini_api.complete(pool, 'why', system_instruction='be brief'))
        payload = calls[0]['payload']
        assert payload['contents'][0]['parts'] == [{'text': 'why'}]
        assert payload['systemInstruction'] == {'parts': [{'text': 'be brief'}]}

    def test_daily_quota_rotates_to_the_other_key(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (429, quota_error('GenerateRequestsPerDayPerProjectPerModel')),
            (200, text_response('second key answered')),
        ])
        answer, _ = run(gemini_api.complete(pool, 'question'))
        assert answer == 'second key answered'
        assert calls[0]['api_key'] != calls[1]['api_key']
        assert calls[1]['model'] == 'model-a'  # same model, different key

    def test_model_exhausted_everywhere_falls_back_to_the_next_model(
            self, pool, monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        calls = _responder(monkeypatch, [
            daily, daily, (200, text_response('fallback model answered')),
        ])
        answer, lease = run(gemini_api.complete(pool, 'question'))
        assert answer == 'fallback model answered'
        assert lease.model == 'model-b'
        assert [c['model'] for c in calls] == ['model-a', 'model-a', 'model-b']

    def test_everything_spent_raises_no_capacity_with_a_hint(self, pool, monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        _responder(monkeypatch, [daily] * 4)
        with pytest.raises(gemini_api.NoCapacityError) as excinfo:
            run(gemini_api.complete(pool, 'question'))
        assert excinfo.value.retry_after > 0

    def test_rejected_key_is_benched_and_the_next_one_is_tried(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (400, {'error': {'status': 'INVALID_ARGUMENT',
                             'message': 'API key not valid'}}),
            (200, text_response('survivor answered')),
        ])
        answer, _ = run(gemini_api.complete(pool, 'question'))
        assert answer == 'survivor answered'
        assert calls[0]['api_key'] != calls[1]['api_key']
        # Benched, not retired — one rejection may be a transient blip.
        assert pool.key_count() == 2

    def test_a_second_rejection_on_a_later_call_retires_the_key(self, monkeypatch):
        clock = FakeClock()
        db = FakeLlmDb()
        db.llm_add_key('AIzaSyOnlyKeyInThePool000')
        solo = KeyPool(db, ['model-a'], now_fn=clock)
        rejection = (400, {'error': {'status': 'INVALID_ARGUMENT',
                                     'message': 'API key not valid'}})

        _responder(monkeypatch, [rejection])
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(solo, 'first'))
        assert solo.key_count() == 1  # benched, still in the pool

        clock.advance(700)  # past the bench cooldown
        _responder(monkeypatch, [rejection])
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(solo, 'second'))
        assert solo.key_count() == 0
        assert db.llm_get_keys() == []

    def test_server_error_retries_on_another_bucket(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (503, {'error': {'message': 'overloaded'}}),
            (200, text_response('retried fine')),
        ])
        answer, _ = run(gemini_api.complete(pool, 'question'))
        assert answer == 'retried fine'
        assert len(calls) == 2

    def test_transport_failure_retries_on_another_bucket(self, pool, monkeypatch):
        attempts = []

        async def flaky(api_key, model, payload, session=None):
            attempts.append(api_key)
            if len(attempts) == 1:
                raise gemini_api._AIOHTTP_CLIENT_ERROR('connection reset')
            return 200, text_response('recovered')

        monkeypatch.setattr(gemini_api, 'generate_once', flaky)
        answer, _ = run(gemini_api.complete(pool, 'question'))
        assert answer == 'recovered'
        assert len(attempts) == 2

    def test_an_unknown_model_is_dropped_not_retried_across_keys(
            self, pool, monkeypatch):
        # Every key fails a bad model id identically, so one 404 is enough to
        # retire that rung — but the next rung still gets its turn.
        calls = _responder(monkeypatch, [
            (404, {'error': {'message': 'models/model-a is not found'}}),
            (200, text_response('from the fallback')),
        ])
        answer, _ = run(gemini_api.complete(pool, 'question'))
        assert answer == 'from the fallback'
        assert [call['model'] for call in calls] == ['model-a', 'model-b']

    def test_one_model_selector_fails_fast_without_burning_the_pool(
            self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (404, {'error': {'message': 'models/model-a is not found'}}),
        ])
        with pytest.raises(gemini_api.ModelUnavailableError):
            run(gemini_api.complete(pool, 'question', models=['model-a']))
        assert len(calls) == 1

    def test_a_ladder_of_unknown_models_fails_loudly(self, pool, monkeypatch):
        # LLM_MODELS is simply wrong; only a moderator can fix that, so it
        # must not be reported as a passing quota problem.
        calls = _responder(monkeypatch, [
            (404, {'error': {'message': 'models/model-a is not found'}}),
            (404, {'error': {'message': 'models/model-b is not found'}}),
        ])
        with pytest.raises(gemini_api.ModelUnavailableError):
            run(gemini_api.complete(pool, 'question'))
        assert len(calls) == 2  # one probe per model, not per bucket

    def test_malformed_request_is_not_retried_across_keys(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (400, {'error': {'status': 'INVALID_ARGUMENT',
                             'message': 'contents is required'}}),
        ])
        with pytest.raises(gemini_api.GeminiError):
            run(gemini_api.complete(pool, 'question'))
        assert len(calls) == 1

    def test_no_keys_raises_no_keys_error(self, monkeypatch):
        empty = KeyPool(FakeLlmDb(), ['model-a'], now_fn=FakeClock())
        _responder(monkeypatch, [])
        with pytest.raises(gemini_api.NoKeysError):
            run(gemini_api.complete(empty, 'question'))


class TestTruncateError:
    def test_short_text_is_unchanged(self):
        assert gemini_api.truncate_error('boom') == 'boom'

    def test_whitespace_is_collapsed(self):
        assert gemini_api.truncate_error('a\n\n   b') == 'a b'

    def test_long_text_is_clamped(self):
        clamped = gemini_api.truncate_error('x' * 5000)
        assert len(clamped) <= gemini_api._MAX_ERROR_CHARS

    def test_empty_is_empty(self):
        assert gemini_api.truncate_error(None) == ''

    def test_a_google_html_error_page_cannot_overflow_an_embed(self, pool,
                                                               monkeypatch):
        # A non-JSON 4xx hands back the whole error page; unbounded, that blows
        # the 4096-character embed limit and the user sees nothing at all.
        html = '<html>' + ('padding ' * 5000) + '</html>'
        _responder(monkeypatch, [(418, {'error': {'message': html}})])
        with pytest.raises(gemini_api.GeminiError) as excinfo:
            run(gemini_api.complete(pool, 'question'))
        assert len(str(excinfo.value)) <= gemini_api._MAX_ERROR_CHARS


class TestAttemptAccounting:
    def test_stats_records_a_successful_attempt(self, pool, monkeypatch):
        _responder(monkeypatch, [(200, text_response('ok'))])
        stats = {}
        run(gemini_api.complete(pool, 'question', stats=stats))
        assert stats['attempts'] == 1

    def test_stats_counts_every_upstream_request(self, pool, monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        _responder(monkeypatch, [daily, daily, (200, text_response('ok'))])
        stats = {}
        run(gemini_api.complete(pool, 'question', stats=stats))
        assert stats['attempts'] == 3

    def test_stats_is_filled_in_even_when_the_call_fails(self, pool, monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        _responder(monkeypatch, [daily] * 6)
        stats = {}
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(pool, 'question', stats=stats))
        assert stats['attempts'] == 4  # 2 keys x 2 models, then acquire() dries up

    def test_no_attempts_recorded_when_the_pool_is_already_dry(self, pool,
                                                              monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        _responder(monkeypatch, [daily] * 6)
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(pool, 'first'))
        stats = {}
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(pool, 'second', stats=stats))
        assert stats['attempts'] == 0


class TestGivingUp:
    def test_a_drained_pool_reports_a_real_wait(self, pool, monkeypatch):
        daily = (429, quota_error('GenerateRequestsPerDayPerProjectPerModel'))
        _responder(monkeypatch, [daily] * 6)
        with pytest.raises(gemini_api.NoCapacityError) as excinfo:
            run(gemini_api.complete(pool, 'question'))
        assert excinfo.value.attempts_exhausted is False
        assert excinfo.value.retry_after > 0

    def test_a_pool_larger_than_the_ceiling_reports_attempts_exhausted(
            self, monkeypatch):
        # 6 keys x 3 models = 18 buckets, more than complete() will walk. The
        # untried buckets may be perfectly healthy, so quoting a wait derived
        # from the blocked ones — or "unknown" — would be a lie.
        db = FakeLlmDb()
        for index in range(6):
            db.llm_add_key(f'AIzaSyKeyNumber{index}00000000')
        big = KeyPool(db, ['model-a', 'model-b', 'model-c'], now_fn=FakeClock())
        calls = _responder(
            monkeypatch, [(503, {'error': {'message': 'overloaded'}})] * 50)
        with pytest.raises(gemini_api.NoCapacityError) as excinfo:
            run(gemini_api.complete(big, 'question'))
        assert len(calls) == gemini_api._MAX_ATTEMPTS_CEILING
        assert excinfo.value.attempts_exhausted is True
        assert excinfo.value.retry_after is None


class TestToolFallback:
    """A tool one model family accepts can be a 400 on an older fallback.

    Before this, a plain 400 was treated as a malformed request and stopped
    the loop outright — one unsupported tool turned every ``;llm`` into a hard
    failure instead of an answer without URL reading.
    """

    _REJECTION = (400, {'error': {
        'status': 'INVALID_ARGUMENT',
        'message': 'Tool use with function calling is unsupported'}})

    def test_a_rejected_tool_is_dropped_and_the_call_retried(self, pool,
                                                             monkeypatch):
        calls = _responder(monkeypatch,
                           [self._REJECTION, (200, text_response('answer'))])
        answer, _ = run(gemini_api.complete(pool, 'question',
                                            tools=[{'url_context': {}}]))
        assert answer == 'answer'
        assert 'tools' in calls[0]['payload']
        assert 'tools' not in calls[1]['payload']

    def test_tool_rejection_wins_over_model_unavailable_classification(
            self, pool, monkeypatch):
        rejection = (400, {'error': {
            'status': 'INVALID_ARGUMENT',
            'message': 'Model model-a does not support the url_context tool',
        }})
        calls = _responder(monkeypatch,
                           [rejection, (200, text_response('answer'))])
        answer, lease = run(gemini_api.complete(
            pool, 'question', tools=[{'url_context': {}}]))
        assert answer == 'answer' and lease.model == 'model-a'
        assert [call['model'] for call in calls] == ['model-a', 'model-a']

    def test_the_rejected_bucket_is_not_burned(self, monkeypatch):
        # A one-bucket pool has nothing to fall back to, so the retry has to
        # reuse the bucket the tool was rejected on. Excluding it — the normal
        # treatment for a failed attempt — would drain the pool instead.
        db = FakeLlmDb()
        db.llm_add_key('AIzaSyOnlyKeyInThePool000')
        solo = KeyPool(db, ['model-a'], now_fn=FakeClock())
        calls = _responder(monkeypatch,
                           [self._REJECTION, (200, text_response('answer'))])
        answer, _ = run(gemini_api.complete(solo, 'question',
                                            tools=[{'url_context': {}}]))
        assert answer == 'answer'
        assert [call['model'] for call in calls] == ['model-a', 'model-a']

    def test_tools_stay_off_for_the_rest_of_the_call(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            self._REJECTION,
            (503, {'error': {'message': 'overloaded'}}),
            (200, text_response('answer')),
        ])
        run(gemini_api.complete(pool, 'question', tools=[{'url_context': {}}]))
        assert all('tools' not in call['payload'] for call in calls[1:])

    def test_a_real_bad_request_still_fails_fast(self, pool, monkeypatch):
        calls = _responder(monkeypatch, [
            (400, {'error': {'status': 'INVALID_ARGUMENT',
                             'message': 'contents is required'}})])
        with pytest.raises(gemini_api.GeminiError):
            run(gemini_api.complete(pool, 'question',
                                    tools=[{'url_context': {}}]))
        assert len(calls) == 1


class TestBucketsAreNotRetried:
    def test_a_failing_bucket_is_not_leased_twice(self, pool, monkeypatch):
        # report_transient leaves the bucket unblocked, so without exclude the
        # loop could lease the same one again and burn the attempt ceiling on
        # a single sick bucket instead of walking the pool.
        calls = _responder(
            monkeypatch, [(503, {'error': {'message': 'overloaded'}})] * 10)
        with pytest.raises(gemini_api.NoCapacityError):
            run(gemini_api.complete(pool, 'question'))
        seen = [(call['api_key'], call['model']) for call in calls]
        assert len(seen) == len(set(seen)) == 4  # 2 keys x 2 models

"""Thin async client for the Gemini generative API.

Talks to the **native** REST endpoint rather than Google's OpenAI-compatibility
shim. The shim is convenient if you already have OpenAI code, but it flattens
the structured error details that ``llm_keypool`` needs in order to tell a
per-minute 429 from a per-day one, and it makes image input more awkward.
Neither trade is worth making here.

Network I/O lives in ``generate_once``; payload shaping and response parsing
are pure functions so they can be tested without a key or a socket. The
retry-across-buckets loop is ``complete``.
"""
import asyncio
import base64
import logging

import aiohttp

from tle.util._gemini_reliability import (
    is_invalid_key_error, is_model_unavailable_error,
    is_tool_unsupported_error, model_ladder as _model_ladder,
    record_stats as _record_stats,
)

logger = logging.getLogger(__name__)

_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)
_TRANSPORT_ERRORS = (_AIOHTTP_CLIENT_ERROR, TimeoutError)

BASE_URL = 'https://generativelanguage.googleapis.com/v1beta'
_REQUEST_TIMEOUT = 45
# A non-JSON failure hands back whatever the edge returned — routinely a full
# Google HTML error page. Unbounded, that string reaches an embed description
# and blows the 4096-character limit, so the user sees nothing at all.
_MAX_ERROR_CHARS = 400
# Ceiling on buckets tried per call, so one command cannot walk a large pool.
_MAX_ATTEMPTS_CEILING = 12
_MIN_ATTEMPTS = 4


class GeminiError(Exception):
    """Base class for every failure this module reports."""


class NoCapacityError(GeminiError):
    """No bucket produced an answer.

    ``attempts_exhausted`` distinguishes the two ways that happens: the pool
    genuinely had nothing available (``False`` — ``retry_after`` says when it
    will), or the per-call attempt ceiling was hit while healthy buckets
    remained untried (``True`` — retrying immediately is reasonable). Reporting
    the second as the first tells users to wait for a quota that is not
    actually spent.
    """

    def __init__(self, message, retry_after=None, attempts_exhausted=False):
        super().__init__(message)
        self.retry_after = retry_after
        self.attempts_exhausted = attempts_exhausted


class NoKeysError(GeminiError):
    """No API keys are configured at all."""


class BlockedError(GeminiError):
    """The request or response was withheld by Google's safety filters."""


class ModelUnavailableError(GeminiError):
    """The configured model id does not exist or is not accessible."""


class EmptyOutputBudgetError(GeminiError):
    """The output budget ran out before any visible text was produced.

    On a thinking model, reasoning tokens come out of ``maxOutputTokens``, so a
    budget that looks generous for an answer can be consumed entirely by
    reasoning — yielding a 200 response with no text.
    """


# ── Pure payload helpers ────────────────────────────────────────────────

def build_parts(prompt, images=None):
    """Build the ``parts`` array for one user turn.

    ``images`` is an iterable of ``(mime_type, raw_bytes)``. Gemini wants
    inline binary base64-encoded.
    """
    parts = []
    for mime_type, raw in images or []:
        parts.append({'inline_data': {
            'mime_type': mime_type,
            'data': base64.b64encode(raw).decode('ascii'),
        }})
    if prompt:
        parts.append({'text': prompt})
    return parts


def build_payload(parts, system_instruction=None, max_output_tokens=None,
                  temperature=None, thinking=None, response_mime_type=None,
                  response_schema=None, tools=None):
    """Assemble a ``generateContent`` request body.

    ``thinking`` is the encoded reasoning config from ``llm_models`` — either
    ``{'thinkingLevel': ...}`` or ``{'thinkingBudget': 0}`` — and is nested
    under ``generationConfig.thinkingConfig``.
    """
    payload = {'contents': [{'role': 'user', 'parts': parts}]}
    if tools:
        payload['tools'] = [dict(tool) for tool in tools]
    if system_instruction:
        payload['systemInstruction'] = {'parts': [{'text': system_instruction}]}
    generation_config = {}
    if max_output_tokens is not None:
        generation_config['maxOutputTokens'] = max_output_tokens
    if temperature is not None:
        generation_config['temperature'] = temperature
    if thinking:
        generation_config['thinkingConfig'] = dict(thinking)
    if response_mime_type:
        generation_config['responseMimeType'] = response_mime_type
    if response_schema:
        generation_config['responseSchema'] = dict(response_schema)
    if generation_config:
        payload['generationConfig'] = generation_config
    return payload


def extract_text(payload):
    """Pull the answer text out of a ``generateContent`` response.

    Raises ``BlockedError`` when the model returned nothing because a filter
    intervened — distinguishing that from an empty answer matters, because the
    first is worth telling the user about and the second is a bug.
    """
    payload = payload or {}
    feedback = payload.get('promptFeedback') or {}
    if feedback.get('blockReason'):
        raise BlockedError(
            f"The prompt was blocked by Gemini's safety filters "
            f"({feedback['blockReason']}).")

    candidates = payload.get('candidates') or []
    if not candidates:
        raise GeminiError('Gemini returned no candidates.')

    candidate = candidates[0]
    parts = ((candidate.get('content') or {}).get('parts')) or []
    text = ''.join(part.get('text', '') for part in parts if isinstance(part, dict))

    finish_reason = candidate.get('finishReason')
    if not text.strip():
        if finish_reason in ('SAFETY', 'PROHIBITED_CONTENT', 'BLOCKLIST',
                             'RECITATION', 'SPII'):
            raise BlockedError(
                f"Gemini withheld the answer ({finish_reason}).")
        if finish_reason == 'MAX_TOKENS':
            # Reasoning tokens draw on the same output budget, so a small
            # maxOutputTokens can be spent entirely on thinking and return no
            # visible text at all. Naming it beats "empty answer", which reads
            # like a model quirk rather than a setting to raise.
            raise EmptyOutputBudgetError(
                'Gemini produced no text — the output budget was used up '
                'before any answer was written. Raise LLM_MAX_OUTPUT_TOKENS '
                'or lower the reasoning tier.')
        raise GeminiError(f'Gemini returned an empty answer '
                          f'(finishReason={finish_reason}).')

    if finish_reason == 'MAX_TOKENS':
        text += '\n\n*(truncated — hit the output length limit)*'
    return text


def truncate_error(text, limit=_MAX_ERROR_CHARS):
    """Clamp an upstream error string to something an embed can carry."""
    if not text:
        return ''
    text = ' '.join(str(text).split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


# ── Network ─────────────────────────────────────────────────────────────

async def generate_once(api_key, model, payload, session=None):
    """POST one ``generateContent`` request.

    Returns ``(status, decoded_json)``. Transport failures raise
    ``_AIOHTTP_CLIENT_ERROR``; HTTP errors are returned, not raised, so the
    caller can classify them against the key pool.
    """
    url = f'{BASE_URL}/models/{model}:generateContent'
    headers = {'x-goog-api-key': api_key, 'Content-Type': 'application/json'}
    close_session = session is None
    if close_session:
        session = aiohttp.ClientSession()
    try:
        timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)
        async with session.post(url, json=payload, headers=headers,
                                timeout=timeout) as resp:
            try:
                body = await resp.json(content_type=None)
            except Exception:  # noqa: BLE001 — non-JSON error pages happen
                body = {'error': {'message': truncate_error(await resp.text())}}
            if resp.status != 200:
                debug_headers = {
                    name: resp.headers.get(name)
                    for name in (
                        'Retry-After',
                        'X-Request-Id',
                        'X-GUploader-UploadID',
                        'Date',
                    )
                    if resp.headers.get(name)
                }
                error = (body or {}).get('error') or {}
                logger.warning(
                    'Gemini HTTP error model=%s status=%s headers=%s '
                    'error_status=%s message=%s',
                    model, resp.status, debug_headers, error.get('status'),
                    truncate_error(error.get('message'), limit=1000))
            elif payload.get('tools'):
                usage = (body or {}).get('usageMetadata') or {}
                tool_names = ','.join(
                    next(iter(tool), '?')
                    for tool in payload.get('tools') or [])
                logger.info(
                    'Gemini tool usage model=%s tools=%s prompt_tokens=%s '
                    'tool_tokens=%s output_tokens=%s total_tokens=%s',
                    model, tool_names,
                    usage.get('promptTokenCount'),
                    usage.get('toolUsePromptTokenCount'),
                    usage.get('candidatesTokenCount'),
                    usage.get('totalTokenCount'))
            return resp.status, body
    finally:
        if close_session:
            await session.close()


async def complete(pool, prompt, images=None, system_instruction=None,
                   max_output_tokens=None, temperature=None, session=None,
                   max_attempts=None, stats=None, models=None, tier=None,
                   response_mime_type=None, response_schema=None, tools=None):
    """Run one prompt against the pool, rotating buckets until one answers.

    Walks ``(key, model)`` buckets — cheapest model across every key first,
    then the fallback model — reporting each outcome back to the pool so it
    learns which buckets are spent.

    ``max_attempts`` defaults to the pool's own size (bounded), so a large pool
    is not abandoned half-walked. ``stats``, if given, is a dict this fills in
    with ``attempts`` — the number of requests actually put on the wire, which
    the caller needs in order to bill a failed command fairly.

    ``models`` pins the ladder (a user-chosen model); ``tier`` is a reasoning
    level applied per attempt, since the 2.5 and 3.x families encode it
    differently and a fallback can cross that boundary.
    """
    from tle.util import llm_keypool, llm_models

    if pool.key_count() == 0:
        raise NoKeysError('No Gemini API keys are configured.')

    ladder = _model_ladder(models if models is not None else pool.models)
    # A configured ladder and an explicit multi-model ladder both fall back.
    # An explicit one-model selector remains pinned.
    allow_model_fallback = len(ladder) > 1
    if not ladder:
        _record_stats(stats, 0)
        raise ModelUnavailableError('No Gemini models are configured.')
    if max_attempts is None:
        # One more than the bucket count, so a pool small enough to walk fully
        # gets a final acquire() that returns None — that is what distinguishes
        # "genuinely out of quota" from "hit the ceiling", and without the
        # spare attempt the two are indistinguishable.
        buckets = pool.key_count() * max(1, len(ladder))
        max_attempts = max(_MIN_ATTEMPTS,
                           min(buckets + 1, _MAX_ATTEMPTS_CEILING))

    parts = build_parts(prompt, images)

    last_error = None
    last_model_error = None
    attempts = 0
    pool_drained = False
    attempted_buckets = set()
    # Tools are dropped for the rest of the call if a model rejects them; the
    # ladder spans model families and a tool one family supports is a 400 on
    # another. An answer without URL reading beats no answer at all.
    active_tools = tools
    # Models that answered 404 during this call. A bad model id is a config
    # or access error are skipped for this call while weaker rungs remain.
    unavailable_models = set()
    recorded = False

    def _record(payload=None):
        nonlocal recorded
        if not recorded:
            _record_stats(stats, attempts, payload)
            recorded = True

    for _ in range(max_attempts):
        active_models = [model for model in ladder
                         if model not in unavailable_models]
        if not active_models:
            _record()
            raise ModelUnavailableError(last_model_error)
        lease = await pool.acquire(
            models=active_models, exclude=attempted_buckets)
        if lease is None:
            pool_drained = True
            break
        attempted_buckets.add((lease.key_id, lease.model))

        # Built per attempt: a fallback can cross model families, and "off"
        # means thinkingBudget on 2.5 but has no thinkingLevel equivalent.
        payload = build_payload(
            parts, system_instruction=system_instruction,
            max_output_tokens=max_output_tokens, temperature=temperature,
            thinking=llm_models.thinking_config(lease.model, tier),
            response_mime_type=response_mime_type,
            response_schema=response_schema, tools=active_tools)

        attempts += 1
        try:
            status, body = await generate_once(lease.api_key, lease.model,
                                               payload, session=session)
        except asyncio.CancelledError:
            # The request was already put on the wire. Preserve truthful
            # accounting and briefly cool the bucket before the outer request
            # deadline propagates cancellation to its caller.
            _record()
            pool.report_transient(lease)
            raise
        except _TRANSPORT_ERRORS as err:
            logger.warning('Gemini transport error on key=%s model=%s: %s',
                           lease.key_id, lease.model, err)
            pool.report_transient(lease)
            last_error = f'network error: {err}'
            continue

        if status == 200:
            pool.report_success(lease)
            try:
                answer = extract_text(body)
            except (BlockedError, EmptyOutputBudgetError):
                _record(body)
                raise
            except GeminiError as err:
                _record_stats(stats, 0, body)
                candidate = ((body or {}).get('candidates') or [{}])[0]
                logger.warning(
                    'Gemini HTTP 200 without usable text on key=%s model=%s '
                    'tools=%s finish_reason=%s',
                    lease.key_id, lease.model,
                    [next(iter(tool), '?') for tool in (active_tools or [])],
                    candidate.get('finishReason'))
                last_error = str(err)
                continue
            _record(body)
            return answer, lease

        message = truncate_error(
            ((body or {}).get('error') or {}).get('message')) or f'HTTP {status}'

        if status == 429:
            scope, retry_after = llm_keypool.classify_quota_error(body)
            logger.info('Gemini 429 on key=%s model=%s scope=%s retry_after=%s',
                        lease.key_id, lease.model, scope, retry_after)
            pool.report_quota(lease, scope, retry_after, message=message)
            last_error = message
            continue

        if active_tools and is_tool_unsupported_error(status, body):
            logger.warning('Model %s rejected tools %s (%s) — retrying '
                           'without them', lease.model,
                           [next(iter(tool), '?') for tool in active_tools],
                           message)
            active_tools = None
            pool.report_success(lease)
            attempted_buckets.discard((lease.key_id, lease.model))
            last_error = message
            continue

        if is_model_unavailable_error(status, body):
            # This is model-wide, not credential-specific. Skip every other
            # key for this model during this call and continue down the
            # caller-supplied weaker-model ladder when one exists.
            unavailable_models.add(lease.model)
            last_model_error = (
                f'Model `{lease.model}` is not available: {message}')
            last_error = message
            logger.warning('Gemini model unavailable, falling back from %s: %s',
                           lease.model, message)
            if not allow_model_fallback:
                _record()
                raise ModelUnavailableError(last_model_error)
            continue

        if is_invalid_key_error(status, body):
            pool.report_invalid(lease, message=message)
            last_error = message
            continue

        if status >= 500:
            logger.warning('Gemini %d on key=%s model=%s: %s',
                           status, lease.key_id, lease.model, message)
            pool.report_transient(lease)
            last_error = message
            continue

        # Anything else (a genuinely malformed request) will fail identically
        # on every other bucket, so stop.
        _record()
        raise GeminiError(message)

    _record()
    if len(unavailable_models) == len(ladder):
        raise ModelUnavailableError(last_model_error)
    if pool_drained:
        raise NoCapacityError(
            last_error or 'All Gemini keys are rate-limited or out of quota.',
            retry_after=pool.retry_after_hint(models=[
                model for model in ladder
                if model not in unavailable_models]))
    # The ceiling stopped us, not the quota — buckets may still be healthy, so
    # do not quote a wait derived only from the blocked ones.
    raise NoCapacityError(
        last_error or f'Gave up after {attempts} failed attempts.',
        attempts_exhausted=True)

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
import base64
import logging

import aiohttp

logger = logging.getLogger(__name__)

_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)

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
                  temperature=None, thinking=None):
    """Assemble a ``generateContent`` request body.

    ``thinking`` is the encoded reasoning config from ``llm_models`` — either
    ``{'thinkingLevel': ...}`` or ``{'thinkingBudget': 0}`` — and is nested
    under ``generationConfig.thinkingConfig``.
    """
    payload = {'contents': [{'role': 'user', 'parts': parts}]}
    if system_instruction:
        payload['systemInstruction'] = {'parts': [{'text': system_instruction}]}
    generation_config = {}
    if max_output_tokens is not None:
        generation_config['maxOutputTokens'] = max_output_tokens
    if temperature is not None:
        generation_config['temperature'] = temperature
    if thinking:
        generation_config['thinkingConfig'] = dict(thinking)
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


def is_invalid_key_error(status, payload):
    """True when a 4xx means "this key is no good" rather than "bad request".

    A revoked key — the fate of any key that reaches a public channel or a
    git push — shows up here, and should retire the key rather than the call.
    """
    if status not in (400, 401, 403):
        return False
    error = (payload or {}).get('error') or {}
    blob = f"{error.get('status', '')} {error.get('message', '')}".upper()
    return ('API_KEY' in blob or 'API KEY' in blob
            or 'PERMISSION_DENIED' in blob or 'UNAUTHENTICATED' in blob
            or 'CONSUMER_SUSPENDED' in blob)


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
            return resp.status, body
    finally:
        if close_session:
            await session.close()


async def complete(pool, prompt, images=None, system_instruction=None,
                   max_output_tokens=None, temperature=None, session=None,
                   max_attempts=None, stats=None, models=None, tier=None):
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

    ladder = models or pool.models
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
    attempts = 0
    pool_drained = False

    def _record():
        if stats is not None:
            stats['attempts'] = attempts

    for _ in range(max_attempts):
        lease = await pool.acquire(models=models)
        if lease is None:
            pool_drained = True
            break

        # Built per attempt: a fallback can cross model families, and "off"
        # means thinkingBudget on 2.5 but has no thinkingLevel equivalent.
        payload = build_payload(
            parts, system_instruction=system_instruction,
            max_output_tokens=max_output_tokens, temperature=temperature,
            thinking=llm_models.thinking_config(lease.model, tier))

        attempts += 1
        try:
            status, body = await generate_once(lease.api_key, lease.model,
                                               payload, session=session)
        except _AIOHTTP_CLIENT_ERROR as err:
            logger.warning('Gemini transport error on key=%s model=%s: %s',
                           lease.key_id, lease.model, err)
            pool.report_transient(lease)
            last_error = f'network error: {err}'
            continue

        if status == 200:
            pool.report_success(lease)
            _record()
            return extract_text(body), lease

        message = truncate_error(
            ((body or {}).get('error') or {}).get('message')) or f'HTTP {status}'

        if status == 429:
            scope, retry_after = llm_keypool.classify_quota_error(body)
            logger.info('Gemini 429 on key=%s model=%s scope=%s retry_after=%s',
                        lease.key_id, lease.model, scope, retry_after)
            pool.report_quota(lease, scope, retry_after, message=message)
            last_error = message
            continue

        if is_invalid_key_error(status, body):
            pool.report_invalid(lease, message=message)
            last_error = message
            continue

        if status == 404:
            # A bad model id is a config error — every key will fail the same
            # way, so fail loudly instead of burning the pool discovering it.
            _record()
            raise ModelUnavailableError(
                f'Model `{lease.model}` is not available: {message}')

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
    if pool_drained:
        raise NoCapacityError(
            last_error or 'All Gemini keys are rate-limited or out of quota.',
            retry_after=pool.retry_after_hint(models=models))
    # The ceiling stopped us, not the quota — buckets may still be healthy, so
    # do not quote a wait derived only from the blocked ones.
    raise NoCapacityError(
        last_error or f'Gave up after {attempts} failed attempts.',
        attempts_exhausted=True)

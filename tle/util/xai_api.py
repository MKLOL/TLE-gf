"""Small async client and key rotator for xAI Chat Completions."""
import base64
import logging
from collections import namedtuple

import aiohttp

logger = logging.getLogger(__name__)

_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)
_TRANSPORT_ERRORS = (_AIOHTTP_CLIENT_ERROR, TimeoutError)

BASE_URL = 'https://api.x.ai/v1'
_REQUEST_TIMEOUT = 60
_MAX_ERROR_CHARS = 400
_MAX_ATTEMPTS = 8
_IMAGE_MIMES = frozenset(('image/jpeg', 'image/png'))

Lease = namedtuple('XaiLease', 'key_id api_key label model')


class XaiError(Exception):
    """Base class for xAI failures safe to handle in the cog."""


class NoKeysError(XaiError):
    """No xAI credentials are configured."""


class AuthenticationError(XaiError):
    """Every attempted credential was rejected."""


class AccessDeniedError(XaiError):
    """The key or its team lacks model/credit access."""


class RateLimitError(XaiError):
    """All attempted keys were rate-limited."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class ModelUnavailableError(XaiError):
    """The configured xAI model is unknown or inaccessible."""


class BlockedError(XaiError):
    """xAI refused to provide an answer."""


class ServiceUnavailableError(XaiError):
    """xAI or the network failed transiently on every key."""


class NoCapacityError(XaiError):
    """Configured keys failed for a mixture of retryable reasons."""


class XaiKeyPool:
    """Round-robin view of active xAI keys in the shared LLM key table."""

    def __init__(self, db, model):
        self.db = db
        self.model = model
        self._keys = []
        self._cursor = 0
        self.reload()

    def reload(self):
        self._keys = list(self.db.llm_get_keys(
            active_only=True, provider='xai'))
        if self._keys:
            self._cursor %= len(self._keys)
        else:
            self._cursor = 0

    def key_count(self):
        return len(self._keys)

    def leases(self, max_attempts=None):
        """Return a rotating snapshot, trying each key at most once."""
        if not self._keys:
            return []
        limit = min(len(self._keys), max_attempts or _MAX_ATTEMPTS,
                    _MAX_ATTEMPTS)
        start = self._cursor % len(self._keys)
        self._cursor = (start + 1) % len(self._keys)
        ordered = self._keys[start:] + self._keys[:start]
        return [Lease(row.id, row.api_key, row.label, self.model)
                for row in ordered[:limit]]


def is_supported_image(content_type):
    """True for the PNG/JPEG image inputs xAI currently accepts."""
    return _normalize_image_mime(content_type) in _IMAGE_MIMES


def _normalize_image_mime(content_type):
    mime = (content_type or '').split(';')[0].strip().lower()
    return 'image/jpeg' if mime == 'image/jpg' else mime


def build_user_content(prompt, images=None):
    """Build a text turn or OpenAI-style multimodal content array."""
    supported = []
    for mime_type, raw in images or []:
        mime = _normalize_image_mime(mime_type)
        if not is_supported_image(mime):
            logger.info('Skipping image type unsupported by xAI: %s', mime)
            continue
        encoded = base64.b64encode(raw).decode('ascii')
        supported.append({
            'type': 'image_url',
            'image_url': {
                'url': f'data:{mime};base64,{encoded}',
                'detail': 'high',
            },
        })
    if not supported:
        return prompt or ''
    if prompt:
        supported.append({'type': 'text', 'text': prompt})
    return supported


def build_payload(model, prompt, images=None, system_instruction=None,
                  max_output_tokens=None, temperature=None,
                  reasoning_effort=None):
    """Assemble one stateless Chat Completions request."""
    messages = []
    if system_instruction:
        messages.append({'role': 'system', 'content': system_instruction})
    messages.append({'role': 'user',
                     'content': build_user_content(prompt, images)})
    payload = {'model': model, 'messages': messages, 'stream': False}
    if max_output_tokens is not None:
        payload['max_tokens'] = max_output_tokens
    if temperature is not None:
        payload['temperature'] = temperature
    if reasoning_effort is not None:
        payload['reasoning_effort'] = reasoning_effort
    return payload


def extract_text(payload):
    """Return visible answer text and the response's actual model id."""
    payload = payload or {}
    choices = payload.get('choices') or []
    if not choices:
        raise XaiError('xAI returned no choices.')
    choice = choices[0] or {}
    message = choice.get('message') or {}
    content = message.get('content')
    if isinstance(content, list):
        content = ''.join(
            part.get('text', '') for part in content
            if isinstance(part, dict) and part.get('type') in (None, 'text'))
    text = content if isinstance(content, str) else ''
    if not text.strip():
        refusal = message.get('refusal')
        if refusal:
            raise BlockedError(f'xAI refused the request: {truncate_error(refusal)}')
        if choice.get('finish_reason') in ('content_filter', 'safety'):
            raise BlockedError('xAI withheld the answer for safety reasons.')
        raise XaiError(
            f'xAI returned an empty answer (finish_reason={choice.get("finish_reason")}).')
    if choice.get('finish_reason') == 'length':
        text += '\n\n*(truncated — hit the output length limit)*'
    return text, payload.get('model')


def truncate_error(text, limit=_MAX_ERROR_CHARS):
    """Collapse and clamp upstream error text before displaying/logging it."""
    if not text:
        return ''
    text = ' '.join(str(text).split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


def error_message(payload):
    """Extract a bounded message from any known xAI error shape."""
    if not isinstance(payload, dict):
        return truncate_error(payload)
    payload = payload or {}
    error = payload.get('error') if isinstance(payload, dict) else payload
    if isinstance(error, dict):
        error = error.get('message') or error.get('detail') or error.get('code')
    if not error and isinstance(payload, dict):
        error = payload.get('message') or payload.get('detail')
    return truncate_error(error)


def _is_bad_key_error(status, message):
    """Some xAI clusters report an incorrect credential as HTTP 400."""
    if status not in (400, 401):
        return False
    normalized = (message or '').lower().replace('_', ' ')
    return status == 401 or 'api key' in normalized or 'authentication' in normalized


def _retry_after(headers, payload):
    headers = {str(key).lower(): value for key, value in (headers or {}).items()}
    raw = headers.get('retry-after')
    if raw is None and isinstance(payload, dict):
        raw = payload.get('retry_after')
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


async def generate_once(api_key, payload, session=None):
    """POST one request and return ``(status, decoded body, headers)``."""
    headers = {'Authorization': f'Bearer {api_key}',
               'Content-Type': 'application/json'}
    close_session = session is None
    if close_session:
        session = aiohttp.ClientSession()
    try:
        timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)
        async with session.post(f'{BASE_URL}/chat/completions', json=payload,
                                headers=headers, timeout=timeout) as resp:
            try:
                body = await resp.json(content_type=None)
            except Exception:  # noqa: BLE001 - edge pages are sometimes HTML
                body = {'error': {'message': truncate_error(await resp.text())}}
            return resp.status, body, dict(resp.headers)
    finally:
        if close_session:
            await session.close()


async def complete(pool, prompt, images=None, system_instruction=None,
                   max_output_tokens=None, temperature=None,
                   reasoning_effort=None, session=None, stats=None,
                   max_attempts=None):
    """Try active xAI keys in round-robin order until one answers."""
    leases = pool.leases(max_attempts=max_attempts)
    if not leases:
        raise NoKeysError('No xAI API keys are configured.')

    attempts = 0
    failures = []
    rate_retry_after = None

    def record():
        if stats is not None:
            stats['attempts'] = attempts

    for lease in leases:
        payload = build_payload(
            lease.model, prompt, images=images,
            system_instruction=system_instruction,
            max_output_tokens=max_output_tokens, temperature=temperature,
            reasoning_effort=reasoning_effort)
        attempts += 1
        try:
            status, body, headers = await generate_once(
                lease.api_key, payload, session=session)
        except _TRANSPORT_ERRORS as err:
            logger.warning('xAI transport error on key=%s: %s',
                           lease.key_id, err)
            failures.append(ServiceUnavailableError('network error'))
            continue

        message = error_message(body) or f'HTTP {status}'
        if status == 200:
            try:
                answer, actual_model = extract_text(body)
            except XaiError:
                record()
                raise
            record()
            if actual_model:
                lease = lease._replace(model=actual_model)
            return answer, lease
        if _is_bad_key_error(status, message):
            failures.append(AuthenticationError(message))
            continue
        if status == 403:
            failures.append(AccessDeniedError(message))
            continue
        if status == 429:
            hint = _retry_after(headers, body)
            if hint is not None:
                rate_retry_after = (hint if rate_retry_after is None
                                    else min(rate_retry_after, hint))
            failures.append(RateLimitError(
                message, retry_after=rate_retry_after))
            continue
        if status == 404:
            record()
            raise ModelUnavailableError(message)
        if status >= 500:
            failures.append(ServiceUnavailableError(message))
            continue

        record()
        raise XaiError(message)

    record()
    if not failures:
        raise XaiError('No xAI key produced an answer.')
    failure_types = {type(error) for error in failures}
    if len(failure_types) == 1:
        raise failures[-1]
    raise NoCapacityError(
        'Configured xAI keys failed for different retryable reasons.')

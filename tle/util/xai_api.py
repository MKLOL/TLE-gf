"""Small async client and key rotator for xAI Chat Completions."""
import asyncio
import base64
import logging

import aiohttp

from tle.util.xai_keypool import Lease, XaiKeyPool

logger = logging.getLogger(__name__)

_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)
_TRANSPORT_ERRORS = (_AIOHTTP_CLIENT_ERROR, TimeoutError)

BASE_URL = 'https://api.x.ai/v1'
_REQUEST_TIMEOUT = 60
_MAX_ERROR_CHARS = 400
_MAX_ATTEMPTS = 8
_IMAGE_MIMES = frozenset(('image/jpeg', 'image/png'))


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

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


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


def is_model_unavailable_error(status, message):
    """True when the model id, rather than a credential, cannot be used."""
    if status == 404:
        return True
    if status not in (400, 403):
        return False
    normalized = (message or '').lower().replace('_', ' ')
    unavailable = ('not found', 'not available', 'unknown model',
                   'unsupported', 'not supported', 'does not exist')
    return 'model' in normalized and any(
        marker in normalized for marker in unavailable)


def is_model_access_error(status, message):
    """Identify a 403 scoped to one model, excluding billing/team failures."""
    if status != 403:
        return False
    normalized = (message or '').lower().replace('_', ' ')
    if any(word in normalized for word in (
            'credit', 'billing', 'balance', 'fund', 'subscription')):
        return False
    access = ('access', 'permission', 'authoriz', 'entitle', 'available')
    return 'model' in normalized and any(word in normalized for word in access)


def _record_stats(stats, attempts, payload=None):
    if stats is None:
        return
    stats['attempts'] = stats.get('attempts', 0) + attempts
    usage = (payload or {}).get('usage') or {}
    fields = {
        'input_tokens': 'prompt_tokens',
        'output_tokens': 'completion_tokens',
        'total_tokens': 'total_tokens',
    }
    for target, source in fields.items():
        value = usage.get(source)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            stats[target] = stats.get(target, 0) + value
    raw_cost = usage.get('cost_in_usd_ticks')
    try:
        ticks = int(raw_cost)
    except (TypeError, ValueError):
        ticks = None
    if ticks is not None and ticks >= 0:
        # xAI defines 1 USD as 10^10 ticks; one micro-USD is 10^4 ticks.
        microusd = (ticks + 9_999) // 10_000
        stats['cost_microusd'] = stats.get('cost_microusd', 0) + microusd


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
                   max_attempts=None, models=None):
    """Try healthy xAI key/model buckets, falling back down the ladder."""
    if pool.key_count() == 0:
        raise NoKeysError('No xAI API keys are configured.')

    ladder = pool.models if models is None else models
    if isinstance(ladder, str):
        ladder = [ladder]
    ladder = list(dict.fromkeys(model for model in ladder or [] if model))
    if not ladder:
        _record_stats(stats, 0)
        raise ModelUnavailableError('No xAI models are configured.')

    attempt_limit = min(_MAX_ATTEMPTS, max_attempts or _MAX_ATTEMPTS)
    # Health changes while walking the snapshot. Pull enough candidates for
    # skipped same-model or newly-benched buckets not to consume the on-wire
    # attempt budget.
    leases = pool.leases(
        max_attempts=pool.candidate_count(models=ladder), models=ladder)

    attempts = 0
    failures = []
    rate_retry_after = None
    unavailable_models = set()
    recorded = False

    def record(payload=None):
        nonlocal recorded
        if not recorded:
            _record_stats(stats, attempts, payload)
            recorded = True

    for lease in leases:
        if attempts >= attempt_limit:
            break
        if lease.model in unavailable_models or not pool.is_available(lease):
            continue
        payload = build_payload(
            lease.model, prompt, images=images,
            system_instruction=system_instruction,
            max_output_tokens=max_output_tokens, temperature=temperature,
            reasoning_effort=reasoning_effort)
        attempts += 1
        try:
            status, body, headers = await generate_once(
                lease.api_key, payload, session=session)
        except asyncio.CancelledError:
            # A deadline can cancel this coroutine after the provider has the
            # request. Count that attempt and avoid immediately reusing the
            # same key/model bucket for the answer stage.
            record()
            pool.report_transient(lease, message='request cancelled')
            raise
        except _TRANSPORT_ERRORS as err:
            logger.warning('xAI transport error on key=%s model=%s: %s',
                           lease.key_id, lease.model, err)
            pool.report_transient(lease, message=err)
            failures.append(ServiceUnavailableError('network error'))
            continue

        message = error_message(body) or f'HTTP {status}'
        if status == 200:
            pool.report_success(lease)
            record(body)
            try:
                answer, actual_model = extract_text(body)
            except XaiError:
                raise
            if actual_model:
                lease = lease._replace(model=actual_model)
            return answer, lease
        if status in (400, 404) and is_model_unavailable_error(status, message):
            pool.report_model_unavailable(lease, message=message)
            unavailable_models.add(lease.model)
            failures.append(ModelUnavailableError(message))
            logger.warning('xAI model unavailable, falling back from %s: %s',
                           lease.model, message)
            continue
        if _is_bad_key_error(status, message):
            pool.report_invalid(lease, message=message)
            failures.append(AuthenticationError(message))
            continue
        if status == 403:
            model_specific = is_model_access_error(status, message)
            pool.report_access(lease, message=message,
                               model_specific=model_specific)
            failures.append(AccessDeniedError(message))
            continue
        if status == 429:
            hint = _retry_after(headers, body)
            applied = pool.report_rate_limit(
                lease, retry_after=hint, message=message)
            rate_retry_after = (applied if rate_retry_after is None
                                else min(rate_retry_after, applied))
            failures.append(RateLimitError(
                message, retry_after=rate_retry_after))
            continue
        if status >= 500:
            pool.report_transient(lease, message=message)
            failures.append(ServiceUnavailableError(message))
            continue

        record()
        raise XaiError(message)

    record()
    if not failures:
        raise NoCapacityError(
            'All xAI keys or models are temporarily unavailable.',
            retry_after=pool.retry_after_hint(models=ladder))
    failure_types = {type(error) for error in failures}
    if len(failure_types) == 1:
        raise failures[-1]
    raise NoCapacityError(
        'Configured xAI keys failed for different retryable reasons.',
        retry_after=pool.retry_after_hint(models=ladder))

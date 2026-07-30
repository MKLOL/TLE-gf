"""Async Gemini text client, model validation, and quota-aware fallback."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

import aiohttp

from tle.util.gemini_keys import (
    GeminiKeyCycleExhausted,
    GeminiKeyPool,
)


logger = logging.getLogger(__name__)

_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models'
_TIMEOUT_SECONDS = 300


class GeminiError(RuntimeError):
    """Base error shown to users by the Gemini cog."""


class GeminiNoQuotaError(GeminiError):
    """Raised after every eligible model/key combination returns no quota."""


class GeminiQuotaError(GeminiError):
    """Internal signal for an HTTP 429 response."""


@dataclass(frozen=True)
class GeminiModel:
    api_name: str
    display_name: str
    reasoning_tiers: Tuple[str, ...]


@dataclass(frozen=True)
class GeminiModelRequest:
    model: GeminiModel
    reasoning: Optional[str] = None


@dataclass(frozen=True)
class GeminiResult:
    text: str
    model: GeminiModel
    reasoning: Optional[str]


_OFF_LOW_MEDIUM_HIGH = ('off', 'low', 'medium', 'high')
_MINIMAL_LOW_MEDIUM_HIGH = ('minimal', 'low', 'medium', 'high')

MODELS: Dict[str, GeminiModel] = {
    model.api_name: model
    for model in (
        GeminiModel(
            'gemini-2.5-flash',
            'Gemini 2.5 Flash',
            _OFF_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-2.5-flash-lite',
            'Gemini 2.5 Flash Lite',
            _OFF_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-3-flash',
            'Gemini 3 Flash',
            _MINIMAL_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-3.1-flash-lite',
            'Gemini 3.1 Flash Lite',
            _MINIMAL_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-3.5-flash',
            'Gemini 3.5 Flash',
            _MINIMAL_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-3.5-flash-lite',
            'Gemini 3.5 Flash Lite',
            _MINIMAL_LOW_MEDIUM_HIGH,
        ),
        GeminiModel(
            'gemini-3.6-flash',
            'Gemini 3.6 Flash',
            _MINIMAL_LOW_MEDIUM_HIGH,
        ),
    )
}

DEFAULT_MODELS = (
    MODELS['gemini-3.1-flash-lite'],
    MODELS['gemini-3.5-flash-lite'],
)


def parse_model_spec(value: str) -> GeminiModelRequest:
    """Parse ``3.1-flash-lite-low`` or its ``gemini-``-prefixed form."""
    normalized = value.lower()
    if not normalized.startswith('gemini-'):
        normalized = f'gemini-{normalized}'

    model = MODELS.get(normalized)
    if model is not None:
        return GeminiModelRequest(model)

    for api_name in sorted(MODELS, key=len, reverse=True):
        prefix = f'{api_name}-'
        if not normalized.startswith(prefix):
            continue
        model = MODELS[api_name]
        reasoning = normalized[len(prefix):]
        if reasoning not in model.reasoning_tiers:
            choices = ', '.join(model.reasoning_tiers)
            raise ValueError(
                f'Invalid reasoning mode `{reasoning}` for '
                f'{model.display_name}. Choose one of: {choices}.'
            )
        return GeminiModelRequest(model, reasoning)

    raise ValueError(f'Unsupported Gemini model `{value}`.')


def parse_command_request(
    request: str,
) -> Tuple[Sequence[GeminiModelRequest], str]:
    """Split an optional model/reasoning token from the user's query."""
    first, separator, remainder = request.partition(' ')
    normalized = first.lower()
    looks_like_model = (
        normalized.startswith('gemini-')
        or ('-' in normalized and normalized[0].isdigit())
    )
    if not looks_like_model:
        return tuple(GeminiModelRequest(model) for model in DEFAULT_MODELS), request

    try:
        model_request = parse_model_spec(first)
    except ValueError as exc:
        raise GeminiError(str(exc)) from exc
    query = remainder.strip()
    if not separator or not query:
        raise GeminiError('A query is required after the model name.')
    return (model_request,), query


class GeminiClient:
    def __init__(self, key_pool: GeminiKeyPool):
        self.key_pool = key_pool
        self._session = None
        self._request_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def generate(
        self,
        query: str,
        model_requests: Sequence[GeminiModelRequest],
        *,
        system_prompt: Optional[str] = None,
        generation_config: Optional[dict] = None,
    ) -> GeminiResult:
        """Try each requested model and every key once until one succeeds."""
        async with self._request_lock:
            return await self._generate(
                query,
                model_requests,
                system_prompt,
                generation_config,
            )

    async def _generate(
        self,
        query: str,
        model_requests: Sequence[GeminiModelRequest],
        system_prompt: Optional[str],
        generation_config: Optional[dict],
    ) -> GeminiResult:
        for model_request in model_requests:
            model = model_request.model
            cycle = self.key_pool.cycle()

            while True:
                try:
                    key = cycle.next_key()
                except GeminiKeyCycleExhausted:
                    break

                logger.info(
                    'Gemini request using %s with %s',
                    model.api_name,
                    key.label,
                )
                try:
                    text = await self._generate_once(
                        query,
                        model_request,
                        key.value,
                        system_prompt,
                        generation_config,
                    )
                except GeminiQuotaError:
                    logger.info(
                        'Gemini quota exhausted for %s with %s; rotating',
                        model.api_name,
                        key.label,
                    )
                    continue
                finally:
                    cycle.complete_call(key)
                return GeminiResult(text, model, model_request.reasoning)

        raise GeminiNoQuotaError('No API Quota left')

    async def _generate_once(
        self,
        query: str,
        model_request: GeminiModelRequest,
        api_key: str,
        system_prompt: Optional[str],
        generation_config: Optional[dict],
    ) -> str:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)
            self._session = aiohttp.ClientSession(timeout=timeout)

        request_config = dict(generation_config or {})
        if model_request.reasoning is not None:
            request_config['thinkingConfig'] = {
                'thinkingLevel': model_request.reasoning,
            }
        payload = {
            'contents': [{
                'role': 'user',
                'parts': [{'text': query}],
            }],
        }
        if system_prompt is not None:
            payload['systemInstruction'] = {
                'parts': [{'text': system_prompt}],
            }
        if request_config:
            payload['generationConfig'] = request_config

        url = f'{_BASE_URL}/{model_request.model.api_name}:generateContent'
        try:
            async with self._session.post(
                url,
                headers={
                    'x-goog-api-key': api_key,
                    'Content-Type': 'application/json',
                },
                json=payload,
            ) as response:
                data = await response.json(content_type=None)
                if response.status == 429:
                    raise GeminiQuotaError('Gemini API quota exhausted.')
                if response.status >= 400:
                    error = data.get('error') or {}
                    message = error.get('message') or str(data)
                    raise GeminiError(
                        f'Gemini API returned HTTP {response.status}: {message}'
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise GeminiError(f'Gemini API request failed: {exc}') from exc

        feedback = data.get('promptFeedback') or {}
        if feedback.get('blockReason'):
            raise GeminiError(
                f'Gemini rejected the prompt: {feedback["blockReason"]}.'
            )

        candidates = data.get('candidates') or []
        if not candidates:
            raise GeminiError('Gemini returned no response.')
        content = candidates[0].get('content') or {}
        parts = content.get('parts') or []
        text = ''.join(part.get('text', '') for part in parts).strip()
        if not text:
            raise GeminiError('Gemini returned an empty response.')
        return text

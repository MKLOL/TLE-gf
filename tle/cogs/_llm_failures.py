"""Sanitized user-facing descriptions for LLM provider failures."""

from tle.util import gemini_api, xai_api
from tle.cogs import _llm_format as llm_format


def describe_gemini_failure(error):
    if isinstance(error, gemini_api.NoCapacityError):
        if error.attempts_exhausted:
            return ('Gemini failed on every fallback I tried. Try again in '
                    'a moment.')
        if error.retry_after:
            return ('All Gemini key/model fallbacks are rate-limited, out of '
                    'quota, or cooling down. Try again in '
                    f'{llm_format.format_duration(error.retry_after)}.')
        return 'All Gemini capacity is unavailable right now. Try again later.'
    if isinstance(error, gemini_api.BlockedError):
        return str(error)
    if isinstance(error, gemini_api.ModelUnavailableError):
        return ('Every configured Gemini fallback is unavailable. The bot '
                'owner should check `LLM_MODELS`.')
    if isinstance(error, gemini_api.NoKeysError):
        return 'No Gemini API keys are configured.'
    return 'Gemini request failed unexpectedly. Try again shortly.'


def describe_xai_failure(error):
    if isinstance(error, xai_api.NoKeysError):
        return 'No xAI API keys are configured.'
    if isinstance(error, xai_api.AuthenticationError):
        return ('xAI rejected the configured credentials. The bot owner '
                'should replace the environment key.')
    if isinstance(error, xai_api.AccessDeniedError):
        return ('xAI denied access. The team may be unfunded; check credits, '
                'billing, and model permissions in the xAI Console.')
    if isinstance(error, xai_api.RateLimitError):
        if error.retry_after is not None:
            return (f'xAI is rate-limited. Try again in '
                    f'{llm_format.format_duration(error.retry_after)}.')
        return 'xAI is rate-limited. Try again shortly.'
    if isinstance(error, xai_api.ModelUnavailableError):
        return ('Every configured Grok fallback is unavailable. The bot '
                'owner should check `XAI_MODELS`.')
    if isinstance(error, xai_api.BlockedError):
        return str(error)
    if isinstance(error, xai_api.ServiceUnavailableError):
        return 'xAI is temporarily unavailable. Try again shortly.'
    if isinstance(error, xai_api.NoCapacityError):
        if error.retry_after is not None:
            return (f'Grok capacity is cooling down. Try again in '
                    f'{llm_format.format_duration(error.retry_after)}.')
        return ('No configured xAI key/model fallback can answer right now. '
                'The bot owner can inspect `;llm grokstatus` and '
                '`;llm grokkeylist`.')
    return 'xAI request failed unexpectedly. Try again shortly.'

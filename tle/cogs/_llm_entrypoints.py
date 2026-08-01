"""Canonical provider selectors, literal triggers, and usage text."""

import re


PROVIDER_TRIGGER = re.compile(
    r'^\s*@(gemini|grok)(?:\s+|$)', re.IGNORECASE)

_SELECTORS = {
    '+gemini': 'gemini',
    '+grok': 'grok',
}

_USAGE = {
    'gemini': (
        'Usage: `@gemini <question>` or '
        '`;ai +gemini <question>` (`;llm` also works).'),
    'grok': (
        'Usage: `@grok <question>` or '
        '`;ai +grok <question>` (`;llm` also works).'),
}


def parse_provider(question):
    """Return provider, remainder, and whether a selector was explicit."""
    if question is None:
        return 'gemini', None, False
    parts = question.strip().split(maxsplit=1)
    provider = _SELECTORS.get(parts[0].casefold()) if parts else None
    if provider is None:
        return 'gemini', question, False
    remainder = parts[1].strip() if len(parts) > 1 else None
    return provider, remainder, True


def split_provider(question):
    """Compatibility parser returning only ``(provider, remainder)``."""
    provider, remainder, _ = parse_provider(question)
    return provider, remainder


def usage(provider):
    return _USAGE[provider]

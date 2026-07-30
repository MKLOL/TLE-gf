"""Catalog of selectable Gemini models and their reasoning tiers.

Lets a user steer one question — ``;llm 3.5-flash-high <question>`` — without
letting them name arbitrary strings at the API. Only ids in this catalog are
accepted, so a typo becomes "unknown model" rather than a 404 that burns a
request and retires nothing.

Reasoning is set through ``generationConfig.thinkingConfig.thinkingLevel``
(lowercase enum) on the 3.x family. The 2.5 family predates ``thinkingLevel``
for "no thinking at all", which is expressed as ``thinkingBudget: 0`` instead —
hence the per-family encoding rather than one shared switch.

Model ids verified against ai.google.dev/gemini-api/docs/models (July 2026).
"""
from collections import namedtuple

# tiers: what this model accepts, cheapest reasoning first.
# aliases: shortest first — aliases[0] is what gets displayed. The longer
# spellings stay accepted so anything already typed keeps working.
ModelSpec = namedtuple('ModelSpec', 'model_id tiers aliases')

_3X_TIERS = ('minimal', 'low', 'medium', 'high')
_25_TIERS = ('off', 'low', 'medium', 'high')

# The short form is <version><family letter>: f = flash, l = flash-lite.
CATALOG = (
    ModelSpec('gemini-3.6-flash', _3X_TIERS,
              ('3.6f', '3.6', '3.6-flash')),
    ModelSpec('gemini-3.5-flash', _3X_TIERS,
              ('3.5f', '3.5', '3.5-flash')),
    ModelSpec('gemini-3.5-flash-lite', _3X_TIERS,
              ('3.5l', '3.5-lite', '3.5-flash-lite')),
    ModelSpec('gemini-3.1-flash-lite', _3X_TIERS,
              ('3.1l', '3.1', 'lite', '3.1-lite', '3.1-flash-lite')),
    ModelSpec('gemini-2.5-flash', _25_TIERS,
              ('2.5f', '2.5', '2.5-flash')),
    ModelSpec('gemini-2.5-flash-lite', _25_TIERS,
              ('2.5l', '2.5-lite', '2.5-flash-lite')),
    ModelSpec('gemini-2.5-pro', ('low', 'medium', 'high'),
              ('pro', '2.5p', '2.5-pro')),
)

# Tier shorthand. Single letters are unambiguous here because a tier only ever
# appears after a dash, where a model name cannot be — so `3.5l-l` is
# flash-lite at low reasoning, not two model names.
_TIER_ALIASES = {
    'min': 'minimal', 'minimal': 'minimal',
    'l': 'low', 'low': 'low',
    'm': 'medium', 'med': 'medium', 'medium': 'medium',
    'h': 'high', 'high': 'high',
    'off': 'off', 'no': 'off', '0': 'off',
}
# Longest first, so `-minimal` is not eaten by the `min` alias.
_TIER_SUFFIXES = sorted(_TIER_ALIASES, key=len, reverse=True)

_BY_ALIAS = {}
for _spec in CATALOG:
    _BY_ALIAS[_spec.model_id.lower()] = _spec
    for _alias in _spec.aliases:
        _BY_ALIAS[_alias.lower()] = _spec


def find(name):
    """Look up a ModelSpec by id or alias, or None."""
    return _BY_ALIAS.get((name or '').strip().lower())


def parse_selector(token):
    """Parse a leading ``model`` or ``model-tier`` token.

    Returns ``(ModelSpec, tier_or_None)``, or None when the token names no
    known model — which is how a plain question keeps its first word.

    Raises ValueError when the model is real but the tier is not one it
    supports, so `;llm 2.5-pro-off ...` gets a usable message instead of
    silently ignoring the tier.
    """
    token = (token or '').strip().lower()
    if not token:
        return None

    spec = find(token)
    if spec is not None:
        return spec, None

    for suffix in _TIER_SUFFIXES:
        if not token.endswith('-' + suffix):
            continue
        spec = find(token[:-len(suffix) - 1])
        if spec is None:
            continue
        tier = _TIER_ALIASES[suffix]
        if tier not in spec.tiers:
            raise ValueError(
                f'`{spec.aliases[0]}` ({spec.model_id}) does not support the '
                f'`{tier}` reasoning tier. It accepts: '
                f'{", ".join(spec.tiers)}.')
        return spec, tier
    return None


def split_selector(text):
    """Split ``"3.5-flash-high why is this TLE?"`` into (selector, question).

    Returns ``(ModelSpec_or_None, tier_or_None, remaining_text)``. When the
    first word is not a model, everything is the question.
    """
    text = (text or '').strip()
    if not text:
        return None, None, text
    first, _, rest = text.partition(' ')
    parsed = parse_selector(first)
    if parsed is None:
        return None, None, text
    spec, tier = parsed
    return spec, tier, rest.strip()


# Sentinel tier meaning "whatever the least reasoning this model allows is".
# The families disagree on the name — 2.5 calls it `off`, 3.x calls it
# `minimal` — so a caller that just wants a cheap, fast answer cannot name a
# tier that works across a fallback. Since `tiers` is ordered cheapest-first,
# resolving this per model is a lookup.
LEAST = 'least'


def thinking_config(model_id, tier):
    """Encode a reasoning tier for ``generationConfig.thinkingConfig``.

    Returns None when there is nothing to send (no tier requested, or an
    unknown model — in which case letting Google pick its default is safer
    than guessing an encoding).
    """
    if not tier:
        return None
    spec = find(model_id)
    if spec is None:
        return None
    if tier == LEAST:
        tier = spec.tiers[0]
    if tier not in spec.tiers:
        return None
    if tier == 'off':
        # The 2.5 family has no "off" level; a zero budget is how thinking is
        # disabled there.
        return {'thinkingBudget': 0}
    return {'thinkingLevel': tier}


_TIER_SHORTHAND = '`-min` `-l` `-m` `-h`, and `-off` where supported'


def describe_catalog():
    """One line per model, for the help/error text."""
    return '\n'.join(
        f'`{spec.aliases[0]}` — {spec.model_id}'
        for spec in CATALOG)


def describe_tiers():
    """How to append a reasoning tier."""
    return (f'Add a reasoning tier with {_TIER_SHORTHAND} — e.g. `3.5f-h`.\n'
            f'`pro` has no `-off`; the 2.5 models have no `-min`.')

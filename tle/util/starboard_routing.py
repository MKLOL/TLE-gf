"""Cheap fallback routing, using language cues rather than server emoji lists.

Explicit board/alias configuration takes precedence at the call site. This
classifier only chooses a destination; it never merges reaction counts.
Unknown/ambiguous names default to starboard. Sadness, laughter and shock
are intentionally not pillboard cues, since their meaning is contextual.
"""
import re
import unicodedata
from functools import lru_cache

STAR = '\N{WHITE MEDIUM STAR}'
PILL = '\N{PILL}'
_CUSTOM = re.compile(r'<a?:([A-Za-z0-9_]+):[0-9]+>')
_CAMEL = re.compile(r'(?<=[a-z0-9])(?=[A-Z])')
_NEGATIONS = ('reverse',)
_PILL_WORDS = frozenset({
    'pill', 'pills', 'skull', 'clown', 'dead', 'death', 'dying',
    'dumb', 'idiot', 'stupid', 'copium', 'cope', 'gun', 'pistol',
    'kms', 'rip', 'flag', 'lying',
})


def _has_cue(words, cues):
    # Long cues also work at compound-name edges (e.g. lowercase catclown).
    # Short words require a boundary: "rip" must not match "trip".
    # Require a plausible other word in compounds: "spill" is not "pill".
    return any(word in (cue, cue + 's') or (len(cue) >= 4 and
               len(word) >= len(cue) + 3 and
               (word.startswith(cue) or word.endswith(cue)))
               for word in words for cue in cues)


@lru_cache(maxsize=2048)
def fallback_board(emoji):
    """Return ⭐, 💊 or None (opt-out) for an unconfigured reaction string."""
    custom = _CUSTOM.fullmatch(emoji)
    if custom:
        name = _CAMEL.sub(' ', custom[1])
        is_flag = custom[1].casefold().startswith('flag')
    else:
        # Unicode names provide the same vocabulary without a symbol map.
        if emoji.isascii():
            return None
        name = ' '.join(unicodedata.name(c, '') for c in emoji)
        # Country flags are pairs of regional indicators; their Unicode
        # character names contain no "flag" word. Other flags do.
        is_flag = len(emoji) == 2 and all('\U0001f1e6' <= c <= '\U0001f1ff' for c in emoji)
    words = re.findall(r'[a-z]+', name.casefold())
    compact = ''.join(words)
    negated = any(word in _NEGATIONS for word in words)
    for prefix in _NEGATIONS:
        if compact.startswith(prefix):
            remainder = compact[len(prefix):]
            if _has_cue([remainder], _PILL_WORDS | {'star'}):
                negated = True
                break
    if negated:
        return None  # Reverse reactions opt out of both boards.
    return PILL if is_flag or _has_cue(words, _PILL_WORDS) else STAR

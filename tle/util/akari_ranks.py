"""Akari rating bands used for rating-graph backgrounds and tier display.

Split out of :mod:`tle.util.akari_rating` to keep that module under the
project's 500-line limit. These describe how a rating maps to a coloured tier;
they are pure data plus one lookup and have no bearing on the rating math.
Re-exported from ``akari_rating`` for backwards compatibility.
"""

from collections import namedtuple


# Same shape as tle.util.codeforces_api.Rank — kept local so this module
# doesn't depend on the CF API (and so the stubbed test environment can import
# it without extra setup).  plot_rating_bg only reads .low/.high/.color_graph.
_AkariRank = namedtuple(
    '_AkariRank', 'low high title title_abbr color_graph color_embed')


# Akari-specific rating bands for the rating graph background.  These differ
# from CF's: the default 1200 sits in "Expert blue" (rewarding for newcomers)
# and the lower tiers are tighter, so a year of damped daily play actually
# spans a few coloured bands instead of staying entirely in Newbie gray.
# Colours are reused from CF's tier palette so the visual associations carry
# over (green = improving, red = elite).  Tourist tier is collapsed into LGM
# because no Akari player can realistically reach ≥4000 under this damping.
AKARI_RANKS = (
    _AkariRank(-10 ** 9, 1000, 'Newbie', 'N', '#CCCCCC', 0x808080),
    _AkariRank(1000, 1100, 'Pupil', 'P', '#77FF77', 0x008000),
    _AkariRank(1100, 1200, 'Specialist', 'S', '#77DDBB', 0x03a89e),
    _AkariRank(1200, 1300, 'Expert', 'E', '#AAAAFF', 0x0000ff),
    _AkariRank(1300, 1400, 'Candidate Master', 'CM', '#FF88FF', 0xaa00aa),
    _AkariRank(1400, 1500, 'Master', 'M', '#FFCC88', 0xff8c00),
    _AkariRank(1500, 1600, 'International Master', 'IM', '#FFBB55', 0xf57500),
    _AkariRank(1600, 1800, 'Grandmaster', 'GM', '#FF7777', 0xff3030),
    _AkariRank(1800, 2000, 'International Grandmaster', 'IGM', '#FF3333', 0xff0000),
    _AkariRank(2000, 10 ** 9, 'Legendary Grandmaster', 'LGM', '#AA0000', 0xcc0000),
)


def rank_for_rating(rating):
    """Return the :data:`AKARI_RANKS` entry that covers ``rating``.

    Bands are half-open ``[low, high)`` and the first/last extend to ±1e9,
    so every finite rating maps to exactly one rank.  Pass a rounded display
    rating to keep boundary behaviour predictable (e.g. 1100.0 → Specialist).
    """
    for rank in AKARI_RANKS:
        if rank.low <= rating < rank.high:
            return rank
    raise ValueError(f'Rating {rating} outside known Akari rank range.')

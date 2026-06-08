"""Shared rating engine exports for minigames.

The implementation lives in ``akari_rating`` for compatibility with existing
Akari commands/tests, but new minigame code should import rating primitives from
this module.
"""

from tle.util.akari_rating import (  # noqa: F401
    HistoryPoint,
    RatingState,
    compute_ratings,
    compute_round,
    rank_participants,
)

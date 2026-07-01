"""Pure helpers and constants for the codeforces cog.

Split out of ``codeforces.py`` to keep each module under the line limit.
"""
from typing import List

from discord.ext import commands

_GITGUD_NO_SKIP_TIME = 2 * 60 * 60
_GITGUD_SCORE_DISTRIB = (1, 2, 3, 5, 8, 12, 17, 23)
_GITGUD_SCORE_DISTRIB_MIN = -400
_GITGUD_SCORE_DISTRIB_MAX = 300
_ONE_WEEK_DURATION = 7 * 24 * 60 * 60
_GITGUD_MORE_POINTS_START_TIME = 1680300000
# Completing a gitgud challenge also credits the betting wallet with this many
# coins per base gitgud point. Always applied to the *base* score, never the
# end-of-month-doubled monthly points — the coin rate is a flat 5x.
_GITGUD_COIN_MULTIPLIER = 5


def _calculateGitgudScoreForDelta(delta):
    if (delta <= _GITGUD_SCORE_DISTRIB_MIN):
        return _GITGUD_SCORE_DISTRIB[0]
    if (delta >= _GITGUD_SCORE_DISTRIB_MAX):
        return _GITGUD_SCORE_DISTRIB[-1]
    index = (delta - _GITGUD_SCORE_DISTRIB_MIN)//100
    return _GITGUD_SCORE_DISTRIB[index]


def _gitgudTagPenaltyDelta(base_delta, num_tags):
    """Shrink a challenge's payout by the number of requested tags.

    Points are worth ``base_score // (num_tags + 1)`` (floored, never below 1),
    so one tag already halves the reward and piling on tags collapses it toward
    the 1-point floor. This defangs tag-spam: banning every hard category so an
    easy high-rated problem slips through used to still pay near-max points.

    The whole system derives points from the stored ``rating_delta`` via
    :func:`_calculateGitgudScoreForDelta`, so we translate the reduced score
    back into a delta on that score ladder, rounding DOWN to the nearest
    achievable rung (never inflating). Returns ``base_delta`` unchanged when no
    tags were requested.
    """
    if num_tags <= 0:
        return base_delta
    target = max(1, _calculateGitgudScoreForDelta(base_delta) // (num_tags + 1))
    # Walk the ascending score ladder and keep the delta of the largest rung
    # whose score is still <= target. DISTRIB[i] is reached at this delta.
    penalized = _GITGUD_SCORE_DISTRIB_MIN  # rung 0 -> 1 point, the floor
    for i, score in enumerate(_GITGUD_SCORE_DISTRIB):
        if score > target:
            break
        penalized = _GITGUD_SCORE_DISTRIB_MIN + i * 100
    return penalized


class CodeforcesCogError(commands.CommandError):
    pass


def getEloWinProbability(ra: float, rb: float) -> float:
    return 1.0 / (1 + 10**((rb - ra) / 400.0))


def composeRatings(left: float, right: float, ratings: List[float]) -> int:
    for tt in range(20):
        r = (left + right) / 2.0

        rWinsProbability = 1.0
        for rating, count in ratings:
            rWinsProbability *= getEloWinProbability(r, rating)**count

        if rWinsProbability < 0.5:
            left = r
        else:
            right = r
    return round((left + right) / 2)

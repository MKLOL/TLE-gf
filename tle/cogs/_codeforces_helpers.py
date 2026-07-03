"""Pure helpers and constants for the codeforces cog.

Split out of ``codeforces.py`` to keep each module under the line limit.
"""
from typing import List

from discord.ext import commands

_GITGUD_NO_SKIP_TIME = 2 * 60 * 60
_GITGUD_SCORE_DISTRIB = (1, 2, 3, 5, 8, 12, 17, 23)
_GITGUD_SCORE_DISTRIB_MIN = -400
_GITGUD_SCORE_DISTRIB_MAX = 300
_GITGUD_EXACT_SCORE_DELTA_BASE = -10**9
_ONE_WEEK_DURATION = 7 * 24 * 60 * 60
_GITGUD_MORE_POINTS_START_TIME = 1680300000
# Completing a gitgud challenge also credits the betting wallet with this many
# coins per base gitgud point. Always applied to the *base* score, never the
# end-of-month-doubled monthly points — the coin rate is a flat 5x.
_GITGUD_COIN_MULTIPLIER = 5
_GITGUD_FREE_REQUIRED_TAGS = {'div1'}
_GITGUD_FREE_BANNED_TAGS = {'div3', 'div4', 'edu'}


def _gitgudEncodeExactScoreAsDelta(score):
    return _GITGUD_EXACT_SCORE_DELTA_BASE - score


def _gitgudDecodeExactScoreDelta(delta):
    score = _GITGUD_EXACT_SCORE_DELTA_BASE - delta
    if _GITGUD_SCORE_DISTRIB[0] <= score <= _GITGUD_SCORE_DISTRIB[-1]:
        return score
    return None


def _calculateGitgudScoreForDelta(delta):
    exact_score = _gitgudDecodeExactScoreDelta(delta)
    if exact_score is not None:
        return exact_score
    if (delta <= _GITGUD_SCORE_DISTRIB_MIN):
        return _GITGUD_SCORE_DISTRIB[0]
    if (delta >= _GITGUD_SCORE_DISTRIB_MAX):
        return _GITGUD_SCORE_DISTRIB[-1]
    index = (delta - _GITGUD_SCORE_DISTRIB_MIN)//100
    return _GITGUD_SCORE_DISTRIB[index]


def _gitgudPenalisedTagCount(tags, bantags):
    """How many requested tags subtract points.

    Every ``+`` require and ``~`` ban counts like a topic tag unless it is a
    division filter that makes the pool harder instead of easier:

    * ``+div1`` is free because it restricts to the hardest division.
    * ``~div3``, ``~div4`` and ``~edu`` are free because they remove easier
      contest pools.

    Banning div1 (``~div1``) still counts, since that only makes the pool
    easier.
    """
    required = sum(1 for tag in tags
                   if tag.strip().lower() not in _GITGUD_FREE_REQUIRED_TAGS)
    banned = sum(1 for tag in bantags
                 if tag.strip().lower() not in _GITGUD_FREE_BANNED_TAGS)
    return required + banned


def _gitgudTagPenaltyDelta(base_delta, num_tags):
    """Shrink a challenge's payout by the number of requested tags.

    Points are worth ``ceil(base_score / (num_tags + 1))`` (never below 1), so
    one tag roughly halves the reward and tag-spamming an easy high-rated
    problem past the filters collapses it toward the 1-point floor.

    The whole system derives points from the stored ``rating_delta`` via
    :func:`_calculateGitgudScoreForDelta`, so penalised scores are stored as a
    reserved exact-score delta. Returns ``base_delta`` unchanged when no tags
    were requested.
    """
    if num_tags <= 0:
        return base_delta
    base_score = _calculateGitgudScoreForDelta(base_delta)
    target = max(1, (base_score + num_tags) // (num_tags + 1))
    return _gitgudEncodeExactScoreAsDelta(target)


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

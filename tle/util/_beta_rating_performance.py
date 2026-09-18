"""Proper-score updates and monotone event performance for beta minigames."""

import math


_RATING_POINT_SCALE = 2.0
_ELO_SCALE = _RATING_POINT_SCALE * 400.0 / math.log(10.0)
# Keep ten percent of the ordinary log-loss gradient so a confident model can
# never dismiss contradictory evidence. The other ninety percent supplies the
# bounded influence of the Brier gradient.
_BRIER_BLEND = 0.90
_PERFORMANCE_SEARCH_MARGIN = _RATING_POINT_SCALE * 800.0
_PERFORMANCE_SEARCH_ITERS = 60


def _sigmoid(value):
    """Numerically stable logistic function."""
    if value >= 0:
        exp_neg = math.exp(-value)
        return 1.0 / (1.0 + exp_neg)
    exp_pos = math.exp(value)
    return exp_pos / (1.0 + exp_pos)


def _elo_expected(
        rating, opponent_rating, *, point_scale=_RATING_POINT_SCALE):
    point_scale = float(point_scale)
    if not math.isfinite(point_scale) or point_scale <= 0:
        raise ValueError('Beta rating point scale must be finite and positive.')
    elo_scale = point_scale * 400.0 / math.log(10.0)
    return _sigmoid((float(rating) - float(opponent_rating)) / elo_scale)


def _field_expected(
        performance, field_ratings, *, point_scale=_RATING_POINT_SCALE):
    return sum(
        _elo_expected(performance, rating, point_scale=point_scale)
        for rating in field_ratings
    ) / len(field_ratings)


def _proper_residual(score, expected):
    """Negative rating-logit gradient of the blended proper scoring loss."""
    brier_weight = 4.0 * expected * (1.0 - expected)
    weight = 1.0 - _BRIER_BLEND + _BRIER_BLEND * brier_weight
    return weight * (score - expected)


def _performance_rating(
        field_ratings, target_score, *, point_scale=_RATING_POINT_SCALE):
    """Invert the field expectation into one unique event performance.

    The proper-score update deliberately has a bounded-influence gradient.
    Using that non-convex composite itself for display can expose multiple
    local minima and invert two players' result order.  Performance therefore
    uses the ordinary monotone field expectation: the rating whose expected
    score equals the player's mean soft result against this exact field.
    """
    point_scale = float(point_scale)
    if not math.isfinite(point_scale) or point_scale <= 0:
        raise ValueError('Beta rating point scale must be finite and positive.')
    if not field_ratings:
        raise ValueError('Performance requires a non-empty rating field.')
    target_score = float(target_score)
    if not math.isfinite(target_score) or not 0 < target_score < 1:
        raise ValueError(
            f'Performance score must be finite and in (0, 1), '
            f'got {target_score!r}.')

    span = point_scale * 800.0
    lo = min(field_ratings) - span
    hi = max(field_ratings) + span

    # A neutral self-result keeps normal replay targets strictly inside
    # (0, 1), but expand defensively for unusually wide imported fields.
    while _field_expected(
            lo, field_ratings, point_scale=point_scale) > target_score:
        span *= 2.0
        lo = min(field_ratings) - span
    span = point_scale * 800.0
    while _field_expected(
            hi, field_ratings, point_scale=point_scale) < target_score:
        span *= 2.0
        hi = max(field_ratings) + span

    for _ in range(_PERFORMANCE_SEARCH_ITERS):
        mid = (lo + hi) / 2.0
        if _field_expected(
                mid, field_ratings, point_scale=point_scale) < target_score:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0

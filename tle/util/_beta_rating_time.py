"""Time validation and hybrid pair scoring for beta minigame ratings."""

import math

from tle.util._beta_rating_performance import _sigmoid


_TIME_MARGIN_WIDTH = 0.35
_HEAD_TO_HEAD_WEIGHT = 0.15
# Bounds one pair's evidence, not a player's rating change. It activates only
# beyond a 16.4x raw-time ratio and prevents numerical 0/1 separation.
_TIME_MARGIN_LOGIT_LIMIT = 8.0


def _time_log(time_seconds):
    """Return the raw log-time used by the daily performance bracket."""
    try:
        if isinstance(time_seconds, int):
            seconds = time_seconds
        else:
            seconds = float(time_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f'Queens time must be numeric, got {time_seconds!r}.') from exc
    if (
            isinstance(seconds, float) and not math.isfinite(seconds)
            or seconds <= 0):
        raise ValueError(
            f'Queens time must be finite and positive, '
            f'got {time_seconds!r}.')
    # Keep integer inputs as integers so unexpectedly huge legacy values can be
    # logged without overflowing an intermediate float conversion.
    return math.log(seconds)


def _result_time_seconds(time_seconds):
    """Validate one stored result time, returning its integral seconds."""
    try:
        seconds = int(time_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f'Queens result time must be an integer, got {time_seconds!r}.'
        ) from exc
    if seconds <= 0:
        raise ValueError(
            f'Queens result time must be positive, got {time_seconds!r}.')
    if not isinstance(time_seconds, str) and time_seconds != seconds:
        raise ValueError(
            f'Queens result time must be an integer, got {time_seconds!r}.')
    _time_log(seconds)
    return seconds


def _soft_time_score(time_self, time_other):
    """Return continuous pair evidence from the raw time ratio."""
    return _soft_time_score_from_logs(
        _time_log(time_self), _time_log(time_other))


def _soft_time_score_from_logs(log_self, log_other):
    """Return bounded pair evidence from two already-transformed times."""
    logit = (log_other - log_self) / _TIME_MARGIN_WIDTH
    logit = max(
        -_TIME_MARGIN_LOGIT_LIMIT,
        min(_TIME_MARGIN_LOGIT_LIMIT, logit),
    )
    return _sigmoid(logit)


def _hard_time_score(time_self, time_other):
    """Return the strict faster/slower result, with exact ties neutral."""
    if time_self == time_other:
        return 0.5
    return float(time_self < time_other)


def _blend_pair_score(margin_score, head_to_head_score):
    """Blend continuous margin evidence with one bounded hard result."""
    return (
        (1.0 - _HEAD_TO_HEAD_WEIGHT) * margin_score
        + _HEAD_TO_HEAD_WEIGHT * head_to_head_score
    )


def _hybrid_time_score(time_self, time_other):
    """Return the 85% time-margin, 15% head-to-head pair score."""
    self_seconds = _result_time_seconds(time_self)
    other_seconds = _result_time_seconds(time_other)
    return _blend_pair_score(
        _soft_time_score(self_seconds, other_seconds),
        _hard_time_score(self_seconds, other_seconds),
    )

"""Akari adapter for the margin-aware beta rating replay."""

import math

from tle.util.queens_improved_rating import (
    _blend_pair_score,
    _hybrid_time_score,
    _result_time_seconds,
    _soft_time_score_from_logs,
    _time_log,
    compute_queens_improved_ratings,
)


# One downward step makes ``1 - score`` round back to exactly 0.5. Two keep
# both sides of an extreme accuracy mismatch strictly separated.
_STRICT_ACCURACY_LOSS = math.nextafter(
    math.nextafter(0.5, 0.0), 0.0)


def _validated_accuracy(raw_accuracy):
    """Return one validated integral Akari accuracy percentage."""
    try:
        accuracy = int(raw_accuracy)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f'Akari accuracy must be an integer, got {raw_accuracy!r}.'
        ) from exc
    if not isinstance(raw_accuracy, str) and raw_accuracy != accuracy:
        raise ValueError(
            f'Akari accuracy must be an integer, got {raw_accuracy!r}.')
    if not 0 <= accuracy <= 100:
        raise ValueError(
            f'Akari accuracy must be between 0 and 100, got {accuracy}.')
    return accuracy


def _akari_accuracy(row):
    return _validated_accuracy(getattr(row, 'accuracy', None))


def _softplus(value):
    """Return ``log(1 + exp(value))`` without overflow."""
    return max(value, 0.0) + math.log1p(math.exp(-abs(value)))


def _lower_accuracy_score(lower_time_log, higher_time_log):
    """Score ``lower_time + higher_time`` against ``higher_time``."""
    ratio_log = _softplus(lower_time_log - higher_time_log)
    score = _soft_time_score_from_logs(ratio_log, 0.0)
    return min(score, _STRICT_ACCURACY_LOSS)


def _akari_time_log(row):
    seconds = _result_time_seconds(getattr(row, 'time_seconds', None))
    return _time_log(seconds)


def _akari_beta_pair_score(row, opponent):
    """Blend accuracy-first margin evidence with the hard result."""
    accuracy = _akari_accuracy(row)
    opponent_accuracy = _akari_accuracy(opponent)
    if accuracy == opponent_accuracy:
        return _hybrid_time_score(
            row.time_seconds, opponent.time_seconds)

    time_log = _akari_time_log(row)
    opponent_time_log = _akari_time_log(opponent)
    if accuracy < opponent_accuracy:
        margin_score = _lower_accuracy_score(time_log, opponent_time_log)
    else:
        margin_score = 1.0 - _lower_accuracy_score(
            opponent_time_log, time_log)
    return _blend_pair_score(
        margin_score, float(accuracy > opponent_accuracy))


def _akari_beta_performance_pair_score(row, opponent):
    """Accuracy hierarchy for monotone event performance display."""
    accuracy = _akari_accuracy(row)
    opponent_accuracy = _akari_accuracy(opponent)
    time_seconds = _result_time_seconds(row.time_seconds)
    opponent_time_seconds = _result_time_seconds(opponent.time_seconds)
    if accuracy != opponent_accuracy:
        return float(accuracy > opponent_accuracy)
    return _hybrid_time_score(time_seconds, opponent_time_seconds)


def compute_akari_beta_ratings(rows, **kwargs):
    """Replay Akari with additive rating evidence and hierarchical perf."""
    return compute_queens_improved_ratings(
        rows,
        pair_score_fn=_akari_beta_pair_score,
        performance_pair_score_fn=_akari_beta_performance_pair_score,
        row_validator_fn=_akari_accuracy,
        **kwargs,
    )

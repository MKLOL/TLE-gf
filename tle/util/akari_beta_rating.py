"""Akari adapter for the margin-aware beta rating replay."""

import math

from tle.util.queens_improved_rating import (
    _soft_time_score_from_logs,
    _time_log,
    compute_queens_improved_ratings,
)


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


def _akari_accuracy_multiplier(accuracy):
    """Diminishing time penalty: 100%=1x, 99%=sqrt(2)x, and so on."""
    accuracy = _validated_accuracy(accuracy)
    return math.sqrt(101 - accuracy)


def _akari_effective_time_log(row):
    """Combine raw time and accuracy without an accuracy cliff.

    A 99% result must be about 29% faster to tie a perfect result.  Lower
    percentages keep worsening, but the square root avoids the explosive
    ``2x, 3x, ...`` penalty that would quickly recreate hard 0/1 outcomes.
    """
    accuracy = _akari_accuracy(row)
    return (
        _time_log(getattr(row, 'time_seconds', None))
        + 0.5 * math.log(101 - accuracy)
    )


def _akari_beta_pair_score(row, opponent):
    """Compare smoothly accuracy-adjusted Akari times."""
    return _soft_time_score_from_logs(
        _akari_effective_time_log(row),
        _akari_effective_time_log(opponent),
    )


def compute_akari_beta_ratings(rows, **kwargs):
    """Replay Akari with smooth accuracy-adjusted time outcomes."""
    return compute_queens_improved_ratings(
        rows,
        pair_score_fn=_akari_beta_pair_score,
        row_validator_fn=_akari_accuracy,
        **kwargs,
    )

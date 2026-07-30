"""Akari adapter for the margin-aware beta rating replay."""

from tle.util.queens_improved_rating import (
    _soft_time_score,
    compute_queens_improved_ratings,
)


def _akari_beta_pair_score(row, opponent):
    """Respect Akari quality first, softening time only within one class."""
    perfect = bool(getattr(row, 'is_perfect', False))
    other_perfect = bool(getattr(opponent, 'is_perfect', False))
    if perfect != other_perfect:
        return 1.0 if perfect else 0.0

    accuracy = int(getattr(row, 'accuracy', 0))
    other_accuracy = int(getattr(opponent, 'accuracy', 0))
    if accuracy != other_accuracy:
        return 1.0 if accuracy > other_accuracy else 0.0

    return _soft_time_score(
        getattr(row, 'time_seconds', None),
        getattr(opponent, 'time_seconds', None),
    )


def compute_akari_beta_ratings(rows, **kwargs):
    """Replay Akari with quality-aware outcomes and soft same-class times."""
    return compute_queens_improved_ratings(
        rows, pair_score_fn=_akari_beta_pair_score, **kwargs)

"""Pure tests for Akari's quality-aware beta rating adapter."""

import math
import random
from collections import namedtuple

from tle.util.akari_beta_rating import compute_akari_beta_ratings


Result = namedtuple(
    'Result',
    'message_id user_id puzzle_number puzzle_date '
    'time_seconds is_perfect accuracy raw_content',
)


def _row(user_id, *, seconds, perfect, accuracy, message_id=None, puzzle=1):
    return Result(
        message_id=str(message_id or user_id),
        user_id=str(user_id),
        puzzle_number=puzzle,
        puzzle_date='2026-07-30',
        time_seconds=seconds,
        is_perfect=perfect,
        accuracy=accuracy,
        raw_content='',
    )


def _one_round(rows):
    histories = {}
    states = compute_akari_beta_ratings(rows, histories=histories)
    return states, histories


def test_perfect_result_beats_faster_imperfect_result():
    states, histories = _one_round([
        _row(1, seconds=120, perfect=True, accuracy=100),
        _row(2, seconds=5, perfect=False, accuracy=99),
    ])

    assert states['1'].rating > 1200 > states['2'].rating
    assert histories['1'][0].performance > histories['2'][0].performance


def test_higher_accuracy_beats_faster_lower_accuracy():
    states, histories = _one_round([
        _row(1, seconds=120, perfect=False, accuracy=80),
        _row(2, seconds=5, perfect=False, accuracy=79),
    ])

    assert states['1'].rating > 1200 > states['2'].rating
    assert histories['1'][0].performance > histories['2'][0].performance


def test_same_quality_uses_soft_time_margin_and_equal_results_tie():
    close_states, _ = _one_round([
        _row(1, seconds=12, perfect=True, accuracy=100),
        _row(2, seconds=13, perfect=True, accuracy=100),
    ])
    wide_states, _ = _one_round([
        _row(1, seconds=12, perfect=True, accuracy=100),
        _row(2, seconds=30, perfect=True, accuracy=100),
    ])
    tied_states, tied_histories = _one_round([
        _row(1, seconds=12, perfect=False, accuracy=95),
        _row(2, seconds=12, perfect=False, accuracy=95),
    ])

    assert 0 < close_states['1'].rating - 1200
    assert (
        close_states['1'].rating - 1200
        < wide_states['1'].rating - 1200
    )
    assert {state.rating for state in tied_states.values()} == {1200}
    assert {
        point.performance
        for history in tied_histories.values()
        for point in history
    } == {1200}


def test_round_is_zero_sum_finite_and_deterministic():
    rows = [
        _row(1, seconds=80, perfect=True, accuracy=100),
        _row(2, seconds=15, perfect=False, accuracy=99),
        _row(3, seconds=20, perfect=False, accuracy=99),
        _row(4, seconds=10, perfect=False, accuracy=80),
    ]
    expected, expected_histories = _one_round(rows)
    shuffled = list(rows)
    random.Random(173).shuffle(shuffled)
    actual, actual_histories = _one_round(shuffled)

    assert actual == expected
    assert actual_histories == expected_histories
    assert abs(sum(state.rating - 1200 for state in actual.values())) < 1e-9
    assert all(math.isfinite(state.rating) for state in actual.values())


def test_solo_day_stays_unrated():
    states, histories = _one_round([
        _row(1, seconds=20, perfect=True, accuracy=100),
    ])

    assert states['1'].rating == 1200
    assert states['1'].games == 0
    assert histories['1'][0].performance is None

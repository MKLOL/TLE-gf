"""Pure tests for Akari's quality-aware beta rating adapter."""

import math
import random
from collections import namedtuple
from decimal import Decimal

from tle.util.akari_beta_rating import (
    _akari_beta_pair_score,
    compute_akari_beta_ratings,
)
from tle.util.queens_improved_rating import _soft_time_score


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


def test_accuracy_uses_parameter_free_sqrt_effective_time():
    baseline = _row(
        1, seconds=100, perfect=True, accuracy=100)
    for accuracy in (100, 99, 98, 97, 90, 68, 0):
        candidate = _row(
            2, seconds=100, perfect=False, accuracy=accuracy)
        multiplier = math.sqrt(101 - accuracy)
        expected = _soft_time_score(100 * multiplier, 100)
        assert math.isclose(
            _akari_beta_pair_score(candidate, baseline),
            expected,
            rel_tol=0,
            abs_tol=1e-15,
        )


def test_accuracy_speed_tradeoff_has_the_expected_break_even():
    perfect = _row(
        1, seconds=100, perfect=True, accuracy=100)
    tied_99 = _row(
        2, seconds=100 / math.sqrt(2),
        perfect=False, accuracy=99)
    faster_99 = _row(
        3, seconds=60, perfect=False, accuracy=99)
    slower_99 = _row(
        4, seconds=80, perfect=False, accuracy=99)
    exactly_tied_97 = _row(
        5, seconds=50, perfect=False, accuracy=97)

    assert math.isclose(
        _akari_beta_pair_score(tied_99, perfect),
        0.5,
        rel_tol=0,
        abs_tol=1e-15,
    )
    assert _akari_beta_pair_score(faster_99, perfect) > 0.5
    assert _akari_beta_pair_score(slower_99, perfect) < 0.5
    assert math.isclose(
        _akari_beta_pair_score(exactly_tied_97, perfect),
        0.5,
        rel_tol=0,
        abs_tol=1e-15,
    )


def test_equal_time_99_percent_has_a_meaningful_smooth_penalty():
    perfect = _row(
        1, seconds=100, perfect=True, accuracy=100)
    almost = _row(
        2, seconds=100, perfect=False, accuracy=99)

    expected = _soft_time_score(100 * math.sqrt(2), 100)
    score = _akari_beta_pair_score(almost, perfect)
    assert math.isclose(score, expected, rel_tol=0, abs_tol=1e-15)
    assert 0.27 < score < 0.28

    states, histories = _one_round([perfect, almost])
    assert states['1'].rating > 1200 > states['2'].rating
    assert histories['1'][0].performance > histories['2'][0].performance


def test_perfect_flag_does_not_add_a_hidden_accuracy_tier():
    marked_perfect = _row(
        1, seconds=50, perfect=True, accuracy=100)
    plain_100 = _row(
        2, seconds=50, perfect=False, accuracy=100)

    assert _akari_beta_pair_score(marked_perfect, plain_100) == 0.5
    assert _akari_beta_pair_score(plain_100, marked_perfect) == 0.5


def test_performance_follows_effective_time_across_accuracy_levels():
    rows = [
        _row(1, seconds=100, perfect=True, accuracy=100),
        _row(2, seconds=60, perfect=False, accuracy=99),
        _row(3, seconds=80, perfect=False, accuracy=98),
        _row(4, seconds=40, perfect=False, accuracy=90),
        _row(5, seconds=20, perfect=False, accuracy=68),
        # 50 * sqrt(4) ties 100 * sqrt(1) exactly.
        _row(6, seconds=50, perfect=False, accuracy=97),
    ]
    _states, histories = _one_round(rows)
    performances = {
        int(user_id): points[0].performance
        for user_id, points in histories.items()
    }
    effective = {
        int(row.user_id): (
            row.time_seconds * math.sqrt(101 - row.accuracy)
        )
        for row in rows
    }

    for left in effective:
        for right in effective:
            if effective[left] < effective[right]:
                assert performances[left] > performances[right]
            elif effective[left] == effective[right]:
                assert math.isclose(
                    performances[left], performances[right],
                    rel_tol=0, abs_tol=1e-9)


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


def test_invalid_accuracy_is_quarantined_after_first_result_locking():
    rows = [
        _row(1, seconds=20, perfect=False, accuracy=101, message_id=1),
        # A later valid result must not replace the malformed locked first one.
        _row(1, seconds=5, perfect=True, accuracy=100, message_id=99),
        _row(2, seconds=20, perfect=False, accuracy=-1, message_id=2),
        _row(
            5, seconds=20, perfect=False,
            accuracy=Decimal('99.5'), message_id=5),
        _row(3, seconds=20, perfect=True, accuracy=100, message_id=3),
        _row(4, seconds=30, perfect=True, accuracy=100, message_id=4),
    ]

    states = compute_akari_beta_ratings(rows)

    assert set(states) == {'3', '4'}
    assert states['3'].games == states['4'].games == 1
    assert states['3'].rating > 1200 > states['4'].rating

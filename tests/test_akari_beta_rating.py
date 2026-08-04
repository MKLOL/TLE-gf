"""Pure tests for Akari's quality-aware beta rating adapter."""

import math
import random
from collections import namedtuple
from decimal import Decimal

from tle.util.akari_beta_rating import (
    _akari_beta_pair_score,
    _akari_beta_performance_pair_score,
    compute_akari_beta_ratings,
)
from tle.util.queens_improved_rating import (
    _FIELD_DEFLATION,
    _HEAD_TO_HEAD_WEIGHT,
    _hybrid_time_score,
    _soft_time_score,
)


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


def test_additive_pair_score_uses_higher_accuracy_time_as_denominator():
    higher = _row(1, seconds=100, perfect=True, accuracy=100)
    lower = _row(2, seconds=50, perfect=False, accuracy=99)

    margin_lower = _soft_time_score(50 + 100, 100)
    expected_lower = (1 - _HEAD_TO_HEAD_WEIGHT) * margin_lower
    lower_score = _akari_beta_pair_score(lower, higher)

    assert math.isclose(lower_score, expected_lower, rel_tol=0, abs_tol=1e-15)
    assert lower_score < 0.425
    assert math.isclose(
        _akari_beta_pair_score(higher, lower),
        1.0 - expected_lower,
        rel_tol=0,
        abs_tol=1e-15,
    )


def test_lower_accuracy_never_wins_or_benefits_from_taking_longer():
    higher = _row(1, seconds=100, perfect=True, accuracy=100)
    lower_rows = [
        _row(2, seconds=1, perfect=False, accuracy=99),
        _row(3, seconds=100, perfect=False, accuracy=99),
        _row(4, seconds=200, perfect=False, accuracy=99),
    ]
    scores = [
        _akari_beta_pair_score(lower, higher) for lower in lower_rows
    ]

    assert 0 < scores[2] < scores[1] < scores[0] < 0.5
    equally_slow_but_much_lower = _row(
        5, seconds=100, perfect=False, accuracy=0)
    assert (
        _akari_beta_pair_score(equally_slow_but_much_lower, higher)
        == scores[1]
    )


def test_extreme_ratio_keeps_a_strict_complementary_accuracy_result():
    higher = _row(
        1, seconds=2 ** 63 - 1, perfect=True, accuracy=100)
    lower = _row(2, seconds=1, perfect=False, accuracy=99)

    lower_score = _akari_beta_pair_score(lower, higher)
    higher_score = _akari_beta_pair_score(higher, lower)

    assert 0 < lower_score < 0.5 < higher_score < 1
    assert lower_score + higher_score == 1.0


def test_large_integral_string_times_are_normalized_before_scoring():
    rows = [
        _row(
            1, seconds='1' + '0' * 309,
            perfect=True, accuracy=100),
        _row(2, seconds='1', perfect=False, accuracy=99),
    ]

    states, histories = _one_round(rows)

    assert set(states) == {'1', '2'}
    assert all(math.isfinite(state.rating) for state in states.values())
    assert histories['1'][0].performance > histories['2'][0].performance


def test_pair_scores_are_complementary_across_accuracy_and_time():
    rows = [
        _row(1, seconds=1, perfect=True, accuracy=100),
        _row(2, seconds=10, perfect=False, accuracy=100),
        _row(3, seconds=2, perfect=False, accuracy=99),
        _row(4, seconds=200, perfect=False, accuracy=0),
        _row(5, seconds=10 ** 10_000, perfect=False, accuracy=50),
    ]
    for left in rows:
        for right in rows:
            assert math.isclose(
                _akari_beta_pair_score(left, right)
                + _akari_beta_pair_score(right, left),
                1.0,
                rel_tol=0,
                abs_tol=1e-15,
            )


def test_equal_accuracy_blends_time_margin_with_head_to_head_result():
    fast = _row(1, seconds=10, perfect=False, accuracy=95)
    slow = _row(2, seconds=20, perfect=False, accuracy=95)
    tied = _row(3, seconds=10, perfect=False, accuracy=95)

    assert _akari_beta_pair_score(fast, slow) == _hybrid_time_score(10, 20)
    assert _akari_beta_pair_score(fast, slow) > 0.5
    assert _akari_beta_pair_score(slow, fast) < 0.5
    assert _akari_beta_pair_score(fast, tied) == 0.5


def test_display_performance_uses_a_hard_accuracy_hierarchy():
    higher = _row(1, seconds=1000, perfect=True, accuracy=100)
    lower = _row(2, seconds=1, perfect=False, accuracy=99)

    assert _akari_beta_performance_pair_score(higher, lower) == 1.0
    assert _akari_beta_performance_pair_score(lower, higher) == 0.0

    equal_accuracy_slow = _row(
        3, seconds=10, perfect=False, accuracy=99)
    assert _akari_beta_performance_pair_score(
        equal_accuracy_slow, lower) == _hybrid_time_score(10, 1)


def test_perfect_flag_does_not_add_a_hidden_accuracy_tier():
    marked_perfect = _row(
        1, seconds=50, perfect=True, accuracy=100)
    plain_100 = _row(
        2, seconds=50, perfect=False, accuracy=100)

    assert _akari_beta_pair_score(marked_perfect, plain_100) == 0.5
    assert _akari_beta_pair_score(plain_100, marked_perfect) == 0.5


def test_performance_is_accuracy_first_then_time_even_in_adversarial_field():
    rows = [
        _row(1, seconds=1000, perfect=True, accuracy=100),
        _row(2, seconds=1, perfect=False, accuracy=99),
        _row(3, seconds=1, perfect=False, accuracy=98),
        _row(4, seconds=2, perfect=False, accuracy=98),
    ]
    _states, histories = _one_round(rows)
    performances = {
        int(user_id): points[0].performance
        for user_id, points in histories.items()
    }

    assert (
        performances[1] > performances[2]
        > performances[3] > performances[4]
    )


def test_random_fields_keep_performance_in_accuracy_time_order():
    rng = random.Random(20260801)
    for _trial in range(40):
        rows = [
            _row(
                user_id,
                seconds=rng.randint(1, 3600),
                perfect=False,
                accuracy=rng.randint(0, 100),
            )
            for user_id in range(1, rng.randint(2, 20) + 1)
        ]
        states, histories = _one_round(rows)
        ordered = sorted(
            rows,
            key=lambda row: (-row.accuracy, row.time_seconds),
        )
        performances = {
            user_id: histories[user_id][0].performance
            for user_id in states
        }

        for better, worse in zip(ordered, ordered[1:]):
            better_result = (better.accuracy, -better.time_seconds)
            worse_result = (worse.accuracy, -worse.time_seconds)
            if better_result == worse_result:
                assert math.isclose(
                    performances[better.user_id],
                    performances[worse.user_id],
                    rel_tol=0,
                    abs_tol=1e-9,
                )
            else:
                assert (
                    performances[better.user_id]
                    > performances[worse.user_id]
                )


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
    assert {state.rating for state in tied_states.values()} == {
        1200 - _FIELD_DEFLATION
    }
    assert {
        point.performance
        for history in tied_histories.values()
        for point in history
    } == {1200}


def test_round_has_fixed_field_deflation_and_is_deterministic():
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
    assert math.isclose(
        sum(state.rating - 1200 for state in actual.values()),
        -len(actual) * _FIELD_DEFLATION,
        abs_tol=1e-9,
    )
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

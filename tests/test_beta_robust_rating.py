"""Properties of the shared robust beta update and performance inversion."""

import math
import random

from tle.util.akari_beta_rating import compute_akari_beta_ratings
from tle.util.queens_improved_rating import (
    _FIELD_DEFLATION,
    _RATING_K,
    _compute_round,
    _compute_round_from_pair_score,
    _elo_expected,
    _field_expected,
    _hybrid_time_score,
)

from tests.test_akari_beta_rating import _row


_ALPHA = 0.90


def _blend_weight(expected):
    return (1 - _ALPHA) + _ALPHA * 4 * expected * (1 - expected)


def _manual_deltas(ratings, times):
    users = sorted(ratings)
    count = len(users)
    deltas = {}
    for user in users:
        residual = 0.0
        for opponent in users:
            if opponent == user:
                continue
            score = _hybrid_time_score(times[user], times[opponent])
            expected = _elo_expected(
                ratings[user], ratings[opponent])
            residual += (
                _blend_weight(expected) * (score - expected))
        deltas[user] = _RATING_K * residual / count
    return deltas


def _mean_hybrid_score(user, ratings, times):
    return sum(
        0.5 if opponent == user
        else _hybrid_time_score(times[user], times[opponent])
        for opponent in sorted(ratings)
    ) / len(ratings)


def test_round_uses_pairwise_blended_proper_residuals():
    ratings = {
        'favorite': 2380.0,
        'middle': 1210.0,
        'underdog': 710.0,
        'peer': 1160.0,
    }
    times = {
        'favorite': 400,
        'middle': 35,
        'underdog': 18,
        'peer': 70,
    }

    expected = _manual_deltas(ratings, times)
    updates = _compute_round(ratings, times)

    for user in ratings:
        assert math.isclose(
            updates[user].delta, expected[user],
            rel_tol=0, abs_tol=1e-10,
        )
    assert abs(sum(update.delta for update in updates.values())) < 1e-10


def test_separate_performance_scores_do_not_change_rating_deltas():
    ratings = {'a': 1200.0, 'b': 1200.0}

    def rating_score(user, _opponent):
        return 0.6 if user == 'a' else 0.4

    def performance_score(user, _opponent):
        return 1.0 if user == 'a' else 0.0

    ordinary = _compute_round_from_pair_score(ratings, rating_score)
    separated = _compute_round_from_pair_score(
        ratings, rating_score,
        performance_pair_score_fn=performance_score)

    assert separated['a'].delta == ordinary['a'].delta
    assert separated['b'].delta == ordinary['b'].delta
    assert abs(sum(update.delta for update in separated.values())) < 1e-12
    assert separated['a'].performance > ordinary['a'].performance
    assert separated['b'].performance < ordinary['b'].performance


def test_skipped_performance_does_not_call_its_pair_callback():
    def unexpected(_user, _opponent):
        raise AssertionError('performance callback should not run')

    updates = _compute_round_from_pair_score(
        {'a': 1200.0, 'b': 1200.0},
        lambda user, _opponent: 0.6 if user == 'a' else 0.4,
        compute_performance=False,
        performance_pair_score_fn=unexpected)

    assert all(update.performance is None for update in updates.values())


def test_blend_keeps_an_extreme_probability_floor_without_full_elo_force():
    expected = 0.999
    weight = _blend_weight(expected)
    assert 0.10 < weight < 0.104

    # The surprise still has at least 10% of its ordinary Elo residual, unlike
    # a pure Brier gradient, while remaining strongly attenuated.
    ordinary = 0 - expected
    blended = weight * ordinary
    assert abs(ordinary) * 0.10 < abs(blended) < abs(ordinary) * 0.104


def test_random_rounds_conserve_points_and_are_translation_invariant():
    rng = random.Random(20260730)
    for _trial in range(80):
        count = rng.randint(2, 20)
        users = [f'u{index}' for index in range(count)]
        ratings = {
            user: rng.uniform(300, 2700)
            for user in users
        }
        times = {
            user: rng.randint(1, 3600)
            for user in users
        }
        updates = _compute_round(ratings, times)

        assert abs(sum(update.delta for update in updates.values())) < 1e-9
        natural_bound = _RATING_K * (count - 1) / count
        assert all(
            abs(update.delta) < natural_bound
            and math.isfinite(update.performance)
            for update in updates.values()
        )

        shifted = _compute_round(
            {user: rating + 777 for user, rating in ratings.items()},
            times,
        )
        for user in users:
            assert math.isclose(
                shifted[user].delta, updates[user].delta,
                rel_tol=0, abs_tol=1e-9,
            )
            assert math.isclose(
                shifted[user].performance,
                updates[user].performance + 777,
                rel_tol=0, abs_tol=1e-8,
            )


def test_performance_uniquely_inverts_the_mean_field_score():
    ratings = {
        'a': 740.0,
        'b': 2320.0,
        'c': 791.0,
    }
    times = {
        'a': 278,
        'b': 487,
        'c': 68,
    }
    updates = _compute_round(ratings, times)

    for user, update in updates.items():
        target = _mean_hybrid_score(user, ratings, times)
        assert math.isclose(
            _field_expected(
                update.performance, list(ratings.values())),
            target,
            rel_tol=0,
            abs_tol=1e-12,
        )


def test_performance_never_inverts_result_order_in_multiroot_field():
    # The blended proper loss has multiple stationary branches in this field.
    # Independent nearest-branch selection used to put the fastest player
    # below the middle player. The monotone field inversion is unambiguous.
    ratings = {
        'slow': 672.6748482137704,
        'middle': 2290.319660140686,
        'fast': 756.1681619308143,
    }
    times = {'slow': 557, 'middle': 315, 'fast': 262}
    updates = _compute_round(ratings, times)

    assert (
        updates['fast'].performance
        > updates['middle'].performance
        > updates['slow'].performance
    )


def test_tied_results_share_performance_even_with_unequal_ratings():
    # This deliberately has two stable blended-loss roots. Tied players must
    # still receive one common event performance; selecting a different root
    # merely because each player has a different prior would break tied ranks.
    ratings = {
        'high': 2057.707193831535,
        'low': -399.9569791093322,
        'middle': 600.1172734764771,
    }
    times = {user: 30 for user in ratings}
    updates = _compute_round(ratings, times)

    performances = [update.performance for update in updates.values()]
    assert max(performances) - min(performances) < 1e-8
    assert updates['low'].delta > 0
    assert updates['high'].delta < 0


def test_akari_replay_uses_the_same_blended_update_after_ratings_diverge():
    rows = [
        _row(1, seconds=10, perfect=True, accuracy=100, puzzle=1),
        _row(2, seconds=30, perfect=True, accuracy=100, puzzle=1),
        _row(1, seconds=30, perfect=True, accuracy=100,
             message_id=101, puzzle=2),
        _row(2, seconds=10, perfect=True, accuracy=100,
             message_id=102, puzzle=2),
    ]
    states = compute_akari_beta_ratings(rows)

    first_score = _hybrid_time_score(10, 30)
    first_delta = _RATING_K / 2 * (first_score - 0.5)
    pre_one = 1200 + first_delta - _FIELD_DEFLATION
    pre_two = 1200 - first_delta - _FIELD_DEFLATION
    second_score = _hybrid_time_score(30, 10)
    second_expected = _elo_expected(pre_one, pre_two)
    second_delta = (
        _RATING_K / 2
        * _blend_weight(second_expected)
        * (second_score - second_expected)
    )

    assert math.isclose(
        states['1'].rating,
        pre_one + second_delta - _FIELD_DEFLATION,
        rel_tol=0, abs_tol=1e-9,
    )
    assert math.isclose(
        states['2'].rating,
        2400 - 4 * _FIELD_DEFLATION - states['1'].rating,
        rel_tol=0, abs_tol=1e-9,
    )


def test_replay_can_limit_performance_work_to_one_results_day():
    rows = [
        _row(1, seconds=10, perfect=True, accuracy=100, puzzle=1),
        _row(2, seconds=20, perfect=True, accuracy=100, puzzle=1),
        _row(1, seconds=20, perfect=True, accuracy=100,
             message_id=101, puzzle=2),
        _row(2, seconds=10, perfect=True, accuracy=100,
             message_id=102, puzzle=2),
    ]
    histories = {}
    limited = compute_akari_beta_ratings(
        rows, histories=histories, performance_puzzles={2})
    full = compute_akari_beta_ratings(rows)

    assert limited == full
    assert all(points[0].performance is None for points in histories.values())
    assert all(
        points[1].performance is not None for points in histories.values())

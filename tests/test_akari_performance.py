"""Displayed event performance on the canonical ladder.

These pin the three properties the previous ``2 * need - rating`` formula did
not have: identical results print identical numbers, a win or a last place is
finite, and performance never contradicts the finish order.
"""

import math
from collections import namedtuple

from tle.util.akari_rating import (
    _expected_losses,
    _pow10,
    compute_ratings,
    compute_round,
    event_performance,
)

Row = namedtuple('Row', 'user_id puzzle_number accuracy time_seconds is_perfect')


def _row(user_id, puzzle, time_seconds, accuracy=100, is_perfect=True):
    return Row(user_id, puzzle, accuracy, time_seconds, is_perfect)


def _perf(ratings, ranks):
    """Run one round for its performances only."""
    out = {}
    compute_round(ratings, ranks, performances=out)
    return out


class TestEventPerformanceMath:
    def test_two_equal_players_are_symmetric_about_their_rating(self):
        # Both at 1200: the winner needs x_P = 3 * x_1200 to make the two
        # loss terms sum to 0.5, i.e. exactly 400 * log10(3) above 1200.
        field = [_pow10(1200.0), _pow10(1200.0)]
        offset = 400.0 * math.log10(3.0)
        assert abs(event_performance(field, 1) - (1200 + offset)) < 0.01
        assert abs(event_performance(field, 2) - (1200 - offset)) < 0.01

    def test_solves_the_stated_equation(self):
        field = [_pow10(r) for r in (900.0, 1200.0, 1450.0, 1700.0)]
        for rank in (1, 2, 3, 4):
            perf = event_performance(field, rank)
            assert abs(_expected_losses(_pow10(perf), field) - (rank - 0.5)) < 1e-3

    def test_finite_at_both_extremes(self):
        # The self-excluding seed(P) == rank definition is unbounded here; this
        # one must land inside the field's neighbourhood.
        field = [_pow10(1200.0)] * 8
        assert 1200 < event_performance(field, 1) < 2200
        assert 200 < event_performance(field, 8) < 1200

    def test_strictly_decreasing_in_rank(self):
        field = [_pow10(r) for r in (1000.0, 1150.0, 1300.0, 1450.0, 1600.0)]
        perfs = [event_performance(field, rank) for rank in range(1, 6)]
        assert perfs == sorted(perfs, reverse=True)

    def test_depends_only_on_the_field_and_rank(self):
        # Same multiset of ratings, listed in a different order.
        a = [_pow10(r) for r in (1000.0, 1500.0, 1200.0)]
        b = [_pow10(r) for r in (1500.0, 1200.0, 1000.0)]
        for rank in (1, 2, 3):
            assert abs(event_performance(a, rank) - event_performance(b, rank)) < 1e-9


class TestPerformanceInRounds:
    def test_tied_players_get_identical_performance(self):
        # The old formula split this pair by ~12 points purely on their own
        # ratings; identical results must print an identical number.
        ratings = {'a': 1373.4, 'b': 1286.1, 'c': 1678.8, 'd': 941.0}
        ranks = {'a': 1, 'b': 1, 'c': 3, 'd': 4}
        perfs = _perf(ratings, ranks)
        assert abs(perfs['a'] - perfs['b']) < 1e-9

    def test_tie_safety_holds_across_a_wide_rating_gap(self):
        ratings = {'low': 800.0, 'high': 2000.0, 'mid': 1400.0}
        ranks = {'low': 1, 'high': 1, 'mid': 3}
        perfs = _perf(ratings, ranks)
        assert abs(perfs['low'] - perfs['high']) < 1e-9

    def test_performance_never_contradicts_finish_order(self):
        ratings = {'a': 1700.0, 'b': 900.0, 'c': 1250.0, 'd': 1500.0, 'e': 1000.0}
        ranks = {'a': 5, 'b': 1, 'c': 3, 'd': 4, 'e': 2}
        perfs = _perf(ratings, ranks)
        by_perf = sorted(perfs, key=lambda u: -perfs[u])
        assert [ranks[u] for u in by_perf] == [1, 2, 3, 4, 5]

    def test_winner_of_a_two_player_day_is_finite(self):
        perfs = _perf({'a': 1200.0, 'b': 1200.0}, {'a': 1, 'b': 2})
        assert all(1 < value < 8000 for value in perfs.values())
        assert perfs['a'] > 1200 > perfs['b']

    def test_not_populated_when_caller_does_not_ask(self):
        # The extra binary search must not run on the recompute hot path.
        deltas = compute_round({'a': 1200.0, 'b': 1200.0}, {'a': 1, 'b': 2})
        assert set(deltas) == {'a', 'b'}


class TestPerformanceInHistories:
    def test_history_carries_the_field_inversion(self):
        rows = [_row('a', 1, 30), _row('b', 1, 60)]
        histories = {}
        compute_ratings(rows, rank_fn=_rank_by_time, histories=histories)
        field = [_pow10(1200.0), _pow10(1200.0)]
        assert abs(histories['a'][0].performance - event_performance(field, 1)) < 1e-9

    def test_tied_history_points_agree(self):
        # Day 1 moves 'a' and 'b' apart in rating; day 2 they tie exactly.
        rows = [_row('a', 1, 10), _row('b', 1, 99), _row('c', 1, 50),
                _row('a', 2, 30), _row('b', 2, 30), _row('c', 2, 90)]
        histories = {}
        compute_ratings(rows, rank_fn=_rank_by_time, histories=histories)
        assert histories['a'][0].rating != histories['b'][0].rating
        assert abs(histories['a'][1].performance - histories['b'][1].performance) < 1e-9

    def test_solo_day_has_no_performance(self):
        histories = {}
        compute_ratings([_row('a', 1, 30)], rank_fn=_rank_by_time,
                        histories=histories)
        assert histories['a'][0].performance is None


def _rank_by_time(rows):
    ordered = sorted(rows, key=lambda row: int(row.time_seconds))
    ranks, current, prev = {}, 0, None
    for index, row in enumerate(ordered):
        seconds = int(row.time_seconds)
        if prev is None or seconds != prev:
            current, prev = index + 1, seconds
        ranks[str(row.user_id)] = current
    return ranks

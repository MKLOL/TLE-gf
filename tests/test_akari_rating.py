"""Tests for the Codeforces-style Akari rating engine (tle/util/akari_rating.py).

These exercise the pure algorithm only — no DB, no discord.
"""
import random

from tle import constants
from tle.util import akari_rating
from tle.util.akari_rating import (
    RatingState, compute_ratings, compute_round, rank_participants,
)

from tests.akari_rating_test_utils import _day, _row


class TestRankParticipants:
    def test_perfect_beats_imperfect_then_accuracy_then_time(self):
        rows = _day(1, [
            ('slow_perfect', True, 100, 300),
            ('fast_perfect', True, 100, 30),
            ('high_acc', False, 90, 50),
            ('low_acc', False, 40, 10),
        ])
        ranks = rank_participants(rows)
        assert ranks == {
            'fast_perfect': 1,
            'slow_perfect': 2,
            'high_acc': 3,
            'low_acc': 4,
        }

    def test_identical_results_share_a_rank(self):
        rows = _day(1, [
            ('a', True, 100, 50),
            ('b', True, 100, 50),   # identical to a
            ('c', False, 80, 90),
        ])
        ranks = rank_participants(rows)
        assert ranks['a'] == ranks['b'] == 1   # tie shares the better rank
        assert ranks['c'] == 3                  # standard competition ("1-1-3")


class TestComputeRound:
    def test_equal_ratings_winner_gains_loser_loses(self):
        deltas = compute_round({'w': 1200.0, 'l': 1200.0}, {'w': 1, 'l': 2},
                               damping=1.0)
        assert deltas['w'] > 0
        assert deltas['l'] < 0

    def test_single_participant_no_change(self):
        assert compute_round({'a': 1200.0}, {'a': 1}) == {'a': 0.0}

    def test_damping_scales_linearly(self):
        ratings = {'w': 1300.0, 'l': 1100.0}
        ranks = {'w': 2, 'l': 1}  # upset: the lower-rated player wins
        full = compute_round(ratings, ranks, damping=1.0)
        quarter = compute_round(ratings, ranks, damping=0.25)
        for user in ratings:
            assert quarter[user] == full[user] * 0.25

    def test_upset_moves_more_than_expected_result(self):
        ratings = {'fav': 1600.0, 'dog': 1000.0}
        expected = compute_round(ratings, {'fav': 1, 'dog': 2}, damping=1.0)
        upset = compute_round(ratings, {'fav': 2, 'dog': 1}, damping=1.0)
        # The underdog winning swings ratings far more than the favorite winning.
        assert abs(upset['dog']) > abs(expected['dog'])


class TestComputeRatings:
    @staticmethod
    def _rank_by_time_only(rows):
        ordered = sorted(rows, key=lambda row: row.time_seconds)
        return {str(row.user_id): rank for rank, row in enumerate(ordered, start=1)}

    def test_new_players_start_at_1200(self):
        states = compute_ratings(_day(1, [('a', True, 100, 30),
                                          ('b', False, 50, 200)]))
        # Both seeded at 1200, then one contest moves them apart.
        assert states['a'].rating > 1200 > states['b'].rating

    def test_custom_rank_fn_changes_daily_rating_order(self):
        rows = _day(1, [
            ('fast_imperfect', False, 0, 10),
            ('slow_perfect', True, 100, 200),
        ])

        akari_states = compute_ratings(rows)
        generic_states = compute_ratings(rows, rank_fn=self._rank_by_time_only)

        assert akari_states['slow_perfect'].rating > akari_states['fast_imperfect'].rating
        assert generic_states['fast_imperfect'].rating > generic_states['slow_perfect'].rating

    def test_solo_day_leaves_rating_untouched(self):
        states = compute_ratings([_row('a', 1, perfect=True)])
        assert states['a'].rating == float(constants.AKARI_START_RATING)
        assert states['a'].games == 0

    def test_max_puzzle_drops_garbage_numbers(self):
        rows = []
        for puzzle in (1, 2):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        # A troll post with an absurd number, plus a non-positive one — both dropped.
        rows += _day(9223372036854775806, [('a', True, 100, 10), ('b', True, 100, 10)])
        rows += _day(0, [('a', True, 100, 10), ('b', True, 100, 10)])
        states = compute_ratings(rows, max_puzzle=100)
        assert states['a'].games == 2       # only puzzles 1 and 2 counted
        assert states['a'].last_puzzle == 2  # not the garbage number

    def test_consistent_winner_climbs_but_slowly(self):
        rows = []
        for puzzle in range(1, 6):
            rows += _day(puzzle, [
                ('win', True, 100, 30),
                ('mid', True, 100, 90),
                ('low', False, 70, 200),
            ])
        states = compute_ratings(rows)
        assert states['win'].rating > states['mid'].rating > states['low'].rating
        assert states['win'].games == 5
        # "Way less volatile": five clean wins move the leader by far less than
        # the undamped (full-strength CF) engine would.
        undamped = compute_ratings(rows, damping=1.0)
        assert (states['win'].rating - 1200) < 0.3 * (undamped['win'].rating - 1200) + 1

    def test_deterministic_regardless_of_row_order(self):
        rows = []
        for puzzle in range(1, 8):
            rows += _day(puzzle, [
                ('a', True, 100, 30 + puzzle),
                ('b', True, 100, 60),
                ('c', False, 80, 120),
                ('d', False, 50, 240),
            ])
        baseline = compute_ratings(rows)
        shuffled = rows[:]
        random.Random(20260602).shuffle(shuffled)
        other = compute_ratings(shuffled)
        assert baseline.keys() == other.keys()
        for user in baseline:
            assert abs(baseline[user].rating - other[user].rating) < 1e-9

    def test_tie_every_day_is_symmetric(self):
        rows = []
        for puzzle in range(1, 6):
            rows += _day(puzzle, [('a', True, 100, 50), ('b', True, 100, 50)])
        states = compute_ratings(rows)
        assert abs(states['a'].rating - states['b'].rating) < 1e-9

    def test_ratings_stay_float_no_per_day_rounding(self):
        # At quarter damping, daily deltas are small; if the engine rounded to an
        # int each day many would vanish. Keeping floats means the leader lands on
        # a non-integer rating and keeps a non-zero last change.
        rows = []
        for puzzle in range(1, 11):
            rows += _day(puzzle, [('a', True, 100, 30),
                                  ('b', False, 60, 200)])
        states = compute_ratings(rows)
        assert states['a'].rating != round(states['a'].rating)
        assert states['a'].last_delta != 0.0

    def test_peak_tracks_high_water_mark(self):
        # 'a' wins early (rating rises), then loses repeatedly (rating falls);
        # peak must retain the earlier high, not the final rating.
        rows = _day(1, [('a', True, 100, 30), ('b', False, 50, 200)])
        for puzzle in range(2, 8):
            rows += _day(puzzle, [('a', False, 20, 300), ('b', True, 100, 20)])
        states = compute_ratings(rows)
        assert states['a'].peak >= states['a'].rating
        assert states['a'].peak > 1200  # captured the early win
        assert states['a'].rating < 1200  # net loser overall

    def test_search_iters_kept_small_for_speed(self):
        # Guard the perf-vs-precision knob: replaying on every result change must
        # stay cheap, so the binary search stays well under CF's textbook depth.
        assert akari_rating._SEARCH_ITERS <= 30


class TestDecay:
    @staticmethod
    def _winner_then_absent(absent_days):
        # 'a' beats 'b' on days 1-3 (a climbs above 1200), then 'a' is absent
        # while 'b' and 'c' keep playing days 4..(3+absent_days).
        rows = []
        for puzzle in range(1, 4):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        for puzzle in range(4, 4 + absent_days):
            rows += _day(puzzle, [('b', True, 100, 40), ('c', False, 50, 200)])
        return compute_ratings(rows)['a']

    def test_ratingstate_decay_fields_default_to_zero(self):
        state = RatingState('a', 1300.0, 4, 1300.0, 5.0)
        assert state.skip_streak == 0
        assert state.last_puzzle == 0

    def test_decay_starts_on_first_absent_day(self):
        active = self._winner_then_absent(0)
        one_day_off = self._winner_then_absent(1)
        assert one_day_off.rating < active.rating  # decay bites immediately
        assert one_day_off.rating > 1200           # never crosses the default

    def test_longer_absences_yield_lower_ratings(self):
        early = self._winner_then_absent(2)
        late = self._winner_then_absent(11)
        assert late.skip_streak > early.skip_streak
        # Absence keeps eroding rating, even when per-day deltas shrink (rate
        # caps but gap-to-default closes, so later daily losses are smaller in
        # absolute terms — the *cumulative* loss is still bigger).
        assert late.rating < early.rating

    def test_decay_pulls_high_rating_toward_default(self):
        active = self._winner_then_absent(0)
        long_absent = self._winner_then_absent(40)
        assert 1200 < long_absent.rating < active.rating
        assert long_absent.skip_streak == 40
        assert long_absent.last_puzzle == 3  # last day actually played

    def test_sub_default_absentees_freeze(self):
        # 'd' loses repeatedly to 'e', landing below 1200, then disappears.
        # Under the zero-sum design we don't create rating ex nihilo, so 'd'
        # stays put — no free drift back up while inactive.
        rows = []
        for puzzle in range(1, 6):
            rows += _day(puzzle, [('d', False, 10, 300), ('e', True, 100, 20)])
        low = compute_ratings(rows)['d'].rating
        assert low < 1200
        for puzzle in range(6, 45):
            rows += _day(puzzle, [('e', True, 100, 20), ('f', False, 40, 200)])
        frozen = compute_ratings(rows)['d']
        assert frozen.rating == low
        assert frozen.skip_streak == 39  # absent for puzzles 6..44

    def test_playing_resets_skip_streak(self):
        rows = []
        for puzzle in range(1, 4):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        for puzzle in range(4, 12):
            rows += _day(puzzle, [('b', True, 100, 40), ('c', False, 50, 200)])
        rows += _day(12, [('a', True, 100, 30), ('b', False, 60, 200)])  # a returns
        a = compute_ratings(rows)['a']
        assert a.skip_streak == 0
        assert a.last_puzzle == 12

    def test_active_players_never_decay(self):
        rows = []
        for puzzle in range(1, 11):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', True, 100, 60)])
        states = compute_ratings(rows)
        assert states['a'].skip_streak == 0
        assert states['b'].skip_streak == 0

"""Tests for the Codeforces-style Akari rating engine (tle/util/akari_rating.py).

These exercise the pure algorithm only — no DB, no discord.
"""
import random
from types import SimpleNamespace

from tle import constants
from tle.util import akari_rating
from tle.util.akari_rating import (
    RatingState, compute_ratings, compute_round, rank_participants,
)


def _row(user_id, puzzle_number, *, perfect=False, accuracy=0, time_seconds=100):
    return SimpleNamespace(
        user_id=user_id,
        puzzle_number=puzzle_number,
        is_perfect=perfect,
        accuracy=accuracy,
        time_seconds=time_seconds,
    )


def _day(puzzle_number, players):
    """players: list of (user_id, perfect, accuracy, time_seconds)."""
    return [
        _row(uid, puzzle_number, perfect=p, accuracy=a, time_seconds=t)
        for uid, p, a, t in players
    ]


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


class TestFirstSkipLastPlaceDecay:
    """Experimental `+test` decay (``first_skip_last_place=True``):
    an above-default player's first absent day costs a virtual last-place
    finish against that day's real field; later absent days (and solo days,
    and sub-default absentees) fall back to the percentage rule."""

    _DAY4 = [('b', True, 100, 40), ('c', False, 50, 200)]

    @classmethod
    def _base_rows(cls):
        # 'a' beats 'b' on days 1-3 (a climbs above 1200), then sits out
        # day 4 while 'b' and 'c' play.
        rows = []
        for puzzle in range(1, 4):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += _day(4, cls._DAY4)
        return rows

    def test_first_skip_costs_the_last_place_delta(self):
        rows = self._base_rows()
        pre = compute_ratings(rows[:6])  # replay days 1-3 only
        a_pre = pre['a'].rating
        assert a_pre > 1200

        # Hand-build the engine's hypothetical: 'a' inserted strictly last
        # into day 4's real field at its pre-day-4 ratings ('c' seeds 1200).
        day_ratings = {'b': pre['b'].rating, 'c': 1200.0}
        ranks = rank_participants(_day(4, self._DAY4))
        hyp = compute_round({**day_ratings, 'a': a_pre}, {**ranks, 'a': 3})
        expected_delta = max(min(0.0, hyp['a']), 1200.0 - a_pre)

        states = compute_ratings(rows, first_skip_last_place=True)
        assert abs(states['a'].rating - (a_pre + expected_delta)) < 1e-9

    def test_first_skip_is_zero_sum(self):
        # The absence is on the final day, so both decay variants run the
        # exact same contests and redistribute their (different) pools fully
        # — total guild rating must come out identical.
        rows = self._base_rows()
        default_total = sum(
            s.rating for s in compute_ratings(rows).values())
        test_total = sum(
            s.rating
            for s in compute_ratings(rows, first_skip_last_place=True).values())
        assert abs(default_total - test_total) < 1e-9

    def test_sub_default_first_skip_freezes(self):
        # 'd' sinks below 1200, then misses a day: the last-place rule only
        # applies above the default, so 'd' falls through to the percentage
        # rule, whose clamp freezes sub-default absentees.
        rows = []
        for puzzle in range(1, 6):
            rows += _day(puzzle, [('d', False, 10, 300), ('e', True, 100, 20)])
        low = compute_ratings(rows, first_skip_last_place=True)['d'].rating
        assert low < 1200
        rows += _day(6, [('e', True, 100, 20), ('f', False, 40, 200)])
        frozen = compute_ratings(rows, first_skip_last_place=True)['d'].rating
        assert frozen == low

    def test_solo_day_falls_back_to_percentage_rule(self):
        # A 1-player day has no field to finish last in, so the flag is a
        # no-op: identical result with and without it.
        rows = []
        for puzzle in range(1, 4):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += _day(4, [('b', True, 100, 40)])
        with_flag = compute_ratings(
            rows, first_skip_last_place=True)['a'].rating
        without = compute_ratings(rows)['a'].rating
        assert abs(with_flag - without) < 1e-9

    def test_later_skips_use_flat_percentage_when_max_pinned(self):
        # The cog pins decay_max to decay_base for `+test`, killing the ramp:
        # the second absent day loses exactly base-rate of the remaining gap.
        rows = self._base_rows()
        kwargs = dict(first_skip_last_place=True,
                      decay_max=constants.AKARI_DECAY_BASE)
        after_one = compute_ratings(rows, **kwargs)['a'].rating
        rows += _day(5, [('b', True, 100, 40), ('c', False, 50, 200)])
        after_two = compute_ratings(rows, **kwargs)['a'].rating
        expected = after_one + (1200.0 - after_one) * constants.AKARI_DECAY_BASE
        assert abs(after_two - expected) < 1e-9

    def test_first_skip_never_drops_below_default(self):
        # Sink 'b' and 'c' far below 1200, let 'a' creep just above it with
        # one expected win, then have 'a' skip a 'b'-vs-'c' day.  The virtual
        # last-place loss against the sunken field exceeds 'a's tiny gap, so
        # the floor must bind and 'a' lands exactly on the default.
        rows = []
        for puzzle in range(1, 31):
            rows += _day(puzzle, [
                ('z', True, 100, 20),
                ('b', False, 30, 300),
                ('c', False, 20, 300),
            ])
        rows += _day(31, [
            ('a', True, 100, 30), ('b', False, 60, 200), ('c', False, 50, 250)])
        pre = compute_ratings(rows, first_skip_last_place=True)
        a_pre = pre['a'].rating
        assert a_pre > 1200

        day32 = [('b', False, 60, 200), ('c', False, 50, 250)]
        hyp = compute_round(
            {'b': pre['b'].rating, 'c': pre['c'].rating, 'a': a_pre},
            {**rank_participants(_day(32, day32)), 'a': 3})
        # Precondition: this scenario's loss really is bigger than the gap.
        assert hyp['a'] < 1200.0 - a_pre

        rows += _day(32, day32)
        states = compute_ratings(rows, first_skip_last_place=True)
        assert abs(states['a'].rating - 1200.0) < 1e-9


class TestHistoryDecayCapture:
    @staticmethod
    def _winner_then_absent_rows(absent_days):
        # Mirrors TestDecay._winner_then_absent: 'a' wins puzzles 1-3 then is
        # absent for ``absent_days`` further days while 'b' and 'c' keep playing.
        rows = []
        for puzzle in range(1, 4):
            rows += _day(puzzle, [('a', True, 100, 30), ('b', False, 60, 200)])
        for puzzle in range(4, 4 + absent_days):
            rows += _day(puzzle, [('b', True, 100, 40), ('c', False, 50, 200)])
        return rows

    def test_decay_days_omitted_by_default(self):
        rows = self._winner_then_absent_rows(10)
        histories = {}
        compute_ratings(rows, histories=histories)
        # 'a' played puzzles 1-3 only; default history has only the 3 played days.
        assert len(histories['a']) == 3
        assert all(not h.is_decay for h in histories['a'])

    def test_include_decay_emits_one_point_per_absent_day(self):
        rows = self._winner_then_absent_rows(10)
        histories = {}
        compute_ratings(
            rows, histories=histories, include_decay_in_history=True)
        a_history = histories['a']
        assert len(a_history) == 13  # 3 played + 10 decay
        assert [h.is_decay for h in a_history] == [False] * 3 + [True] * 10
        assert [h.puzzle_number for h in a_history] == list(range(1, 14))

    def test_decay_points_record_post_decay_rating(self):
        rows = self._winner_then_absent_rows(5)
        histories = {}
        states = compute_ratings(
            rows, histories=histories, include_decay_in_history=True)
        decay_points = [h for h in histories['a'] if h.is_decay]
        # Last decay point's rating matches the final RatingState.
        assert decay_points[-1].rating == states['a'].rating
        # First absent day pulls 'a' (who was above 1200) downward immediately.
        assert decay_points[0].delta < 0

    def test_decay_points_have_no_performance(self):
        rows = self._winner_then_absent_rows(5)
        histories = {}
        compute_ratings(
            rows, histories=histories, include_decay_in_history=True)
        for h in histories['a']:
            if h.is_decay:
                assert h.performance is None

    def test_decay_point_borrows_puzzle_date_from_participants(self):
        # 'a' is absent on puzzle 2; the row's puzzle_date must come from the
        # other players' rows on that day, not the absent user.
        row1 = SimpleNamespace(
            user_id='a', puzzle_number=1, is_perfect=True,
            accuracy=100, time_seconds=30, puzzle_date='2026-06-01')
        row2 = SimpleNamespace(
            user_id='b', puzzle_number=1, is_perfect=False,
            accuracy=50, time_seconds=200, puzzle_date='2026-06-01')
        row3 = SimpleNamespace(
            user_id='b', puzzle_number=2, is_perfect=True,
            accuracy=100, time_seconds=40, puzzle_date='2026-06-02')
        row4 = SimpleNamespace(
            user_id='c', puzzle_number=2, is_perfect=False,
            accuracy=20, time_seconds=300, puzzle_date='2026-06-02')
        histories = {}
        compute_ratings(
            [row1, row2, row3, row4],
            histories=histories, include_decay_in_history=True)
        decay_points = [h for h in histories['a'] if h.is_decay]
        assert len(decay_points) == 1
        assert decay_points[0].puzzle_date == '2026-06-02'

    def test_played_day_history_points_have_is_decay_false(self):
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        histories = {}
        compute_ratings(
            rows, histories=histories, include_decay_in_history=True)
        assert histories['a'][0].is_decay is False
        assert histories['b'][0].is_decay is False


class TestCurrentPuzzleGate:
    @staticmethod
    def _two_day_rows():
        # 'a' wins puzzle 1.  Puzzle 2: 'b' and 'c' play, 'a' is absent.
        rows = []
        rows += _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += _day(2, [('b', True, 100, 40), ('c', False, 50, 200)])
        return rows

    def test_absence_decay_skipped_for_current_puzzle(self):
        # Puzzle 2 is "today" — 'a' must not be decayed yet.
        states = compute_ratings(self._two_day_rows(), current_puzzle_number=2)
        assert states['a'].skip_streak == 0
        assert states['a'].last_puzzle == 1

    def test_absence_decay_applies_once_day_concluded(self):
        # Same data, but "today" is puzzle 3 — puzzle 2 is now in the past for 'a'.
        states = compute_ratings(self._two_day_rows(), current_puzzle_number=3)
        assert states['a'].skip_streak == 1

    def test_contest_math_still_runs_for_current_puzzle(self):
        # Players who DID post on the current day still get rating change.
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        states = compute_ratings(rows, current_puzzle_number=1)
        assert states['a'].rating > 1200 > states['b'].rating
        assert states['a'].games == 1
        assert states['b'].games == 1

    def test_history_omits_decay_for_current_puzzle(self):
        histories = {}
        compute_ratings(
            self._two_day_rows(), current_puzzle_number=2,
            histories=histories, include_decay_in_history=True)
        # 'a' didn't play puzzle 2 (today) → no decay point for it.
        assert len(histories['a']) == 1
        assert not any(h.is_decay for h in histories['a'])

    def test_future_lookahead_puzzles_never_decay_others(self):
        # A row at puzzle today+1 (within max_puzzle lookahead) is a future day:
        # absent players must not be decayed for it.
        rows = []
        rows += _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += _day(2, [('b', True, 100, 40), ('c', False, 50, 200)])  # future
        states = compute_ratings(
            rows, max_puzzle=10, current_puzzle_number=1)
        # Even though puzzle 2 has rows, it's at/after current_puzzle so 'a'
        # is not punished for missing it.
        assert states['a'].skip_streak == 0

    def test_none_disables_the_gate(self):
        # Default behaviour (used by tests that don't simulate "today") still
        # decays every concluded puzzle in the data.
        states = compute_ratings(self._two_day_rows())
        assert states['a'].skip_streak == 1


class TestZeroSumTransfer:
    @staticmethod
    def _total(states):
        return sum(s.rating for s in states.values())

    def test_decay_and_transfer_balance_per_day(self):
        # Day 1: 'a' wins, 'b' loses (gets 'a' above 1200).  Day 2: 'b' and 'c'
        # tie (identical perfect results), 'a' absent → decay-transfer fires.
        # Tying isolates the transfer mechanic: with equal contest deltas, any
        # asymmetry in b.delta vs c.delta would have to come from the pool
        # split — which must be equal too.
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += _day(2, [('b', True, 100, 40), ('c', True, 100, 40)])
        histories = {}
        compute_ratings(rows, histories=histories, include_decay_in_history=True)
        a_day2 = next(p for p in histories['a']
                      if p.puzzle_number == 2 and p.is_decay)
        b_day2 = next(p for p in histories['b'] if p.puzzle_number == 2)
        c_day2 = next(p for p in histories['c'] if p.puzzle_number == 2)
        # The decay-transfer step is internally balanced.  Day-2 contest math
        # has its own structural CF leak of -0.25 × n_players, which is the
        # only thing left over once the pool moves.
        contest_leak = -constants.AKARI_RATING_DAMPING * 2
        day2_total_delta = a_day2.delta + b_day2.delta + c_day2.delta
        assert abs(day2_total_delta - contest_leak) < 1e-6

    def test_pool_split_equally_between_tied_active_players(self):
        # The equal-split property: when active players are symmetrically
        # placed (same pre-rating, same result), their post-transfer deltas
        # must match exactly.  Setup: 'a' is the absent coaster (above 1200
        # after day 1); 'b' and 'c' are brand-new on day 2 (both seeded at
        # 1200), and tie perfectly.  Symmetric inputs → equal contest deltas
        # → equal transfer shares → equal final deltas.
        rows = _day(1, [('a', True, 100, 30), ('x', False, 60, 200)])
        rows += _day(2, [('b', True, 100, 40), ('c', True, 100, 40)])
        histories = {}
        compute_ratings(rows, histories=histories, include_decay_in_history=True)
        b_day2 = next(p for p in histories['b'] if p.puzzle_number == 2)
        c_day2 = next(p for p in histories['c'] if p.puzzle_number == 2)
        assert abs(b_day2.delta - c_day2.delta) < 1e-9

    def test_solo_active_collects_entire_pool(self):
        # Day 1: 'a' beats 'b' (a > 1200).  Day 2: only 'b' plays.
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += [_row('b', 2, perfect=True, time_seconds=40)]
        states = compute_ratings(rows)
        # Solo days produce no contest delta but the transfer pool still flows
        # to the lone active player, who is 'b'.
        baseline = compute_ratings(
            _day(1, [('a', True, 100, 30), ('b', False, 60, 200)]))
        a_loss = baseline['a'].rating - states['a'].rating
        b_gain = states['b'].rating - baseline['b'].rating
        assert a_loss > 0
        assert abs(a_loss - b_gain) < 1e-6  # all of a's loss went to b

    def test_no_above_default_absentees_means_no_pool(self):
        # 'd' is sub-1200 (lost to e), absent on day 6.  No above-1200
        # absentees means no pool — 'e' and 'f' only get their contest delta.
        rows = []
        for puzzle in range(1, 6):
            rows += _day(puzzle, [('d', False, 20, 300), ('e', True, 100, 30)])
        rows += _day(6, [('e', True, 100, 30), ('f', False, 40, 200)])
        states = compute_ratings(rows)
        # Reference: same data but with the day-6 absentee 'd' replaced by a
        # never-seen player — no decay loop runs at all.
        ref_rows = []
        for puzzle in range(1, 6):
            ref_rows += _day(puzzle,
                             [('e', True, 100, 30), ('d', False, 20, 300)])
        ref_rows += _day(6, [('e', True, 100, 30), ('f', False, 40, 200)])
        ref = compute_ratings(ref_rows)
        # 'e' on day 6 in both worlds: identical, because 'd' contributed
        # nothing to the pool (frozen sub-default).
        assert abs(states['e'].rating - ref['e'].rating) < 1e-9

    def test_transfer_share_appears_in_played_history_point(self):
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += [_row('b', 2, perfect=True, time_seconds=40)]
        histories = {}
        compute_ratings(rows, histories=histories)
        # 'b' played day 2 alone. The played point's delta and rating should
        # reflect the transfer received from absent 'a'.
        b_day2 = histories['b'][-1]
        assert b_day2.puzzle_number == 2
        assert b_day2.delta > 0  # share > 0, no contest delta on solo day
        assert b_day2.rating > 1200 or b_day2.rating == histories['b'][0].rating + b_day2.delta

    def test_transfer_share_lifts_peak(self):
        # 'b' was net-losing on day 1, but absorbs 'a's full decay pool on
        # day 2 — that should be reflected in their peak.
        rows = _day(1, [('a', True, 100, 30), ('b', False, 60, 200)])
        rows += [_row('b', 2, perfect=True, time_seconds=40)]
        states = compute_ratings(rows)
        assert states['b'].peak >= states['b'].rating
        assert states['b'].peak > 1200 - 1  # peak ≥ post-transfer rating

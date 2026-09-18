"""Pure-model tests for the Monday-Sunday weekly server recap."""

import datetime as dt
from types import SimpleNamespace

from tle.cogs._minigame_weekly import (
    RatingChange,
    build_week_recap,
    daily_rating_changes,
    rating_changes,
    rows_in_range,
    top_and_bottom,
    week_bounds,
)


_MONDAY = dt.date(2026, 8, 3)
_SUNDAY = dt.date(2026, 8, 9)


def _row(day, user_id, seconds, *, perfect=True, accuracy=100, message_id=1):
    if isinstance(day, str):
        day = dt.date.fromisoformat(day)
    return SimpleNamespace(
        message_id=str(message_id),
        user_id=str(user_id),
        puzzle_number=day.toordinal(),
        puzzle_date=day.isoformat(),
        time_seconds=seconds,
        is_perfect=perfect,
        accuracy=accuracy,
    )


def _week(offset_rows):
    """Build rows from ``{day_offset: {user_id: seconds}}``."""
    rows = []
    message_id = 0
    for offset, entries in offset_rows.items():
        day = _MONDAY + dt.timedelta(days=offset)
        for user_id, seconds in entries.items():
            message_id += 1
            rows.append(_row(day, user_id, seconds, message_id=message_id))
    return rows


def _recap(rows, *, anchor=None, today=_SUNDAY, viewer_id=None,
           daily_ratings=None):
    return build_week_recap(
        rows,
        display_name='Daily Akari',
        anchor_date=anchor or _MONDAY,
        today=today,
        viewer_id=viewer_id,
        daily_ratings=daily_ratings,
    )


class TestWeekBounds:
    def test_monday_anchors_its_own_week(self):
        assert week_bounds(_MONDAY) == (_MONDAY, _SUNDAY)

    def test_sunday_anchors_the_preceding_monday(self):
        assert week_bounds(_SUNDAY) == (_MONDAY, _SUNDAY)

    def test_rows_outside_the_span_are_dropped(self):
        rows = [
            _row(_MONDAY - dt.timedelta(days=1), 10, 60),
            _row(_MONDAY, 10, 60),
            _row(_SUNDAY, 10, 60),
            _row(_SUNDAY + dt.timedelta(days=1), 10, 60),
        ]
        assert len(rows_in_range(rows, _MONDAY, _SUNDAY)) == 2


class TestDayByDay:
    def test_every_weekday_gets_a_slot_even_when_unplayed(self):
        recap = _recap(_week({0: {10: 60}}))
        assert len(recap.days) == 7
        assert [day.date for day in recap.days] == [
            _MONDAY + dt.timedelta(days=offset) for offset in range(7)]
        assert recap.days[1].winner_ids == []
        assert recap.days[1].participants == 0

    def test_fastest_perfect_result_wins_the_day(self):
        recap = _recap(_week({0: {10: 90, 20: 60, 30: 75}}))
        monday = recap.days[0]
        assert monday.winner_ids == ['20']
        assert monday.participants == 3
        assert monday.tied is False

    def test_equal_results_mark_the_day_tied(self):
        recap = _recap(_week({0: {10: 60, 20: 60, 30: 90}}))
        monday = recap.days[0]
        assert monday.winner_ids == ['10', '20']
        assert monday.tied is True

    def test_recap_carries_the_reference_day_for_future_slots(self):
        # The render distinguishes "not played yet" from "nobody played".
        today = _MONDAY + dt.timedelta(days=2)
        recap = _recap(_week({0: {10: 60}}), today=today)
        assert recap.today == today
        assert [day.date > recap.today for day in recap.days] == [
            False, False, False, True, True, True, True]

    def test_repeat_submissions_count_one_participant(self):
        rows = [
            _row(_MONDAY, 10, 90, message_id=1),
            _row(_MONDAY, 10, 60, message_id=2),
        ]
        assert _recap(rows).days[0].participants == 1


class TestLeadersAndPersonal:
    def test_leaders_separate_solo_from_tied_wins(self):
        recap = _recap(_week({
            0: {10: 60, 20: 90},   # 10 wins outright
            1: {10: 60, 20: 60},   # tied
            2: {20: 55, 10: 70},   # 20 wins outright
        }))
        assert recap.leaders == [('10', 1, 1), ('20', 1, 1)]

    def test_personal_section_reports_the_viewer_week(self):
        recap = _recap(_week({
            0: {10: 60, 20: 90},
            1: {10: 80, 20: 80},
        }), viewer_id='10')
        personal = recap.personal
        assert personal.days_played == 2
        assert personal.perfects == 2
        assert personal.solo_wins == 1
        assert personal.tied_wins == 1
        assert personal.rank == 1

    def test_board_ranks_by_total_wins_then_solo_wins(self):
        recap = _recap(_week({
            0: {10: 60, 20: 60},   # tied: both get a shared win
            1: {20: 55, 10: 70},   # 20 wins outright
            2: {30: 50, 10: 70},   # 30 wins outright
        }))
        # 20 leads on total (2). 30 and 10 both have 1, so the solo count
        # breaks the tie and 30's outright win outranks 10's shared one.
        assert recap.leaders == [('20', 1, 1), ('30', 1, 0), ('10', 0, 1)]

    def test_rank_matches_the_board_that_is_rendered(self):
        recap = _recap(_week({
            0: {10: 60, 20: 60},   # tied
            1: {20: 55, 10: 70},   # 20 wins outright
        }), viewer_id='10')
        assert recap.personal.solo_wins == 0
        assert recap.personal.tied_wins == 1
        # 10 shares a win, so it is on the board and ranked, just below 20.
        assert recap.personal.rank == 2
        assert [entry[0] for entry in recap.leaders] == ['20', '10']

    def test_personal_section_is_absent_for_a_non_player(self):
        recap = _recap(_week({0: {10: 60, 20: 90}}), viewer_id='99')
        assert recap.personal is None

    def test_counts_cover_the_week_not_the_whole_history(self):
        rows = _week({0: {10: 60, 20: 90}})
        rows += [_row(_MONDAY - dt.timedelta(days=7), 30, 50, message_id=99)]
        recap = _recap(rows)
        assert recap.player_count == 2
        assert recap.result_count == 2


class TestSuperlatives:
    def _labels(self, recap):
        return {entry.label: entry for entry in recap.superlatives}

    def test_fastest_solve_names_the_day(self):
        recap = _recap(_week({0: {10: 90, 20: 60}, 1: {10: 70}}))
        fastest = self._labels(recap)['Fastest solve']
        assert fastest.user_id == '20'
        assert fastest.value == 60
        assert fastest.detail == 'Mon'

    def test_perfect_counts_are_not_a_standout(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 60}, 2: {10: 60}})
        assert 'Most perfects' not in self._labels(_recap(rows))

    def test_best_average_is_its_own_table_not_a_standout(self):
        assert 'Best average' not in self._labels(
            _recap(_week({0: {10: 100, 20: 10}, 1: {10: 100}})))

    def test_most_improved_compares_against_the_previous_week(self):
        rows = _week({0: {10: 200, 20: 100}, 1: {10: 200, 20: 100}})
        previous = [
            _row(_MONDAY - dt.timedelta(days=7), 10, 400, message_id=51),
            _row(_MONDAY - dt.timedelta(days=6), 10, 400, message_id=52),
            _row(_MONDAY - dt.timedelta(days=7), 20, 110, message_id=53),
            _row(_MONDAY - dt.timedelta(days=6), 20, 110, message_id=54),
        ]
        improved = self._labels(_recap(rows + previous))['Most improved']
        assert improved.user_id == '10'
        assert improved.value == 200


class TestRatings:
    def test_in_progress_week_reports_no_rated_movement(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 70, 20: 80}})
        recap = _recap(rows, today=_MONDAY + dt.timedelta(days=2))
        assert recap.in_progress is True
        assert recap.ratings == []

    def test_completed_week_rates_the_winner_up_and_the_loser_down(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 70, 20: 80}})
        recap = _recap(rows, today=_SUNDAY + dt.timedelta(days=1))
        assert recap.in_progress is False
        # Sorted by delta, so the week's winner leads the movement table.
        assert [change.user_id for change in recap.ratings] == ['10', '20']
        assert recap.ratings[0].delta > 0 > recap.ratings[1].delta
        assert recap.ratings[0].new > recap.ratings[0].old

    def test_rating_diff_ignores_an_unrated_intervening_week(self):
        """A one-player week must not leak the prior week's delta through."""
        rows = _week({0: {10: 60, 20: 90}})              # rated
        solo_monday = _MONDAY + dt.timedelta(days=7)
        rows.append(_row(solo_monday, 10, 60, message_id=80))  # not rated
        changes = rating_changes(
            rows, {}, solo_monday, solo_monday + dt.timedelta(days=6))
        assert changes == []

    def test_a_first_rated_week_moves_from_the_shared_start(self):
        rows = _week({0: {10: 60, 20: 90}})
        changes = rating_changes(rows, {}, _MONDAY, _SUNDAY)
        assert len(changes) == 2
        assert all(change.old == changes[0].old for change in changes)
        assert changes[0].delta > 0


class TestDailyRatingLadder:
    def test_daily_and_weekly_ladders_are_reported_separately(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 70, 20: 80}})
        daily = daily_rating_changes(rows, _MONDAY, _SUNDAY)
        assert [change.user_id for change in daily] == ['10', '20']
        assert daily[0].delta > 0 > daily[1].delta
        # The daily ladder moves per puzzle, the weekly one per contest, so the
        # two must not be assumed equal.
        recap = _recap(rows, today=_SUNDAY + dt.timedelta(days=1),
                       daily_ratings=daily)
        assert recap.daily_ratings == daily
        assert recap.ratings != daily

    def test_delta_reconciles_with_the_reported_ratings(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 70, 20: 80}})
        for change in daily_rating_changes(rows, _MONDAY, _SUNDAY):
            assert round(change.new - change.old, 6) == round(change.delta, 6)

    def test_only_the_requested_week_is_counted(self):
        rows = _week({0: {10: 60, 20: 90}})
        rows += [
            _row(_MONDAY + dt.timedelta(days=7), 10, 60, message_id=70),
            _row(_MONDAY + dt.timedelta(days=7), 20, 90, message_id=71),
        ]
        first = daily_rating_changes(rows, _MONDAY, _SUNDAY)
        both = daily_rating_changes(
            rows, _MONDAY, _SUNDAY + dt.timedelta(days=7))
        gain = {c.user_id: c.delta for c in first}['10']
        assert {c.user_id: c.delta for c in both}['10'] > gain

    def test_a_player_who_never_showed_up_is_omitted(self):
        rows = _week({0: {10: 60, 20: 90}, 1: {10: 70, 20: 80}})
        # 30 only played the previous week, so this week is pure decay.
        rows += [
            _row(_MONDAY - dt.timedelta(days=7), 30, 50, message_id=60),
            _row(_MONDAY - dt.timedelta(days=7), 10, 55, message_id=61),
        ]
        assert '30' not in {
            change.user_id
            for change in daily_rating_changes(rows, _MONDAY, _SUNDAY)}


class TestTopAndBottom:
    def _changes(self, deltas):
        return [RatingChange(str(i), 1200, 1200 + d, d)
                for i, d in enumerate(deltas, start=1)]

    def test_three_each_way_on_a_long_list(self):
        changes = self._changes([90, 70, 50, 30, -10, -40, -80])
        best, worst = top_and_bottom(changes)
        assert [c.delta for c in best] == [90, 70, 50]
        assert [c.delta for c in worst] == [-10, -40, -80]

    def test_a_short_list_never_repeats_a_player(self):
        changes = self._changes([50, 20, -30, -60])
        best, worst = top_and_bottom(changes)
        assert [c.delta for c in best] == [50, 20, -30]
        assert [c.delta for c in worst] == [-60]
        assert not {c.user_id for c in best} & {c.user_id for c in worst}


class TestPaceLeaderboard:
    def test_a_partial_week_is_not_ranked_at_all(self):
        """Cherry-picking the easy days is the bias this board exists to stop."""
        # Mon/Tue are fast days, Sat/Sun slow. 20 plays only the fast days.
        recap = _recap(_week({
            0: {10: 3, 20: 6, 30: 6, 40: 9},
            1: {10: 4, 20: 8, 30: 8, 40: 12},
            5: {10: 30, 30: 60, 40: 90},
            6: {10: 40, 30: 80, 40: 120},
        }))
        ranked = {entry.user_id: entry for entry in recap.paces}
        assert '20' not in ranked
        assert set(ranked) == {'10', '30', '40'}
        assert [entry.user_id for entry in recap.paces][0] == '10'

    def test_pace_is_the_plain_geometric_mean_with_no_shrinkage(self):
        # 20 is the median every day; 10 is exactly twice as fast every day.
        recap = _recap(_week({
            0: {10: 30, 20: 60, 30: 90},
            1: {10: 30, 20: 60, 30: 90},
            2: {10: 30, 20: 60, 30: 90},
            3: {10: 30, 20: 60, 30: 90},
        }))
        paces = {entry.user_id: entry for entry in recap.paces}
        assert round(paces['20'].pace, 6) == 1.0
        assert round(paces['10'].pace, 6) == 2.0

    def test_a_slow_day_and_a_fast_day_cancel(self):
        """Geometric mean: half-median and double-median average to median."""
        recap = _recap(_week({
            0: {10: 30, 20: 60, 30: 60},
            1: {10: 120, 20: 60, 30: 60},
        }))
        paces = {entry.user_id: entry for entry in recap.paces}
        assert round(paces['10'].pace, 6) == 1.0

    def test_pace_can_disagree_with_raw_average_for_the_same_days(self):
        """Even at equal attendance, the slow days dominate a raw average."""
        # Mon median 10, Sun median 100. B is far better on the easy day,
        # A is better on the hard one.
        rows = [
            _row(_MONDAY, 10, 10, message_id=1),    # A
            _row(_MONDAY, 20, 2, message_id=2),     # B
            _row(_MONDAY, 30, 10, message_id=3),
            _row(_MONDAY + dt.timedelta(days=6), 10, 50, message_id=4),
            _row(_MONDAY + dt.timedelta(days=6), 20, 100, message_id=5),
            _row(_MONDAY + dt.timedelta(days=6), 30, 100, message_id=6),
        ]
        paces = {entry.user_id: entry for entry in _recap(rows).paces}
        # A has the better wall-clock average ...
        assert paces['10'].seconds < paces['20'].seconds
        # ... but B beat each day's field by more, so B leads on pace.
        assert paces['20'].pace > paces['10'].pace

    def test_a_solo_day_is_not_scored_and_is_not_required(self):
        # Tuesday has one player, so it neither scores nor blocks anyone.
        recap = _recap(_week({
            0: {10: 30, 20: 60},
            1: {10: 5},
            2: {10: 30, 20: 60},
        }))
        paces = {entry.user_id: entry for entry in recap.paces}
        assert paces['10'].days == 2
        assert '20' in paces

    def test_a_players_own_best_counts_once_per_day(self):
        rows = [
            _row(_MONDAY, 10, 90, message_id=1),
            _row(_MONDAY, 10, 30, message_id=2),
            _row(_MONDAY, 20, 60, message_id=3),
            _row(_MONDAY + dt.timedelta(days=1), 10, 30, message_id=4),
            _row(_MONDAY + dt.timedelta(days=1), 20, 60, message_id=5),
        ]
        paces = {entry.user_id: entry for entry in _recap(rows).paces}
        assert paces['10'].days == 2
        assert paces['10'].seconds == 30



class TestPaceEligibility:
    """Pace counts every posted result, identically in both games."""

    def test_a_slow_or_partial_day_still_counts(self):
        rows = [
            _row(_MONDAY, 10, 100, message_id=1),
            _row(_MONDAY, 20, 200, message_id=2),
            _row(_MONDAY + dt.timedelta(days=1), 10, 100, message_id=3),
            _row(_MONDAY + dt.timedelta(days=1), 20, 200, message_id=4),
            # 30 posts a fast partial grid on both days.
            _row(_MONDAY, 30, 5, perfect=False, accuracy=20, message_id=5),
            _row(_MONDAY + dt.timedelta(days=1), 30, 5, perfect=False,
                 accuracy=20, message_id=6),
        ]
        for kwargs in ({}, {'is_eligible': lambda _row: True}):
            recap = build_week_recap(
                rows, display_name='Daily Akari', anchor_date=_MONDAY,
                today=_SUNDAY, top_kwargs=kwargs)
            assert [entry.user_id for entry in recap.paces][0] == '30'

    def test_a_partial_day_keeps_the_full_week_requirement_satisfied(self):
        rows = [
            _row(_MONDAY, 10, 100, message_id=1),
            _row(_MONDAY, 20, 200, message_id=2),
            _row(_MONDAY + dt.timedelta(days=1), 10, 100, message_id=3),
            _row(_MONDAY + dt.timedelta(days=1), 20, 200, message_id=4),
            # 10 shows up on Wednesday without solving it cleanly; the day
            # still counts as attended, so 10 stays on the board.
            _row(_MONDAY + dt.timedelta(days=2), 10, 50, perfect=False,
                 accuracy=80, message_id=5),
            _row(_MONDAY + dt.timedelta(days=2), 20, 200, message_id=6),
            _row(_MONDAY + dt.timedelta(days=2), 30, 300, message_id=7),
        ]
        recap = build_week_recap(
            rows, display_name='Daily Akari', anchor_date=_MONDAY,
            today=_SUNDAY, top_kwargs={})
        ranked = {entry.user_id for entry in recap.paces}
        assert '10' in ranked and '20' in ranked
        # 30 only played one of the three days.
        assert '30' not in ranked

    def test_the_leader_is_the_first_entry_not_the_first_solo_winner(self):
        """The header names leaders[0]; scanning for a solo win misnames it."""
        recap = _recap(_week({
            0: {10: 60, 20: 60},   # 10 and 20 tie
            1: {10: 60, 20: 60},
            2: {10: 60, 20: 60},
            3: {30: 50, 10: 70},   # 30 wins one outright
        }))
        assert recap.leaders[0][0] in ('10', '20')
        # 30 is the first entry with a solo win but the last on the board.
        assert recap.leaders[-1] == ('30', 1, 0)
        leader_total = recap.leaders[0][1] + recap.leaders[0][2]
        assert leader_total == 3

"""Pure tests for the experimental Queens soft-bracket Elo replay."""

import math
import random
from collections import namedtuple

from tle.util.queens_improved_rating import (
    _MAX_DAILY_CHANGE,
    _compute_round,
    _soft_time_score,
    compute_queens_improved_ratings,
)


Result = namedtuple(
    'Result',
    'message_id user_id puzzle_number puzzle_date '
    'time_seconds is_perfect accuracy raw_content',
)


def _row(user_id, puzzle, time_seconds, *, message_id=None):
    if message_id is None:
        message_id = puzzle * 1000 + int(str(user_id).strip('u') or 0)
    return Result(
        message_id=str(message_id),
        user_id=str(user_id),
        puzzle_number=puzzle,
        puzzle_date=f'2026-06-{puzzle:02d}',
        time_seconds=time_seconds,
        is_perfect=True,
        accuracy=100,
        raw_content='',
    )


def _day(puzzle, results):
    return [
        _row(user_id, puzzle, time_seconds, message_id=puzzle * 1000 + index)
        for index, (user_id, time_seconds) in enumerate(results, start=1)
    ]


def test_time_spacing_shapes_the_soft_bracket_and_performance():
    assert _soft_time_score(12, 12) == 0.5
    close_advantage = _soft_time_score(12, 13) - 0.5
    wider_advantage = _soft_time_score(13, 16) - 0.5
    assert 0 < close_advantage < wider_advantage

    times = {
        str(index): seconds
        for index, seconds in enumerate((7, 8, 10, 12, 13, 16, 20, 25))
    }
    updates = _compute_round({user: 1200 for user in times}, times)
    by_time = {
        seconds: updates[user].performance for user, seconds in times.items()
    }

    assert by_time[12] - by_time[13] < by_time[13] - by_time[16]
    assert abs((by_time[12] - by_time[13]) - 25.35) < 0.1
    assert abs((by_time[13] - by_time[16]) - 67.76) < 0.1


def test_replay_is_deterministic_and_dedupes_by_first_message():
    rows = []
    for puzzle in range(1, 7):
        rows.extend(_day(puzzle, [
            ('u1', 10 + puzzle),
            ('u2', 30),
            ('u3', 50 - puzzle),
            ('u4', 80),
        ]))
    # A later, faster resubmission must not replace u2's locked first result.
    rows.append(_row('u2', 3, 1, message_id=3999))

    histories_a = {}
    expected = compute_queens_improved_ratings(rows, histories=histories_a)
    shuffled = list(rows)
    random.Random(20260729).shuffle(shuffled)
    histories_b = {}
    actual = compute_queens_improved_ratings(shuffled, histories=histories_b)

    assert actual == expected
    assert histories_b == histories_a
    assert len(histories_a['u2']) == 6


def test_solo_days_seed_players_without_rating_signal():
    histories = {}
    states = compute_queens_improved_ratings(
        [_row('u1', 1, 18), _row('u2', 2, 12)],
        histories=histories,
        include_decay_in_history=True,
    )

    for user_id, puzzle in [('u1', 1), ('u2', 2)]:
        state = states[user_id]
        assert state.rating == 1200
        assert state.games == 0
        assert state.peak == 1200
        assert state.last_delta == 0
        assert state.skip_streak == 0
        assert state.last_puzzle == puzzle
        assert len(histories[user_id]) == 1
        point = histories[user_id][0]
        assert point.delta == 0
        assert point.performance is None
        assert point.is_decay is False


def test_equal_times_share_performance_and_stay_symmetric():
    rows = []
    for puzzle in range(1, 6):
        rows.extend(_day(puzzle, [('u1', 20), ('u2', 20), ('u3', 20)]))
    histories = {}
    states = compute_queens_improved_ratings(rows, histories=histories)

    assert {state.rating for state in states.values()} == {1200}
    assert {state.games for state in states.values()} == {5}
    for points in histories.values():
        assert all(point.rating == 1200 for point in points)
        assert all(abs(point.performance - 1200) < 1e-9 for point in points)


def test_rating_delta_and_performance_follow_the_daily_result():
    histories = {}
    states = compute_queens_improved_ratings(
        _day(1, [('winner', 10), ('middle', 20), ('loser', 30)]),
        histories=histories,
    )

    assert (
        states['winner'].rating
        > states['middle'].rating
        > states['loser'].rating
    )
    performances = [
        histories[user_id][0].performance
        for user_id in ('winner', 'middle', 'loser')
    ]
    assert all(math.isfinite(value) for value in performances)
    assert performances[0] > performances[1] > performances[2]
    for user_id in states:
        point = histories[user_id][0]
        assert (point.delta > 0) == (point.performance > 1200)
        assert (point.delta < 0) == (point.performance < 1200)


def test_round_is_field_normalized_zero_sum_and_capped():
    def equivalent_field(size):
        ratings = {'fast': 1200}
        times = {'fast': 10}
        for index in range(size - 1):
            ratings[f'slow{index}'] = 1200
            times[f'slow{index}'] = 20
        return _compute_round(ratings, times)

    field_10 = equivalent_field(10)
    field_20 = equivalent_field(20)

    # Doubling a realistic field changes one result by only the shrinking
    # neutral-self prior, rather than multiplying it by the opponent count.
    ratio = field_20['fast'].delta / field_10['fast'].delta
    assert 1 < ratio < 1.06

    upset_ratings = {'favorite': 2500, 'a': 1200, 'b': 1100, 'c': 1000}
    upset_times = {'favorite': 1000, 'a': 10, 'b': 20, 'c': 30}
    upset = _compute_round(upset_ratings, upset_times)
    deltas = [update.delta for update in upset.values()]
    assert max(map(abs, deltas)) <= _MAX_DAILY_CHANGE
    assert math.isclose(max(map(abs, deltas)), _MAX_DAILY_CHANGE)
    assert abs(sum(deltas)) < 1e-12


def test_one_time_outlier_cannot_flatten_the_middle():
    ratings = {str(index): 1200 for index in range(8)}
    ordinary_times = dict(zip(ratings, (7, 8, 10, 12, 13, 16, 20, 25)))
    outlier_times = dict(ordinary_times)
    outlier_times['7'] = 2500

    ordinary = _compute_round(ratings, ordinary_times)
    outlier = _compute_round(ratings, outlier_times)

    for user_id in list(ratings)[:-1]:
        assert abs(outlier[user_id].delta - ordinary[user_id].delta) < 6
    assert (
        outlier['3'].performance - outlier['4'].performance
        < outlier['4'].performance - outlier['5'].performance
    )


def test_history_contract_and_inactivity_state():
    rows = _day(1, [('u1', 10), ('u2', 30)])
    rows.extend(_day(2, [('u2', 10), ('u3', 30)]))
    histories = {}
    states = compute_queens_improved_ratings(
        rows,
        histories=histories,
        include_decay_in_history=True,
    )

    # Inactivity changes metadata only: never visible rating or history.
    assert states['u1'].rating == histories['u1'][0].rating
    assert states['u1'].last_delta == 0
    assert states['u1'].skip_streak == 1
    assert states['u1'].last_puzzle == 1
    assert len(histories['u1']) == 1
    assert histories['u1'][0].is_decay is False

    for user_id, points in histories.items():
        assert len(points) == states[user_id].games
        for point in points:
            assert point.puzzle_number >= 1
            assert math.isfinite(point.rating)
            assert math.isfinite(point.delta)
            assert point.performance is None or math.isfinite(point.performance)


def test_max_puzzle_and_compatibility_kwargs_are_honored():
    rows = _day(1, [('fast', 10), ('slow', 30)])
    rows.extend(_day(999, [('fast', 10), ('slow', 30)]))

    def reverse_rank(_day_rows):
        return {'fast': 2, 'slow': 1}

    states = compute_queens_improved_ratings(
        rows,
        max_puzzle=10,
        rank_fn=reverse_rank,
        damping=999,
    )

    # The margin model accepts shared-engine kwargs but intentionally measures
    # the Queens times themselves rather than replacing them with ordinal rank.
    assert states['fast'].rating > states['slow'].rating
    assert states['fast'].games == 1
    assert states['fast'].last_puzzle == 1

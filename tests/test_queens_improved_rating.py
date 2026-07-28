"""Pure tests for the experimental Queens Glicko-2 replay."""

import math
import random
from collections import namedtuple

from tle.util.queens_improved_rating import (
    _rate_player,
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


def test_official_glicko2_worked_example_translated_to_1200():
    # Glickman's example is 1500/200/.06 against 1400/30 (win),
    # 1550/100 (loss), and 1700/300 (loss). Translate every rating by
    # -300 because this engine's public scale is centered at 1200.
    update = _rate_player(
        1200,
        200,
        0.06,
        [(1100, 30, 1), (1250, 100, 0), (1400, 300, 0)],
    )

    assert abs(update.rating - 1164.06) < 0.02
    assert abs(update.rd - 151.52) < 0.01
    assert abs(update.volatility - 0.059996) < 0.000001


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


def test_equal_times_share_a_rank_and_stay_symmetric():
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


def test_rating_and_performance_follow_the_daily_standing():
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


def test_newcomer_with_more_uncertainty_responds_more():
    rows = []
    # Repeated equal results establish u1 and u2 without moving their means,
    # while their RDs contract.
    for puzzle in range(1, 13):
        rows.extend(_day(puzzle, [('u1', 20), ('u2', 20)]))
    # New u3 ties established u1 for first and both beat u2. Their evidence is
    # equivalent, but u3's larger uncertainty should permit the larger update.
    rows.extend(_day(13, [('u1', 10), ('u3', 10), ('u2', 30)]))
    histories = {}
    compute_queens_improved_ratings(rows, histories=histories)

    established_delta = histories['u1'][-1].delta
    newcomer_delta = histories['u3'][0].delta
    assert newcomer_delta > established_delta > 0


def test_history_contract_and_inactivity_state():
    rows = _day(1, [('u1', 10), ('u2', 30)])
    rows.extend(_day(2, [('u2', 10), ('u3', 30)]))
    histories = {}
    states = compute_queens_improved_ratings(
        rows,
        histories=histories,
        include_decay_in_history=True,
    )

    # Inactivity changes hidden uncertainty only. It must not invent a visible
    # rating delta or an is_decay history point.
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


def test_max_puzzle_and_custom_rank_function_are_honored():
    rows = _day(1, [('fast', 10), ('slow', 30)])
    rows.extend(_day(999, [('fast', 10), ('slow', 30)]))

    def reverse_rank(day_rows):
        ordered = sorted(day_rows, key=lambda row: row.time_seconds, reverse=True)
        return {str(row.user_id): index for index, row in enumerate(ordered, 1)}

    states = compute_queens_improved_ratings(
        rows,
        max_puzzle=10,
        rank_fn=reverse_rank,
        damping=999,  # Compatibility kwargs from the shared engine are ignored.
    )

    assert states['slow'].rating > states['fast'].rating
    assert states['slow'].games == 1
    assert states['slow'].last_puzzle == 1

"""Pure tests for the experimental Queens soft-bracket Elo replay."""

import math
import random
from collections import namedtuple
from decimal import Decimal

import pytest

from tle.util.queens_improved_rating import (
    _ELO_SCALE,
    _HEAD_TO_HEAD_WEIGHT,
    _RATING_K,
    _TIME_MARGIN_LOGIT_LIMIT,
    _TIME_MARGIN_WIDTH,
    _compute_round,
    _hybrid_time_score,
    _soft_time_score,
    _time_log,
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


def test_time_spacing_shapes_the_hybrid_bracket_and_performance():
    assert _RATING_K == 124
    assert _HEAD_TO_HEAD_WEIGHT == 0.15
    assert _soft_time_score(12, 12) == 0.5
    close_advantage = _soft_time_score(12, 13) - 0.5
    wider_advantage = _soft_time_score(13, 16) - 0.5
    assert 0 < close_advantage < wider_advantage
    assert _hybrid_time_score(12, 13) == (
        0.85 * _soft_time_score(12, 13) + 0.15
    )
    assert _hybrid_time_score(13, 12) == (
        0.85 * _soft_time_score(13, 12)
    )
    assert _hybrid_time_score(12, 12) == 0.5

    times = {
        str(index): seconds
        for index, seconds in enumerate((7, 8, 10, 12, 13, 16, 20, 25))
    }
    updates = _compute_round({user: 1200 for user in times}, times)
    by_time = {
        seconds: updates[user].performance for user, seconds in times.items()
    }

    assert by_time[12] - by_time[13] < by_time[13] - by_time[16]
    assert abs((by_time[12] - by_time[13]) - 77.52) < 0.1
    assert abs((by_time[13] - by_time[16]) - 160.91) < 0.1
    # The wider player-facing point scale should make the existing rank bands
    # meaningful while the underlying closeness response stays unchanged.
    assert updates['0'].delta > 30
    assert updates['7'].delta < -30


def test_photo_finish_gets_fifteen_percent_head_to_head_result():
    win = _hybrid_time_score(100, 101)
    loss = _hybrid_time_score(101, 100)

    assert math.isclose(win, 0.5810408654380644, abs_tol=1e-15)
    assert 0.575 < win < 1
    assert 0 < loss < 0.425
    assert math.isclose(win + loss, 1.0, abs_tol=1e-15)

    # Distinct enormous integers can share one floating-point logarithm. The
    # hard component must compare their exact validated seconds instead.
    huge = 10 ** 400
    assert _soft_time_score(huge, huge + 1) == 0.5
    assert _hybrid_time_score(huge, huge + 1) == 0.575


def test_extreme_pair_evidence_is_symmetric_and_never_separates():
    ordinary = _soft_time_score(1, 2)
    extreme = _soft_time_score(1, 10 ** 10_000)
    reverse = _soft_time_score(10 ** 10_000, 1)
    cap_ratio = math.exp(_TIME_MARGIN_WIDTH * _TIME_MARGIN_LOGIT_LIMIT)
    beyond_cap = _soft_time_score(1, cap_ratio * 10)

    assert 0 < reverse < ordinary < extreme < 1
    assert extreme == beyond_cap
    assert abs(extreme + reverse - 1) < 1e-15
    assert _soft_time_score(30, 30) == 0.5


def test_time_transform_uses_raw_positive_seconds_without_an_offset():
    assert _time_log(1) == 0
    assert _time_log(4) == math.log(4)
    expected = 1 / (1 + math.exp(-math.log(2) / _TIME_MARGIN_WIDTH))
    assert abs(_soft_time_score(1, 2) - expected) < 1e-15
    with pytest.raises(ValueError):
        _time_log(0)


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


def test_solo_days_seed_players_and_track_concluded_absences():
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
        assert state.last_puzzle == puzzle
        point = histories[user_id][0]
        assert point.delta == 0
        assert point.performance is None
        assert point.is_decay is False
    assert states['u1'].skip_streak == 1
    assert states['u2'].skip_streak == 0
    assert len(histories['u1']) == 2
    assert histories['u1'][-1].is_decay is True
    assert histories['u1'][-1].delta == 0
    assert len(histories['u2']) == 1


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


def test_round_is_field_normalized_zero_sum_and_naturally_bounded():
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
    # The proper-score gradient naturally attenuates a wildly surprising
    # result; this is formula-driven rather than a post-hoc ±32 clamp.
    assert 10 < max(map(abs, deltas)) < 32
    assert max(map(abs, deltas)) < _RATING_K
    assert abs(sum(deltas)) < 1e-12


def test_one_time_outlier_cannot_flatten_the_middle():
    ratings = {str(index): 1200 for index in range(8)}
    ordinary_times = dict(zip(ratings, (7, 8, 10, 12, 13, 16, 20, 25)))
    outlier_times = dict(ordinary_times)
    outlier_times['7'] = 2500

    ordinary = _compute_round(ratings, ordinary_times)
    outlier = _compute_round(ratings, outlier_times)

    for user_id in list(ratings)[:-1]:
        assert abs(outlier[user_id].delta - ordinary[user_id].delta) < 12
    assert (
        outlier['3'].performance - outlier['4'].performance
        < outlier['4'].performance - outlier['5'].performance
    )


def test_random_rounds_preserve_points_and_natural_delta_bound():
    rng = random.Random(20260729)
    for trial in range(100):
        count = rng.randint(2, 25)
        users = [f'u{index}' for index in range(count)]
        ratings = {
            user: rng.uniform(400, 2400)
            for user in users
        }
        times = {
            user: rng.randint(1, 7200)
            for user in users
        }
        updates = _compute_round(ratings, times)

        assert abs(sum(update.delta for update in updates.values())) < 1e-10
        natural_bound = _RATING_K * (count - 1) / count
        for user, update in updates.items():
            assert abs(update.delta) <= natural_bound + 1e-10
            assert math.isfinite(update.performance)

        ordered = sorted(users, key=lambda user: times[user])
        for faster, slower in zip(ordered, ordered[1:]):
            assert (
                updates[faster].performance
                >= updates[slower].performance
            )

        shifted_ratings = {
            user: rating + 777 for user, rating in ratings.items()
        }
        shifted = _compute_round(shifted_ratings, times)
        for user in users:
            assert abs(shifted[user].delta - updates[user].delta) < 1e-10
            assert abs(
                shifted[user].performance
                - updates[user].performance
                - 777
            ) < 1e-9


def test_one_changed_time_has_bounded_influence_on_every_other_player():
    rng = random.Random(731984)
    for count in range(2, 26):
        users = [f'u{index}' for index in range(count)]
        ratings = {
            user: rng.uniform(600, 2200)
            for user in users
        }
        times = {
            user: rng.randint(2, 600)
            for user in users
        }
        victim = users[count // 2]
        ordinary = _compute_round(ratings, times)
        corrupted_times = dict(times)
        corrupted_times[victim] = 10 ** 400
        corrupted = _compute_round(ratings, corrupted_times)

        for user in users:
            change = abs(corrupted[user].delta - ordinary[user].delta)
            if user == victim:
                assert change <= _RATING_K * (count - 1) / count + 1e-10
            else:
                assert change <= _RATING_K / count + 1e-10


def test_replay_conserves_starting_mean_as_players_enter():
    rng = random.Random(424242)
    rows = []
    next_message = 1
    for puzzle in range(1, 31):
        # The observed pool grows, and some days deliberately have only one
        # participant.  Neither entry nor a solo day may create rating drift.
        observed = min(12, 1 + puzzle // 2)
        field_size = 1 if puzzle % 7 == 0 else min(observed, 2 + puzzle % 8)
        for user_index in rng.sample(range(observed), field_size):
            rows.append(_row(
                f'u{user_index}',
                puzzle,
                rng.randint(3, 300),
                message_id=next_message,
            ))
            next_message += 1

    states = compute_queens_improved_ratings(rows)

    assert len(states) == 12
    assert abs(sum(state.rating for state in states.values()) - 12 * 1200) < 1e-9


def test_malformed_times_are_quarantined_from_improved_replay():
    rows = [
        _row('u1', 1, -5, message_id=1),
        # A later valid share must not replace the malformed locked first one.
        _row('u1', 1, 5, message_id=99),
        _row('u2', 1, 10, message_id=2),
        _row('u3', 1, 20, message_id=3),
        _row('u4', 2, 0, message_id=4),
        _row('u5', 2, math.nan, message_id=5),
        _row('u6', 2, 1.5, message_id=6),
        _row('u7', 2, math.inf, message_id=7),
        _row('u8', 2, None, message_id=8),
        _row('u9', 2, 'not-a-time', message_id=9),
        _row('u12', 2, Decimal('1.5'), message_id=12),
        # Quarantining u11 leaves a valid solo day. It remains uncontested but
        # can receive the zero-sum pool from an above-start absentee.
        _row('u10', 3, 15, message_id=10),
        _row('u11', 3, -1, message_id=11),
    ]

    states = compute_queens_improved_ratings(rows)

    assert set(states) == {'u2', 'u3', 'u10'}
    assert states['u2'].rating > 1200 > states['u3'].rating
    assert states['u10'].rating > 1200
    assert states['u10'].games == 0
    assert math.isclose(
        sum(state.rating for state in states.values()), 3600,
        abs_tol=1e-9,
    )
    with pytest.raises(ValueError):
        _time_log(-1)
    with pytest.raises(ValueError):
        _time_log(math.inf)
    assert math.isfinite(_time_log(10 ** 10_000))


def test_history_contract_and_inactivity_state():
    rows = _day(1, [('u1', 10), ('u2', 30)])
    rows.extend(_day(2, [('u2', 10), ('u3', 30)]))
    histories = {}
    states = compute_queens_improved_ratings(
        rows,
        histories=histories,
        include_decay_in_history=True,
    )

    # Above-start inactivity is a visible, optional history point.
    assert states['u1'].rating == histories['u1'][-1].rating
    assert states['u1'].rating < histories['u1'][0].rating
    assert states['u1'].last_delta == histories['u1'][-1].delta < 0
    assert states['u1'].skip_streak == 1
    assert states['u1'].last_puzzle == 1
    assert len(histories['u1']) == 2
    assert histories['u1'][0].is_decay is False
    assert histories['u1'][-1].is_decay is True

    for user_id, points in histories.items():
        assert sum(not point.is_decay for point in points) == states[user_id].games
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

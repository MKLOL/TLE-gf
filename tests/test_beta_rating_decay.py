"""Decay behavior shared by the Queens and Akari beta ladders."""

import math
from collections import namedtuple

from tle import constants
from tle.cogs._mgimpl_rating import ImplRatingMixin
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.util.akari_beta_rating import compute_akari_beta_ratings
from tle.util.queens_improved_rating import compute_queens_improved_ratings


Result = namedtuple(
    'Result',
    'message_id user_id puzzle_number puzzle_date '
    'time_seconds is_perfect accuracy raw_content',
)


def _row(user_id, puzzle_number, time_seconds, *, message_id):
    return Result(
        message_id=str(message_id),
        user_id=str(user_id),
        puzzle_number=puzzle_number,
        puzzle_date=f'2026-06-{puzzle_number:02d}',
        time_seconds=time_seconds,
        is_perfect=True,
        accuracy=100,
        raw_content='',
    )


def _two_day_rows(*, malformed_second_day=False):
    rows = [
        _row('fast', 1, 10, message_id=1),
        _row('slow', 1, 30, message_id=2),
    ]
    rows.append(_row(
        'slow', 2, 0 if malformed_second_day else 20, message_id=3))
    return rows


def _without_decay(rows, **kwargs):
    return compute_queens_improved_ratings(
        rows, decay_base=0.0, decay_max=0.0, **kwargs)


def test_beta_decay_is_zero_sum_and_solo_player_receives_the_pool():
    rows = _two_day_rows()
    baseline = _without_decay(rows)
    histories = {}
    states = compute_queens_improved_ratings(
        rows, histories=histories, include_decay_in_history=True)

    expected_loss = (
        (1200.0 - baseline['fast'].rating) * constants.AKARI_DECAY_BASE
    )
    assert expected_loss < 0
    assert math.isclose(
        states['fast'].rating,
        baseline['fast'].rating + expected_loss,
        abs_tol=1e-9,
    )
    assert math.isclose(
        states['slow'].rating,
        baseline['slow'].rating - expected_loss,
        abs_tol=1e-9,
    )
    assert math.isclose(
        states['fast'].rating + states['slow'].rating, 2400.0,
        abs_tol=1e-9,
    )
    assert states['fast'].skip_streak == 1
    assert states['fast'].last_delta == expected_loss
    assert states['slow'].games == 1
    assert histories['fast'][-1].is_decay is True
    assert histories['fast'][-1].delta == expected_loss
    assert histories['slow'][-1].is_decay is False
    assert histories['slow'][-1].delta == -expected_loss


def test_current_beta_puzzle_does_not_decay_absent_players():
    rows = _two_day_rows()
    baseline = _without_decay(rows)
    states = compute_queens_improved_ratings(
        rows, current_puzzle_number=2)

    assert states['fast'].rating == baseline['fast'].rating
    assert states['slow'].rating == baseline['slow'].rating
    assert states['fast'].skip_streak == 0
    assert states['fast'].last_delta > 0


def test_fully_invalid_beta_day_is_ignored_instead_of_triggering_decay():
    rows = _two_day_rows(malformed_second_day=True)
    first_day = _without_decay(rows[:2])
    histories = {}
    states = compute_queens_improved_ratings(
        rows, histories=histories, include_decay_in_history=True)

    assert states == first_day
    assert all(len(points) == 1 for points in histories.values())


def test_sub_start_beta_absentee_freezes_but_streak_advances():
    rows = [
        _row('fast', 1, 10, message_id=1),
        _row('slow', 1, 30, message_id=2),
        _row('fast', 2, 20, message_id=3),
    ]
    baseline = _without_decay(rows)
    histories = {}
    states = compute_queens_improved_ratings(
        rows, histories=histories, include_decay_in_history=True)

    assert states['slow'].rating == baseline['slow'].rating
    assert states['slow'].last_delta == 0
    assert states['slow'].skip_streak == 1
    assert histories['slow'][-1].is_decay is True
    assert histories['slow'][-1].delta == 0


def test_akari_beta_adapter_uses_the_shared_decay_engine():
    rows = _two_day_rows()
    baseline = compute_akari_beta_ratings(
        rows, decay_base=0.0, decay_max=0.0)
    states = compute_akari_beta_ratings(rows)

    assert states['fast'].rating < baseline['fast'].rating
    assert states['slow'].rating > baseline['slow'].rating
    assert math.isclose(
        sum(state.rating for state in states.values()), 2400.0,
        abs_tol=1e-9,
    )


def test_queens_beta_runtime_enables_decay_without_changing_canonical():
    mixin = ImplRatingMixin()
    canonical = mixin._minigame_compute_kwargs(
        QUEENS_GAME, improved=False)
    beta = mixin._minigame_compute_kwargs(QUEENS_GAME, improved=True)

    assert canonical['decay_base'] == 0.0
    assert canonical['decay_max'] == 0.0
    assert beta['decay_base'] == constants.AKARI_DECAY_BASE
    assert beta['decay_max'] == constants.AKARI_DECAY_MAX
    assert beta['decay_grace'] == constants.AKARI_DECAY_GRACE
    assert isinstance(beta['current_puzzle_number'], int)

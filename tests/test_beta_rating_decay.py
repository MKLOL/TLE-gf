"""Akari and Queens beta decay behavior."""

import datetime as dt
import math
from collections import namedtuple

from tle import constants
from tle.cogs import _minigame_queens as queens_module
from tle.cogs import _minigame_queens_cog as queens_cog_module
from tle.cogs._mgimpl_rating import ImplRatingMixin
from tle.cogs._minigame_queens import QUEENS_GAME, current_puzzle_number
from tle.util.akari_beta_rating import compute_akari_beta_ratings
from tle.util.queens_improved_rating import (
    _FIELD_DEFLATION,
    compute_queens_improved_ratings,
)


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


def _with_akari_decay(rows, **kwargs):
    return compute_queens_improved_ratings(
        rows,
        decay_base=constants.AKARI_DECAY_BASE,
        decay_max=constants.AKARI_DECAY_MAX,
        decay_grace=constants.AKARI_DECAY_GRACE,
        **kwargs,
    )


def test_beta_decay_preserves_field_deflation_and_solo_receives_the_pool():
    rows = _two_day_rows()
    baseline = _without_decay(rows)
    histories = {}
    states = _with_akari_decay(
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
        states['fast'].rating + states['slow'].rating,
        2400.0 - 2 * _FIELD_DEFLATION,
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
    states = _with_akari_decay(
        rows, current_puzzle_number=2)

    assert states['fast'].rating == baseline['fast'].rating
    assert states['slow'].rating == baseline['slow'].rating
    assert states['fast'].skip_streak == 0
    assert states['fast'].last_delta > 0


def test_fully_invalid_beta_day_is_ignored_instead_of_triggering_decay():
    rows = _two_day_rows(malformed_second_day=True)
    first_day = _without_decay(rows[:2])
    histories = {}
    states = _with_akari_decay(
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
    states = _with_akari_decay(
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
        sum(state.rating for state in states.values()),
        2400.0 - 2 * _FIELD_DEFLATION,
        abs_tol=1e-9,
    )


def test_queens_beta_runtime_keeps_canonical_decay_policy():
    mixin = ImplRatingMixin()
    canonical = mixin._minigame_compute_kwargs(
        QUEENS_GAME, improved=False)
    beta = mixin._minigame_compute_kwargs(QUEENS_GAME, improved=True)

    assert canonical['decay_base'] == constants.QUEENS_DECAY_BASE
    assert canonical['decay_max'] == constants.QUEENS_DECAY_MAX
    assert beta == canonical
    assert beta['decay_base'] == constants.QUEENS_DECAY_BASE
    assert beta['decay_max'] == constants.QUEENS_DECAY_MAX
    assert beta['decay_grace'] == constants.QUEENS_DECAY_GRACE
    # The in-progress puzzle must not decay players who haven't posted yet.
    assert beta['current_puzzle_number'] == current_puzzle_number()


def test_queens_decay_gate_follows_the_pacific_puzzle_day(monkeypatch):
    """The open day is LinkedIn's, not the host's ``date.today()``."""
    monkeypatch.setattr(
        queens_module, '_queens_current_puzzle_date',
        lambda: dt.date(2026, 6, 8))
    assert current_puzzle_number() == 769

    monkeypatch.setattr(
        queens_module, '_queens_current_puzzle_date',
        lambda: dt.date(2026, 6, 9))
    assert current_puzzle_number() == 770


def test_queens_calendar_has_a_single_source_of_truth():
    """The cog module re-exports the anchor helpers, never a second copy."""
    assert (queens_cog_module._queens_puzzle_number_for_date
            is queens_module._queens_puzzle_number_for_date)
    assert (queens_cog_module._queens_date_for_puzzle_number
            is queens_module._queens_date_for_puzzle_number)
    assert (queens_cog_module._queens_current_puzzle_date
            is queens_module._queens_current_puzzle_date)
    assert (queens_cog_module._QUEENS_ANCHOR_DATE
            is queens_module._QUEENS_ANCHOR_DATE)
    assert (queens_cog_module._QUEENS_ANCHOR_NUMBER
            == queens_module._QUEENS_ANCHOR_NUMBER)


def test_queens_beta_engine_defaults_to_no_decay():
    rows = _two_day_rows()

    assert compute_queens_improved_ratings(rows) == _without_decay(rows)

"""Policy tests for the beta ladder's field-only anti-inflation step."""

import math

from tle.util.queens_improved_rating import (
    _FIELD_DEFLATION,
    _RoundUpdate,
    _apply_field_correction,
)


def test_field_correction_centers_then_deflates_every_player_equally():
    updates = {
        'a': _RoundUpdate(delta=7.0, performance=1500.0),
        'b': _RoundUpdate(delta=1.0, performance=1200.0),
        'c': _RoundUpdate(delta=-2.0, performance=900.0),
    }

    corrected = _apply_field_correction(updates)

    assert math.isclose(
        sum(update.delta for update in corrected.values()),
        -len(updates) * _FIELD_DEFLATION,
        abs_tol=1e-12,
    )
    for left, right in (('a', 'b'), ('b', 'c')):
        assert math.isclose(
            corrected[left].delta - corrected[right].delta,
            updates[left].delta - updates[right].delta,
            abs_tol=1e-12,
        )
    assert {
        user: update.performance for user, update in corrected.items()
    } == {
        user: update.performance for user, update in updates.items()
    }


def test_field_correction_does_not_rate_a_solo_update():
    update = _RoundUpdate(delta=0.0, performance=None)

    assert _apply_field_correction({'solo': update}) == {'solo': update}

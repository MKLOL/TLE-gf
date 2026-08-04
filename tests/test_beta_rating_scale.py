"""Calibration invariants for the independent beta rating coordinates."""

import math

from tle.util._beta_rating_performance import (
    _PERFORMANCE_SEARCH_MARGIN,
    _RATING_POINT_SCALE,
)
from tle.util.akari_beta_rating import (
    _AKARI_RATING_K,
    _AKARI_RATING_POINT_SCALE,
)
from tle.util.queens_improved_rating import (
    _BASE_RATING_K,
    _RATING_K,
    _compute_round,
)


def test_game_scales_use_coherent_elo_k_and_performance_coordinates():
    assert _RATING_POINT_SCALE == 2.0
    assert _RATING_K == _RATING_POINT_SCALE * _BASE_RATING_K == 124.0
    assert _PERFORMANCE_SEARCH_MARGIN == 1600.0

    assert _AKARI_RATING_POINT_SCALE == 1.75
    assert (
        _AKARI_RATING_K
        == _AKARI_RATING_POINT_SCALE * _BASE_RATING_K
        == 108.5
    )


def test_akari_coordinate_preserves_the_latent_raw_round():
    queens_scale = _RATING_POINT_SCALE
    akari_scale = _AKARI_RATING_POINT_SCALE
    ratio = akari_scale / queens_scale
    queens_ratings = {'fast': 1575.0, 'middle': 1210.0, 'slow': 890.0}
    akari_ratings = {
        user: 1200.0 + ratio * (rating - 1200.0)
        for user, rating in queens_ratings.items()
    }
    times = {'fast': 10, 'middle': 19, 'slow': 47}

    queens = _compute_round(
        queens_ratings, times, rating_point_scale=queens_scale)
    akari = _compute_round(
        akari_ratings, times, rating_point_scale=akari_scale)

    for user in times:
        assert math.isclose(
            akari[user].delta,
            ratio * queens[user].delta,
            rel_tol=0,
            abs_tol=1e-12,
        )
        assert math.isclose(
            akari[user].performance,
            1200.0 + ratio * (queens[user].performance - 1200.0),
            rel_tol=0,
            abs_tol=1e-9,
        )

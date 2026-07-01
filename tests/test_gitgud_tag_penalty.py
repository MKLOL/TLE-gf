"""Tests for the gitgud tag-count point penalty.

``;gitgud`` divides a challenge's payout by ``(number of requested tags + 1)``
after division tags are stripped, never dropping below 1 point. One tag already
halves the reward; piling on tags collapses it toward the floor. This defangs
tag-spam: banning every hard category so an easy high-rated problem slips past
the filters used to still pay near-max points. The whole system derives points
from the stored ``rating_delta``, so the penalty is expressed as a delta on the
score ladder -- these tests pin the *resulting score* rather than the
intermediate delta.
"""
import pytest  # noqa: F401

from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudTagPenaltyDelta,
    _GITGUD_SCORE_DISTRIB,
)


def _score(base_delta, num_tags):
    """Score actually awarded for a challenge at ``base_delta`` requested with
    ``num_tags`` tags -- the composition the live code performs."""
    return _calculateGitgudScoreForDelta(
        _gitgudTagPenaltyDelta(base_delta, num_tags))


def _ladder_floor(target):
    """Largest achievable ladder score <= target (the reward is discrete)."""
    best = _GITGUD_SCORE_DISTRIB[0]
    for s in _GITGUD_SCORE_DISTRIB:
        if s <= target:
            best = s
        else:
            break
    return best


# Deltas that land squarely on each rung of the score ladder.
_MAX_DELTA = 300        # -> 23, the top rung
_MID_DELTA = 0          # -> 8
_LOW_DELTA = -100       # -> 5


class TestNoTagsIsUntouched:
    def test_zero_tags_returns_base_delta_unchanged(self):
        assert _gitgudTagPenaltyDelta(_MAX_DELTA, 0) == _MAX_DELTA
        assert _gitgudTagPenaltyDelta(-1234, 0) == -1234

    def test_zero_tags_keeps_full_score(self):
        assert _score(_MAX_DELTA, 0) == 23
        assert _score(_MID_DELTA, 0) == 8


class TestOneTagAlreadyHalves:
    # One tag divides by two, so it is never a free full-points pick.
    def test_one_tag_divides_by_two(self):
        # 23 // 2 == 11 -> nearest rung at or below 11 is 8.
        assert _score(_MAX_DELTA, 1) == 8

    def test_one_tag_is_less_than_no_tags(self):
        assert _score(_MAX_DELTA, 1) < _score(_MAX_DELTA, 0)


class TestDivisionByTagCountPlusOne:
    @pytest.mark.parametrize('num_tags', range(1, 30))
    def test_matches_floored_division_rounded_to_ladder(self, num_tags):
        base = _calculateGitgudScoreForDelta(_MAX_DELTA)  # 23
        expected = _ladder_floor(max(1, base // (num_tags + 1)))
        assert _score(_MAX_DELTA, num_tags) == expected

    def test_two_tags_divides_by_three(self):
        # 23 // 3 == 7 -> nearest rung at or below 7 is 5.
        assert _score(_MAX_DELTA, 2) == 5

    def test_three_tags_divides_by_four(self):
        # 23 // 4 == 5, itself a rung -> no rounding loss.
        assert _score(_MAX_DELTA, 3) == 5

    def test_exact_ladder_hit_is_not_rounded(self):
        # delta 200 -> score 17, then 17 // (1 + 1) == 8, itself a rung.
        seventeen = 200
        assert _calculateGitgudScoreForDelta(seventeen) == 17
        assert _score(seventeen, 1) == 8


class TestNeverBelowOne:
    def test_heavy_tag_spam_collapses_to_one_point(self):
        # The motivating case: 23 tags on a top-rung problem pays exactly 1.
        # 23 // 24 == 0 -> floored to 1.
        assert _score(_MAX_DELTA, 23) == 1

    def test_more_tags_than_points_still_pays_one(self):
        assert _score(_MID_DELTA, 100) == 1     # 8 // 101 == 0 -> floored to 1
        assert _score(_LOW_DELTA, 9) == 1       # 5 // 10 == 0  -> floored to 1

    def test_already_minimal_base_stays_one(self):
        # A very negative delta already scores 1; the penalty can't push it to 0.
        assert _score(-5000, 7) == 1

    def test_result_is_always_a_positive_ladder_value(self):
        for delta in (-5000, -400, -100, 0, 200, 300, 9000):
            for n in range(0, 40):
                s = _score(delta, n)
                assert s >= 1
                assert s in _GITGUD_SCORE_DISTRIB

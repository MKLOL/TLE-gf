"""Tests for the gitgud tag-count point penalty.

``;gitgud`` penalises tags two ways (after division tags are stripped):

* any tag at all costs a flat ``_GITGUD_TAG_BASE_PENALTY`` off the delta -- a
  single tag is never a free full-points pick;
* from the second tag on, points are additionally divided by the tag count,
  never dropping below 1.

This defangs tag-spam: banning every hard category so an easy high-rated
problem slips past the filters used to still pay near-max points. The whole
system derives points from the stored ``rating_delta``, so the penalty is
expressed as a delta on the score ladder -- these tests pin the *resulting
score* rather than the intermediate delta.
"""
import pytest  # noqa: F401

from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudTagPenaltyDelta,
    _GITGUD_SCORE_DISTRIB,
    _GITGUD_TAG_BASE_PENALTY,
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
_MAX_DELTA = 300        # -> 23 raw; -> 12 after the flat -200
_MID_DELTA = 0          # -> 8 raw;  -> 3 after the flat -200
_LOW_DELTA = -100       # -> 5 raw;  -> 2 after the flat -200


class TestNoTagsIsUntouched:
    def test_zero_tags_returns_base_delta_unchanged(self):
        assert _gitgudTagPenaltyDelta(_MAX_DELTA, 0) == _MAX_DELTA
        assert _gitgudTagPenaltyDelta(-1234, 0) == -1234

    def test_zero_tags_keeps_full_score(self):
        assert _score(_MAX_DELTA, 0) == 23
        assert _score(_MID_DELTA, 0) == 8


class TestSingleTagAppliesFlatPenaltyOnly:
    # One tag costs the flat -200 but is NOT divided (division starts at two).
    @pytest.mark.parametrize('delta,expected', [
        (_MAX_DELTA, 12),   # 300 - 200 -> 100 -> 12
        (_MID_DELTA, 3),    # 0 - 200 -> -200 -> 3
        (_LOW_DELTA, 2),    # -100 - 200 -> -300 -> 2
        (-400, 1)])
    def test_one_tag_is_flat_penalty_no_division(self, delta, expected):
        assert _score(delta, 1) == expected

    def test_one_tag_matches_the_old_flat_penalty(self):
        # A single tag reproduces the pre-division behaviour exactly.
        for delta in (_MAX_DELTA, _MID_DELTA, _LOW_DELTA, 450, -50):
            expected = _calculateGitgudScoreForDelta(
                delta - _GITGUD_TAG_BASE_PENALTY)
            assert _score(delta, 1) == expected

    def test_one_tag_is_never_the_free_full_score(self):
        # The whole point of keeping the flat penalty: one tag < zero tags.
        assert _score(_MAX_DELTA, 1) < _score(_MAX_DELTA, 0)


class TestDivisionStartsAtTwoTags:
    @pytest.mark.parametrize('num_tags', range(1, 30))
    def test_matches_floored_division_rounded_to_ladder(self, num_tags):
        # Base for the division is the score AFTER the flat penalty.
        base = _calculateGitgudScoreForDelta(
            _MAX_DELTA - _GITGUD_TAG_BASE_PENALTY)  # 12
        expected = _ladder_floor(max(1, base // num_tags))
        assert _score(_MAX_DELTA, num_tags) == expected

    def test_two_tags_halves_the_penalised_score(self):
        # 300 -> 100 -> 12, then 12 // 2 == 6 -> nearest rung at/below 6 is 5.
        assert _score(_MAX_DELTA, 2) == 5

    def test_three_tags(self):
        # 12 // 3 == 4 -> nearest rung at/below 4 is 3.
        assert _score(_MAX_DELTA, 3) == 3

    def test_exact_ladder_hit_is_not_rounded(self):
        # delta 400 -> 200 -> 17, then 17 // 2 == 8, itself a rung.
        assert _calculateGitgudScoreForDelta(400 - _GITGUD_TAG_BASE_PENALTY) == 17
        assert _score(400, 2) == 8


class TestNeverBelowOne:
    def test_heavy_tag_spam_collapses_to_one_point(self):
        # The motivating case: 23 tags on a top-rung problem pays exactly 1.
        assert _score(_MAX_DELTA, 23) == 1

    def test_more_tags_than_points_still_pays_one(self):
        assert _score(_MID_DELTA, 100) == 1     # 3 // 100 == 0 -> floored to 1
        assert _score(_LOW_DELTA, 9) == 1       # 2 // 9 == 0  -> floored to 1

    def test_already_minimal_base_stays_one(self):
        # A very negative delta already scores 1; the penalty can't push it to 0.
        assert _score(-5000, 7) == 1

    def test_result_is_always_a_positive_ladder_value(self):
        for delta in (-5000, -400, -100, 0, 200, 300, 9000):
            for n in range(0, 40):
                s = _score(delta, n)
                assert s >= 1
                assert s in _GITGUD_SCORE_DISTRIB

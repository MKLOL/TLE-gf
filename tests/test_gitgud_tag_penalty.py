"""Tests for the gitgud tag-count point penalty.

``;gitgud`` divides a challenge's payout by ``(number of penalised tags + 1)``,
rounded up and never dropping below 1 point. One tag roughly halves the reward;
piling on tags collapses it toward the floor. This defangs tag-spam: banning
every hard category so an easy high-rated problem slips past the filters used
to still pay near-max points. These tests pin the *resulting score* rather than
the internal ``rating_delta`` encoding used to store penalised scores.
"""
import pytest  # noqa: F401

from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudPenalisedTagCount,
    _gitgudTagPenaltyDelta,
    _GITGUD_SCORE_DISTRIB,
)


def _score(base_delta, num_tags):
    """Score actually awarded for a challenge at ``base_delta`` requested with
    ``num_tags`` tags -- the composition the live code performs."""
    return _calculateGitgudScoreForDelta(
        _gitgudTagPenaltyDelta(base_delta, num_tags))


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
    def test_one_tag_divides_by_two_rounded_up(self):
        assert _score(_MAX_DELTA, 1) == 12

    def test_one_tag_is_less_than_no_tags(self):
        assert _score(_MAX_DELTA, 1) < _score(_MAX_DELTA, 0)

    def test_one_tag_can_award_non_ladder_score(self):
        assert _score(_MID_DELTA, 1) == 4


class TestDivisionByTagCountPlusOne:
    @pytest.mark.parametrize('num_tags', range(1, 30))
    def test_matches_ceiling_division(self, num_tags):
        base = _calculateGitgudScoreForDelta(_MAX_DELTA)  # 23
        expected = max(1, (base + num_tags) // (num_tags + 1))
        assert _score(_MAX_DELTA, num_tags) == expected

    def test_two_tags_divides_by_three(self):
        assert _score(_MAX_DELTA, 2) == 8

    def test_three_tags_divides_by_four(self):
        assert _score(_MAX_DELTA, 3) == 6

    def test_exact_integer_division_hit_is_not_rounded(self):
        twelve = 100
        assert _calculateGitgudScoreForDelta(twelve) == 12
        assert _score(twelve, 1) == 6

    def test_edu_single_tag_halves_top_score_rounded_up(self):
        num_tags = _gitgudPenalisedTagCount(['edu'], [])
        assert num_tags == 1
        assert _score(_MAX_DELTA, num_tags) == 12


class TestPenalisedTagCount:
    """Which requested tags actually subtract points."""

    def test_no_tags_is_zero(self):
        assert _gitgudPenalisedTagCount([], []) == 0

    def test_plain_topic_tags_all_count(self):
        assert _gitgudPenalisedTagCount(['dp', 'graphs'], []) == 2

    def test_bans_count(self):
        assert _gitgudPenalisedTagCount([], ['fft', 'flows']) == 2

    def test_require_div1_is_free(self):
        # The whole point: +div1 (hardest division) never subtracts.
        assert _gitgudPenalisedTagCount(['div1'], []) == 0

    def test_require_div1_is_free_regardless_of_case_or_space(self):
        assert _gitgudPenalisedTagCount([' Div1 ', 'DIV1'], []) == 0

    def test_edu_and_other_divisions_still_count(self):
        # These narrow to easier pools -- they were the dodge the user hit.
        assert _gitgudPenalisedTagCount(['edu'], []) == 1
        assert _gitgudPenalisedTagCount(['div2', 'div3', 'div4'], []) == 3

    def test_banning_div1_still_counts(self):
        # ~div1 removes the hardest problems, so it only makes things easier.
        assert _gitgudPenalisedTagCount([], ['div1']) == 1

    def test_banning_lower_divisions_is_free(self):
        # ;gitgud 3000 ~div3 ~div4 ~edu should not lose points for excluding
        # easier contest pools.
        assert _gitgudPenalisedTagCount([], ['div3', 'div4', 'edu']) == 0

    def test_banning_lower_divisions_is_free_regardless_of_case_or_space(self):
        assert _gitgudPenalisedTagCount([], [' Div3 ', 'DIV4', ' Edu ']) == 0

    def test_div1_free_but_the_rest_of_the_mix_counts(self):
        # +div1 exempt; +dp and ~fft still count -> 2.
        assert _gitgudPenalisedTagCount(['div1', 'dp'], ['fft']) == 2

    def test_lower_division_bans_do_not_hide_topic_tags(self):
        assert _gitgudPenalisedTagCount(['dp'], ['div3', 'fft', 'edu']) == 2


class TestNeverBelowOne:
    def test_heavy_tag_spam_collapses_to_one_point(self):
        # The motivating case: more tags than points pays exactly 1.
        assert _score(_MAX_DELTA, 23) == 1

    def test_more_tags_than_points_still_pays_one(self):
        assert _score(_MID_DELTA, 100) == 1
        assert _score(_LOW_DELTA, 9) == 1

    def test_already_minimal_base_stays_one(self):
        # A very negative delta already scores 1; the penalty can't push it to 0.
        assert _score(-5000, 7) == 1

    def test_result_is_always_in_score_bounds(self):
        for delta in (-5000, -400, -100, 0, 200, 300, 9000):
            for n in range(0, 40):
                s = _score(delta, n)
                assert s >= 1
                assert s <= _GITGUD_SCORE_DISTRIB[-1]

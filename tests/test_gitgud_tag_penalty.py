"""Tests for the gitgud tag-count point penalty.

``;gitgud`` divides normal challenge points by ``penalised_tag_count + 1``,
rounded up and never below 1. The numeric rating argument is not a tag; only
parsed ``+`` and ``~`` filters can affect the tag count.
"""
import pytest

from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudPenalisedTagCount,
    _gitgudTagPenaltyDelta,
    _GITGUD_SCORE_DISTRIB,
)
from tle.util import cf_format


def _score(base_delta, num_tags):
    """Score actually awarded for a challenge at ``base_delta`` requested with
    ``num_tags`` penalised tags -- the composition the live code performs."""
    return _calculateGitgudScoreForDelta(
        _gitgudTagPenaltyDelta(base_delta, num_tags))


# Deltas that land squarely on each rung of the score ladder.
_MAX_DELTA = 300        # -> 23
_HIGH_DELTA = 200       # -> 17
_MID_DELTA = 0          # -> 8
_LOW_DELTA = -100       # -> 5


class TestNoTagsIsUntouched:
    def test_zero_tags_returns_base_delta_unchanged(self):
        assert _gitgudTagPenaltyDelta(_MAX_DELTA, 0) == _MAX_DELTA
        assert _gitgudTagPenaltyDelta(-1234, 0) == -1234

    def test_zero_tags_keeps_full_score(self):
        assert _score(_MAX_DELTA, 0) == 23
        assert _score(_MID_DELTA, 0) == 8


class TestTagPenalty:
    @pytest.mark.parametrize('delta,expected', [
        (_MAX_DELTA, 12),   # ceil(23 / 2)
        (_HIGH_DELTA, 9),   # ceil(17 / 2)
        (_MID_DELTA, 4),    # ceil(8 / 2)
        (_LOW_DELTA, 3),    # ceil(5 / 2)
        (-400, 1),
    ])
    def test_one_penalised_tag_halves_score_rounded_up(self, delta, expected):
        assert _score(delta, 1) == expected

    def test_one_tag_is_less_than_no_tags(self):
        assert _score(_MAX_DELTA, 1) < _score(_MAX_DELTA, 0)

    @pytest.mark.parametrize('num_tags', range(1, 30))
    def test_matches_ceiling_division(self, num_tags):
        base = _calculateGitgudScoreForDelta(_MAX_DELTA)
        expected = max(1, (base + num_tags) // (num_tags + 1))
        assert _score(_MAX_DELTA, num_tags) == expected

    def test_two_penalised_tags_divides_by_three_rounded_up(self):
        assert _score(_MAX_DELTA, 2) == 8

    def test_can_award_non_ladder_score(self):
        assert _score(_HIGH_DELTA, 1) == 9

    def test_command_rating_argument_is_not_a_tag(self):
        args = ('3000', '+edu', '~div3', '~div4')
        tags = cf_format.parse_tags(args, prefix='+')
        bantags = cf_format.parse_tags(args, prefix='~')

        assert tags == ['edu']
        assert bantags == ['div3', 'div4']
        assert _gitgudPenalisedTagCount(tags, bantags) == 1
        assert _score(_MAX_DELTA, 1) == 12

    def test_user_examples(self):
        # User rating 2200, so these are problem rating minus user rating.
        assert _score(2500 - 2200, 0) == 23
        assert _score(2400 - 2200, 0) == 17
        assert _score(2500 - 2200, _gitgudPenalisedTagCount(['edu'], [])) == 12
        assert _score(
            2400 - 2200,
            _gitgudPenalisedTagCount([], ['div3', 'div4'])) == 17
        assert _score(2400 - 2200, _gitgudPenalisedTagCount(['dp'], [])) == 9
        assert _score(2500 - 2200, _gitgudPenalisedTagCount(['div1'], [])) == 23


class TestPenalisedTagCount:
    """Which requested tags actually switch to the tagged ladder."""

    def test_no_tags_is_zero(self):
        assert _gitgudPenalisedTagCount([], []) == 0

    def test_plain_topic_tags_all_count(self):
        assert _gitgudPenalisedTagCount(['dp', 'graphs'], []) == 2

    def test_bans_count(self):
        assert _gitgudPenalisedTagCount([], ['fft', 'flows']) == 2

    def test_require_div1_is_free(self):
        assert _gitgudPenalisedTagCount(['div1'], []) == 0

    def test_require_div1_is_free_regardless_of_case_or_space(self):
        assert _gitgudPenalisedTagCount([' Div1 ', 'DIV1'], []) == 0

    def test_edu_and_other_divisions_still_count(self):
        assert _gitgudPenalisedTagCount(['edu'], []) == 1
        assert _gitgudPenalisedTagCount(['div2', 'div3', 'div4'], []) == 3

    def test_banning_div1_still_counts(self):
        assert _gitgudPenalisedTagCount([], ['div1']) == 1

    def test_banning_lower_divisions_is_free(self):
        assert _gitgudPenalisedTagCount([], ['div3', 'div4', 'edu']) == 0

    def test_banning_lower_divisions_is_free_regardless_of_case_or_space(self):
        assert _gitgudPenalisedTagCount([], [' Div3 ', 'DIV4', ' Edu ']) == 0

    def test_div1_free_but_the_rest_of_the_mix_counts(self):
        assert _gitgudPenalisedTagCount(['div1', 'dp'], ['fft']) == 2

    def test_lower_division_bans_do_not_hide_topic_tags(self):
        assert _gitgudPenalisedTagCount(['dp'], ['div3', 'fft', 'edu']) == 2


class TestScoreBounds:
    def test_already_minimal_base_stays_one(self):
        assert _score(-5000, 7) == 1

    def test_result_is_always_in_score_bounds(self):
        for delta in (-5000, -400, -100, 0, 200, 300, 9000):
            for n in range(0, 40):
                s = _score(delta, n)
                assert s >= 1
                assert s <= _GITGUD_SCORE_DISTRIB[-1]

"""Tests for the gitgud tagged score ladder.

``;gitgud`` uses the normal score ladder with no penalised tags. With any
penalised tag, it uses the tagged ladder from ``;help gitgud``: a flat -200
rating-delta shift. The numeric rating argument is not a tag; only parsed ``+``
and ``~`` filters can affect the tag count.
"""
import pytest

from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudPenalisedTagCount,
    _gitgudTagPenaltyDelta,
    _GITGUD_SCORE_DISTRIB,
    _GITGUD_TAG_BASE_PENALTY,
)
from tle.util import cf_format


def _score(base_delta, num_tags):
    """Score actually awarded for a challenge at ``base_delta`` requested with
    ``num_tags`` penalised tags -- the composition the live code performs."""
    return _calculateGitgudScoreForDelta(
        _gitgudTagPenaltyDelta(base_delta, num_tags))


# Deltas that land squarely on each rung of the untagged score ladder.
_MAX_DELTA = 300        # -> 23; tagged -> 12
_MID_DELTA = 0          # -> 8;  tagged -> 3
_LOW_DELTA = -100       # -> 5;  tagged -> 2


class TestNoTagsIsUntouched:
    def test_zero_tags_returns_base_delta_unchanged(self):
        assert _gitgudTagPenaltyDelta(_MAX_DELTA, 0) == _MAX_DELTA
        assert _gitgudTagPenaltyDelta(-1234, 0) == -1234

    def test_zero_tags_keeps_full_score(self):
        assert _score(_MAX_DELTA, 0) == 23
        assert _score(_MID_DELTA, 0) == 8


class TestTaggedLadder:
    @pytest.mark.parametrize('delta,expected', [
        (_MAX_DELTA, 12),   # 300 - 200 -> 100 -> 12
        (_MID_DELTA, 3),    # 0 - 200 -> -200 -> 3
        (_LOW_DELTA, 2),    # -100 - 200 -> -300 -> 2
        (-400, 1),
    ])
    def test_any_penalised_tag_uses_tagged_ladder(self, delta, expected):
        assert _score(delta, 1) == expected

    def test_one_tag_is_less_than_no_tags(self):
        assert _score(_MAX_DELTA, 1) < _score(_MAX_DELTA, 0)

    def test_tagged_ladder_is_flat_delta_penalty(self):
        for delta in (_MAX_DELTA, _MID_DELTA, _LOW_DELTA, 450, -50):
            expected = _calculateGitgudScoreForDelta(
                delta - _GITGUD_TAG_BASE_PENALTY)
            assert _score(delta, 1) == expected

    def test_more_tags_do_not_change_the_tagged_ladder(self):
        for num_tags in range(1, 30):
            assert _score(_MAX_DELTA, num_tags) == 12

    def test_command_rating_argument_is_not_a_tag(self):
        args = ('3000', '+edu', '~div3', '~div4')
        tags = cf_format.parse_tags(args, prefix='+')
        bantags = cf_format.parse_tags(args, prefix='~')

        assert tags == ['edu']
        assert bantags == ['div3', 'div4']
        assert _gitgudPenalisedTagCount(tags, bantags) == 1
        assert _score(_MAX_DELTA, 1) == 12


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

    def test_result_is_always_a_positive_ladder_value(self):
        for delta in (-5000, -400, -100, 0, 200, 300, 9000):
            for n in range(0, 40):
                s = _score(delta, n)
                assert s >= 1
                assert s in _GITGUD_SCORE_DISTRIB

"""Formula behavior tests for rpoll."""
import datetime
from collections import namedtuple

import pytest

from tle.cogs.rpoll import (_apply_formula, _calculate_gitgud_score_for_delta,
                            _get_monthly_gitgud_score, _get_vote_weight)
from tle.util import codeforces_common as cf_common
from tests.rpoll_test_utils import FakeRpollDb


class TestApplyFormula:
    def test_sum_basic(self):
        assert _apply_formula('sum', [1200, 1800]) == 3000

    def test_sum_empty(self):
        assert _apply_formula('sum', []) == 0

    def test_sum_single(self):
        assert _apply_formula('sum', [1500]) == 1500

    def test_sum_with_zero(self):
        assert _apply_formula('sum', [0, 1500]) == 1500

    def test_exp_empty(self):
        assert _apply_formula('exp', []) == 0

    def test_exp_single_zero_rating(self):
        assert _apply_formula('exp', [0]) == 100

    def test_exp_single_800(self):
        assert _apply_formula('exp', [800]) == 400

    def test_exp_single_2000(self):
        assert _apply_formula('exp', [2000]) == 3200

    def test_exp_multiple(self):
        assert _apply_formula('exp', [800, 2000]) == 3600

    def test_exp_higher_rating_weighs_more(self):
        single_high = _apply_formula('exp', [2400])
        two_mid = _apply_formula('exp', [1200, 1200])
        assert single_high > two_mid

    def test_team_empty(self):
        assert _apply_formula('team', []) == 0

    def test_team_single_rating(self):
        assert _apply_formula('team', [1500]) == 1500

    def test_team_two_equal_ratings(self):
        assert _apply_formula('team', [1500, 1500]) == 1653

    def test_team_mixed_ratings(self):
        assert _apply_formula('team', [1500, 2000]) == 2018

    def test_team_zero_rating_does_not_change_positive_vote(self):
        assert _apply_formula('team', [0, 1500]) == 1500

    def test_osu_empty(self):
        assert _apply_formula('osu', []) == 0

    def test_osu_single_rating(self):
        assert _apply_formula('osu', [1500]) == 1500

    def test_osu_sorts_before_weighting(self):
        assert _apply_formula('osu', [1000, 2000]) == 2670

    def test_osu_multiple_ratings(self):
        assert _apply_formula('osu', [2400, 1200, 1200]) == 3743

    def test_osu_zero_rating_does_not_change_positive_vote(self):
        assert _apply_formula('osu', [0, 1500]) == 1500

    def test_unknown_formula_falls_back_to_sum(self):
        assert _apply_formula('unknown', [1200, 1800]) == 3000


class TestGitgudFormulaHelpers:
    def test_gitgud_score_for_delta_midrange(self):
        assert _calculate_gitgud_score_for_delta(0) == 8

    def test_gitgud_score_for_delta_low_cap(self):
        assert _calculate_gitgud_score_for_delta(-500) == 1

    def test_gitgud_score_for_delta_high_cap(self):
        assert _calculate_gitgud_score_for_delta(500) == 23

    @pytest.fixture
    def fake_db(self):
        database = FakeRpollDb()
        original = cf_common.user_db
        cf_common.user_db = database
        try:
            yield database
        finally:
            cf_common.user_db = original
            database.close()

    def test_monthly_gitgud_score_for_poll_month(self, fake_db):
        march = datetime.datetime(2026, 3, 17, 12, 0, 0).timestamp()
        march_5 = datetime.datetime(2026, 3, 5, 12, 0, 0).timestamp()
        march_10 = datetime.datetime(2026, 3, 10, 12, 0, 0).timestamp()
        april_2 = datetime.datetime(2026, 4, 2, 12, 0, 0).timestamp()
        fake_db._seed_monthly_gitgud_entry('user1', march_5, march_10, 0)
        fake_db._seed_monthly_gitgud_entry('user1', april_2, april_2, 300)
        assert _get_monthly_gitgud_score('user1', march) == 8

    def test_monthly_gitgud_score_applies_double_points_in_last_week(self, fake_db):
        march = datetime.datetime(2026, 3, 17, 12, 0, 0).timestamp()
        march_26 = datetime.datetime(2026, 3, 26, 12, 0, 0).timestamp()
        march_27 = datetime.datetime(2026, 3, 27, 12, 0, 0).timestamp()
        fake_db._seed_monthly_gitgud_entry('user1', march_26, march_27, 0)
        assert _get_monthly_gitgud_score('user1', march) == 16

    def test_vote_weight_uses_gg_score(self, fake_db):
        fake_db._seed_gudgitter_score('user1', 42)
        Poll = namedtuple('Poll', 'formula created_at')
        poll = Poll(formula='gg', created_at=datetime.datetime(2026, 3, 17, 12, 0, 0).timestamp())
        assert _get_vote_weight(poll, 'user1', 123) == 42

    def test_vote_weight_uses_mgg_score_for_poll_month(self, fake_db):
        created_at = datetime.datetime(2026, 3, 17, 12, 0, 0).timestamp()
        march_5 = datetime.datetime(2026, 3, 5, 12, 0, 0).timestamp()
        march_10 = datetime.datetime(2026, 3, 10, 12, 0, 0).timestamp()
        fake_db._seed_monthly_gitgud_entry('user1', march_5, march_10, 0)
        Poll = namedtuple('Poll', 'formula created_at')
        poll = Poll(formula='mgg', created_at=created_at)
        assert _get_vote_weight(poll, 'user1', 123) == 8

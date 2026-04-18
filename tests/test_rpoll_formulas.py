"""Formula behavior tests for rpoll."""
import datetime
from collections import namedtuple

import pytest

from tle.cogs.rpoll import (_apply_formula, _calculate_gitgud_score_for_delta,
                            _get_monthly_gitgud_score, _get_vote_weight,
                            _refresh_poll_ratings)
from tle.util import codeforces_common as cf_common
from tests.rpoll_test_utils import CHANNEL, GUILD, FakeRpollDb


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

    def test_fffff_empty(self):
        assert _apply_formula('fffff', []) == 0

    def test_fffff_single_1900(self):
        # max(0, 1 + 0/1600) * 100 = 100
        assert _apply_formula('fffff', [1900]) == 100

    def test_fffff_single_300(self):
        # max(0, 1 + (300 - 1900)/1600) = max(0, 0) = 0 → 0
        assert _apply_formula('fffff', [300]) == 0

    def test_fffff_below_threshold(self):
        # 1 + (100 - 1900)/1600 = 1 - 1.125 = -0.125 → max(0, -0.125) = 0
        assert _apply_formula('fffff', [100]) == 0

    def test_fffff_multiple(self):
        # 1900: max(0, 1) = 1; 2700: max(0, 1 + 800/1600) = 1.5
        # total = 2.5 * 100 = 250
        assert _apply_formula('fffff', [1900, 2700]) == 250

    def test_fffff_high_rating(self):
        # max(0, 1 + (3500 - 1900)/1600) = max(0, 2.0) = 2.0 → 200
        assert _apply_formula('fffff', [3500]) == 200

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


class TestRefreshPollRatings:
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

    def test_picks_up_rating_change(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        fake_db._seed_cf_user(111, GUILD, 'alice', 1500)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 1500)

        fake_db._seed_cf_user(111, GUILD, 'alice', 2400)
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        ratings = [row.rating for row in fake_db.get_rpoll_vote_ratings(pid)]
        assert ratings == [2400]

    def test_picks_up_handle_relink(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        fake_db._seed_cf_user(111, GUILD, 'alice', 1500)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 1500)

        fake_db._seed_cf_user(111, GUILD, 'bob', 2100)
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        ratings = [row.rating for row in fake_db.get_rpoll_vote_ratings(pid)]
        assert ratings == [2100]

    def test_unlinked_user_becomes_zero(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        fake_db._seed_cf_user(111, GUILD, 'alice', 1500)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 1500)

        fake_db.conn.execute('DELETE FROM user_handle WHERE user_id = ?', ('111',))
        fake_db.conn.commit()
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        ratings = [row.rating for row in fake_db.get_rpoll_vote_ratings(pid)]
        assert ratings == [0]

    def test_multiple_voters_each_refreshed(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        fake_db._seed_cf_user(111, GUILD, 'alice', 1500)
        fake_db._seed_cf_user(222, GUILD, 'bob', 1800)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 1500)
        fake_db.toggle_rpoll_vote(pid, 222, 1, 1800)

        fake_db._seed_cf_user(111, GUILD, 'alice', 1700)
        fake_db._seed_cf_user(222, GUILD, 'bob', 2000)
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        rows = {row.option_index: row.rating for row in fake_db.get_rpoll_vote_ratings(pid)}
        assert rows == {0: 1700, 1: 2000}

    def test_refreshes_all_options_for_same_user(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        fake_db._seed_cf_user(111, GUILD, 'alice', 1500)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 1500)
        fake_db.toggle_rpoll_vote(pid, 111, 1, 1500)

        fake_db._seed_cf_user(111, GUILD, 'alice', 2200)
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        ratings = [row.rating for row in fake_db.get_rpoll_vote_ratings(pid)]
        assert ratings == [2200, 2200]

    def test_no_voters_is_no_op(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='sum')
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)
        assert fake_db.get_rpoll_vote_ratings(pid) == []

    def test_gg_formula_uses_current_gitgud_score(self, fake_db):
        pid = fake_db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='gg')
        fake_db._seed_gudgitter_score(111, 10)
        fake_db.toggle_rpoll_vote(pid, 111, 0, 10)

        fake_db._seed_gudgitter_score(111, 75)
        poll = fake_db.get_rpoll(pid)
        _refresh_poll_ratings(poll, GUILD)

        ratings = [row.rating for row in fake_db.get_rpoll_vote_ratings(pid)]
        assert ratings == [75]

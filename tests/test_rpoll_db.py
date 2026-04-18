"""DB behavior tests for rpoll."""
import time

from tests.rpoll_test_utils import CHANNEL, GUILD, db


class TestCreateRpoll:
    def test_creates_poll_and_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Best algo?', ['BFS', 'DFS'], 'user1', 1000.0)
        assert pid is not None
        poll = db.get_rpoll(pid)
        assert poll.question == 'Best algo?'
        assert poll.guild_id == str(GUILD)
        assert poll.message_id is None

        opts = db.get_rpoll_options(pid)
        assert len(opts) == 2
        assert opts[0].label == 'BFS'
        assert opts[0].option_index == 0
        assert opts[1].label == 'DFS'
        assert opts[1].option_index == 1

    def test_auto_increments_poll_id(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['C', 'D'], 'u', 2.0)
        assert p2 > p1

    def test_five_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B', 'C', 'D', 'E'], 'u', 1.0)
        opts = db.get_rpoll_options(pid)
        assert len(opts) == 5
        assert [o.label for o in opts] == ['A', 'B', 'C', 'D', 'E']

    def test_default_expires_at(self, db):
        now = 1000.0
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', now)
        poll = db.get_rpoll(pid)
        assert poll.expires_at == now + 86400

    def test_custom_expires_at(self, db):
        now = 1000.0
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', now, expires_at=now + 3600)
        poll = db.get_rpoll(pid)
        assert poll.expires_at == now + 3600

    def test_poll_starts_open(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        poll = db.get_rpoll(pid)
        assert poll.closed == 0


class TestMessageId:
    def test_set_and_get(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 999888777)
        poll = db.get_rpoll(pid)
        assert poll.message_id == '999888777'

    def test_get_by_message_id(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 999888777)
        poll = db.get_rpoll_by_message_id(999888777)
        assert poll is not None
        assert poll.poll_id == pid

    def test_get_by_message_id_not_found(self, db):
        assert db.get_rpoll_by_message_id(12345) is None


class TestToggleVote:
    def test_vote_adds(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        added = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert added is True

    def test_vote_toggles_off(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        removed = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert removed is False

    def test_vote_toggle_on_again(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        added = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert added is True

    def test_multiple_options_same_user(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B', 'C'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user1', 1, 1500)
        totals = db.get_rpoll_totals(pid)
        totals_map = {row.option_index: row.total_rating for row in totals}
        assert totals_map[0] == 1500
        assert totals_map[1] == 1500

    def test_multiple_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user2', 0, 2000)
        db.toggle_rpoll_vote(pid, 'user3', 1, 1800)
        totals = db.get_rpoll_totals(pid)
        totals_map = {row.option_index: row.total_rating for row in totals}
        assert totals_map[0] == 3500
        assert totals_map[1] == 1800


class TestRpollTotals:
    def test_empty_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        totals = db.get_rpoll_totals(pid)
        assert len(totals) == 0

    def test_sums_ratings(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        db.toggle_rpoll_vote(pid, 'u3', 1, 2400)
        totals = db.get_rpoll_totals(pid)
        totals_map = {row.option_index: row.total_rating for row in totals}
        assert totals_map[0] == 3000
        assert totals_map[1] == 2400

    def test_after_unvote_total_decreases(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        totals = db.get_rpoll_totals(pid)
        totals_map = {row.option_index: row.total_rating for row in totals}
        assert totals_map[0] == 1800

    def test_zero_rating_counts(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 0)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1500)
        totals = db.get_rpoll_totals(pid)
        totals_map = {row.option_index: row.total_rating for row in totals}
        assert totals_map[0] == 1500


class TestVoteCount:
    def test_no_votes(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_vote_count(pid) == 0

    def test_counts_distinct_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 1, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 2000)
        assert db.get_rpoll_vote_count(pid) == 2

    def test_unvote_decreases_count(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        assert db.get_rpoll_vote_count(pid) == 0


class TestUserRating:
    def test_linked_user(self, db):
        db._seed_cf_user('user1', GUILD, 'tourist', 3800)
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 3800

    def test_unlinked_user(self, db):
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 0

    def test_linked_but_no_rating(self, db):
        db._seed_cf_user('user1', GUILD, 'newbie', None)
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 0

    def test_different_guilds(self, db):
        db._seed_cf_user('user1', GUILD, 'tourist', 3800)
        rating = db.get_rpoll_user_rating('user1', 999999)
        assert rating == 0


class TestGitgudScores:
    def test_all_time_gitgud_score(self, db):
        db._seed_gudgitter_score('user1', 42)
        assert db.get_gudgitter_score('user1') == 42

    def test_missing_all_time_gitgud_score_defaults_zero(self, db):
        assert db.get_gudgitter_score('missing') == 0

    def test_monthly_gitgud_entries_for_user(self, db):
        db._seed_monthly_gitgud_entry('user1', 1000, 2000, 150)
        db._seed_monthly_gitgud_entry('user2', 1000, 2000, 250)
        rows = db.get_gudgitters_timerange_for_user('user1', 1500, 2500)
        assert len(rows) == 1
        assert rows[0].rating_delta == 150
        assert rows[0].issue_time == 1000


class TestActiveRpolls:
    def test_returns_only_posted_polls(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.set_rpoll_message_id(p1, 111)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].poll_id == p1

    def test_empty_when_none_posted(self, db):
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert len(db.get_all_active_rpolls()) == 0

    def test_excludes_closed_polls(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 111)
        db.close_rpoll(pid)
        assert len(db.get_all_active_rpolls()) == 0

    def test_includes_open_excludes_closed(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.set_rpoll_message_id(p1, 111)
        db.set_rpoll_message_id(p2, 222)
        db.close_rpoll(p1)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].poll_id == p2


class TestCloseRpoll:
    def test_closes_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll(pid).closed == 0
        db.close_rpoll(pid)
        assert db.get_rpoll(pid).closed == 1

    def test_close_idempotent(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.close_rpoll(pid)
        db.close_rpoll(pid)
        assert db.get_rpoll(pid).closed == 1


class TestExpiredUnclosedRpolls:
    def test_no_expired_polls(self, db):
        now = time.time()
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', now, expires_at=now + 3600)
        assert db.get_expired_unclosed_rpolls() == []

    def test_finds_expired_poll(self, db):
        past = time.time() - 100
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400, expires_at=past)
        db.set_rpoll_message_id(pid, 111)
        expired = db.get_expired_unclosed_rpolls()
        assert len(expired) == 1
        assert expired[0].poll_id == pid

    def test_ignores_closed_polls(self, db):
        past = time.time() - 100
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400, expires_at=past)
        db.set_rpoll_message_id(pid, 111)
        db.close_rpoll(pid)
        assert db.get_expired_unclosed_rpolls() == []

    def test_ignores_polls_without_message_id(self, db):
        past = time.time() - 100
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400, expires_at=past)
        assert db.get_expired_unclosed_rpolls() == []

    def test_mixed_expired_and_active(self, db):
        now = time.time()
        past = now - 100
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', past - 86400, expires_at=past)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', now, expires_at=now + 3600)
        db.set_rpoll_message_id(p1, 111)
        db.set_rpoll_message_id(p2, 222)
        expired = db.get_expired_unclosed_rpolls()
        assert len(expired) == 1
        assert expired[0].poll_id == p1


class TestPollIsolation:
    def test_votes_dont_cross_polls(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.toggle_rpoll_vote(p1, 'u1', 0, 2000)
        db.toggle_rpoll_vote(p2, 'u1', 0, 2000)
        assert len(db.get_rpoll_totals(p1)) == 1
        assert len(db.get_rpoll_totals(p2)) == 1

    def test_vote_count_per_poll(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.toggle_rpoll_vote(p1, 'u1', 0, 1500)
        db.toggle_rpoll_vote(p1, 'u2', 0, 1500)
        db.toggle_rpoll_vote(p2, 'u3', 0, 1500)
        assert db.get_rpoll_vote_count(p1) == 2
        assert db.get_rpoll_vote_count(p2) == 1


class TestGetRpollVoters:
    def test_no_voters(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_voters(pid) == []

    def test_single_voter(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        voters = db.get_rpoll_voters(pid)
        assert len(voters) == 1
        assert voters[0].option_index == 0
        assert voters[0].user_id == 'u1'

    def test_multiple_voters_multiple_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B', 'C'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u3', 1, 1800)
        db.toggle_rpoll_vote(pid, 'u1', 2, 1500)
        voters = db.get_rpoll_voters(pid)
        assert len(voters) == 4
        option_0 = [vote for vote in voters if vote.option_index == 0]
        option_1 = [vote for vote in voters if vote.option_index == 1]
        option_2 = [vote for vote in voters if vote.option_index == 2]
        assert {vote.user_id for vote in option_0} == {'u1', 'u2'}
        assert {vote.user_id for vote in option_1} == {'u3'}
        assert {vote.user_id for vote in option_2} == {'u1'}

    def test_unvote_removes_from_voters(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        assert db.get_rpoll_voters(pid) == []


class TestAnonymousPoll:
    def test_create_anonymous_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        poll = db.get_rpoll(pid)
        assert poll.anonymous == 1

    def test_create_non_anonymous_poll_default(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        poll = db.get_rpoll(pid)
        assert poll.anonymous == 0

    def test_anonymous_flag_in_get_by_message_id(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        db.set_rpoll_message_id(pid, 12345)
        poll = db.get_rpoll_by_message_id(12345)
        assert poll.anonymous == 1

    def test_anonymous_flag_in_active_rpolls(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        db.set_rpoll_message_id(pid, 12345)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].anonymous == 1


class TestFormulaDb:
    def test_default_formula_is_sum(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        poll = db.get_rpoll(pid)
        assert poll.formula == 'sum'

    def test_custom_formula_exp(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='exp')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'exp'

    def test_custom_formula_team(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='team')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'team'

    def test_custom_formula_osu(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='osu')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'osu'

    def test_custom_formula_gg(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='gg')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'gg'

    def test_custom_formula_mgg(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='mgg')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'mgg'

    def test_custom_formula_fffff(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='fffff')
        poll = db.get_rpoll(pid)
        assert poll.formula == 'fffff'

    def test_formula_in_get_by_message_id(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='exp')
        db.set_rpoll_message_id(pid, 12345)
        poll = db.get_rpoll_by_message_id(12345)
        assert poll.formula == 'exp'

    def test_formula_in_active_rpolls(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, formula='exp')
        db.set_rpoll_message_id(pid, 12345)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].formula == 'exp'


class TestGetRpollVoterIds:
    def test_no_voters(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_voter_ids(pid) == []

    def test_distinct_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 1, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        ids = sorted(row.user_id for row in db.get_rpoll_voter_ids(pid))
        assert ids == ['u1', 'u2']


class TestUpdateRpollVoterRating:
    def test_updates_all_votes_for_user(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 1, 1500)
        db.update_rpoll_voter_rating(pid, 'u1', 2100)
        ratings = db.get_rpoll_vote_ratings(pid)
        assert sorted(row.rating for row in ratings) == [2100, 2100]

    def test_does_not_touch_other_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        db.update_rpoll_voter_rating(pid, 'u1', 2100)
        ratings = {row.rating for row in db.get_rpoll_vote_ratings(pid)}
        assert ratings == {2100, 1800}

    def test_no_op_when_user_has_no_votes(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.update_rpoll_voter_rating(pid, 'ghost', 9999)
        ratings = [row.rating for row in db.get_rpoll_vote_ratings(pid)]
        assert ratings == [1500]

    def test_scoped_to_poll(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.toggle_rpoll_vote(p1, 'u1', 0, 1500)
        db.toggle_rpoll_vote(p2, 'u1', 0, 1500)
        db.update_rpoll_voter_rating(p1, 'u1', 2100)
        p1_ratings = [row.rating for row in db.get_rpoll_vote_ratings(p1)]
        p2_ratings = [row.rating for row in db.get_rpoll_vote_ratings(p2)]
        assert p1_ratings == [2100]
        assert p2_ratings == [1500]


class TestGetRpollVoteRatings:
    def test_no_votes(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_vote_ratings(pid) == []

    def test_returns_individual_ratings(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 2000)
        db.toggle_rpoll_vote(pid, 'u3', 1, 1800)
        ratings = db.get_rpoll_vote_ratings(pid)
        assert len(ratings) == 3
        opt0 = [(row.option_index, row.rating) for row in ratings if row.option_index == 0]
        opt1 = [(row.option_index, row.rating) for row in ratings if row.option_index == 1]
        assert sorted(rating for _, rating in opt0) == [1500, 2000]
        assert opt1 == [(1, 1800)]

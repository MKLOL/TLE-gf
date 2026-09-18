"""Starboard DB tests for the narcissus (self-star) leaderboard, the same
message tracked under multiple emojis, and backfill-vs-live-reaction races.

Split out of ``test_starboard_edge_cases.py``; shares ``FakeUserDb`` and
constants from ``tests/starboard_test_utils.py``.
"""
import pytest

from tests.starboard_test_utils import FakeUserDb, GUILD_A, GUILD_B, STAR, FIRE, HEART


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Narcissus leaderboard (self-stars)
# =====================================================================

class TestNarcissusLeaderboard:
    """Narcissus is sticky-mark based: a live self-react on a tracked message
    records a permanent mark via the count-update hook (as the live reaction
    path does), so these tests trigger the hook after adding reactors."""

    def _setup_starboard(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)

    def test_self_star_counted(self, db):
        self._setup_starboard(db)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='user1')
        db.add_reactor('100', STAR, 'user1')  # self-star
        db.update_starboard_star_count('100', STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 1
        assert rows[0].user_id == 'user1'
        assert rows[0].self_stars == 1
        assert rows[0].unreacted == 0

    def test_non_self_star_not_counted(self, db):
        self._setup_starboard(db)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='user1')
        db.add_reactor('100', STAR, 'user2')  # not self-star
        db.update_starboard_star_count('100', STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 0

    def test_multiple_self_stars(self, db):
        self._setup_starboard(db)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('101', '201', GUILD_A, STAR, author_id='user1')
        db.add_reactor('100', STAR, 'user1')
        db.add_reactor('101', STAR, 'user1')
        db.update_starboard_star_count('100', STAR, 1)
        db.update_starboard_star_count('101', STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 1
        assert rows[0].self_stars == 2

    def test_ranking_order(self, db):
        self._setup_starboard(db)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('101', '201', GUILD_A, STAR, author_id='user2')
        db.add_starboard_message_v1('102', '202', GUILD_A, STAR, author_id='user2')
        db.add_reactor('100', STAR, 'user1')
        db.add_reactor('101', STAR, 'user2')
        db.add_reactor('102', STAR, 'user2')
        for msg in ('100', '101', '102'):
            db.update_starboard_star_count(msg, STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert rows[0].user_id == 'user2'
        assert rows[0].self_stars == 2
        assert rows[1].user_id == 'user1'
        assert rows[1].self_stars == 1

    def test_excludes_unknown_author(self, db):
        self._setup_starboard(db)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='__UNKNOWN__')
        db.add_reactor('100', STAR, '__UNKNOWN__')
        db.update_starboard_star_count('100', STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 0

    def test_empty_when_no_self_stars(self, db):
        self._setup_starboard(db)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 0

    def test_guild_isolation(self, db):
        self._setup_starboard(db)
        db.add_starboard_emoji(GUILD_B, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('100', '200', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('101', '201', GUILD_B, STAR, author_id='user1')
        db.add_reactor('100', STAR, 'user1')
        db.add_reactor('101', STAR, 'user1')
        db.update_starboard_star_count('100', STAR, 1)
        db.update_starboard_star_count('101', STAR, 1)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert len(rows) == 1
        assert rows[0].self_stars == 1


# =====================================================================
# Same message, different emojis (the core multi-emoji scenario)
# =====================================================================

class TestSameMessageMultiEmoji:
    def test_same_message_tracked_for_multiple_emojis(self, db):
        """A single Discord message can be starboarded by multiple emojis."""
        db.add_starboard_message_v1('msg1', 'sb_star', GUILD_A, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_fire', GUILD_A, FIRE, author_id='u')

        assert db.check_exists_starboard_message_v1('msg1', STAR)
        assert db.check_exists_starboard_message_v1('msg1', FIRE)
        assert not db.check_exists_starboard_message_v1('msg1', HEART)

    def test_live_reaction_sets_author_on_pre_backfill_message(self, db):
        """Simulates a live reaction on a message that hasn't been backfilled yet.
        The live path should set author_id so the message appears in leaderboards."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)  # No author_id
        assert db.get_all_starboard_messages_for_guild(GUILD_A)[0].author_id is None

        # Live reaction path now calls update_starboard_author_and_count
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.author_id == 'user1'
        assert msg.star_count == 5

        # Should now appear in leaderboards
        lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(lb) == 1
        assert lb[0].author_id == 'user1'

    def test_star_counts_independent_per_emoji(self, db):
        db.add_starboard_message_v1('msg1', 'sb_star', GUILD_A, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_fire', GUILD_A, FIRE, author_id='u')

        db.update_starboard_star_count('msg1', STAR, 5)
        db.update_starboard_star_count('msg1', FIRE, 10)

        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        by_emoji = {m.emoji: m for m in msgs}
        assert by_emoji[STAR].star_count == 5
        assert by_emoji[FIRE].star_count == 10

    def test_leaderboards_count_per_emoji(self, db):
        """User has messages in both star and fire — each leaderboard is independent."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        db.add_starboard_emoji(GUILD_A, FIRE, 1, 0xff0000)

        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('m2', 's2', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('m3', 's3', GUILD_A, FIRE, author_id='user1')
        db.update_starboard_star_count('m1', STAR, 5)
        db.update_starboard_star_count('m2', STAR, 5)
        db.update_starboard_star_count('m3', FIRE, 20)

        star_lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        fire_lb = db.get_starboard_leaderboard(GUILD_A, FIRE)
        assert star_lb[0].message_count == 2
        assert fire_lb[0].message_count == 1

        star_slb = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        fire_slb = db.get_starboard_star_leaderboard(GUILD_A, FIRE)
        assert star_slb[0].total_stars == 10
        assert fire_slb[0].total_stars == 20


# =====================================================================
# Backfill vs live reaction race conditions
# =====================================================================

class TestBackfillLiveRace:
    """Tests for the race condition between backfill and live reactions.

    Issue 1: Live path should set author_id (not just star_count) so
             pre-backfill messages appear in leaderboards immediately.
    Issue 2: Backfill should never regress star_count — uses MAX(star_count, ?).
    """

    def test_live_reaction_before_backfill_sets_author(self, db):
        """A live reaction on a pending-backfill message sets author_id."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)  # author_id=None
        # Simulate live reaction path: update_starboard_author_and_count
        db.update_starboard_author_and_count('msg1', STAR, 'user42', 6)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.author_id == 'user42'
        assert msg.star_count == 6

    def test_backfill_overwrites_star_count(self, db):
        """Backfill uses plain SET — it writes whatever the API returned.
        The race window is tiny (single-threaded asyncio) and self-healing."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)

        db.update_starboard_author_and_count('msg1', STAR, 'user1', 10)
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 7)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.star_count == 7  # Last write wins

    def test_unreaction_lowers_star_count(self, db):
        """Removing a reaction should lower star_count — plain SET allows this."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR, author_id='user1')

        db.update_starboard_author_and_count('msg1', STAR, 'user1', 10)
        # Someone removes their reaction
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 9)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.star_count == 9  # Correctly lowered

    def test_backfill_sets_author_after_live_star_count_only(self, db):
        """If live path used update_starboard_star_count (old behavior),
        backfill still sets author_id correctly."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)

        # Simulate old-style live path that only sets star_count
        db.update_starboard_star_count('msg1', STAR, 8)
        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.author_id is None
        assert msg.star_count == 8

        # Backfill runs with count=6, sets author — count also gets overwritten
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 6)
        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.author_id == 'user1'
        assert msg.star_count == 6  # Last write wins

    def test_live_reaction_after_backfill_still_updates(self, db):
        """After backfill completes, live reactions should still work normally."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)

        # Backfill sets author and count
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)

        # Live reaction updates count higher
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 7)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.star_count == 7
        assert msg.author_id == 'user1'

    def test_reaction_remove_after_backfill_can_lower_via_star_count(self, db):
        """update_starboard_star_count (used by reaction remove) does a plain SET,
        so it can lower the count — this is correct behavior for removals."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 10)

        # Reaction removed — count goes down
        db.update_starboard_star_count('msg1', STAR, 9)

        msg = db.get_all_starboard_messages_for_guild(GUILD_A)[0]
        assert msg.star_count == 9  # Correctly lowered

    def test_leaderboard_visible_after_live_reaction_no_backfill(self, db):
        """After a live reaction sets author_id, the message should appear
        in leaderboards even if backfill hasn't run yet."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR)  # No author

        # Initially invisible to leaderboards
        assert len(db.get_starboard_leaderboard(GUILD_A, STAR)) == 0

        # Live reaction sets author and count
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)

        # Now visible
        lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(lb) == 1
        assert lb[0].author_id == 'user1'

        slb = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert len(slb) == 1

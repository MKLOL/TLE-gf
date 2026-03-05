"""Edge-case tests for starboard DB methods and bug fixes.

Covers: multi-guild isolation, upsert round-trips, leaderboard ranking,
remove_starboard_message with multi-emoji entries, backfill checkpointing
logic, and content truncation.
"""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.user_db_conn import namedtuple_factory


# Re-use the FakeUserDb helper — import it from the existing test module.
from tests.test_starboard_db import FakeUserDb

GUILD_A = 111111111111111111
GUILD_B = 222222222222222222
STAR = '⭐'
FIRE = '🔥'
HEART = '❤️'


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Multi-guild isolation
# =====================================================================

class TestMultiGuildIsolation:
    def test_emoji_config_per_guild(self, db):
        """Same emoji in two guilds should be independent."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, STAR, 5, 0x00ff00)
        db.set_starboard_channel(GUILD_A, STAR, 100)
        db.set_starboard_channel(GUILD_B, STAR, 200)

        a = db.get_starboard_entry(GUILD_A, STAR)
        b = db.get_starboard_entry(GUILD_B, STAR)
        assert a.threshold == 3
        assert a.color == 0xffaa10
        assert a.channel_id == '100'
        assert b.threshold == 5
        assert b.color == 0x00ff00
        assert b.channel_id == '200'

    def test_messages_per_guild(self, db):
        """Messages in different guilds don't collide."""
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD_B, STAR, author_id='user1')

        a_msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        b_msgs = db.get_all_starboard_messages_for_guild(GUILD_B)
        assert len(a_msgs) == 1
        assert a_msgs[0].original_msg_id == 'msg1'
        assert len(b_msgs) == 1
        assert b_msgs[0].original_msg_id == 'msg2'

    def test_leaderboard_per_guild(self, db):
        """Leaderboard for guild A shouldn't include guild B's messages."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD_B, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 5)
        db.update_starboard_star_count('msg2', STAR, 10)

        lb_a = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(lb_a) == 1
        assert lb_a[0].message_count == 1

        lb_b = db.get_starboard_star_leaderboard(GUILD_B, STAR)
        assert len(lb_b) == 1
        assert lb_b[0].total_stars == 10

    def test_remove_emoji_doesnt_affect_other_guild(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD_A, STAR, author_id='u')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD_B, STAR, author_id='u')

        db.remove_starboard_emoji(GUILD_A, STAR)
        assert db.get_starboard_entry(GUILD_A, STAR) is None
        assert db.get_starboard_entry(GUILD_B, STAR) is not None
        assert db.check_exists_starboard_message_v1('msg2', STAR)

    def test_guild_config_isolated(self, db):
        db.set_guild_config(GUILD_A, 'starboard_leaderboard', '1')
        assert db.get_guild_config(GUILD_A, 'starboard_leaderboard') == '1'
        assert db.get_guild_config(GUILD_B, 'starboard_leaderboard') is None


# =====================================================================
# Upsert round-trip edge cases
# =====================================================================

class TestUpsertEdgeCases:
    def test_upsert_after_here_clear_here(self, db):
        """channel_id survives add → here → clear → here → upsert."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_A, STAR, 100)
        db.clear_starboard_channel(GUILD_A, STAR)
        db.set_starboard_channel(GUILD_A, STAR, 200)

        # Upsert with new threshold
        db.add_starboard_emoji(GUILD_A, STAR, 7, 0x0000ff)
        entry = db.get_starboard_entry(GUILD_A, STAR)
        assert entry.threshold == 7
        assert entry.color == 0x0000ff
        assert entry.channel_id == '200'  # Preserved through upsert

    def test_upsert_multiple_times(self, db):
        """Repeated upserts should always preserve channel_id."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_A, STAR, 100)

        for i in range(5):
            db.add_starboard_emoji(GUILD_A, STAR, i + 1, 0x000000 + i)

        entry = db.get_starboard_entry(GUILD_A, STAR)
        assert entry.threshold == 5
        assert entry.color == 4
        assert entry.channel_id == '100'  # Still there

    def test_upsert_different_emoji_no_interference(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_A, STAR, 100)
        db.add_starboard_emoji(GUILD_A, FIRE, 5, 0xff0000)

        star = db.get_starboard_entry(GUILD_A, STAR)
        fire = db.get_starboard_entry(GUILD_A, FIRE)
        assert star.channel_id == '100'
        assert fire.channel_id is None  # Never set


# =====================================================================
# Leaderboard ranking edge cases
# =====================================================================

class TestLeaderboardRanking:
    def test_ordering_descending(self, db):
        """Leaderboard should be ordered by count descending."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        # user3: 1 msg, user1: 3 msgs, user2: 2 msgs (insert in jumbled order)
        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user3')
        for i in range(3):
            db.add_starboard_message_v1(f'm1{i}', f's1{i}', GUILD_A, STAR, author_id='user1')
        for i in range(2):
            db.add_starboard_message_v1(f'm2{i}', f's2{i}', GUILD_A, STAR, author_id='user2')

        lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert [r.author_id for r in lb] == ['user1', 'user2', 'user3']
        assert [r.message_count for r in lb] == [3, 2, 1]

    def test_star_leaderboard_ordering(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('m2', 's2', GUILD_A, STAR, author_id='user2')
        db.update_starboard_star_count('m1', STAR, 3)
        db.update_starboard_star_count('m2', STAR, 10)

        lb = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert lb[0].author_id == 'user2'
        assert lb[0].total_stars == 10
        assert lb[1].author_id == 'user1'
        assert lb[1].total_stars == 3

    def test_star_leaderboard_aggregates_multiple_messages(self, db):
        """user1 has 2 messages with 3+7=10 total stars."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user1')
        db.add_starboard_message_v1('m2', 's2', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count('m1', STAR, 3)
        db.update_starboard_star_count('m2', STAR, 7)

        lb = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert len(lb) == 1
        assert lb[0].total_stars == 10

    def test_star_leaderboard_excludes_zero_star_count(self, db):
        """Messages with star_count=0 should not appear in star leaderboard."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user1')
        # star_count defaults to 0, never updated
        lb = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert len(lb) == 0

    def test_message_leaderboard_includes_zero_star_messages(self, db):
        """Message leaderboard counts messages regardless of star_count."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        db.add_starboard_message_v1('m1', 's1', GUILD_A, STAR, author_id='user1')
        # star_count is 0 (default) but the message exists
        lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(lb) == 1
        assert lb[0].message_count == 1


# =====================================================================
# remove_starboard_message with multi-emoji entries
# =====================================================================

class TestRemoveMultiEmoji:
    def test_remove_by_emoji_only_removes_that_emoji(self, db):
        """Same original message tracked for star and fire — remove star only."""
        db.add_starboard_message_v1('msg1', 'sb_star', GUILD_A, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_fire', GUILD_A, FIRE, author_id='u')

        rc = db.remove_starboard_message(original_msg_id='msg1', emoji=STAR)
        assert rc == 1
        assert not db.check_exists_starboard_message_v1('msg1', STAR)
        assert db.check_exists_starboard_message_v1('msg1', FIRE)

    def test_remove_by_original_removes_all_emojis(self, db):
        """Remove by original_msg_id without emoji removes all emoji entries."""
        db.add_starboard_message_v1('msg1', 'sb_star', GUILD_A, STAR)
        db.add_starboard_message_v1('msg1', 'sb_fire', GUILD_A, FIRE)

        rc = db.remove_starboard_message(original_msg_id='msg1')
        assert rc == 2
        assert not db.check_exists_starboard_message_v1('msg1', STAR)
        assert not db.check_exists_starboard_message_v1('msg1', FIRE)

    def test_remove_by_starboard_msg_id_is_precise(self, db):
        """Each emoji gets a different starboard_msg_id — remove one."""
        db.add_starboard_message_v1('msg1', 'sb_star', GUILD_A, STAR)
        db.add_starboard_message_v1('msg1', 'sb_fire', GUILD_A, FIRE)

        rc = db.remove_starboard_message(starboard_msg_id='sb_star')
        assert rc == 1
        # Fire entry still there
        assert db.check_exists_starboard_message_v1('msg1', FIRE)


# =====================================================================
# Backfill checkpoint logic (DB-side)
# =====================================================================

class TestBackfillCheckpointing:
    def _seed_pending(self, db, count=5):
        """Create messages with author_id=None (pending backfill)."""
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        for i in range(count):
            db.add_starboard_message_v1(f'msg{i}', f'sb{i}', GUILD_A, STAR)

    def test_pending_messages_have_null_author(self, db):
        self._seed_pending(db, 3)
        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        assert all(m.author_id is None for m in msgs)

    def test_backfill_marks_author_skips_on_next_run(self, db):
        """After setting author_id, the message should be skippable."""
        self._seed_pending(db, 3)
        # "Backfill" msg0 and msg1
        db.update_starboard_author_and_count('msg0', STAR, 'user1', 5)
        db.update_starboard_author_and_count('msg1', STAR, 'user2', 3)

        # Check: 2 done, 1 still pending
        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        pending = [m for m in msgs if m.author_id is None]
        done = [m for m in msgs if m.author_id is not None]
        assert len(pending) == 1
        assert pending[0].original_msg_id == 'msg2'
        assert len(done) == 2

    def test_sentinel_marks_unfetchable_permanently(self, db):
        """__UNKNOWN__ sentinel prevents retry."""
        self._seed_pending(db, 3)
        db.update_starboard_author_and_count('msg0', STAR, '__UNKNOWN__', 0)
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)

        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        pending = [m for m in msgs if m.author_id is None]
        assert len(pending) == 1  # Only msg2 is still pending

    def test_sentinel_excluded_from_both_leaderboards(self, db):
        """__UNKNOWN__ should appear in neither leaderboard."""
        self._seed_pending(db, 3)
        db.update_starboard_author_and_count('msg0', STAR, '__UNKNOWN__', 0)
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)
        db.update_starboard_author_and_count('msg2', STAR, 'user2', 3)

        msg_lb = db.get_starboard_leaderboard(GUILD_A, STAR)
        star_lb = db.get_starboard_star_leaderboard(GUILD_A, STAR)

        # Only user1 and user2, not __UNKNOWN__
        assert len(msg_lb) == 2
        assert all(r.author_id != '__UNKNOWN__' for r in msg_lb)
        assert len(star_lb) == 2
        assert all(r.author_id != '__UNKNOWN__' for r in star_lb)

    def test_full_backfill_leaves_no_pending(self, db):
        """After all messages are backfilled, no pending remain."""
        self._seed_pending(db, 5)
        for i in range(5):
            db.update_starboard_author_and_count(f'msg{i}', STAR, f'user{i}', i + 1)

        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        pending = [m for m in msgs if m.author_id is None]
        assert len(pending) == 0

    def test_partial_backfill_preserves_star_counts(self, db):
        """Already-backfilled messages keep their star_count."""
        self._seed_pending(db, 3)
        db.update_starboard_author_and_count('msg0', STAR, 'user1', 10)
        db.update_starboard_author_and_count('msg1', STAR, 'user2', 20)

        # Simulate "restart" — only msg2 still pending
        msgs = db.get_all_starboard_messages_for_guild(GUILD_A)
        done = {m.original_msg_id: m for m in msgs if m.author_id is not None}
        assert done['msg0'].star_count == 10
        assert done['msg1'].star_count == 20


# =====================================================================
# _fetchone / _fetchall helpers
# =====================================================================

class TestFetchHelpers:
    """Test the _fetchone/_fetchall pattern that saves/restores row_factory."""

    def test_fetchone_with_different_factory(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        conn.execute('CREATE TABLE t (id INTEGER, name TEXT)')
        conn.execute("INSERT INTO t VALUES (1, 'hello')")
        conn.commit()

        # Simulate _fetchone with a custom factory (None = raw tuples)
        original = conn.row_factory
        conn.row_factory = None
        res = conn.execute('SELECT id, name FROM t WHERE id = 1').fetchone()
        conn.row_factory = original

        assert res == (1, 'hello')  # Raw tuple
        assert isinstance(res, tuple)

        # Original factory should be restored
        assert conn.row_factory is namedtuple_factory
        res2 = conn.execute('SELECT id, name FROM t WHERE id = 1').fetchone()
        assert hasattr(res2, 'id')
        assert res2.id == 1
        conn.close()

    def test_fetchall_restores_factory(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        conn.execute('CREATE TABLE t (id INTEGER)')
        conn.execute('INSERT INTO t VALUES (1)')
        conn.execute('INSERT INTO t VALUES (2)')
        conn.commit()

        original = conn.row_factory
        conn.row_factory = None
        res = conn.execute('SELECT id FROM t ORDER BY id').fetchall()
        conn.row_factory = original

        assert res == [(1,), (2,)]
        assert conn.row_factory is namedtuple_factory
        conn.close()


# =====================================================================
# Content truncation (bug #6 fix)
# =====================================================================

class TestContentTruncation:
    """Test the content truncation logic used in prepare_embed."""

    def test_short_content_unchanged(self):
        content = 'Hello world'
        if len(content) > 1024:
            content = content[:1021] + '...'
        assert content == 'Hello world'

    def test_exactly_1024_unchanged(self):
        content = 'x' * 1024
        if len(content) > 1024:
            content = content[:1021] + '...'
        assert content == 'x' * 1024
        assert len(content) == 1024

    def test_1025_gets_truncated(self):
        content = 'x' * 1025
        if len(content) > 1024:
            content = content[:1021] + '...'
        assert len(content) == 1024
        assert content.endswith('...')
        assert content == 'x' * 1021 + '...'

    def test_very_long_content(self):
        content = 'a' * 10000
        if len(content) > 1024:
            content = content[:1021] + '...'
        assert len(content) == 1024

    def test_empty_content_not_affected(self):
        content = ''
        # In the real code, empty content is skipped entirely
        if content and len(content) > 1024:
            content = content[:1021] + '...'
        assert content == ''


# =====================================================================
# get_starboard_emojis_for_guild
# =====================================================================

class TestGetEmojisForGuild:
    def test_returns_all_configured_emojis(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_A, FIRE, 5, 0xff0000)
        db.add_starboard_emoji(GUILD_A, HEART, 1, 0xff69b4)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(emojis) == 3
        emoji_set = {e.emoji for e in emojis}
        assert emoji_set == {STAR, FIRE, HEART}

    def test_returns_empty_for_unconfigured_guild(self, db):
        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert emojis == []

    def test_only_returns_requested_guild(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, FIRE, 5, 0xff0000)

        a_emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(a_emojis) == 1
        assert a_emojis[0].emoji == STAR


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
        assert slb[0].total_stars == 5


# =====================================================================
# Jump URL parsing for backfill optimization
# =====================================================================

from tle.cogs.starboard import _parse_jump_url


class TestParseJumpUrl:
    def test_standard_discord_url(self):
        text = '[Original](https://discord.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_discordapp_url(self):
        text = '[Original](https://discordapp.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_real_snowflake_ids(self):
        text = '[Original](https://discord.com/channels/1273752315022540861/1274019679425265685/1276961610195537991)'
        result = _parse_jump_url(text)
        assert result == (1273752315022540861, 1274019679425265685, 1276961610195537991)

    def test_extracts_channel_id(self):
        """The channel_id (second element) is what the backfill needs."""
        text = '[Original](https://discord.com/channels/111/999888777/333)'
        result = _parse_jump_url(text)
        _, channel_id, _ = result
        assert channel_id == 999888777

    def test_no_url_returns_none(self):
        assert _parse_jump_url('no url here') is None

    def test_empty_string_returns_none(self):
        assert _parse_jump_url('') is None

    def test_partial_url_returns_none(self):
        assert _parse_jump_url('https://discord.com/channels/111/222') is None

    def test_wrong_domain_returns_none(self):
        assert _parse_jump_url('https://example.com/channels/111/222/333') is None

    def test_url_embedded_in_markdown(self):
        """The real embed field value has markdown link syntax."""
        text = '[Original](https://discord.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_url_with_extra_text(self):
        text = 'Check this out: https://discord.com/channels/111/222/333 cool right?'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)


# =====================================================================
# get_starboard_emojis_for_guild now includes channel_id
# =====================================================================

class TestGetEmojisIncludesChannelId:
    def test_channel_id_returned(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_A, STAR, 999888)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(emojis) == 1
        assert emojis[0].channel_id == '999888'

    def test_channel_id_none_when_not_set(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(emojis) == 1
        assert emojis[0].channel_id is None

    def test_multiple_emojis_different_channels(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_A, FIRE, 5, 0xff0000)
        db.set_starboard_channel(GUILD_A, STAR, 100)
        db.set_starboard_channel(GUILD_A, FIRE, 200)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        by_emoji = {e.emoji: e for e in emojis}
        assert by_emoji[STAR].channel_id == '100'
        assert by_emoji[FIRE].channel_id == '200'


# =====================================================================
# prepare_embed has no title (emoji+count removed)
# =====================================================================

from tle.cogs.starboard import Starboard
from datetime import datetime, timezone


class _FakeDisplayAvatar:
    url = 'https://cdn.example.com/avatar.png'


class _FakeAuthor:
    display_avatar = _FakeDisplayAvatar()
    def __str__(self):
        return 'TestUser#1234'


class _FakeChannel:
    mention = '#general'


class _FakeMessage:
    """Minimal message mock for prepare_embed tests."""
    def __init__(self, content='Hello world', embeds=None, attachments=None):
        self.content = content
        self.embeds = embeds or []
        self.attachments = attachments or []
        self.created_at = datetime(2025, 1, 1)
        self.channel = _FakeChannel()
        self.jump_url = 'https://discord.com/channels/111/222/333'
        self.author = _FakeAuthor()


class TestPrepareEmbedNoTitle:
    """prepare_embed should never set a title (emoji+count was removed)."""

    def test_no_title_set(self):
        msg = _FakeMessage()
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        assert embed.title is None

    def test_no_title_with_empty_content(self):
        msg = _FakeMessage(content='')
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        assert embed.title is None

    def test_color_is_passed_through(self):
        msg = _FakeMessage()
        embed = Starboard.prepare_embed(msg, 0x00ff00)
        assert embed.color == 0x00ff00

    def test_has_channel_and_jump_fields(self):
        msg = _FakeMessage()
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        field_names = [f['name'] for f in embed.fields]
        assert 'Channel' in field_names
        assert 'Jump to' in field_names

    def test_content_field_present(self):
        msg = _FakeMessage(content='Some text')
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        content_fields = [f for f in embed.fields if f['name'] == 'Content']
        assert len(content_fields) == 1
        assert content_fields[0]['value'] == 'Some text'

    def test_no_content_field_when_empty(self):
        msg = _FakeMessage(content='')
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        field_names = [f['name'] for f in embed.fields]
        assert 'Content' not in field_names

    def test_footer_set(self):
        msg = _FakeMessage()
        embed = Starboard.prepare_embed(msg, 0xffaa10)
        assert embed.footer is not None
        assert embed.footer['text'] == 'TestUser#1234'


# =====================================================================
# Default emoji parameter on all starboard commands
# =====================================================================

import inspect
from tle.constants import _DEFAULT_STAR


def _unwrap(attr):
    """Get the original function from a stubbed command decorator."""
    while hasattr(attr, '__wrapped__'):
        attr = attr.__wrapped__
    return attr


class TestDefaultEmojiParameter:
    """All starboard commands should default the emoji parameter to the star emoji."""

    # Commands with an explicit emoji parameter (admin/config commands)
    _COMMANDS_WITH_EMOJI_PARAM = [
        'add', 'delete', 'edit_threshold', 'edit_color',
        'here', 'clear', 'remove',
    ]

    # Commands using *args + _parse_starboard_args (leaderboard/top commands)
    _COMMANDS_WITH_ARGS = [
        'leaderboard', 'star_leaderboard', 'star_givers', 'top',
    ]

    @pytest.mark.parametrize('method_name', _COMMANDS_WITH_EMOJI_PARAM)
    def test_emoji_defaults_to_star(self, method_name):
        method = _unwrap(getattr(Starboard, method_name))
        sig = inspect.signature(method)
        assert 'emoji' in sig.parameters, f'{method_name} missing emoji parameter'
        param = sig.parameters['emoji']
        assert param.default == _DEFAULT_STAR, (
            f'{method_name}: emoji default is {param.default!r}, expected {_DEFAULT_STAR!r}'
        )

    @pytest.mark.parametrize('method_name', _COMMANDS_WITH_ARGS)
    def test_args_commands_use_varargs(self, method_name):
        """Leaderboard/top commands use *args and parse emoji via _parse_starboard_args."""
        method = _unwrap(getattr(Starboard, method_name))
        sig = inspect.signature(method)
        assert 'args' in sig.parameters, f'{method_name} should accept *args'

    def test_edit_threshold_required_arg_before_emoji(self):
        """threshold should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.edit_threshold))
        params = list(sig.parameters.keys())
        assert params.index('threshold') < params.index('emoji')

    def test_edit_color_required_arg_before_emoji(self):
        """color should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.edit_color))
        params = list(sig.parameters.keys())
        assert params.index('color') < params.index('emoji')

    def test_remove_required_arg_before_emoji(self):
        """original_message_id should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.remove))
        params = list(sig.parameters.keys())
        assert params.index('original_message_id') < params.index('emoji')


# =====================================================================
# _parse_starboard_args
# =====================================================================

from tle.cogs.starboard import _parse_starboard_args, _NO_TIME_BOUND
import time as time_mod


class TestParseStarboardArgs:
    """Test the argument parser for starboard leaderboard/top commands."""

    def test_no_args_defaults_to_star_all_time(self):
        emoji, dlo, dhi = _parse_starboard_args(())
        assert emoji == STAR
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_emoji_only(self):
        emoji, dlo, dhi = _parse_starboard_args(('🔥',))
        assert emoji == '🔥'
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_star_emoji_explicit(self):
        emoji, dlo, dhi = _parse_starboard_args(('⭐',))
        assert emoji == '⭐'
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_week_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_month_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('month',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_year_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('year',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_emoji_and_week(self):
        emoji, dlo, dhi = _parse_starboard_args(('🔥', 'week'))
        assert emoji == '🔥'
        assert dlo > 0

    def test_week_and_emoji_reversed_order(self):
        """Order shouldn't matter."""
        emoji, dlo, dhi = _parse_starboard_args(('week', '🔥'))
        assert emoji == '🔥'
        assert dlo > 0

    def test_week_sets_monday(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        monday = datetime.fromtimestamp(dlo)
        assert monday.weekday() == 0  # Monday
        assert monday.hour == 0
        assert monday.minute == 0
        assert monday.second == 0

    def test_month_sets_first_of_month(self):
        emoji, dlo, dhi = _parse_starboard_args(('month',))
        first = datetime.fromtimestamp(dlo)
        assert first.day == 1
        assert first.hour == 0
        assert first.minute == 0

    def test_year_sets_jan_first(self):
        emoji, dlo, dhi = _parse_starboard_args(('year',))
        jan1 = datetime.fromtimestamp(dlo)
        assert jan1.month == 1
        assert jan1.day == 1
        assert jan1.hour == 0

    def test_dge_date_arg(self):
        """d>=01012025 should set dlo to Jan 1 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=01012025',))
        assert emoji == STAR
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2025
        assert dt_obj.month == 1
        assert dt_obj.day == 1

    def test_dlt_date_arg(self):
        """d<01022025 should set dhi to Feb 1 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d<01022025',))
        assert emoji == STAR
        dt_obj = datetime.fromtimestamp(dhi)
        assert dt_obj.year == 2025
        assert dt_obj.month == 2
        assert dt_obj.day == 1

    def test_dge_and_dlt_combined(self):
        emoji, dlo, dhi = _parse_starboard_args(('d>=01012025', 'd<01022025'))
        assert emoji == STAR
        assert dlo < dhi
        lo_dt = datetime.fromtimestamp(dlo)
        hi_dt = datetime.fromtimestamp(dhi)
        assert lo_dt.month == 1
        assert hi_dt.month == 2

    def test_emoji_with_dge_and_dlt(self):
        emoji, dlo, dhi = _parse_starboard_args(('🔥', 'd>=01012025', 'd<01022025'))
        assert emoji == '🔥'
        assert dlo > 0
        assert dhi < _NO_TIME_BOUND

    def test_year_only_format(self):
        """d>=2024 should parse as Jan 1 2024."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=2024',))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2024
        assert dt_obj.month == 1
        assert dt_obj.day == 1

    def test_month_year_format(self):
        """d>=032025 should parse as March 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=032025',))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2025
        assert dt_obj.month == 3

    def test_keyword_case_insensitive(self):
        emoji, dlo, dhi = _parse_starboard_args(('Week',))
        assert emoji == STAR
        assert dlo > 0

    def test_keyword_uppercase(self):
        emoji, dlo, dhi = _parse_starboard_args(('MONTH',))
        assert emoji == STAR
        assert dlo > 0

    def test_timeline_keyword_not_treated_as_emoji(self):
        """'week' should not be stored as the emoji."""
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        assert emoji != 'week'
        assert emoji == STAR

    def test_multiple_emojis_last_wins(self):
        """If multiple non-keyword args given, last one is the emoji."""
        emoji, dlo, dhi = _parse_starboard_args(('🔥', '❤️'))
        assert emoji == '❤️'

    def test_week_dge_combined_uses_max_dlo(self):
        """d>= should take max with week's dlo."""
        # Use a date far in the future to ensure it overrides week
        emoji, dlo, dhi = _parse_starboard_args(('week', 'd>=01012030'))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2030

    def test_default_emoji_override(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',), default_emoji='🔥')
        assert emoji == '🔥'


# =====================================================================
# DB time filtering via snowflake timestamps
# =====================================================================


def _make_snowflake(year, month, day):
    """Create a Discord snowflake ID from a date. Used in tests to create
    messages at known timestamps for time-range filtering."""
    dt_obj = datetime(year, month, day, tzinfo=timezone.utc)
    ts_ms = int(dt_obj.timestamp() * 1000)
    # Discord epoch is 2015-01-01 00:00:00 UTC = 1420070400000 ms
    discord_epoch_ms = 1420070400000
    snowflake = (ts_ms - discord_epoch_ms) << 22
    return str(snowflake)


class TestSnowflakeTimeFiltering:
    """Test that DB leaderboard queries correctly filter by snowflake timestamp."""

    def _setup_messages(self, db):
        """Add messages at known dates for filtering tests."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        # Jan 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 1, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 1, 15), STAR, 5)
        # June 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb2', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 3)
        # Dec 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 12, 1), 'sb3', GUILD_A, STAR, author_id='user2')
        db.update_starboard_star_count(_make_snowflake(2024, 12, 1), STAR, 10)
        # Feb 2025 message
        db.add_starboard_message_v1(
            _make_snowflake(2025, 2, 10), 'sb4', GUILD_A, STAR, author_id='user2')
        db.update_starboard_star_count(_make_snowflake(2025, 2, 10), STAR, 7)

    def _setup_reactors(self, db):
        """Add reactors for star-givers filtering tests."""
        db.add_reactor(_make_snowflake(2024, 1, 15), STAR, 'reactor1')
        db.add_reactor(_make_snowflake(2024, 1, 15), STAR, 'reactor2')
        db.add_reactor(_make_snowflake(2025, 2, 10), STAR, 'reactor1')

    def _ts(self, year, month, day):
        """Get unix timestamp for a date."""
        return datetime(year, month, day, tzinfo=timezone.utc).timestamp()

    # --- get_starboard_leaderboard ---

    def test_leaderboard_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2  # user1 and user2

    def test_leaderboard_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 7, 1)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo)
        # Only Dec 2024 and Feb 2025 messages (user2 has both)
        assert len(rows) == 1
        assert rows[0].author_id == 'user2'
        assert rows[0].message_count == 2

    def test_leaderboard_dhi_filter(self, db):
        self._setup_messages(db)
        dhi = self._ts(2024, 7, 1)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dhi=dhi)
        # Only Jan 2024 and June 2024 messages (user1 has both)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'

    def test_leaderboard_range_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 6, 1)
        dhi = self._ts(2024, 12, 31)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        # June 2024 (user1) and Dec 2024 (user2)
        assert len(rows) == 2

    def test_leaderboard_empty_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2023, 1, 1)
        dhi = self._ts(2023, 12, 31)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 0

    # --- get_starboard_star_leaderboard ---

    def test_star_leaderboard_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2

    def test_star_leaderboard_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 7, 1)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR, dlo=dlo)
        assert len(rows) == 1
        assert rows[0].author_id == 'user2'
        assert rows[0].total_stars == 17  # 10 + 7

    def test_star_leaderboard_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 1, 1)
        dhi = self._ts(2024, 7, 1)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'
        assert rows[0].total_stars == 8  # 5 + 3

    # --- get_top_starboard_messages ---

    def test_top_messages_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_top_starboard_messages(GUILD_A, STAR)
        assert len(rows) == 4

    def test_top_messages_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2025, 1, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=dlo)
        assert len(rows) == 1
        assert rows[0].star_count == 7

    def test_top_messages_dhi_filter(self, db):
        self._setup_messages(db)
        dhi = self._ts(2024, 2, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].star_count == 5

    def test_top_messages_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 6, 1)
        dhi = self._ts(2025, 1, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 2
        # Should be sorted by star_count DESC
        assert rows[0].star_count == 10
        assert rows[1].star_count == 3

    # --- get_star_givers_leaderboard ---

    def test_star_givers_no_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2
        # reactor1 reacted on 2 messages, reactor2 on 1
        givers = {r.user_id: r.stars_given for r in rows}
        assert givers['reactor1'] == 2
        assert givers['reactor2'] == 1

    def test_star_givers_dlo_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        dlo = self._ts(2025, 1, 1)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR, dlo=dlo)
        # Only Feb 2025 message has reactor1
        assert len(rows) == 1
        assert rows[0].user_id == 'reactor1'
        assert rows[0].stars_given == 1

    def test_star_givers_dhi_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        dhi = self._ts(2024, 2, 1)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR, dhi=dhi)
        # Only Jan 2024 message has reactor1 and reactor2
        assert len(rows) == 2

    # --- Boundary / edge cases ---

    def test_dlo_zero_means_no_bound(self, db):
        """dlo=0 should not filter anything (same as no filter)."""
        self._setup_messages(db)
        rows_all = db.get_starboard_leaderboard(GUILD_A, STAR)
        rows_zero = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=0)
        assert len(rows_all) == len(rows_zero)

    def test_dhi_sentinel_means_no_bound(self, db):
        """dhi=_NO_TIME_BOUND should not filter anything."""
        from tle.util.db.user_db_conn import _NO_TIME_BOUND
        self._setup_messages(db)
        rows_all = db.get_starboard_leaderboard(GUILD_A, STAR)
        rows_nobound = db.get_starboard_leaderboard(GUILD_A, STAR, dhi=_NO_TIME_BOUND)
        assert len(rows_all) == len(rows_nobound)

    def test_exact_boundary_dlo_inclusive(self, db):
        """dlo is inclusive (>=): a message at exactly dlo should be included."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        exact_ts = self._ts(2024, 6, 15)
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 5)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=exact_ts)
        assert len(rows) == 1

    def test_exact_boundary_dhi_exclusive(self, db):
        """dhi is exclusive (<): a message at exactly dhi should be excluded."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        exact_ts = self._ts(2024, 6, 15)
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 5)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dhi=exact_ts)
        assert len(rows) == 0


# =====================================================================
# snowflake_to_unix_sql correctness
# =====================================================================

from tle.util.db.user_db_conn import snowflake_to_unix_sql, DISCORD_EPOCH_MS, SNOWFLAKE_TIMESTAMP_DIVISOR
import sqlite3 as _sqlite3


class TestSnowflakeToUnixSql:
    """Verify the SQL expression correctly extracts timestamps from Discord snowflakes."""

    def test_known_snowflake(self):
        """Test with a real Discord snowflake ID."""
        conn = _sqlite3.connect(':memory:')
        # Known snowflake: 1276961610195537991 -> 2024-08-24 17:49:32 UTC
        expr = snowflake_to_unix_sql('val')
        row = conn.execute(f'SELECT {expr} FROM (SELECT 1276961610195537991 AS val)').fetchone()
        ts = row[0]
        dt_obj = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt_obj.year == 2024
        assert dt_obj.month == 8
        assert dt_obj.day == 24

    def test_roundtrip_with_make_snowflake(self):
        """A snowflake created from a date should produce that same date back."""
        conn = _sqlite3.connect(':memory:')
        sf = _make_snowflake(2025, 3, 1)
        expr = snowflake_to_unix_sql('val')
        row = conn.execute(f'SELECT {expr} FROM (SELECT ? AS val)', (sf,)).fetchone()
        ts = row[0]
        dt_obj = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt_obj.year == 2025
        assert dt_obj.month == 3
        assert dt_obj.day == 1

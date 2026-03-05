"""Tests for starboard DB methods in UserDbConn.

We can't easily instantiate UserDbConn (it imports the whole bot), so we
test the DB methods by building the schema directly and calling methods
on a lightweight wrapper.
"""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin


class FakeUserDb(StarboardDbMixin):
    """Test double for starboard DB methods. Inherits real SQL from StarboardDbMixin,
    only needs to set up the schema and provide self.conn."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        """Create the starboard tables (matches create_tables in UserDbConn)."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id    TEXT,
                emoji       TEXT,
                threshold   INTEGER NOT NULL DEFAULT 3,
                color       INTEGER NOT NULL DEFAULT 16755216,
                channel_id  TEXT,
                PRIMARY KEY (guild_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id     TEXT,
                starboard_msg_id    TEXT,
                guild_id            TEXT,
                emoji               TEXT,
                author_id           TEXT,
                star_count          INTEGER DEFAULT 0,
                channel_id          TEXT,
                PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id    TEXT,
                key         TEXT,
                value       TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.commit()

    def close(self):
        self.conn.close()


GUILD = 111111111111111111
STAR = '⭐'
FIRE = '🔥'


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Emoji config CRUD
# =====================================================================

class TestAddStarboardEmoji:
    def test_add_new(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry is not None
        assert entry.threshold == 3
        assert entry.color == 0xffaa10
        assert entry.channel_id is None  # Not set yet

    def test_add_with_int_guild_id(self, db):
        """guild_id should be cast to str internally."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry is not None

    def test_upsert_preserves_channel_id(self, db):
        """Bug #2 fix: ON CONFLICT upsert should preserve channel_id."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD, STAR, 999888777)
        # Verify channel is set
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry.channel_id == '999888777'

        # Now upsert with new threshold — channel_id must survive
        db.add_starboard_emoji(GUILD, STAR, 5, 0xff0000)
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry.threshold == 5
        assert entry.color == 0xff0000
        assert entry.channel_id == '999888777'  # Preserved!

    def test_get_nonexistent(self, db):
        assert db.get_starboard_entry(GUILD, STAR) is None


class TestPerEmojiChannels:
    def test_different_channels_for_different_emojis(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 5, 0xff0000)
        db.set_starboard_channel(GUILD, STAR, 100)
        db.set_starboard_channel(GUILD, FIRE, 200)

        star_entry = db.get_starboard_entry(GUILD, STAR)
        fire_entry = db.get_starboard_entry(GUILD, FIRE)
        assert star_entry.channel_id == '100'
        assert fire_entry.channel_id == '200'

    def test_clear_one_emoji_channel(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 5, 0xff0000)
        db.set_starboard_channel(GUILD, STAR, 100)
        db.set_starboard_channel(GUILD, FIRE, 200)

        db.clear_starboard_channel(GUILD, STAR)
        assert db.get_starboard_entry(GUILD, STAR).channel_id is None
        assert db.get_starboard_entry(GUILD, FIRE).channel_id == '200'

    def test_set_channel_returns_rowcount(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        rc = db.set_starboard_channel(GUILD, STAR, 100)
        assert rc == 1

    def test_set_channel_nonexistent_emoji_returns_zero(self, db):
        rc = db.set_starboard_channel(GUILD, STAR, 100)
        assert rc == 0


class TestUpdateThresholdColor:
    def test_update_threshold(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        rc = db.update_starboard_threshold(GUILD, STAR, 5)
        assert rc == 1
        assert db.get_starboard_entry(GUILD, STAR).threshold == 5

    def test_update_threshold_nonexistent(self, db):
        rc = db.update_starboard_threshold(GUILD, STAR, 5)
        assert rc == 0

    def test_update_color(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        rc = db.update_starboard_color(GUILD, STAR, 0x00ff00)
        assert rc == 1
        assert db.get_starboard_entry(GUILD, STAR).color == 0x00ff00


class TestRemoveStarboardEmoji:
    def test_remove_deletes_emoji_and_messages(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR, author_id='user2')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_starboard_entry(GUILD, STAR) is None
        assert not db.check_exists_starboard_message_v1('msg1', STAR)
        assert not db.check_exists_starboard_message_v1('msg2', STAR)

    def test_remove_doesnt_affect_other_emoji(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 5, 0xff0000)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)
        db.add_starboard_message_v1('msg1', 'sb2', GUILD, FIRE)

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_starboard_entry(GUILD, FIRE) is not None
        assert db.check_exists_starboard_message_v1('msg1', FIRE)


# =====================================================================
# Starboard messages
# =====================================================================

class TestStarboardMessages:
    def test_add_and_check_exists(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        assert db.check_exists_starboard_message_v1('msg1', STAR)
        assert not db.check_exists_starboard_message_v1('msg1', FIRE)
        assert not db.check_exists_starboard_message_v1('msg2', STAR)

    def test_add_with_int_ids(self, db):
        """IDs come as ints from Discord, should be cast to str."""
        db.add_starboard_message_v1(123, 456, GUILD, STAR, author_id=789)
        assert db.check_exists_starboard_message_v1(123, STAR)

    def test_add_duplicate_ignored(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        # Second insert with different starboard_msg_id should be ignored
        db.add_starboard_message_v1('msg1', 'sb2', GUILD, STAR, author_id='user2')
        # Original data preserved
        msgs = db.get_all_starboard_messages_for_guild(GUILD)
        star_msgs = [m for m in msgs if m.emoji == STAR and m.original_msg_id == 'msg1']
        assert len(star_msgs) == 1
        assert star_msgs[0].starboard_msg_id == 'sb1'
        assert star_msgs[0].author_id == 'user1'

    def test_remove_by_starboard_msg_id(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)
        rc = db.remove_starboard_message(starboard_msg_id='sb1')
        assert rc == 1
        assert not db.check_exists_starboard_message_v1('msg1', STAR)

    def test_remove_by_original_and_emoji(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)
        rc = db.remove_starboard_message(original_msg_id='msg1', emoji=STAR)
        assert rc == 1

    def test_remove_by_original_all_emojis(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)
        db.add_starboard_message_v1('msg1', 'sb2', GUILD, FIRE)
        rc = db.remove_starboard_message(original_msg_id='msg1')
        assert rc == 2

    def test_remove_nonexistent(self, db):
        rc = db.remove_starboard_message(starboard_msg_id='nope')
        assert rc == 0

    def test_remove_no_args(self, db):
        rc = db.remove_starboard_message()
        assert rc == 0


# =====================================================================
# Star count tracking
# =====================================================================

class TestStarCount:
    def test_update_star_count(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 7)
        msg = db.get_all_starboard_messages_for_guild(GUILD)[0]
        assert msg.star_count == 7

    def test_update_author_and_count(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)
        db.update_starboard_author_and_count('msg1', STAR, 'user1', 5)
        msg = db.get_all_starboard_messages_for_guild(GUILD)[0]
        assert msg.author_id == 'user1'
        assert msg.star_count == 5


# =====================================================================
# Leaderboards
# =====================================================================

class TestLeaderboards:
    def _seed_messages(self, db):
        """Create test data: user1 has 3 messages, user2 has 1."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        for i in range(3):
            db.add_starboard_message_v1(f'msg{i}', f'sb{i}', GUILD, STAR, author_id='user1')
            db.update_starboard_star_count(f'msg{i}', STAR, 5)
        db.add_starboard_message_v1('msg10', 'sb10', GUILD, STAR, author_id='user2')
        db.update_starboard_star_count('msg10', STAR, 10)

    def test_message_leaderboard(self, db):
        self._seed_messages(db)
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 2
        assert rows[0].author_id == 'user1'
        assert rows[0].message_count == 3
        assert rows[1].author_id == 'user2'
        assert rows[1].message_count == 1

    def test_star_leaderboard(self, db):
        self._seed_messages(db)
        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 2
        # user1: 3 messages * 5 stars = 15 total
        # user2: 1 message * 10 stars = 10 total
        assert rows[0].author_id == 'user1'
        assert rows[0].total_stars == 15
        assert rows[1].author_id == 'user2'
        assert rows[1].total_stars == 10

    def test_leaderboard_excludes_null_author(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR)  # No author_id
        db.update_starboard_star_count('msg1', STAR, 5)
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 0
        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 0

    def test_leaderboard_excludes_unknown_sentinel(self, db):
        """Bug #10 fix: __UNKNOWN__ sentinel should be excluded from leaderboards."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 5)
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR)
        db.update_starboard_author_and_count('msg2', STAR, '__UNKNOWN__', 0)

        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'

        rows = db.get_starboard_star_leaderboard(GUILD, STAR)
        assert len(rows) == 1

    def test_leaderboard_empty(self, db):
        rows = db.get_starboard_leaderboard(GUILD, STAR)
        assert rows == []

    def test_leaderboard_per_emoji(self, db):
        """Leaderboard should only include messages for the queried emoji."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 3, 0xff0000)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, FIRE, author_id='user1')
        db.update_starboard_star_count('msg1', STAR, 5)
        db.update_starboard_star_count('msg2', FIRE, 10)

        star_lb = db.get_starboard_leaderboard(GUILD, STAR)
        assert len(star_lb) == 1
        assert star_lb[0].message_count == 1

        fire_lb = db.get_starboard_star_leaderboard(GUILD, FIRE)
        assert len(fire_lb) == 1
        assert fire_lb[0].total_stars == 10


# =====================================================================
# Guild config
# =====================================================================

class TestGuildConfig:
    def test_get_nonexistent(self, db):
        assert db.get_guild_config(GUILD, 'foo') is None

    def test_set_and_get(self, db):
        db.set_guild_config(GUILD, 'starboard_leaderboard', '1')
        assert db.get_guild_config(GUILD, 'starboard_leaderboard') == '1'

    def test_set_overwrites(self, db):
        db.set_guild_config(GUILD, 'key', 'val1')
        db.set_guild_config(GUILD, 'key', 'val2')
        assert db.get_guild_config(GUILD, 'key') == 'val2'

    def test_delete(self, db):
        db.set_guild_config(GUILD, 'key', 'val')
        db.delete_guild_config(GUILD, 'key')
        assert db.get_guild_config(GUILD, 'key') is None

    def test_per_guild_isolation(self, db):
        db.set_guild_config(GUILD, 'key', 'val1')
        db.set_guild_config(222222, 'key', 'val2')
        assert db.get_guild_config(GUILD, 'key') == 'val1'
        assert db.get_guild_config(222222, 'key') == 'val2'


# =====================================================================
# Int vs str type handling
# =====================================================================

class TestIntStrCasting:
    """Verify that int IDs from Discord work correctly with TEXT columns."""

    def test_guild_id_as_int(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        # Query with int
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry is not None

    def test_message_ids_as_int(self, db):
        db.add_starboard_message_v1(12345, 67890, GUILD, STAR, author_id=11111)
        assert db.check_exists_starboard_message_v1(12345, STAR)
        rc = db.remove_starboard_message(starboard_msg_id=67890)
        assert rc == 1

    def test_channel_id_stored_as_str(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD, STAR, 999888777666)
        entry = db.get_starboard_entry(GUILD, STAR)
        assert entry.channel_id == '999888777666'
        assert isinstance(entry.channel_id, str)


# =====================================================================
# Reactor tracking
# =====================================================================

class TestReactorCrud:
    def test_add_and_get_reactors(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        reactors = db.get_reactors('msg1', STAR)
        assert set(reactors) == {'user1', 'user2'}

    def test_add_reactor_idempotent(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user1')  # Duplicate
        assert db.get_reactor_count('msg1', STAR) == 1

    def test_remove_reactor(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        rc = db.remove_reactor('msg1', STAR, 'user1')
        assert rc == 1
        assert db.get_reactors('msg1', STAR) == ['user2']

    def test_remove_nonexistent_reactor(self, db):
        rc = db.remove_reactor('msg1', STAR, 'user1')
        assert rc == 0

    def test_get_reactor_count(self, db):
        assert db.get_reactor_count('msg1', STAR) == 0
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', STAR, 'user3')
        assert db.get_reactor_count('msg1', STAR) == 3

    def test_reactors_per_emoji_independent(self, db):
        """Same message, different emojis — reactors are independent."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', FIRE, 'user1')
        assert db.get_reactor_count('msg1', STAR) == 2
        assert db.get_reactor_count('msg1', FIRE) == 1

    def test_bulk_add_reactors(self, db):
        db.bulk_add_reactors('msg1', STAR, ['user1', 'user2', 'user3'])
        assert db.get_reactor_count('msg1', STAR) == 3
        assert set(db.get_reactors('msg1', STAR)) == {'user1', 'user2', 'user3'}

    def test_bulk_add_idempotent(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.bulk_add_reactors('msg1', STAR, ['user1', 'user2', 'user3'])
        assert db.get_reactor_count('msg1', STAR) == 3

    def test_bulk_add_empty_list(self, db):
        db.bulk_add_reactors('msg1', STAR, [])
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_int_user_ids(self, db):
        """Discord user IDs are ints — should be cast to str."""
        db.add_reactor('msg1', STAR, 123456789)
        assert db.get_reactors('msg1', STAR) == ['123456789']
        rc = db.remove_reactor('msg1', STAR, 123456789)
        assert rc == 1


class TestMergedReactorCount:
    def test_same_user_two_emojis_counts_once(self, db):
        """User reacted with both star and fire — merged count = 1."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 1

    def test_different_users_two_emojis(self, db):
        """user1 starred, user2 fired — merged count = 2."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user2')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 2

    def test_overlapping_users(self, db):
        """user1 did both, user2 only star, user3 only fire — merged = 3."""
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        db.add_reactor('msg1', FIRE, 'user3')
        assert db.get_merged_reactor_count('msg1', [STAR, FIRE]) == 3

    def test_single_emoji_matches_get_reactor_count(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')
        assert db.get_merged_reactor_count('msg1', [STAR]) == 2
        assert db.get_reactor_count('msg1', STAR) == 2

    def test_empty_emojis_returns_zero(self, db):
        db.add_reactor('msg1', STAR, 'user1')
        assert db.get_merged_reactor_count('msg1', []) == 0


class TestReactorCascadeDelete:
    def test_remove_message_by_emoji_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', STAR, 'user2')

        db.remove_starboard_message(original_msg_id='msg1', emoji=STAR)
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_remove_message_by_starboard_msg_id_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')

        db.remove_starboard_message(starboard_msg_id='sb1')
        assert db.get_reactor_count('msg1', STAR) == 0

    def test_remove_message_all_emojis_cleans_reactors(self, db):
        db.add_starboard_message_v1('msg1', 'sb_s', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_f', GUILD, FIRE, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user2')

        db.remove_starboard_message(original_msg_id='msg1')
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg1', FIRE) == 0

    def test_remove_emoji_cleans_reactors(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg2', STAR, 'user2')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg2', STAR) == 0

    def test_remove_emoji_preserves_other_emoji_reactors(self, db):
        """Removing star shouldn't touch fire reactors."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 3, 0xff0000)
        db.add_starboard_message_v1('msg1', 'sb_s', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_f', GUILD, FIRE, author_id='u')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', FIRE, 'user1')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg1', FIRE) == 1  # Preserved

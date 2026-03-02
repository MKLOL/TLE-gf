"""Tests for starboard DB methods in UserDbConn.

We can't easily instantiate UserDbConn (it imports the whole bot), so we
test the DB methods by building the schema directly and calling methods
on a lightweight wrapper.
"""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.user_db_conn import namedtuple_factory


class FakeUserDb:
    """Mimics the starboard-related parts of UserDbConn using an in-memory DB."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        """Create the v1 starboard tables (matches create_tables in UserDbConn)."""
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

    # --- Copy the actual methods from UserDbConn ---
    # We import them "by hand" to test the real SQL without the full class.

    def get_starboard_entry(self, guild_id, emoji):
        guild_id = str(guild_id)
        query = '''
            SELECT channel_id, threshold, color
            FROM starboard_emoji_v1
            WHERE guild_id = ? AND emoji = ?
        '''
        return self.conn.execute(query, (guild_id, emoji)).fetchone()

    def set_starboard_channel(self, guild_id, emoji, channel_id):
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND emoji = ?',
            (str(channel_id), guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def clear_starboard_channel(self, guild_id, emoji):
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = NULL WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def add_starboard_emoji(self, guild_id, emoji, threshold, color):
        guild_id = str(guild_id)
        self.conn.execute('''
            INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, emoji) DO UPDATE SET
                threshold = excluded.threshold,
                color = excluded.color
        ''', (guild_id, emoji, threshold, color))
        self.conn.commit()

    def remove_starboard_emoji(self, guild_id, emoji):
        guild_id = str(guild_id)
        self.conn.execute(
            'DELETE FROM starboard_emoji_v1 WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        )
        self.conn.execute(
            'DELETE FROM starboard_message_v1 WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        )
        self.conn.commit()

    def update_starboard_threshold(self, guild_id, emoji, threshold):
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET threshold = ? WHERE guild_id = ? AND emoji = ?',
            (threshold, guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def update_starboard_color(self, guild_id, emoji, color):
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET color = ? WHERE guild_id = ? AND emoji = ?',
            (color, guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def add_starboard_message_v1(self, original_msg_id, starboard_msg_id, guild_id, emoji,
                                 author_id=None, channel_id=None):
        self.conn.execute(
            'INSERT OR IGNORE INTO starboard_message_v1 '
            '(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, channel_id) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (str(original_msg_id), str(starboard_msg_id), str(guild_id), emoji,
             str(author_id) if author_id else None,
             str(channel_id) if channel_id else None)
        )
        self.conn.commit()

    def check_exists_starboard_message_v1(self, original_msg_id, emoji):
        query = 'SELECT 1 FROM starboard_message_v1 WHERE original_msg_id = ? AND emoji = ?'
        res = self.conn.execute(query, (str(original_msg_id), emoji)).fetchone()
        return res is not None

    def remove_starboard_message(self, *, original_msg_id=None, emoji=None, starboard_msg_id=None):
        if starboard_msg_id is not None:
            query = 'DELETE FROM starboard_message_v1 WHERE starboard_msg_id = ?'
            rc = self.conn.execute(query, (str(starboard_msg_id),)).rowcount
        elif original_msg_id is not None and emoji is not None:
            query = 'DELETE FROM starboard_message_v1 WHERE original_msg_id = ? AND emoji = ?'
            rc = self.conn.execute(query, (str(original_msg_id), emoji)).rowcount
        elif original_msg_id is not None:
            query = 'DELETE FROM starboard_message_v1 WHERE original_msg_id = ?'
            rc = self.conn.execute(query, (str(original_msg_id),)).rowcount
        else:
            return 0
        self.conn.commit()
        return rc

    def update_starboard_star_count(self, original_msg_id, emoji, count):
        self.conn.execute(
            'UPDATE starboard_message_v1 SET star_count = ? WHERE original_msg_id = ? AND emoji = ?',
            (count, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def update_starboard_author_and_count(self, original_msg_id, emoji, author_id, count):
        self.conn.execute(
            'UPDATE starboard_message_v1 SET author_id = ?, star_count = ? WHERE original_msg_id = ? AND emoji = ?',
            (str(author_id), count, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def get_starboard_leaderboard(self, guild_id, emoji):
        guild_id = str(guild_id)
        query = '''
            SELECT author_id, COUNT(*) as message_count
            FROM starboard_message_v1
            WHERE guild_id = ? AND emoji = ?
                AND author_id IS NOT NULL AND author_id != '__UNKNOWN__'
            GROUP BY author_id
            ORDER BY message_count DESC
        '''
        return self.conn.execute(query, (guild_id, emoji)).fetchall()

    def get_starboard_star_leaderboard(self, guild_id, emoji):
        guild_id = str(guild_id)
        query = '''
            SELECT author_id, SUM(star_count) as total_stars
            FROM starboard_message_v1
            WHERE guild_id = ? AND emoji = ?
                AND author_id IS NOT NULL AND author_id != '__UNKNOWN__'
                AND star_count > 0
            GROUP BY author_id
            ORDER BY total_stars DESC
        '''
        return self.conn.execute(query, (guild_id, emoji)).fetchall()

    def get_all_starboard_messages_for_guild(self, guild_id):
        guild_id = str(guild_id)
        query = 'SELECT * FROM starboard_message_v1 WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_starboard_emojis_for_guild(self, guild_id):
        guild_id = str(guild_id)
        query = 'SELECT emoji, threshold, color FROM starboard_emoji_v1 WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_guild_config(self, guild_id, key):
        guild_id = str(guild_id)
        query = 'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?'
        res = self.conn.execute(query, (guild_id, key)).fetchone()
        return res.value if res else None

    def set_guild_config(self, guild_id, key, value):
        guild_id = str(guild_id)
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (guild_id, key, value)
        )
        self.conn.commit()

    def delete_guild_config(self, guild_id, key):
        guild_id = str(guild_id)
        self.conn.execute(
            'DELETE FROM guild_config WHERE guild_id = ? AND key = ?',
            (guild_id, key)
        )
        self.conn.commit()


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

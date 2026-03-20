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
            CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id    TEXT,
                alias_emoji TEXT,
                main_emoji  TEXT,
                PRIMARY KEY (guild_id, alias_emoji)
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
THUMBS_UP = '\N{THUMBS UP SIGN}'


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

    def test_remove_cleans_up_aliases(self, db):
        """Removing a main emoji should delete its aliases."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, FIRE, STAR)

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_aliases_for_emoji(GUILD, STAR) == []
        assert db.resolve_alias(GUILD, THUMBS_UP) is None
        assert db.resolve_alias(GUILD, FIRE) is None

    def test_remove_cleans_up_alias_reactors(self, db):
        """Removing a main emoji should also delete reactors stored under alias emojis."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='user1')
        # Reactors stored under both main and alias emojis
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user2')

        db.remove_starboard_emoji(GUILD, STAR)
        assert db.get_reactor_count('msg1', STAR) == 0
        assert db.get_reactor_count('msg1', THUMBS_UP) == 0


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
# Emoji alias CRUD
# =====================================================================

class TestAliasAdd:
    def test_add_alias(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        aliases = db.get_aliases_for_emoji(GUILD, STAR)
        assert aliases == [THUMBS_UP]

    def test_add_multiple_aliases(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, FIRE, STAR)
        aliases = db.get_aliases_for_emoji(GUILD, STAR)
        assert set(aliases) == {THUMBS_UP, FIRE}

    def test_replace_alias(self, db):
        """Adding same alias again should update the main emoji."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 3, 0xff0000)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, THUMBS_UP, FIRE)
        assert db.resolve_alias(GUILD, THUMBS_UP) == FIRE

    def test_int_guild_id(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        assert db.resolve_alias(GUILD, THUMBS_UP) == STAR


class TestAliasRemove:
    def test_remove_alias(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        rc = db.remove_starboard_alias(GUILD, THUMBS_UP)
        assert rc == 1
        assert db.resolve_alias(GUILD, THUMBS_UP) is None

    def test_remove_nonexistent(self, db):
        rc = db.remove_starboard_alias(GUILD, THUMBS_UP)
        assert rc == 0

    def test_remove_migrates_alias_reactors_to_main(self, db):
        """Removing an alias should migrate its reactor rows to the main emoji."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='a1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user2')

        db.remove_starboard_alias(GUILD, THUMBS_UP)
        # Alias reactors should now be under the main emoji
        assert db.get_reactor_count('msg1', STAR) == 2
        assert db.get_reactor_count('msg1', THUMBS_UP) == 0

    def test_remove_migrates_without_duplicating(self, db):
        """If a user reacted with both main and alias, migration should not duplicate."""
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='a1')
        db.add_reactor('msg1', STAR, 'user1')
        db.add_reactor('msg1', THUMBS_UP, 'user1')  # Same user, both emojis

        db.remove_starboard_alias(GUILD, THUMBS_UP)
        # user1 should still be counted once under main emoji
        assert db.get_reactor_count('msg1', STAR) == 1
        assert db.get_reactor_count('msg1', THUMBS_UP) == 0

    def test_remove_migrates_across_multiple_messages(self, db):
        """Migration should work for alias reactors spread across multiple messages."""
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_message_v1('msg1', 'sb1', GUILD, STAR, author_id='a1')
        db.add_starboard_message_v1('msg2', 'sb2', GUILD, STAR, author_id='a2')
        db.add_reactor('msg1', THUMBS_UP, 'user1')
        db.add_reactor('msg2', THUMBS_UP, 'user2')

        db.remove_starboard_alias(GUILD, THUMBS_UP)
        assert db.get_reactor_count('msg1', STAR) == 1
        assert db.get_reactor_count('msg2', STAR) == 1
        assert db.get_reactor_count('msg1', THUMBS_UP) == 0
        assert db.get_reactor_count('msg2', THUMBS_UP) == 0

    def test_remove_alias_scoped_to_guild(self, db):
        """Removing an alias in one guild must not affect another guild's reactors."""
        GUILD_B = 222222222222222222
        db.add_starboard_emoji(GUILD, STAR, 1, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, FIRE, 1, 0xff0000)
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD_B, THUMBS_UP, FIRE)
        # Guild A message + reactor
        db.add_starboard_message_v1('msg_a', 'sb_a', GUILD, STAR, author_id='u1')
        db.add_reactor('msg_a', THUMBS_UP, 'user1')
        # Guild B message + reactor
        db.add_starboard_message_v1('msg_b', 'sb_b', GUILD_B, FIRE, author_id='u2')
        db.add_reactor('msg_b', THUMBS_UP, 'user2')

        # Remove alias only in Guild A
        db.remove_starboard_alias(GUILD, THUMBS_UP)

        # Guild A: migrated to STAR
        assert db.get_reactor_count('msg_a', STAR) == 1
        assert db.get_reactor_count('msg_a', THUMBS_UP) == 0
        # Guild B: untouched — still under THUMBS_UP
        assert db.get_reactor_count('msg_b', THUMBS_UP) == 1
        assert db.get_reactor_count('msg_b', FIRE) == 0  # NOT migrated


class TestAliasResolve:
    def test_resolve_existing_alias(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        assert db.resolve_alias(GUILD, THUMBS_UP) == STAR

    def test_resolve_non_alias(self, db):
        assert db.resolve_alias(GUILD, STAR) is None

    def test_per_guild_isolation(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        assert db.resolve_alias(222222, THUMBS_UP) is None


class TestGetAllAliases:
    def test_empty(self, db):
        assert db.get_all_aliases_for_guild(GUILD) == []

    def test_multiple_aliases(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, FIRE, STAR)
        rows = db.get_all_aliases_for_guild(GUILD)
        aliases = {r.alias_emoji: r.main_emoji for r in rows}
        assert aliases == {THUMBS_UP: STAR, FIRE: STAR}


class TestEmojiFamily:
    def test_no_aliases(self, db):
        assert db.get_emoji_family(GUILD, STAR) == [STAR]

    def test_with_aliases(self, db):
        db.add_starboard_alias(GUILD, THUMBS_UP, STAR)
        db.add_starboard_alias(GUILD, FIRE, STAR)
        family = db.get_emoji_family(GUILD, STAR)
        assert family[0] == STAR
        assert set(family) == {STAR, THUMBS_UP, FIRE}


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
# Starboard entries for message
# =====================================================================

class TestGetStarboardEntriesForMessage:
    def test_returns_all_emoji_entries(self, db):
        db.add_starboard_message_v1('msg1', 'sb_s', GUILD, STAR, author_id='u')
        db.add_starboard_message_v1('msg1', 'sb_f', GUILD, FIRE, author_id='u')

        entries = db.get_starboard_entries_for_message('msg1')
        emojis = {e.emoji for e in entries}
        assert emojis == {STAR, FIRE}

    def test_returns_empty_for_unknown(self, db):
        assert db.get_starboard_entries_for_message('unknown') == []


# =====================================================================
# Get all emoji configs for a guild (used by ;starboard show)
# =====================================================================

class TestGetStarboardEmojisForGuild:
    def test_empty(self, db):
        assert db.get_starboard_emojis_for_guild(GUILD) == []

    def test_returns_all_emojis(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD, FIRE, 5, 0xff0000)
        entries = db.get_starboard_emojis_for_guild(GUILD)
        emojis = {e.emoji for e in entries}
        assert emojis == {STAR, FIRE}

    def test_includes_threshold_and_color(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        entries = db.get_starboard_emojis_for_guild(GUILD)
        assert entries[0].threshold == 3
        assert entries[0].color == 0xffaa10

    def test_includes_channel_id(self, db):
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD, STAR, '123456')
        entries = db.get_starboard_emojis_for_guild(GUILD)
        assert entries[0].channel_id == '123456'

    def test_per_guild_isolation(self, db):
        GUILD_B = 222222222222222222
        db.add_starboard_emoji(GUILD, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_B, FIRE, 5, 0xff0000)
        entries = db.get_starboard_emojis_for_guild(GUILD)
        assert len(entries) == 1
        assert entries[0].emoji == STAR

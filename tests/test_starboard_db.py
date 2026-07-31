"""Tests for starboard DB methods in UserDbConn (emoji config, messages,
star counts, guild config).

We can't easily instantiate UserDbConn (it imports the whole bot), so we
test the DB methods by building the schema directly and calling methods
on a lightweight wrapper.  The ``FakeUserDb`` double and shared constants
live in ``tests/starboard_test_utils.py``; alias / isolation / per-user
default tests live in ``test_starboard_db_aliases.py``.
"""
import pytest

# Re-export FakeUserDb here for backwards-compatible imports
# (``from tests.test_starboard_db import FakeUserDb``).
from tests.starboard_test_utils import FakeUserDb, GUILD, STAR, FIRE, THUMBS_UP


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

    def test_delete_by_prefix_is_guild_scoped(self, db):
        db.set_guild_config(GUILD, 'llm_channel:1', '1')
        db.set_guild_config(GUILD, 'llm_channel:2', '0')
        db.set_guild_config(GUILD, 'unrelated', '1')
        db.set_guild_config(222222, 'llm_channel:1', '1')

        assert db.delete_guild_configs_by_prefix(GUILD, 'llm_channel:') == 2
        assert db.get_guild_config(GUILD, 'llm_channel:1') is None
        assert db.get_guild_config(GUILD, 'llm_channel:2') is None
        assert db.get_guild_config(GUILD, 'unrelated') == '1'
        assert db.get_guild_config(222222, 'llm_channel:1') == '1'

    def test_delete_by_prefix_rejects_empty_prefix(self, db):
        with pytest.raises(ValueError, match='must not be empty'):
            db.delete_guild_configs_by_prefix(GUILD, '')

    def test_per_guild_isolation(self, db):
        db.set_guild_config(GUILD, 'key', 'val1')
        db.set_guild_config(222222, 'key', 'val2')
        assert db.get_guild_config(GUILD, 'key') == 'val1'
        assert db.get_guild_config(222222, 'key') == 'val2'

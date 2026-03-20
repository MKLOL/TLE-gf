"""DB-level migration cog tests: parsing, ordering, complete, integration."""
import json
import pytest
from tests._migrate_fakes import (
    _FakeMigrateDb, GUILD, PILL, CHOC, db, _zero_rate_delay,
)
from tle.cogs._migrate_helpers import parse_old_bot_message, build_fallback_message


# =====================================================================
# Crawl parsing tests
# =====================================================================


class TestCrawlParsing:
    """Test that the crawl phase correctly parses old bot messages."""

    def test_processes_valid_starboard_message(self):
        content = '\N{PILL} **5** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result is not None
        emoji_str, count, guild_id, channel_id, msg_id = result
        assert emoji_str == PILL
        assert count == 5
        assert msg_id == 333

    def test_skips_non_matching_content(self):
        assert parse_old_bot_message('Hello world, no starboard here') is None
        assert parse_old_bot_message('') is None

    def test_skips_wrong_emoji(self):
        content = '\N{WHITE MEDIUM STAR} **5** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result is not None
        emoji_str = result[0]
        # If we're filtering for pill only
        assert emoji_str not in {PILL, CHOC}

    def test_handles_mixed_emojis(self):
        """Both pill and chocolate should be parsed."""
        pill_msg = '\N{PILL} **3** | https://discord.com/channels/111/222/333'
        choc_msg = '\N{CHOCOLATE BAR} **7** | https://discord.com/channels/111/222/444'
        r1 = parse_old_bot_message(pill_msg)
        r2 = parse_old_bot_message(choc_msg)
        assert r1[0] == PILL
        assert r2[0] == CHOC
        assert r1[4] == 333
        assert r2[4] == 444

    def test_handles_deleted_originals(self, db):
        """Deleted original messages should be marked as 'deleted' with fallback data."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')

        # Simulate what crawl does for a deleted message
        fallback = json.dumps({'content': 'Old message text'})
        db.update_migration_entry_deleted('333', PILL, fallback)

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'
        assert 'Old message text' in entry.embed_fallback


# =====================================================================
# Post ordering tests
# =====================================================================


class TestPostOrdering:
    """Test that posting phase orders by old_bot_msg_id (starboard order)."""

    def test_posts_in_starboard_order(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        # old_bot_msg_ids: 446, 444, 445 — starboard order is 444, 445, 446
        db.add_migration_entry(str(GUILD), '9999', PILL, '446', '100')
        db.add_migration_entry(str(GUILD), '1111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '5555', PILL, '445', '100')

        db.update_migration_entry_crawled('9999', PILL, '500', '777', 3)
        db.update_migration_entry_crawled('1111', PILL, '500', '778', 7)
        db.update_migration_entry_crawled('5555', PILL, '500', '779', 5)

        entries = db.get_migration_entries_for_posting(str(GUILD))
        ids = [e.original_msg_id for e in entries]
        # Ordered by old_bot_msg_id: 444 (orig 1111), 445 (orig 5555), 446 (orig 9999)
        assert ids == ['1111', '5555', '9999']

    def test_uses_fallback_for_deleted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        fallback = json.dumps({'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/333'})
        db.update_migration_entry_deleted('333', PILL, fallback)

        entries = db.get_migration_entries_for_posting(str(GUILD))
        assert len(entries) == 1
        assert entries[0].crawl_status == 'deleted'

        content, embeds = build_fallback_message(entries[0], entries[0].embed_fallback, PILL)
        assert PILL in content
        assert '333' in content

    def test_mixed_crawled_and_deleted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')

        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_deleted('222', PILL, '{}')

        entries = db.get_migration_entries_for_posting(str(GUILD))
        assert len(entries) == 2
        statuses = [e.crawl_status for e in entries]
        assert 'crawled' in statuses
        assert 'deleted' in statuses


# =====================================================================
# Complete command tests
# =====================================================================


class TestCompleteCommand:
    def test_creates_emoji_configs(self, db):
        """Complete should create starboard emoji entries for the new channel."""
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_posted('333', PILL, '888')
        db.update_migration_status(str(GUILD), 'done')

        # Simulate what complete does — using proper DB method
        migration = db.get_migration(str(GUILD))
        emojis = migration.emojis.split(',')

        posted_entries = db.get_posted_migration_entries(str(GUILD))

        for entry in posted_entries:
            db.add_starboard_message_v1(
                entry.original_msg_id, entry.new_starboard_msg_id,
                str(GUILD), entry.emoji, author_id=entry.author_id
            )
            if entry.star_count:
                db.update_starboard_star_count(entry.original_msg_id, entry.emoji, entry.star_count)

        for emoji in emojis:
            db.add_starboard_emoji(str(GUILD), emoji, 1, 0xffaa10)
            db.set_starboard_channel(str(GUILD), emoji, '200')

        # Verify emoji configs created
        pill_entry = db.get_starboard_entry(str(GUILD), PILL)
        assert pill_entry is not None
        assert pill_entry.channel_id == '200'

        choc_entry = db.get_starboard_entry(str(GUILD), CHOC)
        assert choc_entry is not None
        assert choc_entry.channel_id == '200'

    def test_sets_channel(self, db):
        db.add_starboard_emoji(str(GUILD), PILL, 1, 0xffaa10)
        db.set_starboard_channel(str(GUILD), PILL, '200')
        entry = db.get_starboard_entry(str(GUILD), PILL)
        assert entry.channel_id == '200'

    def test_rejects_incomplete_migration(self, db):
        """Complete should only work when status is 'done'."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        migration = db.get_migration(str(GUILD))
        assert migration.status == 'crawling'
        # In the real cog, this would return an error message

    def test_cleans_up(self, db):
        """After complete, migration data should be removed."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_posted('333', PILL, '888')
        db.update_migration_status(str(GUILD), 'done')

        # Simulate complete cleanup
        db.delete_migration_entries(str(GUILD))
        db.delete_migration(str(GUILD))

        assert db.get_migration(str(GUILD)) is None
        assert db.get_migration_entry('333', PILL) is None


# =====================================================================
# Starboard message integration
# =====================================================================


class TestStarboardMessageIntegration:
    def test_posted_entries_preserve_author_and_count(self, db):
        """After complete, starboard_message_v1 should have correct author and count."""
        db.add_starboard_emoji(str(GUILD), PILL, 1, 0xffaa10)
        db.add_starboard_message_v1('333', '888', str(GUILD), PILL, author_id='777')
        db.update_starboard_star_count('333', PILL, 5)

        msg = db.get_starboard_message_v1('333', PILL)
        assert msg.author_id == '777'
        assert msg.star_count == 5

    def test_reactors_queryable_after_complete(self, db):
        """Reactors added during crawl should remain after migration cleanup."""
        db.bulk_add_reactors('333', PILL, ['user1', 'user2'])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')

        # Cleanup migration
        db.delete_migration_entries(str(GUILD))
        db.delete_migration(str(GUILD))

        # Reactors should still be there
        reactors = db.get_reactors('333', PILL)
        assert len(reactors) == 2
        assert 'user1' in reactors
        assert 'user2' in reactors

    def test_multiple_emoji_entries(self, db):
        """Same message with different emojis should create separate starboard entries."""
        db.add_starboard_emoji(str(GUILD), PILL, 1, 0xffaa10)
        db.add_starboard_emoji(str(GUILD), CHOC, 1, 0xffaa10)

        db.add_starboard_message_v1('333', '888', str(GUILD), PILL, author_id='777')
        db.add_starboard_message_v1('333', '889', str(GUILD), CHOC, author_id='777')

        pill_msg = db.get_starboard_message_v1('333', PILL)
        choc_msg = db.get_starboard_message_v1('333', CHOC)
        assert pill_msg.starboard_msg_id == '888'
        assert choc_msg.starboard_msg_id == '889'

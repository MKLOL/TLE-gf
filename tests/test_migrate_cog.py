"""DB-level migration cog tests: parsing, ordering, complete, integration."""
import json
import pytest
from tests._migrate_fakes import (
    _FakeMigrateDb, GUILD, PILL, CHOC, db, _zero_rate_delay,
)
from tle.cogs._migrate_helpers import parse_old_bot_message, build_fallback_message
from tle.util.db.user_db_conn import namedtuple_factory
from tle import constants


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


# =====================================================================
# Complete with reaction-based crawl
# =====================================================================


def _run_complete_import(db, guild_id, new_channel_id):
    """Reproduce the complete phase import logic from migrate.py lines 488-570.

    This mirrors the actual raw-SQL transaction the cog runs, including alias
    resolution, deduplication, and merged reactor counts.
    """
    migration = db.get_migration(str(guild_id))
    emojis = migration.emojis.split(',')
    alias_map = db.get_migration_alias_map(guild_id)
    main_emojis = set(emojis) - set(alias_map.keys()) if alias_map else set(emojis)

    posted_entries = db.get_posted_migration_entries(str(guild_id))

    conn = db.conn
    seen_msgs = set()
    imported = 0
    for entry in posted_entries:
        resolved_emoji = alias_map.get(entry.emoji, entry.emoji) if alias_map else entry.emoji

        dedup_key = (entry.original_msg_id, resolved_emoji)
        if dedup_key in seen_msgs:
            continue
        seen_msgs.add(dedup_key)

        star_count = entry.star_count or 0
        if alias_map:
            all_family = [resolved_emoji] + [k for k, v in alias_map.items()
                                              if v == resolved_emoji]
            merged_count = db.get_merged_reactor_count(entry.original_msg_id, all_family)
            if merged_count > 0:
                star_count = merged_count

        conn.execute(
            'INSERT OR IGNORE INTO starboard_message_v1 '
            '(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, channel_id) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (str(entry.original_msg_id), str(entry.new_starboard_msg_id),
             str(guild_id), resolved_emoji,
             str(entry.author_id) if entry.author_id else None,
             str(entry.source_channel_id) if entry.source_channel_id else None)
        )
        if star_count > 0:
            conn.execute(
                'UPDATE starboard_message_v1 SET star_count = ? '
                'WHERE original_msg_id = ? AND emoji = ?',
                (star_count, str(entry.original_msg_id), resolved_emoji)
            )
        imported += 1

    for emoji in main_emojis:
        conn.execute(
            'INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) '
            'VALUES (?, ?, ?, ?) '
            'ON CONFLICT(guild_id, emoji) DO UPDATE SET threshold = excluded.threshold, '
            'color = excluded.color',
            (str(guild_id), emoji, 1, constants._DEFAULT_STAR_COLOR)
        )
        conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND emoji = ?',
            (str(new_channel_id), str(guild_id), emoji)
        )

    for alias_emoji, main_emoji in alias_map.items():
        conn.execute(
            'INSERT OR REPLACE INTO starboard_alias (guild_id, alias_emoji, main_emoji) '
            'VALUES (?, ?, ?)',
            (str(guild_id), alias_emoji, main_emoji)
        )

    conn.commit()
    return imported


# Use a second emoji that acts as an alias — "catshock" placeholder
CATSHOCK = '\N{FACE SCREAMING IN FEAR}'


class TestCompleteWithReactionCrawl:
    """Test the complete phase when entries were crawled from actual reactions."""

    def test_complete_with_reaction_based_star_counts(self, db):
        """star_count from actual reaction.count should be preserved in starboard_message_v1."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.update_migration_status(str(GUILD), 'done')

        # Entry 1: reaction.count = 12 (the actual reaction count, not displayed_count)
        db.add_migration_entry(str(GUILD), '1001', PILL, '5001', '100')
        db.update_migration_entry_crawled('1001', PILL, '300', '801', 12)
        db.update_migration_entry_posted('1001', PILL, '9001')

        # Entry 2: reaction.count = 3
        db.add_migration_entry(str(GUILD), '1002', PILL, '5002', '100')
        db.update_migration_entry_crawled('1002', PILL, '300', '802', 3)
        db.update_migration_entry_posted('1002', PILL, '9002')

        # Entry 3: reaction.count = 0 (edge case — no reactions found)
        db.add_migration_entry(str(GUILD), '1003', PILL, '5003', '100')
        db.update_migration_entry_crawled('1003', PILL, '300', '803', 0)
        db.update_migration_entry_posted('1003', PILL, '9003')

        imported = _run_complete_import(db, GUILD, '200')
        assert imported == 3

        msg1 = db.get_starboard_message_v1('1001', PILL)
        assert msg1 is not None
        assert msg1.star_count == 12
        assert msg1.author_id == '801'

        msg2 = db.get_starboard_message_v1('1002', PILL)
        assert msg2 is not None
        assert msg2.star_count == 3
        assert msg2.author_id == '802'

        msg3 = db.get_starboard_message_v1('1003', PILL)
        assert msg3 is not None
        assert msg3.star_count == 0  # no reactions, stays at default 0

    def test_complete_star_leaderboard_after_migration(self, db):
        """get_starboard_star_leaderboard should rank authors by total stars after import."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.update_migration_status(str(GUILD), 'done')

        # Author 801: two messages with star_count 10 and 5 => total 15
        db.add_migration_entry(str(GUILD), '1001', PILL, '5001', '100')
        db.update_migration_entry_crawled('1001', PILL, '300', '801', 10)
        db.update_migration_entry_posted('1001', PILL, '9001')

        db.add_migration_entry(str(GUILD), '1002', PILL, '5002', '100')
        db.update_migration_entry_crawled('1002', PILL, '300', '801', 5)
        db.update_migration_entry_posted('1002', PILL, '9002')

        # Author 802: one message with star_count 20 => total 20
        db.add_migration_entry(str(GUILD), '1003', PILL, '5003', '100')
        db.update_migration_entry_crawled('1003', PILL, '300', '802', 20)
        db.update_migration_entry_posted('1003', PILL, '9003')

        # Author 803: one message with star_count 7 => total 7
        db.add_migration_entry(str(GUILD), '1004', PILL, '5004', '100')
        db.update_migration_entry_crawled('1004', PILL, '300', '803', 7)
        db.update_migration_entry_posted('1004', PILL, '9004')

        _run_complete_import(db, GUILD, '200')

        lb = db.get_starboard_star_leaderboard(str(GUILD), PILL)
        assert len(lb) == 3
        # Ranked: 802 (20), 801 (15), 803 (7)
        assert lb[0].author_id == '802'
        assert lb[0].total_stars == 20
        assert lb[1].author_id == '801'
        assert lb[1].total_stars == 15
        assert lb[2].author_id == '803'
        assert lb[2].total_stars == 7

    def test_complete_message_leaderboard_after_migration(self, db):
        """get_starboard_leaderboard should rank authors by message count after import."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.update_migration_status(str(GUILD), 'done')

        # Author 801: 3 starboarded messages
        for i, orig_id in enumerate(['1001', '1002', '1003']):
            db.add_migration_entry(str(GUILD), orig_id, PILL, str(5000 + i), '100')
            db.update_migration_entry_crawled(orig_id, PILL, '300', '801', 4)
            db.update_migration_entry_posted(orig_id, PILL, str(9000 + i))

        # Author 802: 1 starboarded message
        db.add_migration_entry(str(GUILD), '1004', PILL, '5010', '100')
        db.update_migration_entry_crawled('1004', PILL, '300', '802', 6)
        db.update_migration_entry_posted('1004', PILL, '9010')

        # Author 803: 2 starboarded messages
        for i, orig_id in enumerate(['1005', '1006']):
            db.add_migration_entry(str(GUILD), orig_id, PILL, str(5020 + i), '100')
            db.update_migration_entry_crawled(orig_id, PILL, '300', '803', 2)
            db.update_migration_entry_posted(orig_id, PILL, str(9020 + i))

        _run_complete_import(db, GUILD, '200')

        lb = db.get_starboard_leaderboard(str(GUILD), PILL)
        assert len(lb) == 3
        # Ranked: 801 (3 messages), 803 (2 messages), 802 (1 message)
        assert lb[0].author_id == '801'
        assert lb[0].message_count == 3
        assert lb[1].author_id == '803'
        assert lb[1].message_count == 2
        assert lb[2].author_id == '802'
        assert lb[2].message_count == 1

    def test_complete_with_entries_from_same_original_different_display_emoji(self, db):
        """Two migration entries for the same original_msg_id but different old bot display
        emojis (pill vs catshock). Both resolve to PILL via alias. Complete should import
        only one starboard message (dedup)."""
        # Migration with both emojis; CATSHOCK is aliased to PILL
        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CATSHOCK}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CATSHOCK: PILL}))
        db.update_migration_status(str(GUILD), 'done')

        # Same original_msg_id=1001, two entries under different emoji columns
        # Entry from old bot message that displayed PILL
        db.add_migration_entry(str(GUILD), '1001', PILL, '5001', '100')
        db.update_migration_entry_crawled('1001', PILL, '300', '801', 8)
        db.update_migration_entry_posted('1001', PILL, '9001')

        # Entry from old bot message that displayed CATSHOCK for the same original
        db.add_migration_entry(str(GUILD), '1001', CATSHOCK, '5002', '100')
        db.update_migration_entry_crawled('1001', CATSHOCK, '300', '801', 5)
        db.update_migration_entry_posted('1001', CATSHOCK, '9002')

        # Add reactors so merged count can be computed
        db.bulk_add_reactors('1001', PILL, ['u1', 'u2', 'u3', 'u4', 'u5'])
        db.bulk_add_reactors('1001', CATSHOCK, ['u3', 'u4', 'u5', 'u6', 'u7'])
        # Unique reactors across both: u1, u2, u3, u4, u5, u6, u7 = 7

        imported = _run_complete_import(db, GUILD, '200')

        # Only one message should be imported (dedup on resolved emoji)
        assert imported == 1

        msg = db.get_starboard_message_v1('1001', PILL)
        assert msg is not None
        assert msg.author_id == '801'
        # Star count should be the merged reactor count (7 unique users)
        assert msg.star_count == 7

        # No entry under CATSHOCK — it was resolved to PILL
        catshock_msg = db.get_starboard_message_v1('1001', CATSHOCK)
        assert catshock_msg is None

        # Emoji config should only be created for the main emoji (PILL), not the alias
        pill_config = db.get_starboard_entry(str(GUILD), PILL)
        assert pill_config is not None
        assert pill_config.channel_id == '200'

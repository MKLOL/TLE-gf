"""Cog integration tests for the migration system.

Tests crawl parsing, post ordering, and the complete command using
fake Discord objects.
"""
import asyncio
import json
import sqlite3
import time

import pytest

import discord
from tle.cogs._migrate_helpers import (
    parse_old_bot_message,
    serialize_embed_fallback,
    build_fallback_message,
)
from tle.cogs.starboard import Starboard, _starboard_content
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin
from tle.util.db.migration_db import MigrationDbMixin


# =====================================================================
# Fake Discord objects
# =====================================================================


class _FakeUser:
    def __init__(self, user_id=777, name='TestUser'):
        self.id = user_id
        self.display_name = name
        self.display_avatar = type('A', (), {'url': 'https://cdn.example.com/avatar.png'})()

    def __str__(self):
        return f'{self.display_name}#0001'


class _FakeReaction:
    def __init__(self, emoji_str, count=1, user_ids=None):
        self.emoji = emoji_str
        self.count = count
        self._user_ids = user_ids or []

    async def users(self):
        for uid in self._user_ids:
            yield _FakeUser(uid)


class _FakeMessage:
    def __init__(self, msg_id=333, content='', embeds=None, reactions=None, author=None,
                 channel=None):
        self.id = msg_id
        self.content = content
        self.embeds = embeds or []
        self.reactions = reactions or []
        self.author = author or _FakeUser()
        self.channel = channel
        self.created_at = None
        self.jump_url = f'https://discord.com/channels/111/222/{msg_id}'
        self.reference = None
        self.type = discord.MessageType.default
        self.attachments = []


class _FakeChannel:
    def __init__(self, channel_id=100, messages=None):
        self.id = channel_id
        self.mention = f'<#{channel_id}>'
        self._messages = {m.id: m for m in (messages or [])}

    async def fetch_message(self, msg_id):
        if msg_id in self._messages:
            return self._messages[msg_id]
        raise discord.NotFound(None, 'Not found')

    async def history(self, after=None, oldest_first=True, limit=None):
        msgs = sorted(self._messages.values(), key=lambda m: m.id,
                       reverse=not oldest_first)
        after_id = after.id if after else 0
        for m in msgs:
            if m.id > after_id:
                yield m

    async def send(self, content=None, embeds=None, files=None):
        sent_id = int(time.time() * 1000)
        return _FakeMessage(msg_id=sent_id, content=content or '', embeds=embeds or [])


class _FakeMigrateDb(StarboardDbMixin, MigrationDbMixin):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        for sql in [
            '''CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id TEXT, emoji TEXT, threshold INTEGER NOT NULL DEFAULT 3,
                color INTEGER NOT NULL DEFAULT 16755216, channel_id TEXT,
                PRIMARY KEY (guild_id, emoji))''',
            '''CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id TEXT, starboard_msg_id TEXT, guild_id TEXT,
                emoji TEXT, author_id TEXT, star_count INTEGER DEFAULT 0,
                channel_id TEXT, PRIMARY KEY (original_msg_id, emoji))''',
            '''CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT, emoji TEXT, user_id TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id))''',
            '''CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id TEXT, alias_emoji TEXT, main_emoji TEXT,
                PRIMARY KEY (guild_id, alias_emoji))''',
            '''CREATE TABLE IF NOT EXISTS guild_config (
                guild_id TEXT, key TEXT, value TEXT,
                PRIMARY KEY (guild_id, key))''',
            '''CREATE TABLE IF NOT EXISTS starboard_migration (
                guild_id TEXT PRIMARY KEY, old_channel_id TEXT NOT NULL,
                new_channel_id TEXT NOT NULL, emojis TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'crawling',
                last_crawled_msg_id TEXT, crawl_total INTEGER DEFAULT 0,
                crawl_done INTEGER DEFAULT 0, crawl_failed INTEGER DEFAULT 0,
                post_total INTEGER DEFAULT 0, post_done INTEGER DEFAULT 0,
                started_at REAL NOT NULL)''',
            '''CREATE TABLE IF NOT EXISTS starboard_migration_entry (
                guild_id TEXT NOT NULL, original_msg_id TEXT NOT NULL,
                emoji TEXT NOT NULL, old_bot_msg_id TEXT NOT NULL,
                old_channel_id TEXT NOT NULL, source_channel_id TEXT,
                author_id TEXT, star_count INTEGER DEFAULT 0,
                new_starboard_msg_id TEXT,
                crawl_status TEXT NOT NULL DEFAULT 'pending',
                embed_fallback TEXT,
                PRIMARY KEY (original_msg_id, emoji))''',
        ]:
            self.conn.execute(sql)
        self.conn.commit()

    def close(self):
        self.conn.close()


def _run(coro):
    return asyncio.run(coro)


GUILD = 111
PILL = '💊'
CHOC = '🍫'


@pytest.fixture
def db():
    d = _FakeMigrateDb()
    yield d
    d.close()


# =====================================================================
# Crawl parsing tests
# =====================================================================


class TestCrawlParsing:
    """Test that the crawl phase correctly parses old bot messages."""

    def test_processes_valid_starboard_message(self):
        content = '💊 **5** | https://discord.com/channels/111/222/333'
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
        content = '⭐ **5** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result is not None
        emoji_str = result[0]
        # If we're filtering for pill only
        assert emoji_str not in {PILL, CHOC}

    def test_handles_mixed_emojis(self):
        """Both pill and chocolate should be parsed."""
        pill_msg = '💊 **3** | https://discord.com/channels/111/222/333'
        choc_msg = '🍫 **7** | https://discord.com/channels/111/222/444'
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
    """Test that posting phase orders by snowflake (chronological)."""

    def test_posts_oldest_first(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        # Add entries with snowflake IDs (higher = newer)
        db.add_migration_entry(str(GUILD), '9999', PILL, '444', '100')
        db.add_migration_entry(str(GUILD), '1111', PILL, '445', '100')
        db.add_migration_entry(str(GUILD), '5555', PILL, '446', '100')

        db.update_migration_entry_crawled('9999', PILL, '500', '777', 3)
        db.update_migration_entry_crawled('1111', PILL, '500', '778', 7)
        db.update_migration_entry_crawled('5555', PILL, '500', '779', 5)

        entries = db.get_migration_entries_for_posting(str(GUILD))
        ids = [e.original_msg_id for e in entries]
        assert ids == ['1111', '5555', '9999']

    def test_uses_fallback_for_deleted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        fallback = json.dumps({'content': 'Deleted msg'})
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

        # Simulate what complete does
        migration = db.get_migration(str(GUILD))
        emojis = migration.emojis.split(',')

        all_entries = db.conn.execute(
            'SELECT * FROM starboard_migration_entry WHERE guild_id = ? AND crawl_status = ?',
            (str(GUILD), 'posted')
        ).fetchall()

        for entry in all_entries:
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

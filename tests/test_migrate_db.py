"""Tests for migration DB methods."""
import sqlite3

import pytest

from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin
from tle.util.db.migration_db import MigrationDbMixin


class FakeMigrateDb(StarboardDbMixin, MigrationDbMixin):
    """Test double combining starboard + migration DB methods."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
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
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_migration (
                guild_id            TEXT PRIMARY KEY,
                old_channel_id      TEXT NOT NULL,
                new_channel_id      TEXT NOT NULL,
                emojis              TEXT NOT NULL,
                status              TEXT NOT NULL DEFAULT 'crawling',
                last_crawled_msg_id TEXT,
                crawl_total         INTEGER DEFAULT 0,
                crawl_done          INTEGER DEFAULT 0,
                crawl_failed        INTEGER DEFAULT 0,
                post_total          INTEGER DEFAULT 0,
                post_done           INTEGER DEFAULT 0,
                started_at          REAL NOT NULL,
                alias_map           TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_migration_entry (
                guild_id            TEXT NOT NULL,
                original_msg_id     TEXT NOT NULL,
                emoji               TEXT NOT NULL,
                old_bot_msg_id      TEXT NOT NULL,
                old_channel_id      TEXT NOT NULL,
                source_channel_id   TEXT,
                author_id           TEXT,
                star_count          INTEGER DEFAULT 0,
                new_starboard_msg_id TEXT,
                crawl_status        TEXT NOT NULL DEFAULT 'pending',
                embed_fallback      TEXT,
                last_error          TEXT,
                PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        self.conn.commit()

    def close(self):
        self.conn.close()


GUILD = '111111111111111111'
PILL = '💊'
CHOC = '🍫'


@pytest.fixture
def db():
    d = FakeMigrateDb()
    yield d
    d.close()


# =====================================================================
# Migration lifecycle
# =====================================================================


class TestMigrationLifecycle:
    def test_create_and_get(self, db):
        db.create_migration(GUILD, '100', '200', '💊,🍫', 1000.0)
        m = db.get_migration(GUILD)
        assert m.guild_id == GUILD
        assert m.old_channel_id == '100'
        assert m.new_channel_id == '200'
        assert m.emojis == '💊,🍫'
        assert m.status == 'crawling'
        assert m.started_at == 1000.0

    def test_duplicate_guild_rejected(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        with pytest.raises(Exception):
            db.create_migration(GUILD, '101', '201', '💊', 2000.0)

    def test_status_transitions(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.update_migration_status(GUILD, 'posting')
        assert db.get_migration(GUILD).status == 'posting'
        db.update_migration_status(GUILD, 'done')
        assert db.get_migration(GUILD).status == 'done'

    def test_checkpoint_updates(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.update_migration_checkpoint(GUILD, '555', 3, 1)
        m = db.get_migration(GUILD)
        assert m.last_crawled_msg_id == '555'
        assert m.crawl_done == 3
        assert m.crawl_failed == 1

    def test_post_totals(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.set_migration_post_totals(GUILD, 42)
        assert db.get_migration(GUILD).post_total == 42
        db.update_migration_post_done(GUILD, 10)
        assert db.get_migration(GUILD).post_done == 10

    def test_crawl_total(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.set_migration_crawl_total(GUILD, 100)
        assert db.get_migration(GUILD).crawl_total == 100

    def test_delete(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.delete_migration(GUILD)
        assert db.get_migration(GUILD) is None

    def test_get_nonexistent(self, db):
        assert db.get_migration('999') is None


# =====================================================================
# Migration entries
# =====================================================================


class TestMigrationEntries:
    def test_add_and_get(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        entry = db.get_migration_entry('333', PILL)
        assert entry is not None
        assert entry.guild_id == GUILD
        assert entry.original_msg_id == '333'
        assert entry.emoji == PILL
        assert entry.old_bot_msg_id == '444'
        assert entry.crawl_status == 'pending'

    def test_insert_or_ignore_duplicate(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        # Should not raise
        db.add_migration_entry(GUILD, '333', PILL, '555', '100')
        # Original stays
        entry = db.get_migration_entry('333', PILL)
        assert entry.old_bot_msg_id == '444'

    def test_update_crawled(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        assert entry.source_channel_id == '500'
        assert entry.author_id == '777'
        assert entry.star_count == 5

    def test_update_deleted(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{"content":"hi"}')
        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'
        assert entry.embed_fallback == '{"content":"hi"}'

    def test_update_posted(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_posted('333', PILL, '888')
        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'posted'
        assert entry.new_starboard_msg_id == '888'

    def test_get_for_posting_starboard_order(self, db):
        """Entries should be ordered by old_bot_msg_id ASC (starboard order)."""
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        # original 999 was starboarded first (old_bot_msg_id 444)
        # original 111 was starboarded second (old_bot_msg_id 445)
        db.add_migration_entry(GUILD, '999', PILL, '444', '100')
        db.add_migration_entry(GUILD, '111', PILL, '445', '100')
        db.update_migration_entry_crawled('999', PILL, '500', '777', 3)
        db.update_migration_entry_crawled('111', PILL, '500', '778', 7)
        entries = db.get_migration_entries_for_posting(GUILD)
        assert len(entries) == 2
        # 444 < 445, so original 999 comes first
        assert entries[0].original_msg_id == '999'
        assert entries[1].original_msg_id == '111'

    def test_get_for_posting_includes_deleted(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{}')
        entries = db.get_migration_entries_for_posting(GUILD)
        assert len(entries) == 1
        assert entries[0].crawl_status == 'deleted'

    def test_get_for_posting_excludes_pending_and_posted(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '111', PILL, '444', '100')  # pending
        db.add_migration_entry(GUILD, '222', PILL, '445', '100')
        db.update_migration_entry_crawled('222', PILL, '500', '777', 5)
        db.update_migration_entry_posted('222', PILL, '888')  # posted
        db.add_migration_entry(GUILD, '333', PILL, '446', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '778', 3)  # crawled
        entries = db.get_migration_entries_for_posting(GUILD)
        assert len(entries) == 1
        assert entries[0].original_msg_id == '333'

    def test_count_by_status(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '111', PILL, '444', '100')
        db.add_migration_entry(GUILD, '222', PILL, '445', '100')
        db.update_migration_entry_crawled('222', PILL, '500', '777', 5)
        db.add_migration_entry(GUILD, '333', PILL, '446', '100')
        db.update_migration_entry_deleted('333', PILL, '{}')
        rows = db.count_migration_entries_by_status(GUILD)
        counts = {r.crawl_status: r.cnt for r in rows}
        assert counts['pending'] == 1
        assert counts['crawled'] == 1
        assert counts['deleted'] == 1

    def test_delete_all_entries(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '111', PILL, '444', '100')
        db.add_migration_entry(GUILD, '222', PILL, '445', '100')
        db.delete_migration_entries(GUILD)
        entries = db.get_migration_entries_for_posting(GUILD)
        assert len(entries) == 0


# =====================================================================
# Complete integration: entries → starboard tables
# =====================================================================


class TestCompleteIntegration:
    def test_posted_entries_to_starboard(self, db):
        """Simulate what ;migrate complete does: copy posted entries into starboard tables."""
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_posted('333', PILL, '888')

        # Simulate complete: add to starboard tables
        entry = db.get_migration_entry('333', PILL)
        db.add_starboard_emoji(GUILD, PILL, 1, 0xffaa10)
        db.set_starboard_channel(GUILD, PILL, '200')
        db.add_starboard_message_v1(
            entry.original_msg_id, entry.new_starboard_msg_id,
            GUILD, PILL, author_id=entry.author_id
        )
        db.update_starboard_star_count(entry.original_msg_id, PILL, entry.star_count)

        sb = db.get_starboard_message_v1('333', PILL)
        assert sb is not None
        assert sb.starboard_msg_id == '888'
        assert sb.author_id == '777'
        assert sb.star_count == 5

    def test_reactors_preserved(self, db):
        """Reactors added during crawl should be queryable after complete."""
        db.bulk_add_reactors('333', PILL, ['user1', 'user2', 'user3'])
        reactors = db.get_reactors('333', PILL)
        assert len(reactors) == 3

    def test_star_count_correct(self, db):
        db.add_starboard_emoji(GUILD, PILL, 1, 0xffaa10)
        db.add_starboard_message_v1('333', '888', GUILD, PILL, author_id='777')
        db.update_starboard_star_count('333', PILL, 12)
        msg = db.get_starboard_message_v1('333', PILL)
        assert msg.star_count == 12

    def test_cleanup_removes_migration_data(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.delete_migration_entries(GUILD)
        db.delete_migration(GUILD)
        assert db.get_migration(GUILD) is None
        assert db.get_migration_entry('333', PILL) is None

    def test_multiple_emojis_same_message(self, db):
        """Different emojis on the same original message are separate entries."""
        db.create_migration(GUILD, '100', '200', '💊,🍫', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.add_migration_entry(GUILD, '333', CHOC, '445', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_crawled('333', CHOC, '500', '777', 3)
        entries = db.get_migration_entries_for_posting(GUILD)
        assert len(entries) == 2

    def test_deleted_entries_have_fallback(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{"content":"old msg"}')
        entries = db.get_migration_entries_for_posting(GUILD)
        assert entries[0].embed_fallback == '{"content":"old msg"}'


# =====================================================================
# Deleted entry queries
# =====================================================================


class TestDeletedEntryQueries:
    def test_returns_deleted_entries(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{"content":"hi"}')
        entries = db.get_deleted_migration_entries(GUILD)
        assert len(entries) == 1
        assert entries[0].original_msg_id == '333'

    def test_excludes_crawled_entries(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        entries = db.get_deleted_migration_entries(GUILD)
        assert len(entries) == 0

    def test_excludes_pending_entries(self, db):
        """Pending entries also have NULL source_channel_id but should be excluded."""
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        entries = db.get_deleted_migration_entries(GUILD)
        assert len(entries) == 0

    def test_includes_posted_deleted_entries(self, db):
        """Deleted entries that have been posted still have NULL source_channel_id."""
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, '{}')
        db.update_migration_entry_posted('333', PILL, '888')
        entries = db.get_deleted_migration_entries(GUILD)
        assert len(entries) == 1
        assert entries[0].new_starboard_msg_id == '888'

    def test_chronological_order(self, db):
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '999', PILL, '444', '100')
        db.add_migration_entry(GUILD, '111', PILL, '445', '100')
        db.update_migration_entry_deleted('999', PILL, '{}')
        db.update_migration_entry_deleted('111', PILL, '{}')
        entries = db.get_deleted_migration_entries(GUILD)
        assert entries[0].original_msg_id == '111'
        assert entries[1].original_msg_id == '999'

    def test_mixed_deleted_and_crawled(self, db):
        """Only deleted entries returned when both types exist."""
        db.create_migration(GUILD, '100', '200', '💊', 1000.0)
        db.add_migration_entry(GUILD, '111', PILL, '444', '100')
        db.add_migration_entry(GUILD, '222', PILL, '445', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_deleted('222', PILL, '{}')
        entries = db.get_deleted_migration_entries(GUILD)
        assert len(entries) == 1
        assert entries[0].original_msg_id == '222'

"""Migration database methods — mixin for UserDbConn.

Provides all DB operations needed by the starboard migration cog:
migration lifecycle (create/get/update/delete) and migration entries
(add/update status/query for posting/count/delete).
"""
import logging

logger = logging.getLogger(__name__)


class MigrationDbMixin:
    """Mixin providing starboard migration DB methods. Expects self.conn."""

    # --- Migration lifecycle ---

    def create_migration(self, guild_id, old_channel_id, new_channel_id, emojis, started_at):
        """Create a new migration record. Raises if one already exists for this guild."""
        self.conn.execute(
            'INSERT INTO starboard_migration '
            '(guild_id, old_channel_id, new_channel_id, emojis, started_at) '
            'VALUES (?, ?, ?, ?, ?)',
            (str(guild_id), str(old_channel_id), str(new_channel_id), emojis, started_at)
        )
        self.conn.commit()

    def get_migration(self, guild_id):
        """Get the migration record for a guild, or None."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration WHERE guild_id = ?',
            (str(guild_id),)
        ).fetchone()

    def update_migration_status(self, guild_id, status):
        """Update the migration status (crawling/posting/done/failed)."""
        self.conn.execute(
            'UPDATE starboard_migration SET status = ? WHERE guild_id = ?',
            (status, str(guild_id))
        )
        self.conn.commit()

    def update_migration_checkpoint(self, guild_id, last_crawled_msg_id, crawl_done, crawl_failed):
        """Update crawl checkpoint and counters."""
        self.conn.execute(
            'UPDATE starboard_migration SET last_crawled_msg_id = ?, crawl_done = ?, crawl_failed = ? '
            'WHERE guild_id = ?',
            (str(last_crawled_msg_id), crawl_done, crawl_failed, str(guild_id))
        )
        self.conn.commit()

    def set_migration_crawl_total(self, guild_id, crawl_total):
        """Set the crawl total count."""
        self.conn.execute(
            'UPDATE starboard_migration SET crawl_total = ? WHERE guild_id = ?',
            (crawl_total, str(guild_id))
        )
        self.conn.commit()

    def set_migration_post_totals(self, guild_id, post_total):
        """Set the post total before posting phase begins."""
        self.conn.execute(
            'UPDATE starboard_migration SET post_total = ? WHERE guild_id = ?',
            (post_total, str(guild_id))
        )
        self.conn.commit()

    def update_migration_post_done(self, guild_id, post_done):
        """Update the post-done counter."""
        self.conn.execute(
            'UPDATE starboard_migration SET post_done = ? WHERE guild_id = ?',
            (post_done, str(guild_id))
        )
        self.conn.commit()

    def delete_migration(self, guild_id):
        """Delete the migration record for a guild."""
        self.conn.execute(
            'DELETE FROM starboard_migration WHERE guild_id = ?',
            (str(guild_id),)
        )
        self.conn.commit()

    # --- Migration entries ---

    def add_migration_entry(self, guild_id, original_msg_id, emoji, old_bot_msg_id, old_channel_id):
        """Add a crawled entry. INSERT OR IGNORE for idempotent resume."""
        self.conn.execute(
            'INSERT OR IGNORE INTO starboard_migration_entry '
            '(guild_id, original_msg_id, emoji, old_bot_msg_id, old_channel_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (str(guild_id), str(original_msg_id), emoji, str(old_bot_msg_id), str(old_channel_id))
        )
        self.conn.commit()

    def update_migration_entry_crawled(self, original_msg_id, emoji, source_channel_id,
                                       author_id, star_count):
        """Mark an entry as crawled with resolved data."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ?, source_channel_id = ?, author_id = ?, star_count = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('crawled', str(source_channel_id), str(author_id), star_count,
             str(original_msg_id), emoji)
        )
        self.conn.commit()

    def update_migration_entry_deleted(self, original_msg_id, emoji, embed_fallback_json):
        """Mark an entry as deleted (original not fetchable) with fallback data."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ?, embed_fallback = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('deleted', embed_fallback_json, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def update_migration_entry_posted(self, original_msg_id, emoji, new_starboard_msg_id):
        """Mark an entry as posted to the new starboard channel."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ?, new_starboard_msg_id = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('posted', str(new_starboard_msg_id), str(original_msg_id), emoji)
        )
        self.conn.commit()

    def get_migration_entries_for_posting(self, guild_id):
        """Get entries ready for posting, ordered chronologically by snowflake."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND crawl_status IN (?, ?) '
            'ORDER BY CAST(original_msg_id AS INTEGER) ASC',
            (str(guild_id), 'crawled', 'deleted')
        ).fetchall()

    def count_migration_entries_by_status(self, guild_id):
        """Count entries grouped by crawl_status. Returns list of (crawl_status, count) rows."""
        return self.conn.execute(
            'SELECT crawl_status, COUNT(*) as cnt '
            'FROM starboard_migration_entry WHERE guild_id = ? '
            'GROUP BY crawl_status',
            (str(guild_id),)
        ).fetchall()

    def get_migration_entry(self, original_msg_id, emoji):
        """Get a single migration entry."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry WHERE original_msg_id = ? AND emoji = ?',
            (str(original_msg_id), emoji)
        ).fetchone()

    def delete_migration_entries(self, guild_id):
        """Delete all migration entries for a guild."""
        self.conn.execute(
            'DELETE FROM starboard_migration_entry WHERE guild_id = ?',
            (str(guild_id),)
        )
        self.conn.commit()

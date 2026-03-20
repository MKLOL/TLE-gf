"""Migration database methods — mixin for UserDbConn.

Provides all DB operations needed by the starboard migration cog:
migration lifecycle (create/get/update/delete) and migration entries
(add/update status/query for posting/count/delete).
"""
import json
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

    def set_migration_alias_map(self, guild_id, alias_map_json):
        """Store the alias map (JSON string, e.g. {"🍫":"💊"})."""
        self.conn.execute(
            'UPDATE starboard_migration SET alias_map = ? WHERE guild_id = ?',
            (alias_map_json, str(guild_id))
        )
        self.conn.commit()

    def get_migration_alias_map(self, guild_id):
        """Return the alias map as a dict, or {} if unset."""
        row = self.conn.execute(
            'SELECT alias_map FROM starboard_migration WHERE guild_id = ?',
            (str(guild_id),)
        ).fetchone()
        if row and row.alias_map:
            return json.loads(row.alias_map)
        return {}

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
                                       author_id, star_count, embed_fallback=None):
        """Mark an entry as crawled with resolved data."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ?, source_channel_id = ?, author_id = ?, star_count = ?, '
            'embed_fallback = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('crawled', str(source_channel_id), str(author_id), star_count,
             embed_fallback, str(original_msg_id), emoji)
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
        """Get entries ready for posting, ordered by old bot post (starboard order)."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND crawl_status IN (?, ?) '
            'ORDER BY CAST(old_bot_msg_id AS INTEGER) ASC',
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

    def update_migration_entry_post_failed(self, original_msg_id, emoji):
        """Mark an entry as failed during posting. Preserves all other data."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('post_failed', str(original_msg_id), emoji)
        )
        self.conn.commit()

    def reset_post_failed_entries(self, guild_id):
        """Reset post_failed entries for retry.

        Entries with source_channel_id go back to 'crawled' (will try full render),
        entries without go back to 'deleted' (will use fallback).
        """
        self.conn.execute(
            'UPDATE starboard_migration_entry SET crawl_status = CASE '
            'WHEN source_channel_id IS NOT NULL THEN ? ELSE ? END '
            'WHERE guild_id = ? AND crawl_status = ?',
            ('crawled', 'deleted', str(guild_id), 'post_failed')
        )
        self.conn.commit()

    def get_entries_missing_fallback(self, guild_id):
        """Get entries that have no embed_fallback data, keyed by old_bot_msg_id."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND embed_fallback IS NULL AND crawl_status != ?',
            (str(guild_id), 'pending')
        ).fetchall()

    def set_embed_fallback(self, original_msg_id, emoji, fallback_json):
        """Set embed_fallback on an existing entry."""
        self.conn.execute(
            'UPDATE starboard_migration_entry SET embed_fallback = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            (fallback_json, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def get_all_posted_msg_ids(self, guild_id):
        """Get new_starboard_msg_id for all posted entries (for bulk deletion)."""
        rows = self.conn.execute(
            'SELECT new_starboard_msg_id FROM starboard_migration_entry '
            'WHERE guild_id = ? AND new_starboard_msg_id IS NOT NULL',
            (str(guild_id),)
        ).fetchall()
        return [r.new_starboard_msg_id for r in rows]

    def reset_all_entries_for_repost(self, guild_id):
        """Reset all non-pending entries back to crawled/deleted for re-posting.

        Clears new_starboard_msg_id and last_error. Entries with source_channel_id
        go to 'crawled', entries without go to 'deleted'.
        """
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = CASE '
            'WHEN source_channel_id IS NOT NULL THEN ? ELSE ? END, '
            'new_starboard_msg_id = NULL, last_error = NULL '
            'WHERE guild_id = ? AND crawl_status != ?',
            ('crawled', 'deleted', str(guild_id), 'pending')
        )
        self.conn.commit()

    def get_posted_migration_entries(self, guild_id):
        """Get all entries with crawl_status='posted', ordered chronologically."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND crawl_status = ? '
            'ORDER BY CAST(original_msg_id AS INTEGER) ASC',
            (str(guild_id), 'posted')
        ).fetchall()

    def get_migration_entry(self, original_msg_id, emoji):
        """Get a single migration entry."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry WHERE original_msg_id = ? AND emoji = ?',
            (str(original_msg_id), emoji)
        ).fetchone()

    def update_migration_entry_retry_exhausted(self, original_msg_id, emoji, error_msg):
        """Mark an entry as retry_exhausted with the last error message."""
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = ?, last_error = ? '
            'WHERE original_msg_id = ? AND emoji = ?',
            ('retry_exhausted', error_msg, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def get_retry_exhausted_entries(self, guild_id):
        """Get entries that failed after all retries, ordered chronologically."""
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND crawl_status = ? '
            'ORDER BY CAST(original_msg_id AS INTEGER) ASC',
            (str(guild_id), 'retry_exhausted')
        ).fetchall()

    def reset_retry_exhausted_entries(self, guild_id):
        """Reset retry_exhausted entries for retry.

        Entries with source_channel_id go back to 'crawled',
        entries without go back to 'deleted'. Clears last_error.
        """
        self.conn.execute(
            'UPDATE starboard_migration_entry '
            'SET crawl_status = CASE '
            'WHEN source_channel_id IS NOT NULL THEN ? ELSE ? END, '
            'last_error = NULL '
            'WHERE guild_id = ? AND crawl_status = ?',
            ('crawled', 'deleted', str(guild_id), 'retry_exhausted')
        )
        self.conn.commit()

    def get_deleted_migration_entries(self, guild_id):
        """Get entries where the original message was deleted/inaccessible.

        Identified by NULL source_channel_id (only set for successfully fetched
        messages). Works regardless of current crawl_status (deleted, posted, etc.).
        """
        return self.conn.execute(
            'SELECT * FROM starboard_migration_entry '
            'WHERE guild_id = ? AND source_channel_id IS NULL AND crawl_status != ? '
            'ORDER BY CAST(original_msg_id AS INTEGER) ASC',
            (str(guild_id), 'pending')
        ).fetchall()

    def delete_migration_entries(self, guild_id):
        """Delete all migration entries for a guild."""
        self.conn.execute(
            'DELETE FROM starboard_migration_entry WHERE guild_id = ?',
            (str(guild_id),)
        )
        self.conn.commit()

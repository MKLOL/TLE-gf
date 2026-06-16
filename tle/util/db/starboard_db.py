"""Starboard database methods — extracted from user_db_conn.py as a mixin.

Contains: old starboard compat methods, multi-emoji starboard v1, reactor
tracking, and guild config methods. Leaderboard, emoji-alias and
per-user-default queries live in ``_starboard_db_queries`` (StarboardQueriesDbMixin)
to keep this module under the 500-line limit.
"""
# Snowflake timestamp constants live in their own module to avoid a circular
# import with the leaderboard query mixin; re-exported here so existing
# `from tle.util.db.starboard_db import ...` imports keep working.
from tle.util.db._starboard_db_constants import (
    DISCORD_EPOCH_MS,
    SNOWFLAKE_TIMESTAMP_DIVISOR,
    _NO_TIME_BOUND,
    snowflake_to_unix_sql,
)
from tle.util.db._starboard_db_queries import StarboardQueriesDbMixin


class StarboardDbMixin(StarboardQueriesDbMixin):
    """Mixin providing all starboard DB methods. Expects self.conn to be a sqlite3 connection.

    Leaderboard, alias and per-user-default methods are inherited from
    StarboardQueriesDbMixin (split out to keep this module under 500 lines)."""

    def _create_starboard_tables(self):
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard ('
            'guild_id     TEXT PRIMARY KEY,'
            'channel_id   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard_message ('
            'original_msg_id    TEXT PRIMARY KEY,'
            'starboard_msg_id   TEXT,'
            'guild_id           TEXT'
            ')'
        )
        # Multi-emoji starboard v1 tables
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_config_v1 (
                guild_id    TEXT PRIMARY KEY,
                channel_id  TEXT
            )
        ''')
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
        # Starboard reactors — tracks which users reacted with which emoji
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')
        # Starboard emoji aliases — alias emojis that count toward the main emoji
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id    TEXT,
                alias_emoji TEXT,
                main_emoji  TEXT,
                PRIMARY KEY (guild_id, alias_emoji)
            )
        ''')
        # Per-user default emoji for starboard leaderboard commands
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS user_starboard_default (
                guild_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                emoji    TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')

    # --- Old starboard methods (kept for migration compatibility) ---

    def get_starboard(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM starboard '
                 'WHERE guild_id = ?')
        return self.conn.execute(query, (guild_id,)).fetchone()

    def set_starboard(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO starboard '
                 '(guild_id, channel_id) '
                 'VALUES (?, ?)')
        self.conn.execute(query, (guild_id, channel_id))
        self.conn.commit()

    def clear_starboard(self, guild_id):
        query = ('DELETE FROM starboard '
                 'WHERE guild_id = ?')
        self.conn.execute(query, (guild_id,))
        self.conn.commit()

    def check_exists_starboard_message(self, original_msg_id):
        query = ('SELECT 1 '
                 'FROM starboard_message '
                 'WHERE original_msg_id = ?')
        res = self.conn.execute(query, (original_msg_id,)).fetchone()
        return res is not None

    def clear_starboard_messages_for_guild(self, guild_id):
        query = ('DELETE FROM starboard_message '
                 'WHERE guild_id = ?')
        rc = self.conn.execute(query, (guild_id,)).rowcount
        self.conn.commit()
        return rc

    # --- New multi-emoji starboard methods (v1 tables) ---
    # All IDs are cast to str() at the boundary to avoid SQLite int-vs-TEXT mismatch.
    # Each emoji has its own channel_id in starboard_emoji_v1 (per-emoji channels).

    def get_starboard_entry(self, guild_id, emoji):
        """Get starboard config for a guild+emoji. Returns (channel_id, threshold, color) or None."""
        guild_id = str(guild_id)
        query = '''
            SELECT channel_id, threshold, color
            FROM starboard_emoji_v1
            WHERE guild_id = ? AND emoji = ?
        '''
        return self.conn.execute(query, (guild_id, emoji)).fetchone()

    def set_starboard_channel(self, guild_id, emoji, channel_id):
        """Set the starboard channel for a specific emoji in a guild."""
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND emoji = ?',
            (str(channel_id), guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def clear_starboard_channel(self, guild_id, emoji):
        """Clear the starboard channel for a specific emoji."""
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET channel_id = NULL WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def add_starboard_emoji(self, guild_id, emoji, threshold, color):
        """Add or update an emoji configuration for a guild's starboard.
        Uses ON CONFLICT upsert to preserve channel_id when updating."""
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
        """Remove an emoji from a guild's starboard config, its tracked messages, reactors, and aliases."""
        guild_id = str(guild_id)
        # Collect the emoji family (main + aliases) so we clean up all reactor rows
        alias_emojis = self.get_aliases_for_emoji(guild_id, emoji)
        all_emojis = [emoji] + alias_emojis
        placeholders = ','.join('?' * len(all_emojis))
        # Clean up reactors for messages belonging to this guild+emoji (including alias reactors)
        self.conn.execute(f'''
            DELETE FROM starboard_reactors
            WHERE emoji IN ({placeholders}) AND original_msg_id IN (
                SELECT original_msg_id FROM starboard_message_v1
                WHERE guild_id = ? AND emoji = ?
            )
        ''', (*all_emojis, guild_id, emoji))
        self.conn.execute(
            'DELETE FROM starboard_emoji_v1 WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        )
        self.conn.execute(
            'DELETE FROM starboard_message_v1 WHERE guild_id = ? AND emoji = ?',
            (guild_id, emoji)
        )
        # Clean up aliases pointing to this main emoji
        self.conn.execute(
            'DELETE FROM starboard_alias WHERE guild_id = ? AND main_emoji = ?',
            (guild_id, emoji)
        )
        self.conn.commit()

    def update_starboard_threshold(self, guild_id, emoji, threshold):
        """Update the reaction threshold for an emoji."""
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET threshold = ? WHERE guild_id = ? AND emoji = ?',
            (threshold, guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def update_starboard_color(self, guild_id, emoji, color):
        """Update the embed color for an emoji."""
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'UPDATE starboard_emoji_v1 SET color = ? WHERE guild_id = ? AND emoji = ?',
            (color, guild_id, emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def add_starboard_message_v1(self, original_msg_id, starboard_msg_id, guild_id, emoji,
                                 author_id=None, channel_id=None):
        """Track a new starboard message in v1 table."""
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
        """Check if a message is already tracked in v1 table for this emoji."""
        query = 'SELECT 1 FROM starboard_message_v1 WHERE original_msg_id = ? AND emoji = ?'
        res = self.conn.execute(query, (str(original_msg_id), emoji)).fetchone()
        return res is not None

    def get_starboard_message_v1(self, original_msg_id, emoji):
        """Get a starboard message entry."""
        query = 'SELECT * FROM starboard_message_v1 WHERE original_msg_id = ? AND emoji = ?'
        return self.conn.execute(query, (str(original_msg_id), emoji)).fetchone()

    def remove_starboard_message(self, *, original_msg_id=None, emoji=None, starboard_msg_id=None):
        """Remove starboard message(s) and their reactors.
        Use original_msg_id+emoji or starboard_msg_id."""
        if starboard_msg_id is not None:
            # Look up the message first to cascade-delete reactors
            msg = self.conn.execute(
                'SELECT original_msg_id, emoji FROM starboard_message_v1 WHERE starboard_msg_id = ?',
                (str(starboard_msg_id),)
            ).fetchone()
            if msg:
                self.conn.execute(
                    'DELETE FROM starboard_reactors WHERE original_msg_id = ? AND emoji = ?',
                    (msg.original_msg_id, msg.emoji)
                )
            query = 'DELETE FROM starboard_message_v1 WHERE starboard_msg_id = ?'
            rc = self.conn.execute(query, (str(starboard_msg_id),)).rowcount
        elif original_msg_id is not None and emoji is not None:
            self.conn.execute(
                'DELETE FROM starboard_reactors WHERE original_msg_id = ? AND emoji = ?',
                (str(original_msg_id), emoji)
            )
            query = 'DELETE FROM starboard_message_v1 WHERE original_msg_id = ? AND emoji = ?'
            rc = self.conn.execute(query, (str(original_msg_id), emoji)).rowcount
        elif original_msg_id is not None:
            self.conn.execute(
                'DELETE FROM starboard_reactors WHERE original_msg_id = ?',
                (str(original_msg_id),)
            )
            query = 'DELETE FROM starboard_message_v1 WHERE original_msg_id = ?'
            rc = self.conn.execute(query, (str(original_msg_id),)).rowcount
        else:
            return 0
        self.conn.commit()
        return rc

    # --- Star count tracking ---

    def update_starboard_star_count(self, original_msg_id, emoji, count):
        """Update the star count for a starboard message."""
        self.conn.execute(
            'UPDATE starboard_message_v1 SET star_count = ? WHERE original_msg_id = ? AND emoji = ?',
            (count, str(original_msg_id), emoji)
        )
        self.conn.commit()

    def update_starboard_author_and_count(self, original_msg_id, emoji, author_id, count,
                                          channel_id=None):
        """Update author_id, star_count, and optionally channel_id (used by live reactions and backfill)."""
        if channel_id is not None:
            self.conn.execute(
                'UPDATE starboard_message_v1 SET author_id = ?, star_count = ?, channel_id = ? '
                'WHERE original_msg_id = ? AND emoji = ?',
                (str(author_id), count, str(channel_id), str(original_msg_id), emoji)
            )
        else:
            self.conn.execute(
                'UPDATE starboard_message_v1 SET author_id = ?, star_count = ? '
                'WHERE original_msg_id = ? AND emoji = ?',
                (str(author_id), count, str(original_msg_id), emoji)
            )
        self.conn.commit()

    # --- Reactor tracking ---

    def add_reactor(self, original_msg_id, emoji, user_id):
        """Record a user's reaction. INSERT OR IGNORE (idempotent)."""
        self.conn.execute(
            'INSERT OR IGNORE INTO starboard_reactors (original_msg_id, emoji, user_id) '
            'VALUES (?, ?, ?)',
            (str(original_msg_id), emoji, str(user_id))
        )
        self.conn.commit()

    def remove_reactor(self, original_msg_id, emoji, user_id):
        """Remove a user's reaction. Returns rowcount (0 or 1)."""
        rc = self.conn.execute(
            'DELETE FROM starboard_reactors WHERE original_msg_id = ? AND emoji = ? AND user_id = ?',
            (str(original_msg_id), emoji, str(user_id))
        ).rowcount
        self.conn.commit()
        return rc

    def get_reactors(self, original_msg_id, emoji):
        """Get all user IDs who reacted with this emoji on this message."""
        query = 'SELECT user_id FROM starboard_reactors WHERE original_msg_id = ? AND emoji = ?'
        return [r.user_id for r in self.conn.execute(query, (str(original_msg_id), emoji)).fetchall()]

    def get_reactor_count(self, original_msg_id, emoji):
        """Get the number of unique reactors for this emoji on this message."""
        query = 'SELECT COUNT(*) as cnt FROM starboard_reactors WHERE original_msg_id = ? AND emoji = ?'
        return self.conn.execute(query, (str(original_msg_id), emoji)).fetchone().cnt

    def get_merged_reactor_count(self, original_msg_id, emojis):
        """Count distinct users who reacted with ANY of the given emojis on a message.
        Useful for merging starboards (e.g., star + flame = unique users across both)."""
        if not emojis:
            return 0
        placeholders = ','.join('?' * len(emojis))
        query = (f'SELECT COUNT(DISTINCT user_id) as cnt FROM starboard_reactors '
                 f'WHERE original_msg_id = ? AND emoji IN ({placeholders})')
        return self.conn.execute(query, (str(original_msg_id), *emojis)).fetchone().cnt

    def bulk_add_reactors(self, original_msg_id, emoji, user_ids):
        """Bulk-insert reactors (idempotent via INSERT OR IGNORE)."""
        original_msg_id = str(original_msg_id)
        self.conn.executemany(
            'INSERT OR IGNORE INTO starboard_reactors (original_msg_id, emoji, user_id) '
            'VALUES (?, ?, ?)',
            [(original_msg_id, emoji, str(uid)) for uid in user_ids]
        )
        self.conn.commit()

    def replace_reactors(self, original_msg_id, emojis, new_reactors):
        """Replace all reactors for a message across the given emojis.

        Deletes existing reactor rows for (original_msg_id, emoji IN emojis),
        then bulk-inserts new_reactors: list of (emoji, user_id) tuples.
        """
        original_msg_id = str(original_msg_id)
        if emojis:
            placeholders = ','.join('?' * len(emojis))
            self.conn.execute(
                f'DELETE FROM starboard_reactors WHERE original_msg_id = ? AND emoji IN ({placeholders})',
                (original_msg_id, *emojis)
            )
        if new_reactors:
            self.conn.executemany(
                'INSERT OR IGNORE INTO starboard_reactors (original_msg_id, emoji, user_id) '
                'VALUES (?, ?, ?)',
                [(original_msg_id, emoji, str(uid)) for emoji, uid in new_reactors]
            )
        self.conn.commit()

    def get_starboard_entries_for_message(self, original_msg_id):
        """Get all starboard entries for an original message (across all emojis)."""
        query = 'SELECT * FROM starboard_message_v1 WHERE original_msg_id = ?'
        return self.conn.execute(query, (str(original_msg_id),)).fetchall()

    def get_all_starboard_messages_for_guild(self, guild_id):
        """Get all starboard messages for a guild (used by backfill)."""
        guild_id = str(guild_id)
        query = 'SELECT * FROM starboard_message_v1 WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_starboard_emojis_for_guild(self, guild_id):
        """Get all configured emojis for a guild's starboard."""
        guild_id = str(guild_id)
        query = 'SELECT emoji, threshold, color, channel_id FROM starboard_emoji_v1 WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

    # --- Guild config methods ---

    def get_guild_config(self, guild_id, key):
        """Get a guild config value. Returns the value string or None."""
        guild_id = str(guild_id)
        query = 'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?'
        res = self.conn.execute(query, (guild_id, key)).fetchone()
        return res.value if res else None

    def set_guild_config(self, guild_id, key, value):
        """Set a guild config value."""
        guild_id = str(guild_id)
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (guild_id, key, value)
        )
        self.conn.commit()

    def delete_guild_config(self, guild_id, key):
        """Delete a guild config value."""
        guild_id = str(guild_id)
        self.conn.execute(
            'DELETE FROM guild_config WHERE guild_id = ? AND key = ?',
            (guild_id, key)
        )
        self.conn.commit()

    def get_all_guild_configs(self, guild_id):
        """Get all config entries for a guild."""
        guild_id = str(guild_id)
        query = 'SELECT key, value FROM guild_config WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

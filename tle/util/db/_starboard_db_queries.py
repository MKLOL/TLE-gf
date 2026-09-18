"""Starboard leaderboard, alias and per-user-default DB methods.

Split out of ``starboard_db`` as a mixin to keep that module under the 500-line
limit. ``StarboardDbMixin`` inherits this, so every method resolves on the
combined connection object exactly as before.
"""
from tle.util.db._starboard_db_constants import _NO_TIME_BOUND, snowflake_to_unix_sql


class StarboardQueriesDbMixin:
    """Leaderboard queries, emoji aliases and per-user default emoji."""

    # --- Leaderboard queries with snowflake time filtering ---

    @staticmethod
    def _snowflake_time_filter(col, dlo, dhi):
        """Build SQL clauses + params to filter a Discord snowflake column by timestamp range.
        dlo/dhi are unix timestamps in seconds. 0 and _NO_TIME_BOUND mean no bound."""
        clauses = []
        params = []
        ts_expr = snowflake_to_unix_sql(col)
        if dlo and dlo > 0:
            clauses.append(f'{ts_expr} >= ?')
            params.append(dlo)
        if dhi and dhi < _NO_TIME_BOUND:
            clauses.append(f'{ts_expr} < ?')
            params.append(dhi)
        return clauses, params

    def get_starboard_leaderboard(self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND):
        """Get leaderboard by number of starboarded messages per author.
        Excludes rows with NULL or sentinel author_id (unfetchable during backfill)."""
        guild_id = str(guild_id)
        time_clauses, time_params = self._snowflake_time_filter('original_msg_id', dlo, dhi)
        extra = (' AND ' + ' AND '.join(time_clauses)) if time_clauses else ''
        query = f'''
            SELECT author_id, COUNT(*) as message_count
            FROM starboard_message_v1
            WHERE guild_id = ? AND emoji = ?
                AND author_id IS NOT NULL AND author_id != '__UNKNOWN__'
                {extra}
            GROUP BY author_id
            ORDER BY message_count DESC
        '''
        return self.conn.execute(query, (guild_id, emoji) + tuple(time_params)).fetchall()

    def get_starboard_star_leaderboard(self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND):
        """Get leaderboard by total star count per author.
        Excludes rows with NULL or sentinel author_id (unfetchable during backfill)."""
        guild_id = str(guild_id)
        time_clauses, time_params = self._snowflake_time_filter('original_msg_id', dlo, dhi)
        extra = (' AND ' + ' AND '.join(time_clauses)) if time_clauses else ''
        query = f'''
            SELECT author_id, SUM(star_count) as total_stars
            FROM starboard_message_v1
            WHERE guild_id = ? AND emoji = ?
                AND author_id IS NOT NULL AND author_id != '__UNKNOWN__'
                AND star_count > 0
                {extra}
            GROUP BY author_id
            ORDER BY total_stars DESC
        '''
        return self.conn.execute(query, (guild_id, emoji) + tuple(time_params)).fetchall()

    def get_star_givers_leaderboard(self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND,
                                     emoji_family=None):
        """Get leaderboard of users by number of distinct starboarded messages they reacted on.

        emoji_family: list of emojis to count reactors for (main + aliases).
        If None, only the main emoji is counted. Uses COUNT(DISTINCT m.original_msg_id)
        to avoid double-counting when a user reacted with both main and alias on the same message.
        """
        guild_id = str(guild_id)
        if emoji_family is None:
            emoji_family = [emoji]
        time_clauses, time_params = self._snowflake_time_filter('m.original_msg_id', dlo, dhi)
        extra = (' AND ' + ' AND '.join(time_clauses)) if time_clauses else ''
        placeholders = ','.join('?' * len(emoji_family))
        query = f'''
            SELECT r.user_id, COUNT(DISTINCT m.original_msg_id) as stars_given
            FROM (
                SELECT original_msg_id, emoji, user_id FROM starboard_reactors
                UNION
                SELECT original_msg_id, emoji, user_id FROM starboard_proxy_reactors
            ) r
            JOIN starboard_message_v1 m
                ON r.original_msg_id = m.original_msg_id AND m.emoji = ?
            WHERE m.guild_id = ? AND r.emoji IN ({placeholders})
                {extra}
            GROUP BY r.user_id
            ORDER BY stars_given DESC
        '''
        return self.conn.execute(query, (emoji, guild_id) + tuple(emoji_family) + tuple(time_params)).fetchall()

    # get_narcissus_leaderboard lives in StarboardNarcissusDbMixin — narcissus
    # is sticky-mark based, not a live reactor count (see _starboard_db_narcissus).

    def get_top_starboard_messages(self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND,
                                   author_id=None):
        """Get top starboarded messages sorted by star_count DESC, original_msg_id DESC."""
        guild_id = str(guild_id)
        time_clauses, time_params = self._snowflake_time_filter('original_msg_id', dlo, dhi)
        extra = (' AND ' + ' AND '.join(time_clauses)) if time_clauses else ''
        params = [guild_id, emoji] + list(time_params)
        if author_id is not None:
            extra += ' AND author_id = ?'
            params.append(str(author_id))
        query = f'''
            SELECT original_msg_id, author_id, star_count, channel_id
            FROM starboard_message_v1
            WHERE guild_id = ? AND emoji = ?
                AND author_id IS NOT NULL AND author_id != '__UNKNOWN__'
                AND star_count > 0
                {extra}
            ORDER BY star_count DESC, original_msg_id DESC
        '''
        return self.conn.execute(query, tuple(params)).fetchall()

    # --- Emoji alias methods ---

    def add_starboard_alias(self, guild_id, alias_emoji, main_emoji):
        """Add an alias emoji that counts toward a main emoji's starboard."""
        guild_id = str(guild_id)
        self.conn.execute(
            'INSERT OR REPLACE INTO starboard_alias (guild_id, alias_emoji, main_emoji) '
            'VALUES (?, ?, ?)',
            (guild_id, alias_emoji, main_emoji)
        )
        self.conn.commit()

    def remove_starboard_alias(self, guild_id, alias_emoji):
        """Remove an alias emoji and migrate its reactor rows to the main emoji.

        Reactors stored under the alias are re-inserted under the main emoji
        (INSERT OR IGNORE to skip duplicates), then the alias rows are deleted.
        Returns rowcount (0 or 1) for the alias deletion.
        """
        guild_id = str(guild_id)
        # Look up the main emoji before deleting the alias
        main_emoji = self.resolve_alias(guild_id, alias_emoji)
        if main_emoji is not None:
            table_columns = (
                ('starboard_reactors',
                 '(original_msg_id, emoji, user_id)',
                 'r.original_msg_id, ?, r.user_id'),
                ('starboard_proxy_reactors',
                 '(original_msg_id, emoji, user_id, via_starboard_msg_id)',
                 'r.original_msg_id, ?, r.user_id, r.via_starboard_msg_id'),
            )
            for table, columns, select_cols in table_columns:
                # Migrate alias reactors to main emoji, scoped to this guild's messages
                self.conn.execute(f'''
                    INSERT OR IGNORE INTO {table} {columns}
                    SELECT {select_cols}
                    FROM {table} r
                    WHERE r.emoji = ?
                      AND r.original_msg_id IN (
                          SELECT original_msg_id FROM starboard_message_v1 WHERE guild_id = ?
                      )
                ''', (main_emoji, alias_emoji, guild_id))
                # Delete alias reactor rows only for this guild's messages
                self.conn.execute(f'''
                    DELETE FROM {table}
                    WHERE emoji = ?
                      AND original_msg_id IN (
                          SELECT original_msg_id FROM starboard_message_v1 WHERE guild_id = ?
                      )
                ''', (alias_emoji, guild_id))
        rc = self.conn.execute(
            'DELETE FROM starboard_alias WHERE guild_id = ? AND alias_emoji = ?',
            (guild_id, alias_emoji)
        ).rowcount
        self.conn.commit()
        return rc

    def get_aliases_for_emoji(self, guild_id, main_emoji):
        """Get all alias emojis for a main emoji in a guild."""
        guild_id = str(guild_id)
        rows = self.conn.execute(
            'SELECT alias_emoji FROM starboard_alias WHERE guild_id = ? AND main_emoji = ?',
            (guild_id, main_emoji)
        ).fetchall()
        return [r.alias_emoji for r in rows]

    def resolve_alias(self, guild_id, emoji):
        """Resolve an alias emoji to its main emoji. Returns the main emoji or None if not an alias."""
        guild_id = str(guild_id)
        row = self.conn.execute(
            'SELECT main_emoji FROM starboard_alias WHERE guild_id = ? AND alias_emoji = ?',
            (guild_id, emoji)
        ).fetchone()
        return row.main_emoji if row else None

    def get_all_aliases_for_guild(self, guild_id):
        """Get all aliases for a guild. Returns list of (alias_emoji, main_emoji) rows."""
        guild_id = str(guild_id)
        return self.conn.execute(
            'SELECT alias_emoji, main_emoji FROM starboard_alias WHERE guild_id = ?',
            (guild_id,)
        ).fetchall()

    def get_emoji_family(self, guild_id, main_emoji):
        """Get the main emoji plus all its aliases as a list. Used for union counting."""
        return [main_emoji] + self.get_aliases_for_emoji(guild_id, main_emoji)

    # --- Per-user default emoji ---

    def get_user_starboard_default(self, guild_id, user_id):
        """Return the user's saved default emoji for this guild, or None."""
        row = self.conn.execute(
            'SELECT emoji FROM user_starboard_default WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row.emoji if row else None

    def set_user_starboard_default(self, guild_id, user_id, emoji):
        """Upsert the user's default emoji for ``;starboard`` leaderboard commands."""
        self.conn.execute(
            'INSERT OR REPLACE INTO user_starboard_default (guild_id, user_id, emoji) '
            'VALUES (?, ?, ?)',
            (str(guild_id), str(user_id), emoji)
        )
        self.conn.commit()

    def clear_user_starboard_default(self, guild_id, user_id):
        """Remove the user's saved default emoji. Returns rowcount (0 or 1)."""
        rc = self.conn.execute(
            'DELETE FROM user_starboard_default WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).rowcount
        self.conn.commit()
        return rc

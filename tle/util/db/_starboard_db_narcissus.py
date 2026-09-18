"""Sticky narcissus (self-star) tracking.

The old narcissus leaderboard counted live self-reactions, so un-reacting
erased the self-star.  ``starboard_narcissus`` instead records a permanent
mark the moment an author has a live self-reaction (physical or via a
starboard post) while their message is on the starboard — whether the
react made it reach the board or landed while it was already there.
Marks are unique per (message, main emoji) and survive un-reacting;
re-react/un-react cycles never add a second mark.  The leaderboard shows,
per user, the mark count plus how many marks no longer have a live
self-reaction backing them (the "unreacted" column).
"""
from tle.util.db._starboard_db_constants import _NO_TIME_BOUND


class StarboardNarcissusDbMixin:
    """Sticky self-star marks. Expects ``self.conn`` plus the starboard
    message/reactor/alias tables from the sibling mixins."""

    def _create_narcissus_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_narcissus (
                guild_id        TEXT,
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')

    def record_narcissus_mark(self, guild_id, original_msg_id, emoji, user_id):
        """Permanently record a self-star. INSERT OR IGNORE (idempotent)."""
        self.conn.execute(
            'INSERT OR IGNORE INTO starboard_narcissus '
            '(guild_id, original_msg_id, emoji, user_id) VALUES (?, ?, ?, ?)',
            (str(guild_id), str(original_msg_id), emoji, str(user_id))
        )
        self.conn.commit()

    def _maybe_record_narcissus(self, original_msg_id, emoji):
        """Record a sticky mark if the author currently self-reacts.

        Called from every count update of a tracked message, so it fires on
        the two orderings that matter: the self-react was live when the
        message reached the board, or it landed while the message was already
        there.  Never removes anything — un-reacting is surfaced by the
        leaderboard's ``unreacted`` column instead."""
        row = self.conn.execute(
            'SELECT guild_id, author_id FROM starboard_message_v1 '
            'WHERE original_msg_id = ? AND emoji = ?',
            (str(original_msg_id), emoji)).fetchone()
        if row is None or not row.author_id or row.author_id == '__UNKNOWN__':
            return
        family = self.get_emoji_family(row.guild_id, emoji)
        placeholders = ','.join('?' * len(family))
        live = self.conn.execute(
            f'SELECT 1 FROM ('
            f'SELECT original_msg_id, emoji, user_id FROM starboard_reactors '
            f'UNION ALL '
            f'SELECT original_msg_id, emoji, user_id FROM starboard_proxy_reactors'
            f') WHERE original_msg_id = ? AND user_id = ? '
            f'AND emoji IN ({placeholders}) LIMIT 1',
            (str(original_msg_id), row.author_id, *family)).fetchone()
        if live is None:
            return
        self.record_narcissus_mark(
            row.guild_id, original_msg_id, emoji, row.author_id)

    def seed_narcissus_marks(self, guild_id=None):
        """Record marks for every author with a live self-reaction on an
        already-tracked message, optionally scoped to one guild.

        Same statement as migration 1.52.0's seed.  Needed by ``;migrate
        complete``, whose raw-SQL bulk import bypasses the per-update hook
        and runs long after the startup seed."""
        guild_clause = 'm.guild_id = ? AND' if guild_id is not None else ''
        params = (str(guild_id),) if guild_id is not None else ()
        self.conn.execute(f'''
            INSERT OR IGNORE INTO starboard_narcissus
                (guild_id, original_msg_id, emoji, user_id)
            SELECT m.guild_id, m.original_msg_id, m.emoji, m.author_id
            FROM starboard_message_v1 m
            JOIN (
                SELECT original_msg_id, emoji, user_id FROM starboard_reactors
                UNION
                SELECT original_msg_id, emoji, user_id FROM starboard_proxy_reactors
            ) r ON r.original_msg_id = m.original_msg_id
               AND r.user_id = m.author_id
            WHERE {guild_clause} m.author_id IS NOT NULL
              AND m.author_id != '__UNKNOWN__'
              AND (r.emoji = m.emoji OR EXISTS (
                  SELECT 1 FROM starboard_alias a
                  WHERE a.guild_id = m.guild_id
                    AND a.alias_emoji = r.emoji
                    AND a.main_emoji = m.emoji))
        ''', params)
        self.conn.commit()

    def get_narcissus_leaderboard(self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND,
                                  emoji_family=None):
        """Sticky self-star leaderboard.

        Counts messages the author self-reacted at any point while they were
        on the starboard.  ``unreacted`` is how many of those marks have no
        live self-reaction left (on either surface); re-reacting makes the
        mark live again without adding a second one.
        """
        guild_id = str(guild_id)
        if emoji_family is None:
            emoji_family = [emoji]
        time_clauses, time_params = self._snowflake_time_filter(
            'n.original_msg_id', dlo, dhi)
        extra = (' AND ' + ' AND '.join(time_clauses)) if time_clauses else ''
        placeholders = ','.join('?' * len(emoji_family))
        query = f'''
            SELECT n.user_id,
                   COUNT(*) as self_stars,
                   SUM(CASE WHEN live.user_id IS NULL THEN 1 ELSE 0 END)
                       as unreacted
            FROM starboard_narcissus n
            LEFT JOIN (
                SELECT DISTINCT original_msg_id, user_id
                FROM (
                    SELECT original_msg_id, emoji, user_id FROM starboard_reactors
                    UNION ALL
                    SELECT original_msg_id, emoji, user_id FROM starboard_proxy_reactors
                )
                WHERE emoji IN ({placeholders})
            ) live ON live.original_msg_id = n.original_msg_id
                  AND live.user_id = n.user_id
            WHERE n.guild_id = ? AND n.emoji = ?
                AND n.user_id != '__UNKNOWN__'
                {extra}
            GROUP BY n.user_id
            ORDER BY self_stars DESC
        '''
        return self.conn.execute(
            query,
            tuple(emoji_family) + (guild_id, emoji) + tuple(time_params)
        ).fetchall()

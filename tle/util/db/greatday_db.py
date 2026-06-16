"""Great Day DB methods — extracted from user_db_conn.py.

Owns the ``greatday_signup`` and ``greatday_pick`` tables. The
``greatday_ban`` table is created by a migration only (matching the original
fresh-DB behavior, which did not create it in ``create_tables``).
"""
import logging

logger = logging.getLogger(__name__)


class GreatdayDbMixin:
    """Mixin providing Great Day DB methods."""

    def _create_greatday_tables(self):
        # Great Day signups
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS greatday_signup (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        # Great Day pick history (one row per (guild, user, message))
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS greatday_pick (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                message_id  TEXT NOT NULL,
                picked_at   REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id, message_id)
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_greatday_pick_user
                ON greatday_pick (guild_id, user_id)
        ''')

    def greatday_signup(self, guild_id, user_id):
        """Add a user to the great day list. Returns True if newly added."""
        rc = self.conn.execute(
            'INSERT OR IGNORE INTO greatday_signup (guild_id, user_id) VALUES (?, ?)',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_remove(self, guild_id, user_id):
        """Remove a user from the great day list. Returns True if removed."""
        rc = self.conn.execute(
            'DELETE FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_get_signups(self, guild_id):
        """Return all signed-up user IDs for a guild."""
        return self.conn.execute(
            'SELECT user_id FROM greatday_signup WHERE guild_id = ?',
            (str(guild_id),)).fetchall()

    def greatday_ban(self, guild_id, user_id):
        """Ban a user from great day. Also removes their signup. Returns True if newly banned."""
        rc = self.conn.execute(
            'INSERT OR IGNORE INTO greatday_ban (guild_id, user_id) VALUES (?, ?)',
            (str(guild_id), str(user_id))).rowcount
        self.conn.execute(
            'DELETE FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id)))
        self.conn.commit()
        return rc > 0

    def greatday_unban(self, guild_id, user_id):
        """Unban a user from great day. Returns True if was banned."""
        rc = self.conn.execute(
            'DELETE FROM greatday_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_record_picks(self, guild_id, user_ids, message_id, picked_at):
        """Insert one row per picked user. Idempotent on (guild, user, message)."""
        if not user_ids:
            return 0
        guild_id = str(guild_id)
        message_id = str(message_id)
        cur = self.conn.executemany(
            'INSERT OR IGNORE INTO greatday_pick '
            '(guild_id, user_id, message_id, picked_at) VALUES (?, ?, ?, ?)',
            [(guild_id, str(uid), message_id, picked_at) for uid in user_ids]
        )
        self.conn.commit()
        return cur.rowcount

    def greatday_get_stats(self, guild_id):
        """Return [(user_id, count)] for all users picked in the guild, most-first."""
        return self.conn.execute(
            'SELECT user_id, COUNT(*) AS cnt FROM greatday_pick '
            'WHERE guild_id = ? GROUP BY user_id ORDER BY cnt DESC, user_id ASC',
            (str(guild_id),)
        ).fetchall()

    def greatday_get_count(self, guild_id, user_id):
        """Return how many times a user has been picked in the guild."""
        row = self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM greatday_pick '
            'WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row.cnt

    def greatday_is_banned(self, guild_id, user_id):
        """Check if a user is banned from great day."""
        row = self.conn.execute(
            'SELECT 1 FROM greatday_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).fetchone()
        return row is not None

    def greatday_get_banned(self, guild_id):
        """Return all banned user_ids for the guild."""
        return self.conn.execute(
            'SELECT user_id FROM greatday_ban WHERE guild_id = ? '
            'ORDER BY rowid ASC',
            (str(guild_id),)
        ).fetchall()

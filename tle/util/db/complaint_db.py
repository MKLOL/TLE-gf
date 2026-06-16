"""Complaint DB methods — extracted from user_db_conn.py.

Owns the ``complaint`` table. The ``active`` column is added by a migration
(it is not part of the fresh-DB CREATE here, matching the original behavior).
"""
import logging
import time

logger = logging.getLogger(__name__)


class ComplaintDbMixin:
    """Mixin providing complaint DB methods."""

    def _create_complaint_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS complaint (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id     TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                text         TEXT NOT NULL,
                created_at   REAL NOT NULL,
                message_link TEXT
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_complaint_guild
                ON complaint (guild_id, created_at DESC)
        ''')

    def add_complaint(self, guild_id, user_id, text, message_link=None):
        """Insert a complaint and return its id."""
        guild_id, user_id = str(guild_id), str(user_id)
        cur = self.conn.execute(
            'INSERT INTO complaint (guild_id, user_id, text, created_at, message_link) '
            'VALUES (?, ?, ?, ?, ?)',
            (guild_id, user_id, text, time.time(), message_link)
        )
        self.conn.commit()
        return cur.lastrowid

    def get_complaints(self, guild_id):
        """Return all active complaints for a guild, newest first."""
        guild_id = str(guild_id)
        return self.conn.execute(
            'SELECT id, guild_id, user_id, text, created_at, message_link '
            'FROM complaint WHERE guild_id = ? AND active = 1 ORDER BY created_at DESC',
            (guild_id,)
        ).fetchall()

    def get_complaint(self, complaint_id):
        """Return a single active complaint by id, or None."""
        row = self.conn.execute(
            'SELECT id, guild_id, user_id, text, created_at, message_link '
            'FROM complaint WHERE id = ? AND active = 1',
            (complaint_id,)
        ).fetchone()
        return row

    def delete_complaint(self, complaint_id):
        """Soft-delete a complaint by id. Returns True if a row was deactivated."""
        cur = self.conn.execute(
            'UPDATE complaint SET active = 0 WHERE id = ? AND active = 1',
            (complaint_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_complaints(self, complaint_ids, guild_id):
        """Soft-delete multiple complaints by id, scoped to a guild.

        Returns the number of rows deactivated.
        """
        if not complaint_ids:
            return 0
        guild_id = str(guild_id)
        placeholders = ','.join('?' for _ in complaint_ids)
        cur = self.conn.execute(
            f'UPDATE complaint SET active = 0 '
            f'WHERE id IN ({placeholders}) AND guild_id = ? AND active = 1',
            [*complaint_ids, guild_id]
        )
        self.conn.commit()
        return cur.rowcount

    def count_recent_complaints(self, guild_id, user_id, since):
        """Count complaints by a user in a guild filed since a timestamp.

        Includes soft-deleted (withdrawn/removed) complaints so that the
        rate limit cannot be bypassed by withdrawing and immediately
        refiling. The rate limit caps *filings*, not active complaints.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        row = self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM complaint '
            'WHERE guild_id = ? AND user_id = ? AND created_at >= ?',
            (guild_id, user_id, since)
        ).fetchone()
        return row.cnt

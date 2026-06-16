"""Rating-weighted poll DB methods — extracted from user_db_conn.py.

Owns the ``rpoll``, ``rpoll_option`` and ``rpoll_vote`` tables.
``namedtuple_factory`` is imported lazily from the composing module to avoid
an import cycle.
"""
import logging
import time

logger = logging.getLogger(__name__)


class RpollDbMixin:
    """Mixin providing rating-weighted poll DB methods."""

    def _create_rpoll_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll (
                poll_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                message_id  TEXT,
                question    TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  REAL NOT NULL,
                anonymous   INTEGER NOT NULL DEFAULT 0,
                expires_at  REAL NOT NULL DEFAULT 0,
                closed      INTEGER NOT NULL DEFAULT 0,
                formula     TEXT NOT NULL DEFAULT 'sum'
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll_option (
                poll_id       INTEGER NOT NULL,
                option_index  INTEGER NOT NULL,
                label         TEXT NOT NULL,
                PRIMARY KEY (poll_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll_vote (
                poll_id       INTEGER NOT NULL,
                user_id       TEXT NOT NULL,
                option_index  INTEGER NOT NULL,
                rating        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (poll_id, user_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')

    def create_rpoll(self, guild_id, channel_id, question, options, created_by, created_at,
                     anonymous=False, expires_at=None, formula='sum'):
        """Create a poll and its options. Returns the poll_id."""
        if expires_at is None:
            expires_at = created_at + 86400  # Default 24h
        query = ('INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at, anonymous, expires_at, formula) '
                 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        cur = self.conn.execute(query, (str(guild_id), str(channel_id), question,
                                        str(created_by), created_at, int(anonymous), expires_at, formula))
        poll_id = cur.lastrowid
        for i, label in enumerate(options):
            self.conn.execute(
                'INSERT INTO rpoll_option (poll_id, option_index, label) VALUES (?, ?, ?)',
                (poll_id, i, label)
            )
        self.conn.commit()
        return poll_id

    def set_rpoll_message_id(self, poll_id, message_id):
        """Set the Discord message_id after the poll message is sent."""
        with self.conn:
            self.conn.execute(
                'UPDATE rpoll SET message_id = ? WHERE poll_id = ?',
                (str(message_id), poll_id)
            )

    def get_rpoll(self, poll_id):
        """Get a poll by ID. Returns namedtuple or None."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchone(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_by_message_id(self, message_id):
        """Get a poll by its Discord message_id."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchone(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE message_id = ?',
            params=(str(message_id),), row_factory=namedtuple_factory
        )

    def get_active_rpolls_in_channel(self, channel_id, now):
        """Active (open, not yet expired) polls created in this channel."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE channel_id = ? AND closed = 0 AND expires_at > ? '
            'ORDER BY expires_at ASC',
            params=(str(channel_id), now), row_factory=namedtuple_factory
        )

    def get_rpoll_options(self, poll_id):
        """Get all options for a poll, ordered by index."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT poll_id, option_index, label FROM rpoll_option '
            'WHERE poll_id = ? ORDER BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def toggle_rpoll_vote(self, poll_id, user_id, option_index, rating):
        """Toggle a vote. Returns True if vote was added, False if removed."""
        user_id = str(user_id)
        existing = self.conn.execute(
            'SELECT 1 FROM rpoll_vote WHERE poll_id = ? AND user_id = ? AND option_index = ?',
            (poll_id, user_id, option_index)
        ).fetchone()
        if existing:
            self.conn.execute(
                'DELETE FROM rpoll_vote WHERE poll_id = ? AND user_id = ? AND option_index = ?',
                (poll_id, user_id, option_index)
            )
            self.conn.commit()
            return False
        else:
            self.conn.execute(
                'INSERT INTO rpoll_vote (poll_id, user_id, option_index, rating) VALUES (?, ?, ?, ?)',
                (poll_id, user_id, option_index, rating)
            )
            self.conn.commit()
            return True

    def get_rpoll_totals(self, poll_id):
        """Get the sum of ratings per option. Returns list of (option_index, total_rating)."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT option_index, COALESCE(SUM(rating), 0) AS total_rating '
            'FROM rpoll_vote WHERE poll_id = ? GROUP BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_voters(self, poll_id):
        """Get all voters grouped by option. Returns list of (option_index, user_id) rows."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT option_index, user_id FROM rpoll_vote WHERE poll_id = ? '
            'ORDER BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_vote_count(self, poll_id):
        """Get total number of distinct voters for a poll."""
        row = self._fetchone(
            'SELECT COUNT(DISTINCT user_id) AS cnt FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,)
        )
        return row[0] if row else 0

    def get_rpoll_user_rating(self, user_id, guild_id):
        """Get a user's Codeforces rating for poll voting. Returns 0 if not linked."""
        handle = self.get_handle(user_id, guild_id)
        if handle is None:
            return 0
        user = self.fetch_cf_user(handle)
        if user is None or user.rating is None:
            return 0
        return user.rating

    def get_all_active_rpolls(self):
        """Get all open polls that have a message_id (i.e., were successfully posted)."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE message_id IS NOT NULL AND closed = 0',
            row_factory=namedtuple_factory
        )

    def close_rpoll(self, poll_id):
        """Mark a poll as closed."""
        with self.conn:
            self.conn.execute('UPDATE rpoll SET closed = 1 WHERE poll_id = ?', (poll_id,))

    def get_expired_unclosed_rpolls(self):
        """Get polls that have expired but haven't been closed yet."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE closed = 0 AND expires_at <= ? AND message_id IS NOT NULL',
            params=(time.time(),), row_factory=namedtuple_factory
        )

    def get_rpoll_vote_ratings(self, poll_id):
        """Get individual vote ratings for formula-based scoring."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT option_index, rating FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_voter_ids(self, poll_id):
        """Get distinct user_ids who have voted on a poll."""
        from tle.util.db.user_db_conn import namedtuple_factory
        return self._fetchall(
            'SELECT DISTINCT user_id FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def update_rpoll_voter_rating(self, poll_id, user_id, rating):
        """Update the stored rating for all of a user's votes in a poll."""
        with self.conn:
            self.conn.execute(
                'UPDATE rpoll_vote SET rating = ? WHERE poll_id = ? AND user_id = ?',
                (rating, poll_id, str(user_id))
            )

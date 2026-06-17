"""Rated VC + CFVC cache DB methods — extracted from user_db_conn.py.

Owns the ``rated_vcs``, ``rated_vc_users``, ``rated_vc_settings`` and
``cfvc_cache`` tables. ``RatedVC``, ``_DEFAULT_VC_RATING`` and
``namedtuple_factory`` are imported lazily from the composing module to avoid
an import cycle.
"""
import logging

logger = logging.getLogger(__name__)


class VcDbMixin:
    """Mixin providing rated-VC and CFVC-cache DB methods."""

    def _create_vc_tables(self):
        # Rated VCs stuff:
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vcs" (
                "id"	         INTEGER PRIMARY KEY AUTOINCREMENT,
                "contest_id"     INTEGER NOT NULL,
                "start_time"     REAL,
                "finish_time"    REAL,
                "status"         INTEGER,
                "guild_id"       TEXT
            )
        ''')

        # TODO: Do we need to explicitly specify the fk constraint or just depend on the middleware?
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vc_users" (
                "vc_id"	         INTEGER,
                "user_id"        TEXT NOT NULL,
                "rating"         INTEGER,

                CONSTRAINT fk_vc
                    FOREIGN KEY (vc_id)
                    REFERENCES rated_vcs(id),

                PRIMARY KEY(vc_id, user_id)
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rated_vc_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cfvc_cache (
                handle       TEXT NOT NULL,
                contest_id   INTEGER NOT NULL,
                rank         INTEGER NOT NULL,
                PRIMARY KEY (handle, contest_id)
            )
        ''')

    def create_rated_vc(self, contest_id: int, start_time: float, finish_time: float, guild_id: str, user_ids: [str]):
        """ Creates a rated vc and returns its id.
        """
        from tle.util.db.user_db_conn import RatedVC
        query = ('INSERT INTO rated_vcs '
                 '(contest_id, start_time, finish_time, status, guild_id) '
                 'VALUES ( ?, ?, ?, ?, ?)')
        id = None
        with self.conn:
            id = self.conn.execute(query, (contest_id, start_time, finish_time, RatedVC.ONGOING, guild_id)).lastrowid
            for user_id in user_ids:
                query = ('INSERT INTO rated_vc_users '
                         '(vc_id, user_id) '
                         'VALUES (? , ?)')
                self.conn.execute(query, (id, user_id))
        return id

    def get_rated_vc(self, vc_id: int):
        from tle.util.db.user_db_conn import namedtuple_factory
        query = ('SELECT * '
                'FROM rated_vcs '
                'WHERE id = ? ')
        vc = self._fetchone(query, params=(vc_id,), row_factory=namedtuple_factory)
        return vc

    def get_ongoing_rated_vc_ids(self):
        from tle.util.db.user_db_conn import RatedVC, namedtuple_factory
        query = ('SELECT id '
                 'FROM rated_vcs '
                 'WHERE status = ? '
                 )
        vcs = self._fetchall(query, params=(RatedVC.ONGOING,), row_factory=namedtuple_factory)
        vc_ids = [vc.id for vc in vcs]
        return vc_ids

    def get_rated_vc_user_ids(self, vc_id: int):
        from tle.util.db.user_db_conn import namedtuple_factory
        query = ('SELECT user_id '
                 'FROM rated_vc_users '
                 'WHERE vc_id = ? '
                 )
        users = self._fetchall(query, params=(vc_id,), row_factory=namedtuple_factory)
        user_ids = [user.user_id for user in users]
        return user_ids

    def finish_rated_vc(self, vc_id: int):
        from tle.util.db.user_db_conn import RatedVC
        query = ('UPDATE rated_vcs '
                'SET status = ? '
                'WHERE id = ? ')

        with self.conn:
            self.conn.execute(query, (RatedVC.FINISHED, vc_id))

    def update_vc_rating(self, vc_id: int, user_id: str, rating: int):
        query = ('INSERT OR REPLACE INTO rated_vc_users '
                 '(vc_id, user_id, rating) '
                 'VALUES (?, ?, ?) ')

        with self.conn:
            self.conn.execute(query, (vc_id, user_id, rating))

    def get_vc_rating(self, user_id: str, default_if_not_exist: bool = True):
        from tle.util.db.user_db_conn import _DEFAULT_VC_RATING, namedtuple_factory
        query = ('SELECT MAX(vc_id) AS latest_vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        rating = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).rating
        if rating is None:
            if default_if_not_exist:
                return _DEFAULT_VC_RATING
            return None
        return rating

    def get_vc_rating_history(self, user_id: str):
        """ Return [vc_id, rating].
        """
        from tle.util.db.user_db_conn import namedtuple_factory
        query = ('SELECT vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        ratings = self._fetchall(query, params=(user_id,), row_factory=namedtuple_factory)
        return ratings

    def set_rated_vc_channel(self, guild_id, channel_id):
        self._set_channel_setting('rated_vc_settings', guild_id, channel_id)

    def get_rated_vc_channel(self, guild_id):
        return self._get_channel_setting('rated_vc_settings', guild_id)

    def remove_last_ratedvc_participation(self, user_id: str):
        from tle.util.db.user_db_conn import namedtuple_factory
        query = ('SELECT MAX(vc_id) AS vc_id '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? '
                 )
        vc_id = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).vc_id
        query = ('DELETE FROM rated_vc_users '
                 'WHERE user_id = ? AND vc_id = ? ')
        with self.conn:
            return self.conn.execute(query, (user_id, vc_id)).rowcount

    # CFVC cache methods

    def get_cfvc_cache(self, handle):
        """Return cached CF virtual contest ranks for a handle.
        Returns list of (contest_id, rank) sorted by contest_id.
        """
        query = ('SELECT contest_id, rank FROM cfvc_cache '
                 'WHERE handle = ? ORDER BY contest_id')
        return self.conn.execute(query, (handle.lower(),)).fetchall()

    def save_cfvc_cache(self, handle, entries):
        """Save CF virtual contest ranks. entries: list of (contest_id, rank)."""
        with self.conn:
            for contest_id, rank in entries:
                self.conn.execute(
                    'INSERT OR REPLACE INTO cfvc_cache (handle, contest_id, rank) '
                    'VALUES (?, ?, ?)',
                    (handle.lower(), contest_id, rank))

    def get_cfvc_cached_contest_ids(self, handle):
        """Return set of contest_ids already cached for a handle."""
        query = 'SELECT contest_id FROM cfvc_cache WHERE handle = ?'
        rows = self.conn.execute(query, (handle.lower(),)).fetchall()
        return {row[0] for row in rows}

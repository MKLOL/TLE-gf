"""Great Day DB methods — extracted from user_db_conn.py.

Owns the ``greatday_signup``, ``greatday_pick``, ``greatday_ban`` and
``greatday_signup_event`` tables.

``greatday_signup`` only holds current membership, so joins and leaves are
logged separately in ``greatday_signup_event``. Each event is keyed by the
Discord message that caused it, which makes both live recording and history
backfill idempotent.
"""
import logging

SIGNUP_ACTIONS = ('signup', 'signout')
SIGNUP_HISTORY_AUDIT_KEY = 'greatday_signup_history_audit'

logger = logging.getLogger(__name__)


def create_greatday_signup_event_table(db):
    """Create the signup/signout event log. Shared with migration 1.56.0."""
    db.execute('''
        CREATE TABLE IF NOT EXISTS greatday_signup_event (
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            action      TEXT NOT NULL,
            at          REAL NOT NULL,
            message_id  TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id, message_id)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_greatday_signup_event_user
            ON greatday_signup_event (guild_id, user_id, at)
    ''')


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
        # Also created by migration 1.21.0, which fresh databases skip.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS greatday_ban (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        create_greatday_signup_event_table(self.conn)

    def greatday_signup(self, guild_id, user_id):
        """Add a user to the great day list. Returns True if newly added."""
        rc = self.conn.execute(
            'INSERT OR IGNORE INTO greatday_signup (guild_id, user_id) VALUES (?, ?)',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_signup_with_event(self, guild_id, user_id, at, message_id):
        """Atomically add a signup and its history event."""
        guild_id, user_id = str(guild_id), str(user_id)
        with self.conn:
            rc = self.conn.execute(
                'INSERT OR IGNORE INTO greatday_signup '
                '(guild_id, user_id) VALUES (?, ?)',
                (guild_id, user_id)).rowcount
            if rc:
                self._greatday_insert_signup_event(
                    guild_id, user_id, 'signup', at, message_id)
        return rc > 0

    def greatday_remove(self, guild_id, user_id):
        """Remove a user from the great day list. Returns True if removed."""
        rc = self.conn.execute(
            'DELETE FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_remove_with_event(self, guild_id, user_id, at, message_id):
        """Atomically remove a signup and record its signout."""
        guild_id, user_id = str(guild_id), str(user_id)
        with self.conn:
            rc = self.conn.execute(
                'DELETE FROM greatday_signup '
                'WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)).rowcount
            if rc:
                self._greatday_insert_signup_event(
                    guild_id, user_id, 'signout', at, message_id)
        return rc > 0

    def greatday_get_signups(self, guild_id):
        """Return all signed-up user IDs for a guild."""
        return self.conn.execute(
            'SELECT user_id FROM greatday_signup WHERE guild_id = ?',
            (str(guild_id),)).fetchall()

    def greatday_is_signed_up(self, guild_id, user_id):
        """Check if a user is currently on the great day list."""
        row = self.conn.execute(
            'SELECT 1 FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).fetchone()
        return row is not None

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

    def greatday_ban_with_event(self, guild_id, user_id, at, message_id):
        """Atomically ban a user, remove their signup, and log that removal."""
        guild_id, user_id = str(guild_id), str(user_id)
        with self.conn:
            banned = self.conn.execute(
                'INSERT OR IGNORE INTO greatday_ban '
                '(guild_id, user_id) VALUES (?, ?)',
                (guild_id, user_id)).rowcount
            removed = self.conn.execute(
                'DELETE FROM greatday_signup '
                'WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)).rowcount
            if removed:
                self._greatday_insert_signup_event(
                    guild_id, user_id, 'signout', at, message_id)
        return banned > 0

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

    def greatday_get_latest_pick(self, guild_id, user_id):
        """Return a user's newest recorded pick in a guild, or ``None``."""
        return self.conn.execute(
            'SELECT message_id, picked_at FROM greatday_pick '
            'WHERE guild_id = ? AND user_id = ? '
            'ORDER BY picked_at DESC, CAST(message_id AS INTEGER) DESC, '
            'message_id DESC LIMIT 1',
            (str(guild_id), str(user_id))
        ).fetchone()

    def greatday_get_pick_history(self, guild_id, user_id):
        """Return all of a user's recorded picks, newest first."""
        return self.conn.execute(
            'SELECT message_id, picked_at FROM greatday_pick '
            'WHERE guild_id = ? AND user_id = ? '
            'ORDER BY picked_at DESC, CAST(message_id AS INTEGER) DESC, '
            'message_id DESC',
            (str(guild_id), str(user_id))
        ).fetchall()

    def greatday_get_post_times(self, guild_id):
        """Return the timestamp of every recorded Great Day post, oldest first.

        Picks are stored per user, so a post with several picked users
        collapses to one timestamp.
        """
        return [row.picked_at for row in self.conn.execute(
            'SELECT MIN(picked_at) AS picked_at FROM greatday_pick '
            'WHERE guild_id = ? GROUP BY message_id ORDER BY picked_at ASC',
            (str(guild_id),)
        ).fetchall()]

    def greatday_record_signup_events(self, events):
        """Insert ``(guild_id, user_id, action, at, message_id)`` rows.

        Idempotent on (guild, user, message), so re-running a backfill over
        the same channel history inserts nothing. Returns the number of new
        rows.
        """
        events = list(events)
        if not events:
            return 0
        for _, _, action, _, _ in events:
            if action not in SIGNUP_ACTIONS:
                raise ValueError(f'Unknown signup event action: {action!r}')
        cur = self.conn.executemany(
            'INSERT OR IGNORE INTO greatday_signup_event '
            '(guild_id, user_id, action, at, message_id) VALUES (?, ?, ?, ?, ?)',
            [(str(guild_id), str(user_id), action, at, str(message_id))
             for guild_id, user_id, action, at, message_id in events]
        )
        self.conn.commit()
        return cur.rowcount

    def greatday_record_signup_backfill(self, events, guild_id, audit_status):
        """Atomically store inferred events and the latest audit status."""
        events = list(events)
        guild_id = str(guild_id)
        if not audit_status.startswith(('clean:', 'incomplete:', 'partial:')):
            raise ValueError(f'Unknown signup audit status: {audit_status!r}')
        for event_guild_id, _, action, _, _ in events:
            if str(event_guild_id) != guild_id:
                raise ValueError('Backfill events must belong to the audit guild')
            if action not in SIGNUP_ACTIONS:
                raise ValueError(f'Unknown signup event action: {action!r}')
        params = [
            (str(event_guild_id), str(user_id), action, at, str(message_id))
            for event_guild_id, user_id, action, at, message_id in events
        ]
        with self.conn:
            previous = self.conn.execute(
                'SELECT value FROM guild_config '
                'WHERE guild_id = ? AND key = ?',
                (guild_id, SIGNUP_HISTORY_AUDIT_KEY)).fetchone()
            inserted = self.conn.executemany(
                'INSERT OR IGNORE INTO greatday_signup_event '
                '(guild_id, user_id, action, at, message_id) '
                'VALUES (?, ?, ?, ?, ?)', params).rowcount
            if previous is not None and previous.value.startswith('incomplete:'):
                # Once uncertain inferred rows exist, a later scan cannot prove
                # that those append-only rows were correct.
                audit_status = previous.value
            elif (previous is not None
                  and previous.value.startswith('clean:')
                  and audit_status.startswith('partial:')
                  and inserted == 0):
                # A no-op targeted rescan does not invalidate a completed
                # guild-wide audit. A targeted scan that finds new rows does.
                audit_status = previous.value
            self.conn.execute(
                'INSERT INTO guild_config (guild_id, key, value) '
                'VALUES (?, ?, ?) '
                'ON CONFLICT(guild_id, key) DO UPDATE SET value = excluded.value',
                (guild_id, SIGNUP_HISTORY_AUDIT_KEY, audit_status))
        return inserted

    def _greatday_insert_signup_event(self, guild_id, user_id, action, at,
                                      message_id):
        """Insert one required event inside the caller's transaction."""
        if action not in SIGNUP_ACTIONS:
            raise ValueError(f'Unknown signup event action: {action!r}')
        self.conn.execute(
            'INSERT INTO greatday_signup_event '
            '(guild_id, user_id, action, at, message_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (str(guild_id), str(user_id), action, float(at), str(message_id)))

    def greatday_record_signup_event(self, guild_id, user_id, action, at,
                                     message_id):
        """Record a single signup/signout event. Returns True if new."""
        return self.greatday_record_signup_events(
            [(guild_id, user_id, action, at, message_id)]) > 0

    def greatday_get_signup_events(self, guild_id, user_id):
        """Return a user's signup/signout events, newest first."""
        return self.conn.execute(
            'SELECT action, at, message_id FROM greatday_signup_event '
            'WHERE guild_id = ? AND user_id = ? '
            'ORDER BY at DESC, CAST(message_id AS INTEGER) DESC, '
            'message_id DESC',
            (str(guild_id), str(user_id))
        ).fetchall()

    def greatday_get_last_signup(self, guild_id, user_id):
        """Return a user's newest recorded signup event, or ``None``.

        Signups predating the event log are unknown rather than absent, so a
        caller must not read ``None`` as 'never signed up'.
        """
        return self.conn.execute(
            "SELECT action, at, message_id FROM greatday_signup_event "
            "WHERE guild_id = ? AND user_id = ? AND action = 'signup' "
            'ORDER BY at DESC, CAST(message_id AS INTEGER) DESC, '
            'message_id DESC LIMIT 1',
            (str(guild_id), str(user_id))
        ).fetchone()

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

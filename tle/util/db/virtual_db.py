"""``;virtual`` sessions — a blind random contest whose solves earn gitgud points.

Owns ``virtual_session``. Credited solves are written into the existing
``challenge`` / ``user_challenge`` tables as completed challenges, so the
gitgud leaderboards and ``;gitlog`` pick them up with no special casing.
"""
import json
import logging
import sqlite3

logger = logging.getLogger(__name__)

ACTIVE = 'active'
CLAIMED = 'claimed'


def create_virtual_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS virtual_session (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id     TEXT NOT NULL,
        user_id      TEXT NOT NULL,
        handle       TEXT NOT NULL,
        contest_id   INTEGER NOT NULL,
        contest_name TEXT NOT NULL,
        confirmed_at REAL NOT NULL,
        expires_at   REAL NOT NULL,
        status       TEXT NOT NULL,
        base_rating  INTEGER NOT NULL DEFAULT 0,
        points       INTEGER NOT NULL DEFAULT 0,
        credited     TEXT NOT NULL DEFAULT '[]'
    )''')
    conn.execute('''CREATE INDEX IF NOT EXISTS idx_virtual_session_user
        ON virtual_session (user_id, status)''')
    # Points are global per user, so the one-active-session rule is too, and
    # it is enforced by the database rather than by a check-then-insert:
    # two confirmations racing each other cannot both succeed.
    conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_virtual_session_active
        ON virtual_session (user_id) WHERE status = 'active\'''')


class VirtualDbMixin:
    """Mixin providing ;virtual session methods. Expects ``self.conn`` and
    the challenge tables from ``ChallengeDbMixin``."""

    def _create_virtual_tables(self):
        create_virtual_schema(self.conn)

    def get_active_virtual_session(self, guild_id, user_id):
        """The user's active session, whichever guild it was started in.

        Gitgud points are global per user, so a second guild must see — and
        be blocked by — a session opened in the first. ``guild_id`` is kept
        for the call sites; it is not part of the lookup.
        """
        return self.conn.execute(
            'SELECT * FROM virtual_session WHERE user_id = ? AND status = ? '
            'ORDER BY id DESC LIMIT 1', (str(user_id), ACTIVE)).fetchone()

    def start_virtual_session(self, guild_id, user_id, handle, contest_id,
                              contest_name, confirmed_at, expires_at,
                              base_rating):
        """Open a session; returns its id, or None if one is already active.

        Uniqueness is the partial index's job (see ``create_virtual_schema``),
        so a lost race surfaces as IntegrityError here rather than as two
        active sessions.
        """
        try:
            with self.conn:
                cur = self.conn.execute(
                    'INSERT INTO virtual_session (guild_id, user_id, handle, '
                    'contest_id, contest_name, confirmed_at, expires_at, status, '
                    'base_rating) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (str(guild_id), str(user_id), handle, contest_id,
                     contest_name, confirmed_at, expires_at, ACTIVE, base_rating))
                return cur.lastrowid
        except sqlite3.IntegrityError:
            return None

    def credited_virtual_problems(self, session_id):
        row = self.conn.execute(
            'SELECT credited FROM virtual_session WHERE id = ?',
            (session_id,)).fetchone()
        return set(json.loads(row[0])) if row else set()

    def credit_virtual_solve(self, session_id, user_id, problem, delta,
                             points, finish_time):
        """Record one solved problem as a completed gitgud challenge.

        Returns False if that problem was already credited for the session.
        The challenge row, the score bump and the session bookkeeping are one
        transaction, so a crash cannot leave points without a log line or a
        credited index without points.
        """
        from tle.util.db.user_db_conn import Gitgud
        user_id = str(user_id)
        with self.conn:
            row = self.conn.execute(
                'SELECT confirmed_at, credited FROM virtual_session '
                'WHERE id = ? AND user_id = ? AND status = ?',
                (session_id, user_id, ACTIVE)).fetchone()
            if row is None:
                return False
            confirmed_at, credited = row[0], json.loads(row[1])
            if problem.index in credited:
                return False
            credited.append(problem.index)
            self.conn.execute(
                'INSERT INTO challenge (user_id, issue_time, finish_time, '
                'problem_name, contest_id, p_index, rating_delta, status) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (user_id, confirmed_at, finish_time, problem.name,
                 problem.contestId, problem.index, delta, int(Gitgud.GOTGUD)))
            self.conn.execute(
                'INSERT OR IGNORE INTO user_challenge '
                '(user_id, score, num_completed, num_skipped) VALUES (?, 0, 0, 0)',
                (user_id,))
            self.conn.execute(
                'UPDATE user_challenge SET score = score + ?, '
                'num_completed = num_completed + 1 WHERE user_id = ?',
                (points, user_id))
            self.conn.execute(
                'UPDATE virtual_session SET credited = ?, points = points + ? '
                'WHERE id = ?', (json.dumps(credited), points, session_id))
        return True

    def finish_virtual_session(self, session_id):
        with self.conn:
            changed = self.conn.execute(
                'UPDATE virtual_session SET status = ? WHERE id = ? AND status = ?',
                (CLAIMED, session_id, ACTIVE)).rowcount
        return bool(changed)

    def virtual_session_contest_ids(self, guild_id, user_id):
        """Contests this user has ever confirmed, in any guild or state."""
        return {row[0] for row in self.conn.execute(
            'SELECT contest_id FROM virtual_session WHERE user_id = ?',
            (str(user_id),)).fetchall()}

"""``;virtual`` sessions — a blind random contest whose solves earn gitgud points.

Owns ``virtual_session``. Credited solves are written into the existing
``challenge`` / ``user_challenge`` tables as completed challenges, so the
gitgud leaderboards and ``;gitlog`` pick them up with no special casing.
"""
import json
import logging
import time

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
        points       INTEGER NOT NULL DEFAULT 0,
        credited     TEXT NOT NULL DEFAULT '[]'
    )''')
    conn.execute('''CREATE INDEX IF NOT EXISTS idx_virtual_session_user
        ON virtual_session (guild_id, user_id, status)''')


class VirtualDbMixin:
    """Mixin providing ;virtual session methods. Expects ``self.conn`` and
    the challenge tables from ``ChallengeDbMixin``."""

    def _create_virtual_tables(self):
        create_virtual_schema(self.conn)

    def get_active_virtual_session(self, guild_id, user_id):
        return self.conn.execute(
            'SELECT * FROM virtual_session WHERE guild_id = ? AND user_id = ? '
            'AND status = ? ORDER BY id DESC LIMIT 1',
            (str(guild_id), str(user_id), ACTIVE)).fetchone()

    def start_virtual_session(self, guild_id, user_id, handle, contest_id,
                              contest_name, confirmed_at, expires_at):
        """Open a session; returns its id, or None if one is already active.

        The existence check and the insert share one transaction so two
        confirmations racing each other cannot both succeed.
        """
        with self.conn:
            if self.get_active_virtual_session(guild_id, user_id) is not None:
                return None
            cur = self.conn.execute(
                'INSERT INTO virtual_session (guild_id, user_id, handle, '
                'contest_id, contest_name, confirmed_at, expires_at, status) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (str(guild_id), str(user_id), handle, contest_id, contest_name,
                 confirmed_at, expires_at, ACTIVE))
            return cur.lastrowid

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
        """Contests this user has already been offered, in any state."""
        return {row[0] for row in self.conn.execute(
            'SELECT contest_id FROM virtual_session WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).fetchall()}

    def expire_stale_virtual_sessions(self, now=None):
        """Mark long-expired sessions claimed so they stop blocking new ones.

        Only used as a safety net; the command finalises an expired session
        itself so any late solves are credited first.
        """
        now = time.time() if now is None else now
        with self.conn:
            return self.conn.execute(
                'UPDATE virtual_session SET status = ? WHERE status = ? '
                'AND expires_at < ?', (CLAIMED, ACTIVE, now - 7 * 86400)).rowcount

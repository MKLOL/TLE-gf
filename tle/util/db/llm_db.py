"""LLM DB methods — API key storage and per-bucket quota state.

Owns three tables:

``llm_api_key``
    The Gemini API keys themselves, one row per key. Keys are bot-global (not
    per-guild): whichever moderator adds one, every guild draws on the same
    pool. ``guild_id``/``added_by`` are audit trail only. A ``fingerprint``
    (sha256 of the key) is UNIQUE so re-adding the same key is a no-op rather
    than a duplicate.

``llm_bucket``
    Persisted quota state per ``(key, model)``. Google's free tier meters
    requests-per-day per *project* per *model*, so a key that is dead for
    ``gemini-2.5-flash`` may still be fine for ``gemini-2.5-flash-lite``.
    Only *daily* exhaustion is persisted — per-minute cooldowns expire in
    under a minute, so losing them on restart costs at most one 429.

``llm_usage``
    Per-user, per-UTC-day call counts, so one person cannot drain the shared
    daily allowance for the whole server.

The key material is stored in plaintext: the bot must be able to present it
to Google on every call, so there is nothing to gain from hashing it. Treat
``user.db`` as a secret-bearing file.
"""
import hashlib
import logging
import time

logger = logging.getLogger(__name__)


def key_fingerprint(api_key):
    """Stable identifier for a key, used for dedup without comparing secrets."""
    return hashlib.sha256(api_key.strip().encode('utf-8')).hexdigest()


class LlmDbMixin:
    """Mixin providing LLM key-pool and quota DB methods."""

    def _create_llm_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_api_key (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key     TEXT NOT NULL,
                fingerprint TEXT NOT NULL UNIQUE,
                label       TEXT,
                guild_id    TEXT,
                added_by    TEXT,
                added_at    REAL NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_bucket (
                key_id          INTEGER NOT NULL,
                model           TEXT NOT NULL,
                exhausted_until REAL,
                last_error      TEXT,
                updated_at      REAL,
                PRIMARY KEY (key_id, model)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_usage (
                guild_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                day      TEXT NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id, day)
            )
        ''')

    # ── Key management ──────────────────────────────────────────────────

    def llm_add_key(self, api_key, label=None, guild_id=None, added_by=None):
        """Store one API key. Returns 'added', 'reactivated', or 'duplicate'.

        Re-adding a key that was previously forgotten reactivates it (and
        refreshes its label) rather than creating a second row.
        """
        api_key = api_key.strip()
        fp = key_fingerprint(api_key)
        row = self.conn.execute(
            'SELECT id, active FROM llm_api_key WHERE fingerprint = ?', (fp,)
        ).fetchone()
        if row is not None:
            if row.active:
                return 'duplicate'
            with self.conn:
                self.conn.execute(
                    'UPDATE llm_api_key SET active = 1, api_key = ?, label = ?, '
                    'added_at = ?, added_by = ?, guild_id = ? WHERE id = ?',
                    (api_key, label, time.time(), _s(added_by), _s(guild_id), row.id))
                self.conn.execute(
                    'DELETE FROM llm_bucket WHERE key_id = ?', (row.id,))
            return 'reactivated'
        with self.conn:
            self.conn.execute(
                'INSERT INTO llm_api_key '
                '(api_key, fingerprint, label, guild_id, added_by, added_at, active) '
                'VALUES (?, ?, ?, ?, ?, ?, 1)',
                (api_key, fp, label, _s(guild_id), _s(added_by), time.time()))
        return 'added'

    def llm_get_keys(self, active_only=True):
        """All stored keys, oldest first."""
        query = ('SELECT id, api_key, fingerprint, label, guild_id, added_by, '
                 'added_at, active FROM llm_api_key')
        if active_only:
            query += ' WHERE active = 1'
        query += ' ORDER BY id'
        return self.conn.execute(query).fetchall()

    def llm_forget_key(self, key_id):
        """Deactivate a key by id. Returns True if a row changed."""
        with self.conn:
            cur = self.conn.execute(
                'UPDATE llm_api_key SET active = 0 WHERE id = ? AND active = 1',
                (key_id,))
        return cur.rowcount > 0

    # ── Bucket (per key × model) quota state ────────────────────────────

    def llm_set_bucket_exhausted(self, key_id, model, until, last_error=None):
        """Mark a ``(key, model)`` bucket dead until a unix timestamp."""
        with self.conn:
            self.conn.execute(
                'INSERT INTO llm_bucket (key_id, model, exhausted_until, last_error, updated_at) '
                'VALUES (?, ?, ?, ?, ?) '
                'ON CONFLICT (key_id, model) DO UPDATE SET '
                'exhausted_until = excluded.exhausted_until, '
                'last_error = excluded.last_error, updated_at = excluded.updated_at',
                (key_id, model, until, last_error, time.time()))

    def llm_clear_bucket(self, key_id, model):
        """Drop any exhaustion record for a bucket (used after a success)."""
        with self.conn:
            self.conn.execute(
                'DELETE FROM llm_bucket WHERE key_id = ? AND model = ?',
                (key_id, model))

    def llm_get_buckets(self, now=None):
        """Buckets still exhausted at ``now`` (default: current time)."""
        now = time.time() if now is None else now
        return self.conn.execute(
            'SELECT key_id, model, exhausted_until, last_error, updated_at '
            'FROM llm_bucket WHERE exhausted_until IS NOT NULL AND exhausted_until > ?',
            (now,)).fetchall()

    def llm_purge_expired_buckets(self, now=None):
        """Delete bucket rows whose exhaustion window has passed."""
        now = time.time() if now is None else now
        with self.conn:
            cur = self.conn.execute(
                'DELETE FROM llm_bucket '
                'WHERE exhausted_until IS NULL OR exhausted_until <= ?', (now,))
        return cur.rowcount

    # ── Per-user daily usage ────────────────────────────────────────────

    def llm_bump_usage(self, guild_id, user_id, day):
        """Increment and return a user's call count for ``day`` (a 'YYYY-MM-DD')."""
        with self.conn:
            self.conn.execute(
                'INSERT INTO llm_usage (guild_id, user_id, day, count) VALUES (?, ?, ?, 1) '
                'ON CONFLICT (guild_id, user_id, day) DO UPDATE SET count = count + 1',
                (_s(guild_id), _s(user_id), day))
        return self.llm_get_usage(guild_id, user_id, day)

    def llm_get_usage(self, guild_id, user_id, day):
        """A user's call count for ``day``; 0 if they have not called today."""
        row = self.conn.execute(
            'SELECT count FROM llm_usage WHERE guild_id = ? AND user_id = ? AND day = ?',
            (_s(guild_id), _s(user_id), day)).fetchone()
        return row.count if row else 0

    def llm_top_users(self, guild_id, day, limit=5):
        """Heaviest users for ``day``, busiest first, with the guild total.

        Returns ``(rows, total)``. Nothing is capped per user, so this is how a
        moderator finds out where the shared allowance went.
        """
        rows = self.conn.execute(
            'SELECT user_id, count FROM llm_usage '
            'WHERE guild_id = ? AND day = ? ORDER BY count DESC, user_id LIMIT ?',
            (_s(guild_id), day, limit)).fetchall()
        total_row = self.conn.execute(
            'SELECT COALESCE(SUM(count), 0) AS total FROM llm_usage '
            'WHERE guild_id = ? AND day = ?', (_s(guild_id), day)).fetchone()
        return rows, (total_row.total if total_row else 0)

    def llm_purge_old_usage(self, before_day):
        """Delete usage rows for days strictly before ``before_day``."""
        with self.conn:
            cur = self.conn.execute(
                'DELETE FROM llm_usage WHERE day < ?', (before_day,))
        return cur.rowcount


def _s(value):
    """Discord ids are ints in Python but TEXT in SQLite; None stays None."""
    return None if value is None else str(value)

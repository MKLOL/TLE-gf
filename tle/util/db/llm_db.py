"""LLM DB methods — API key storage and per-bucket quota state.

Owns the core LLM tables:

``llm_api_key``
    Provider-tagged API keys, one row per key. Keys are bot-global (not
    per-guild): whichever moderator adds one, every guild draws on the same
    provider pool. ``guild_id``/``added_by`` are audit trail only. A
    ``fingerprint`` (sha256 of the key) is UNIQUE so re-adding the same key is
    a no-op rather than a duplicate.

``llm_bucket``
    Persisted quota state per ``(key, model)``. Google's free tier meters
    requests-per-day per *project* per *model*, so a key that is dead for
    ``gemini-2.5-flash`` may still be fine for ``gemini-2.5-flash-lite``.
    Only *daily* exhaustion is persisted — per-minute cooldowns expire in
    under a minute, so losing them on restart costs at most one 429.

``llm_usage``
    Per-user, per-UTC-day call counts for moderator visibility.

``llm_xai_request``
    A compact timestamp ledger for Grok-only credit protection: per-guild,
    per-user rolling-window limits plus one bot-wide UTC-day limit. Old rows
    are pruned whenever a request reserves a slot.

``llm_user_ban``
    Guild-scoped request bans. These block both provider routes while leaving
    moderation subcommands available so the ban can always be reversed.

``llm_cooldown``
    Persistent shared admission cooldowns for one server, channel family,
    exact channel, or exact thread.

The key material is stored in plaintext: the bot must be able to present it
to the provider on every call, so there is nothing to gain from hashing it.
Treat ``user.db`` as a secret-bearing file.
"""
import hashlib
import logging
import time

from tle.util.db.llm_telemetry_db import LlmTelemetryDbMixin
from tle.util.db.llm_cooldown_db import LlmCooldownDbMixin

logger = logging.getLogger(__name__)

_KEY_PROVIDERS = frozenset(('gemini', 'xai'))
_XAI_LEDGER_RETENTION_SECONDS = 31 * 86400


class XaiRequestDenial(str):
    """String-compatible guard result with a retry timestamp when finite."""

    def __new__(cls, reason, retry_at=None):
        value = super().__new__(cls, reason)
        value.reason = reason
        value.retry_at = retry_at
        return value


def key_fingerprint(api_key):
    """Stable identifier for a key, used for dedup without comparing secrets."""
    return hashlib.sha256(api_key.strip().encode('utf-8')).hexdigest()


class LlmDbMixin(LlmCooldownDbMixin, LlmTelemetryDbMixin):
    """Mixin providing LLM key-pool and quota DB methods."""

    def _create_llm_tables(self):
        self._create_llm_telemetry_tables()
        self._create_llm_cooldown_tables()
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_api_key (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key     TEXT NOT NULL,
                fingerprint TEXT NOT NULL UNIQUE,
                provider    TEXT NOT NULL DEFAULT 'gemini',
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
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_xai_request (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      TEXT NOT NULL,
                requested_at REAL NOT NULL,
                guild_id     TEXT,
                model        TEXT,
                reserved_microusd INTEGER NOT NULL DEFAULT 0,
                actual_microusd   INTEGER,
                outcome      TEXT
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS llm_xai_request_time
            ON llm_xai_request (requested_at)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS llm_xai_request_user_time
            ON llm_xai_request (user_id, requested_at)
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_user_ban (
                guild_id  TEXT NOT NULL,
                user_id   TEXT NOT NULL,
                banned_by TEXT,
                banned_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS llm_user_ban_guild_time
            ON llm_user_ban (guild_id, banned_at)
        ''')
        # Existing 1.45 databases reach create_tables() before migrations run,
        # so their table does not have ``provider`` yet. The 1.46 upgrade
        # creates the index after adding the column; fresh schemas can do it
        # immediately here.
        key_columns = {
            row[1] for row in self.conn.execute(
                'PRAGMA table_info(llm_api_key)').fetchall()
        }
        if 'provider' in key_columns:
            self.conn.execute('''
                CREATE INDEX IF NOT EXISTS llm_api_key_provider_active
                ON llm_api_key (provider, active)
            ''')

    # ── Key management ──────────────────────────────────────────────────

    def llm_add_key(self, api_key, label=None, guild_id=None, added_by=None,
                    provider='gemini'):
        """Store a key and return an add/reactivate/duplicate/conflict status.

        Re-adding a key that was previously forgotten reactivates it (and
        refreshes its label) rather than creating a second row.
        """
        api_key = api_key.strip()
        provider = _provider(provider)
        fp = key_fingerprint(api_key)
        row = self.conn.execute(
            'SELECT id, active, provider FROM llm_api_key WHERE fingerprint = ?',
            (fp,)
        ).fetchone()
        if row is not None:
            if row.provider != provider:
                return 'provider_conflict'
            if row.active:
                return 'duplicate'
            with self.conn:
                self.conn.execute(
                    'UPDATE llm_api_key SET active = 1, api_key = ?, label = ?, '
                    'added_at = ?, added_by = ?, guild_id = ? '
                    'WHERE id = ?',
                    (api_key, label, time.time(), _s(added_by), _s(guild_id),
                     row.id))
                self.conn.execute(
                    'DELETE FROM llm_bucket WHERE key_id = ?', (row.id,))
            return 'reactivated'
        with self.conn:
            self.conn.execute(
                'INSERT INTO llm_api_key '
                '(api_key, fingerprint, provider, label, guild_id, added_by, '
                'added_at, active) VALUES (?, ?, ?, ?, ?, ?, ?, 1)',
                (api_key, fp, provider, label, _s(guild_id), _s(added_by),
                 time.time()))
        return 'added'

    def llm_get_keys(self, active_only=True, provider='gemini'):
        """Stored keys for one provider, oldest first."""
        query = ('SELECT id, api_key, fingerprint, provider, label, guild_id, '
                 'added_by, added_at, active FROM llm_api_key WHERE provider = ?')
        params = [_provider(provider)]
        if active_only:
            query += ' AND active = 1'
        query += ' ORDER BY id'
        return self.conn.execute(query, params).fetchall()

    def llm_forget_key(self, key_id, provider='gemini'):
        """Crypto-erase a key while retaining its fingerprint audit record."""
        with self.conn:
            cur = self.conn.execute(
                "UPDATE llm_api_key SET active = 0, api_key = '' "
                'WHERE id = ? AND provider = ? AND active = 1',
                (key_id, _provider(provider)))
            if cur.rowcount:
                self.conn.execute(
                    'DELETE FROM llm_bucket WHERE key_id = ?', (key_id,))
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

    # ── Guild-scoped request bans ──────────────────────────────────────

    def llm_ban_user(self, guild_id, user_id, *, banned_by=None, now=None):
        """Ban a user from LLM requests in one guild; return whether added."""
        now = time.time() if now is None else float(now)
        with self.conn:
            cur = self.conn.execute(
                'INSERT OR IGNORE INTO llm_user_ban '
                '(guild_id, user_id, banned_by, banned_at) VALUES (?, ?, ?, ?)',
                (_s(guild_id), _s(user_id), _s(banned_by), now))
        return cur.rowcount > 0

    def llm_unban_user(self, guild_id, user_id):
        """Remove a guild-scoped LLM request ban; return whether removed."""
        with self.conn:
            cur = self.conn.execute(
                'DELETE FROM llm_user_ban WHERE guild_id = ? AND user_id = ?',
                (_s(guild_id), _s(user_id)))
        return cur.rowcount > 0

    def llm_is_user_banned(self, guild_id, user_id):
        """Return whether ``user_id`` is banned from requests in ``guild_id``."""
        row = self.conn.execute(
            'SELECT 1 AS banned FROM llm_user_ban '
            'WHERE guild_id = ? AND user_id = ?',
            (_s(guild_id), _s(user_id))).fetchone()
        return row is not None

    def llm_get_banned_users(self, guild_id):
        """Return a guild's request bans, oldest first."""
        return self.conn.execute(
            'SELECT user_id, banned_by, banned_at FROM llm_user_ban '
            'WHERE guild_id = ? ORDER BY banned_at, user_id',
            (_s(guild_id),)).fetchall()

    # ── Grok credit guard ───────────────────────────────────────────────

    def llm_reserve_xai_request(self, user_id, user_limit, window_seconds,
                                daily_limit, now=None, *, guild_id=None,
                                model=None, reserved_microusd=0,
                                daily_budget_microusd=0, return_id=False,
                                enforce_user_limit=True):
        """Atomically reserve one Grok invocation.

        Returns ``None`` when accepted (or the row id with ``return_id=True``),
        or a string-compatible :class:`XaiRequestDenial`. Its ``retry_at`` is
        the first guaranteed opening, accounting for simultaneous guards. A
        reservation is kept on failure so failures cannot bypass protection.
        ``enforce_user_limit=False`` skips only the personal rolling guard;
        shared daily count and spend protection always remain active.
        """
        now = time.time() if now is None else float(now)
        user_id = str(user_id)
        reserved_microusd = max(0, int(reserved_microusd or 0))
        daily_budget_microusd = max(0, int(daily_budget_microusd or 0))
        window_cutoff = now - max(0, window_seconds)
        day_start = int(now // 86400) * 86400
        retention = max(_XAI_LEDGER_RETENTION_SECONDS,
                        max(0, window_seconds))
        retain_after = min(now - retention, day_start)
        scope_sql = '' if guild_id is None else ' AND guild_id = ?'
        scope_params = () if guild_id is None else (_s(guild_id),)

        with self.conn:
            # The write starts a transaction before the counts, so concurrent
            # callers cannot both observe the final free slot.
            self.conn.execute(
                'DELETE FROM llm_xai_request WHERE requested_at < ?',
                (retain_after,))
            user_count = None
            if enforce_user_limit:
                user_count = self.conn.execute(
                    'SELECT COUNT(*) AS count FROM llm_xai_request '
                    f'WHERE user_id = ? AND requested_at > ?{scope_sql}',
                    (user_id, window_cutoff, *scope_params)).fetchone().count
            daily_count = self.conn.execute(
                'SELECT COUNT(*) AS count FROM llm_xai_request '
                'WHERE requested_at >= ?', (day_start,)).fetchone().count
            denials = []
            if enforce_user_limit and user_count >= user_limit:
                retry_at = None
                if user_limit > 0:
                    row = self.conn.execute(
                        'SELECT requested_at FROM llm_xai_request '
                        f'WHERE user_id = ? AND requested_at > ?{scope_sql} '
                        'ORDER BY requested_at, id LIMIT 1 OFFSET ?',
                        (user_id, window_cutoff, *scope_params,
                         max(0, user_count - user_limit))).fetchone()
                    if row is not None:
                        retry_at = row.requested_at + max(0, window_seconds)
                denials.append(('user', retry_at, 0))
            if daily_count >= daily_limit:
                denials.append(('daily', day_start + 86400, 1))
            if daily_budget_microusd:
                spent = self.conn.execute(
                    'SELECT COALESCE(SUM(COALESCE(actual_microusd, '
                    'reserved_microusd)), 0) AS spent FROM llm_xai_request '
                    'WHERE requested_at >= ?', (day_start,)).fetchone().spent
                if spent + reserved_microusd > daily_budget_microusd:
                    denials.append(('budget', day_start + 86400, 2))

            if denials:
                # Pick the last guard to reopen; a same-time shared guard wins
                # so spend exhaustion remains indistinguishable from count.
                reason, retry_at, _ = max(
                    denials, key=lambda item: (
                        item[1] is not None, item[1] or 0, item[2]))
                return XaiRequestDenial(reason, retry_at)

            cur = self.conn.execute(
                'INSERT INTO llm_xai_request '
                '(user_id, requested_at, guild_id, model, reserved_microusd) '
                'VALUES (?, ?, ?, ?, ?)',
                (user_id, now, _s(guild_id), model, reserved_microusd))
        return cur.lastrowid if return_id else None

    def llm_finalize_xai_request(self, reservation_id, *, actual_microusd=None,
                                 outcome=None, model=None):
        """Reconcile a Grok reservation after the provider path finishes."""
        actual = (None if actual_microusd is None
                  else max(0, int(actual_microusd)))
        with self.conn:
            cur = self.conn.execute(
                'UPDATE llm_xai_request SET actual_microusd = ?, '
                'outcome = ?, model = COALESCE(?, model) WHERE id = ?',
                (actual, _bounded(outcome, 40), _bounded(model, 100),
                 int(reservation_id)))
        return cur.rowcount > 0

    def llm_xai_daily_summary(self, now=None):
        """Private daily count/spend view for the bot owner."""
        now = time.time() if now is None else float(now)
        day_start = int(now // 86400) * 86400
        return self.conn.execute('''
            SELECT COUNT(*) AS calls,
                   COALESCE(SUM(reserved_microusd), 0) AS reserved_microusd,
                   COALESCE(SUM(actual_microusd), 0) AS actual_microusd,
                   COALESCE(SUM(COALESCE(actual_microusd,
                                        reserved_microusd)), 0)
                       AS guarded_microusd
            FROM llm_xai_request WHERE requested_at >= ?
        ''', (day_start,)).fetchone()

    def llm_reset_xai_daily_limits(self, now=None):
        """Clear Grok guard reservations from the current UTC day."""
        now = time.time() if now is None else float(now)
        day_start = int(now // 86400) * 86400
        with self.conn:
            cursor = self.conn.execute(
                'DELETE FROM llm_xai_request '
                'WHERE requested_at >= ? AND requested_at < ?',
                (day_start, day_start + 86400))
        return cursor.rowcount

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

        Returns ``(rows, total)``. This is reporting data; Grok admission is
        enforced separately by :meth:`llm_reserve_xai_request`.
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


def _provider(value):
    """Normalize provider labels at the DB boundary."""
    provider = (value or 'gemini').strip().lower()
    if provider not in _KEY_PROVIDERS:
        raise ValueError(f'Unsupported LLM key provider: {provider}')
    return provider


def _bounded(value, limit):
    if value is None:
        return None
    return ' '.join(str(value).split())[:limit]

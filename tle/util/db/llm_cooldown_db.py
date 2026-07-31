"""Persistent shared cooldowns for LLM prompt admission."""

from collections import namedtuple
import time


GLOBAL_COOLDOWN_CHANNEL = '*'
MAX_LLM_COOLDOWN_SECONDS = 86400
LlmCooldownDenial = namedtuple('LlmCooldownDenial', 'scope retry_at')


class LlmCooldownDbMixin:
    """DB methods for guild-wide and parent-channel LLM cooldowns."""

    def _create_llm_cooldown_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS llm_cooldown (
                guild_id       TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                seconds        INTEGER NOT NULL
                               CHECK (seconds BETWEEN 1 AND 86400),
                last_attempt_at REAL,
                PRIMARY KEY (guild_id, channel_id)
            )
        ''')

    def llm_set_cooldown(self, guild_id, seconds, *, channel_id=None):
        """Set one scope, clearing its active timer; zero removes the scope."""
        seconds = int(seconds)
        if not 0 <= seconds <= MAX_LLM_COOLDOWN_SECONDS:
            raise ValueError('LLM cooldown is outside the supported range')
        scope_id = _scope_id(channel_id)
        with self.conn:
            if seconds == 0:
                cursor = self.conn.execute(
                    'DELETE FROM llm_cooldown '
                    'WHERE guild_id = ? AND channel_id = ?',
                    (str(guild_id), scope_id))
            else:
                cursor = self.conn.execute('''
                    INSERT INTO llm_cooldown
                        (guild_id, channel_id, seconds, last_attempt_at)
                    VALUES (?, ?, ?, NULL)
                    ON CONFLICT (guild_id, channel_id) DO UPDATE SET
                        seconds = excluded.seconds,
                        last_attempt_at = NULL
                ''', (str(guild_id), scope_id, seconds))
        return cursor.rowcount > 0

    def llm_get_cooldown_settings(self, guild_id, channel_id=None):
        """Return ``{'global': seconds, 'channel': seconds}`` when configured."""
        rows = self._llm_cooldown_rows(guild_id, channel_id)
        result = {}
        for row in rows:
            scope = ('global' if row.channel_id == GLOBAL_COOLDOWN_CHANNEL
                     else 'channel')
            result[scope] = row.seconds
        return result

    def llm_cooldown_retry(self, guild_id, channel_id=None, *, now=None):
        """Read the current limiting cooldown without consuming an attempt."""
        now = time.time() if now is None else float(now)
        return _limiting_denial(
            self._llm_cooldown_rows(guild_id, channel_id), now)

    def llm_claim_cooldowns(self, guild_id, channel_id=None, *, now=None):
        """Atomically claim every configured scope or return its latest retry."""
        now = time.time() if now is None else float(now)
        guild_id = str(guild_id)
        targets = _targets(channel_id)
        placeholders = ', '.join('?' for _ in targets)
        params = (guild_id, *targets)
        with self.conn:
            # Start a write transaction before checking timestamps so separate
            # bot processes cannot both observe and claim the same opening.
            self.conn.execute(
                'UPDATE llm_cooldown SET last_attempt_at = last_attempt_at '
                f'WHERE guild_id = ? AND channel_id IN ({placeholders})',
                params)
            rows = self.conn.execute(
                'SELECT channel_id, seconds, last_attempt_at '
                'FROM llm_cooldown '
                f'WHERE guild_id = ? AND channel_id IN ({placeholders})',
                params).fetchall()
            denial = _limiting_denial(rows, now)
            if denial is not None:
                return denial
            if rows:
                self.conn.execute(
                    'UPDATE llm_cooldown SET last_attempt_at = ? '
                    f'WHERE guild_id = ? AND channel_id IN ({placeholders})',
                    (now, *params))
        return None

    def _llm_cooldown_rows(self, guild_id, channel_id):
        targets = _targets(channel_id)
        placeholders = ', '.join('?' for _ in targets)
        return self.conn.execute(
            'SELECT channel_id, seconds, last_attempt_at FROM llm_cooldown '
            f'WHERE guild_id = ? AND channel_id IN ({placeholders})',
            (str(guild_id), *targets)).fetchall()


def _targets(channel_id):
    if channel_id is None:
        return (GLOBAL_COOLDOWN_CHANNEL,)
    return GLOBAL_COOLDOWN_CHANNEL, str(channel_id)


def _scope_id(channel_id):
    return (GLOBAL_COOLDOWN_CHANNEL if channel_id is None
            else str(channel_id))


def _limiting_denial(rows, now):
    active = []
    for row in rows:
        if row.last_attempt_at is None:
            continue
        retry_at = row.last_attempt_at + row.seconds
        if retry_at > now:
            scope = ('global' if row.channel_id == GLOBAL_COOLDOWN_CHANNEL
                     else 'channel')
            active.append(LlmCooldownDenial(scope, retry_at))
    if not active:
        return None
    return max(active, key=lambda item: (
        item.retry_at, item.scope == 'global'))

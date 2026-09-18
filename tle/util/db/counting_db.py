"""Persistent channel state and audit records for the counting cog.

Discord IDs are stored as TEXT.  A channel row is the feature-enable flag and
checkpoint; attempt rows retain only numeric-like messages, whether accepted or
rejected.  Parsing stays in the cog, while this mixin owns atomic persistence.
"""

from collections import namedtuple
from collections.abc import Mapping
import time


COUNTING_RADICES = (2, 10, 16)
_MAX_SQLITE_INTEGER = 2 ** 63 - 1

CountingRecordResult = namedtuple(
    'CountingRecordResult', 'inserted attempt current_count')


class CountingStateConflict(RuntimeError):
    """Raised when a live attempt was classified against a stale checkpoint."""

    def __init__(self, supplied_expected, actual_expected):
        self.supplied_expected = supplied_expected
        self.actual_expected = actual_expected
        super().__init__(
            f'Counting state changed: expected {actual_expected}, '
            f'not {supplied_expected}')


def create_counting_schema(conn):
    """Create the latest counting schema on ``conn``; safe to call repeatedly."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS counting_channel (
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            current_count  INTEGER NOT NULL DEFAULT 0
                           CHECK (current_count >= 0),
            last_message_id TEXT,
            configured_by  TEXT,
            configured_at  REAL NOT NULL,
            updated_at     REAL NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS counting_attempt (
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            message_id     TEXT NOT NULL,
            user_id        TEXT NOT NULL,
            author_name    TEXT NOT NULL,
            content        TEXT NOT NULL,
            created_at     REAL NOT NULL,
            recorded_at    REAL NOT NULL,
            expected_value INTEGER NOT NULL CHECK (expected_value >= 1),
            submitted_value INTEGER,
            accepted       INTEGER NOT NULL CHECK (accepted IN (0, 1)),
            radix          INTEGER CHECK (radix IN (2, 10, 16)),
            reason         TEXT NOT NULL,
            active         INTEGER NOT NULL DEFAULT 1
                           CHECK (active IN (0, 1)),
            PRIMARY KEY (guild_id, channel_id, message_id)
        )
    ''')
    conn.execute('''
        CREATE INDEX IF NOT EXISTS counting_attempt_channel_time
        ON counting_attempt (guild_id, channel_id, created_at, message_id)
    ''')
    conn.execute('''
        CREATE INDEX IF NOT EXISTS counting_attempt_user_result
        ON counting_attempt (guild_id, channel_id, user_id, accepted)
    ''')


class CountingDbMixin:
    """DB methods for enabled counting channels and their attempt ledger."""

    def _create_counting_tables(self):
        create_counting_schema(self.conn)

    def counting_get_channel(self, guild_id, channel_id):
        """Return the persistent channel checkpoint, or ``None`` if stopped."""
        return self.conn.execute(
            'SELECT guild_id, channel_id, current_count, last_message_id, '
            'configured_by, configured_at, updated_at '
            'FROM counting_channel WHERE guild_id = ? AND channel_id = ?',
            (str(guild_id), str(channel_id))).fetchone()

    def counting_get_channels(self, guild_id=None):
        """Return enabled channels, optionally restricted to one guild."""
        if guild_id is None:
            return self.conn.execute(
                'SELECT guild_id, channel_id, current_count, last_message_id, '
                'configured_by, configured_at, updated_at '
                'FROM counting_channel ORDER BY guild_id, channel_id'
            ).fetchall()
        return self.conn.execute(
            'SELECT guild_id, channel_id, current_count, last_message_id, '
            'configured_by, configured_at, updated_at '
            'FROM counting_channel WHERE guild_id = ? ORDER BY channel_id',
            (str(guild_id),)).fetchall()

    def counting_configure(self, guild_id, channel_id, *, current_count=0,
                           last_message_id=None, configured_by=None, now=None):
        """Enable or replace a channel checkpoint after a successful scan."""
        current_count = _count_value(current_count, allow_zero=True)
        now = time.time() if now is None else float(now)
        params = (
            str(guild_id), str(channel_id), current_count,
            _optional_id(last_message_id), _optional_id(configured_by), now, now,
        )
        with self.conn:
            self.conn.execute('''
                INSERT INTO counting_channel
                    (guild_id, channel_id, current_count, last_message_id,
                     configured_by, configured_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (guild_id, channel_id) DO UPDATE SET
                    current_count = excluded.current_count,
                    last_message_id = excluded.last_message_id,
                    configured_by = excluded.configured_by,
                    configured_at = excluded.configured_at,
                    updated_at = excluded.updated_at
            ''', params)
        return self.counting_get_channel(guild_id, channel_id)

    def counting_enable(self, guild_id, channel_id, **kwargs):
        """Compatibility spelling for :meth:`counting_configure`."""
        return self.counting_configure(guild_id, channel_id, **kwargs)

    def counting_get_attempt(self, guild_id, channel_id, message_id):
        """Return one ledger row, used to make gateway replays idempotent."""
        return self.conn.execute(
            _ATTEMPT_SELECT +
            ' WHERE guild_id = ? AND channel_id = ? AND message_id = ?',
            (str(guild_id), str(channel_id), str(message_id))).fetchone()

    def counting_record_attempt(
            self, guild_id, channel_id, message_id, user_id, author_name,
            content, created_at, *, expected_value, submitted_value=None,
            accepted, radix=None, reason=None, recorded_at=None):
        """Record one live numeric attempt and conditionally advance atomically.

        Returns ``None`` if the channel is stopped.  Otherwise returns a
        :class:`CountingRecordResult`; ``inserted`` is false for a replayed
        message.  A stale caller gets :class:`CountingStateConflict` and no
        ledger mutation, allowing it to classify again against current state.
        ``last_message_id`` always identifies the latest accepted count; bad
        attempts are retained in the ledger without moving that checkpoint.
        """
        attempt = _normalize_attempt({
            'message_id': message_id,
            'user_id': user_id,
            'author_name': author_name,
            'content': content,
            'created_at': created_at,
            'recorded_at': recorded_at,
            'expected_value': expected_value,
            'submitted_value': submitted_value,
            'accepted': accepted,
            'radix': radix,
            'reason': reason,
        })
        guild_id, channel_id = str(guild_id), str(channel_id)
        with self.conn:
            # This harmless write acquires SQLite's write transaction before
            # the checkpoint read, serializing separate bot processes too.
            state_cursor = self.conn.execute(
                'UPDATE counting_channel SET updated_at = updated_at '
                'WHERE guild_id = ? AND channel_id = ?',
                (guild_id, channel_id))
            existing = self.counting_get_attempt(
                guild_id, channel_id, attempt['message_id'])
            if existing is not None:
                state = self.counting_get_channel(guild_id, channel_id)
                current = None if state is None else state.current_count
                return CountingRecordResult(False, existing, current)
            if state_cursor.rowcount == 0:
                return None

            state = self.counting_get_channel(guild_id, channel_id)
            actual_expected = state.current_count + 1
            if attempt['expected_value'] != actual_expected:
                raise CountingStateConflict(
                    attempt['expected_value'], actual_expected)

            new_count = actual_expected if attempt['accepted'] \
                else state.current_count
            last_message_id = (attempt['message_id'] if attempt['accepted']
                               else state.last_message_id)
            cursor = self.conn.execute('''
                UPDATE counting_channel
                SET current_count = ?, last_message_id = ?, updated_at = ?
                WHERE guild_id = ? AND channel_id = ? AND current_count = ?
            ''', (
                new_count, last_message_id, attempt['recorded_at'],
                guild_id, channel_id, state.current_count,
            ))
            if cursor.rowcount != 1:
                latest = self.counting_get_channel(guild_id, channel_id)
                latest_expected = None if latest is None \
                    else latest.current_count + 1
                raise CountingStateConflict(
                    attempt['expected_value'], latest_expected)
            self.conn.execute(_ATTEMPT_UPSERT, (
                guild_id, channel_id, *(_attempt_values(attempt))))
            row = self.counting_get_attempt(
                guild_id, channel_id, attempt['message_id'])
            return CountingRecordResult(True, row, new_count)

    def counting_sync_history(
            self, guild_id, channel_id, current_count, last_message_id,
            attempts, *, configured_by=None, configured_at=None,
            recorded_at=None):
        """Atomically replace a checkpoint and its full history snapshot.

        ``attempts`` is an iterable of mappings.  A complete reparse deletes
        the channel's previous ledger and rebuilds it from Discord, so deleted
        or newly ignored messages cannot survive in current statistics.
        """
        current_count = _count_value(current_count, allow_zero=True)
        sync_time = time.time() if recorded_at is None else float(recorded_at)
        configured_at = sync_time if configured_at is None \
            else float(configured_at)
        normalized = []
        for attempt in attempts:
            data = dict(attempt) if isinstance(attempt, Mapping) \
                else dict(attempt._asdict())
            if data.get('recorded_at') is None:
                data['recorded_at'] = sync_time
            normalized.append(_normalize_attempt(data))

        guild_id, channel_id = str(guild_id), str(channel_id)
        with self.conn:
            self.conn.execute(
                'DELETE FROM counting_attempt '
                'WHERE guild_id = ? AND channel_id = ?',
                (guild_id, channel_id))
            self.conn.execute('''
                INSERT INTO counting_channel
                    (guild_id, channel_id, current_count, last_message_id,
                     configured_by, configured_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (guild_id, channel_id) DO UPDATE SET
                    current_count = excluded.current_count,
                    last_message_id = excluded.last_message_id,
                    configured_by = excluded.configured_by,
                    configured_at = excluded.configured_at,
                    updated_at = excluded.updated_at
            ''', (
                guild_id, channel_id, current_count,
                _optional_id(last_message_id), _optional_id(configured_by),
                configured_at, sync_time,
            ))
            self.conn.executemany(
                _ATTEMPT_UPSERT,
                ((guild_id, channel_id, *(_attempt_values(a)))
                 for a in normalized))
        return self.counting_get_channel(guild_id, channel_id)

    def counting_get_attempts(self, guild_id, channel_id, *, user_id=None,
                              accepted=None, include_inactive=False,
                              limit=None):
        """Return attempts oldest first, with optional user/result filters.

        ``include_inactive`` remains for compatibility with ledgers created by
        older releases.  Current full-history reparses replace those rows.
        """
        clauses = ['guild_id = ?', 'channel_id = ?']
        params = [str(guild_id), str(channel_id)]
        if not include_inactive:
            clauses.append('active = 1')
        if user_id is not None:
            clauses.append('user_id = ?')
            params.append(str(user_id))
        if accepted is not None:
            clauses.append('accepted = ?')
            params.append(int(bool(accepted)))
        query = (
            _ATTEMPT_SELECT + ' WHERE ' + ' AND '.join(clauses) +
            ' ORDER BY created_at, LENGTH(message_id), message_id')
        if limit is not None:
            limit = int(limit)
            if limit < 0:
                raise ValueError('limit must not be negative')
            query += ' LIMIT ?'
            params.append(limit)
        return self.conn.execute(query, params).fetchall()

    def counting_get_stats(self, guild_id, channel_id, *, user_id=None):
        """Return aggregate accepted/bad attempt counts for a channel or user."""
        where = 'guild_id = ? AND channel_id = ? AND active = 1'
        params = [str(guild_id), str(channel_id)]
        if user_id is not None:
            where += ' AND user_id = ?'
            params.append(str(user_id))
        return self.conn.execute('''
            SELECT COUNT(*) AS attempt_count,
                   COALESCE(SUM(accepted), 0) AS accepted_count,
                   COALESCE(SUM(CASE WHEN accepted = 0 THEN 1 ELSE 0 END), 0)
                       AS bad_count,
                   COUNT(DISTINCT user_id) AS participant_count
            FROM counting_attempt WHERE ''' + where, params).fetchone()

    def counting_get_user_stats(self, guild_id, channel_id):
        """Return per-user stats ordered by accepted count, then bad count."""
        return self.conn.execute('''
            SELECT user_id, COUNT(*) AS attempt_count,
                   COALESCE(SUM(accepted), 0) AS accepted_count,
                   COALESCE(SUM(CASE WHEN accepted = 0 THEN 1 ELSE 0 END), 0)
                       AS bad_count
            FROM counting_attempt
            WHERE guild_id = ? AND channel_id = ? AND active = 1
            GROUP BY user_id
            ORDER BY accepted_count DESC, bad_count ASC,
                     LENGTH(user_id), user_id
        ''', (str(guild_id), str(channel_id))).fetchall()

    def counting_stop_channel(self, guild_id, channel_id):
        """Disable a channel while retaining its attempt ledger for statistics."""
        with self.conn:
            cursor = self.conn.execute(
                'DELETE FROM counting_channel '
                'WHERE guild_id = ? AND channel_id = ?',
                (str(guild_id), str(channel_id)))
        return cursor.rowcount > 0

    def counting_clear_channel(self, guild_id, channel_id):
        """Delete both the enable/checkpoint row and all channel attempts."""
        params = (str(guild_id), str(channel_id))
        with self.conn:
            attempts = self.conn.execute(
                'DELETE FROM counting_attempt '
                'WHERE guild_id = ? AND channel_id = ?', params)
            channel = self.conn.execute(
                'DELETE FROM counting_channel '
                'WHERE guild_id = ? AND channel_id = ?', params)
        return attempts.rowcount > 0 or channel.rowcount > 0


_ATTEMPT_SELECT = (
    'SELECT guild_id, channel_id, message_id, user_id, author_name, content, '
    'created_at, recorded_at, expected_value, submitted_value, accepted, radix, '
    'reason, active FROM counting_attempt')

_ATTEMPT_UPSERT = '''
    INSERT INTO counting_attempt
        (guild_id, channel_id, message_id, user_id, author_name, content,
         created_at, recorded_at, expected_value, submitted_value, accepted,
         radix, reason, active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    ON CONFLICT (guild_id, channel_id, message_id) DO UPDATE SET
        user_id = excluded.user_id,
        author_name = excluded.author_name,
        content = excluded.content,
        created_at = excluded.created_at,
        expected_value = excluded.expected_value,
        submitted_value = excluded.submitted_value,
        accepted = excluded.accepted,
        radix = excluded.radix,
        reason = excluded.reason,
        active = 1
'''


def _normalize_attempt(attempt):
    expected = attempt.get('expected_value', attempt.get('expected_number'))
    expected = _count_value(expected, allow_zero=False)
    submitted = _optional_sqlite_integer(attempt.get('submitted_value'))
    accepted = attempt.get('accepted')
    if accepted not in (True, False, 0, 1):
        raise ValueError('accepted must be a boolean')
    accepted = bool(accepted)
    radix = attempt.get('radix')
    if radix is not None:
        radix = int(radix)
        if radix not in COUNTING_RADICES:
            raise ValueError('radix must be 2, 10, 16, or None')
    if accepted and radix is None:
        raise ValueError('an accepted attempt must have a radix')
    if accepted and submitted != expected:
        raise ValueError('an accepted attempt must submit the expected value')
    recorded_at = attempt.get('recorded_at')
    recorded_at = time.time() if recorded_at is None else float(recorded_at)
    reason = attempt.get('reason')
    reason = ('accepted' if accepted else 'invalid') if reason is None \
        else str(reason)
    if not reason:
        raise ValueError('reason must not be empty')
    return {
        'message_id': str(attempt['message_id']),
        'user_id': str(attempt['user_id']),
        'author_name': str(attempt['author_name']),
        'content': str(attempt['content']),
        'created_at': float(attempt['created_at']),
        'recorded_at': recorded_at,
        'expected_value': expected,
        'submitted_value': submitted,
        'accepted': accepted,
        'radix': radix,
        'reason': reason,
    }


def _attempt_values(attempt):
    return (
        attempt['message_id'], attempt['user_id'], attempt['author_name'],
        attempt['content'], attempt['created_at'], attempt['recorded_at'],
        attempt['expected_value'], attempt['submitted_value'],
        int(attempt['accepted']), attempt['radix'], attempt['reason'],
    )


def _count_value(value, *, allow_zero):
    value = int(value)
    minimum = 0 if allow_zero else 1
    if not minimum <= value <= _MAX_SQLITE_INTEGER:
        raise ValueError('count is outside SQLite integer range')
    return value


def _optional_sqlite_integer(value):
    if value is None:
        return None
    value = int(value)
    if not -_MAX_SQLITE_INTEGER - 1 <= value <= _MAX_SQLITE_INTEGER:
        return None
    return value


def _optional_id(value):
    return None if value is None else str(value)

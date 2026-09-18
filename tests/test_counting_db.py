"""DB and migration coverage for persistent counting channels."""

import sqlite3

import pytest

from tle.util.db.counting_db import CountingStateConflict
from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import (
    registry,
    upgrade_1_54_0,
    upgrade_1_55_0,
)


@pytest.fixture
def db():
    database = UserDbConn(':memory:')
    try:
        yield database
    finally:
        database.conn.close()


def _attempt(message_id, *, user_id=7, author_name='Ada', content='1',
             created_at=10.0, expected_value=1, submitted_value=1,
             accepted=True, radix=10, reason='accepted', recorded_at=20.0):
    return {
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
    }


class TestCountingSchema:
    def test_fresh_database_has_latest_schema(self, db):
        channel_columns = {
            row.name for row in db.conn.execute(
                'PRAGMA table_info(counting_channel)').fetchall()
        }
        attempt_columns = {
            row.name for row in db.conn.execute(
                'PRAGMA table_info(counting_attempt)').fetchall()
        }

        assert channel_columns == {
            'guild_id', 'channel_id', 'current_count', 'last_message_id',
            'configured_by', 'configured_at', 'updated_at',
        }
        assert attempt_columns == {
            'guild_id', 'channel_id', 'message_id', 'user_id', 'author_name',
            'content', 'created_at', 'recorded_at', 'expected_value',
            'submitted_value', 'accepted', 'radix', 'reason', 'active',
        }
        assert registry.get_current_version(db.conn) == registry.latest_version

    def test_migration_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        try:
            upgrade_1_54_0(conn)
            upgrade_1_54_0(conn)
            conn.execute(
                'SELECT current_count FROM counting_channel').fetchall()
            conn.execute(
                'SELECT submitted_value FROM counting_attempt').fetchall()
        finally:
            conn.close()

    def test_reparse_migration_clears_attempts_but_keeps_channels(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        try:
            upgrade_1_54_0(conn)
            conn.execute(
                'INSERT INTO counting_channel VALUES '
                "('1', '2', 1, '3', '4', 5.0, 6.0)")
            conn.execute('''
                INSERT INTO counting_attempt VALUES
                    ('1', '2', '3', '4', 'Ada', '1', 5.0, 6.0,
                     1, 1, 1, 10, 'correct', 1)
            ''')

            upgrade_1_55_0(conn)
            upgrade_1_55_0(conn)

            assert conn.execute(
                'SELECT guild_id, channel_id FROM counting_channel'
            ).fetchall() == [('1', '2')]
            assert conn.execute(
                'SELECT * FROM counting_attempt').fetchall() == []
        finally:
            conn.close()

    def test_registry_upgrades_existing_153_database(self, tmp_path):
        path = tmp_path / 'user.db'
        raw = sqlite3.connect(path)
        raw.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        raw.execute("INSERT INTO db_version VALUES ('1.53.0')")
        raw.commit()
        raw.close()

        database = UserDbConn(str(path))
        try:
            assert registry.get_current_version(
                database.conn) == registry.latest_version
            database.conn.execute(
                'SELECT expected_value FROM counting_attempt').fetchall()
        finally:
            database.conn.close()


class TestCountingLiveAttempts:
    def test_configure_casts_ids_to_text(self, db):
        row = db.counting_configure(
            100, 200, current_count=8, last_message_id=300,
            configured_by=400, now=5.0)

        assert tuple(row) == ('100', '200', 8, '300', '400', 5.0, 5.0)
        assert db.counting_get_channel('100', '200') == row

    def test_accepted_and_bad_attempts_update_one_checkpoint(self, db):
        db.counting_enable(100, 200, current_count=0, now=1.0)

        accepted = db.counting_record_attempt(
            100, 200, 1000, 7, 'Ada', '0x1', 10.0,
            expected_value=1, submitted_value=1, accepted=True,
            radix=16, reason='accepted', recorded_at=11.0)
        bad = db.counting_record_attempt(
            100, 200, 1001, 8, 'Béla', '11', 12.0,
            expected_value=2, submitted_value=11, accepted=False,
            radix=10, reason='wrong_value', recorded_at=13.0)
        invalid = db.counting_record_attempt(
            100, 200, 1002, 8, 'Béla', '73.7', 14.0,
            expected_value=2, submitted_value=None, accepted=False,
            radix=None, reason='invalid_format', recorded_at=15.0)

        assert accepted.inserted is True
        assert accepted.current_count == 1
        assert accepted.attempt.submitted_value == 1
        assert bad.attempt.accepted == 0
        assert invalid.attempt.radix is None
        state = db.counting_get_channel(100, 200)
        assert (state.current_count, state.last_message_id) == (1, '1000')
        assert [row.message_id for row in db.counting_get_attempts(100, 200)] \
            == ['1000', '1001', '1002']

        stats = db.counting_get_stats(100, 200)
        assert tuple(stats) == (3, 1, 2, 2)
        assert [(row.user_id, row.accepted_count, row.bad_count)
                for row in db.counting_get_user_stats(100, 200)] == [
                    ('7', 1, 0), ('8', 0, 2),
                ]

    def test_duplicate_event_does_not_advance_twice(self, db):
        db.counting_configure(1, 2, now=1.0)
        kwargs = dict(
            expected_value=1, submitted_value=1, accepted=True,
            radix=10, reason='accepted', recorded_at=3.0)

        first = db.counting_record_attempt(
            1, 2, 3, 4, 'User', '1', 2.0, **kwargs)
        replay = db.counting_record_attempt(
            1, 2, 3, 4, 'User', '1', 2.0, **kwargs)

        assert first.inserted is True
        assert replay.inserted is False
        assert replay.attempt.message_id == '3'
        assert replay.current_count == 1
        assert db.counting_get_stats(1, 2).attempt_count == 1

    def test_stale_expected_rolls_back_attempt_and_state(self, db):
        db.counting_configure(1, 2, current_count=4, now=1.0)

        with pytest.raises(CountingStateConflict) as exc_info:
            db.counting_record_attempt(
                1, 2, 99, 5, 'User', '5', 2.0,
                expected_value=4, submitted_value=5, accepted=False,
                radix=10, reason='wrong_value', recorded_at=3.0)

        assert exc_info.value.actual_expected == 5
        assert db.counting_get_attempt(1, 2, 99) is None
        assert db.counting_get_channel(1, 2).current_count == 4

    def test_stopped_channel_does_not_record_new_attempt(self, db):
        result = db.counting_record_attempt(
            1, 2, 3, 4, 'User', '1', 2.0,
            expected_value=1, submitted_value=1, accepted=True,
            radix=10, reason='accepted', recorded_at=3.0)

        assert result is None
        assert db.counting_get_attempt(1, 2, 3) is None


class TestCountingHistorySync:
    def test_sync_is_atomic_and_replaces_history_snapshot(self, db):
        history = [
            _attempt(10, content='1', created_at=1.0),
            _attempt(
                11, user_id=8, author_name='Lin', content='3',
                created_at=2.0, expected_value=2, submitted_value=3,
                accepted=False, radix=10, reason='wrong_value'),
            _attempt(
                12, user_id=8, author_name='Lin', content='10',
                created_at=3.0, expected_value=2, submitted_value=2,
                accepted=True, radix=2, reason='accepted'),
        ]

        state = db.counting_sync_history(
            100, 200, 2, 12, history, configured_by=9,
            configured_at=30.0, recorded_at=20.0)
        assert (state.current_count, state.last_message_id) == (2, '12')

        corrected = _attempt(
            11, user_id=8, author_name='Lin Renamed', content='0x3',
            created_at=2.0, expected_value=2, submitted_value=3,
            accepted=False, radix=16, reason='wrong_value', recorded_at=99.0)
        db.counting_sync_history(
            100, 200, 2, 12, [history[0], corrected, history[2]], configured_by=9,
            configured_at=31.0, recorded_at=40.0)

        rows = db.counting_get_attempts(100, 200)
        assert [row.message_id for row in rows] == ['10', '11', '12']
        changed = db.counting_get_attempt(100, 200, 11)
        assert (changed.author_name, changed.content, changed.radix) == \
            ('Lin Renamed', '0x3', 16)
        assert changed.recorded_at == 99.0

    def test_resync_replaces_rows_missing_from_current_history(self, db):
        history = [
            _attempt(10, created_at=1.0),
            _attempt(11, content='10', created_at=2.0, expected_value=2,
                     submitted_value=2, radix=2),
        ]
        db.counting_sync_history(1, 2, 2, 11, history)

        db.counting_sync_history(1, 2, 1, 10, [history[0]])

        assert [row.message_id for row in db.counting_get_attempts(1, 2)] == ['10']
        assert [row.message_id for row in db.counting_get_attempts(
            1, 2, include_inactive=True)] == ['10']
        assert db.counting_get_stats(1, 2).accepted_count == 1

    def test_invalid_sync_rolls_back_before_configuration(self, db):
        invalid = _attempt(10, accepted=True, radix=None)

        with pytest.raises(ValueError):
            db.counting_sync_history(1, 2, 1, 10, [invalid])

        assert db.counting_get_channel(1, 2) is None

    def test_stop_retains_stats_and_clear_removes_them(self, db):
        db.counting_sync_history(1, 2, 1, 10, [_attempt(10)])

        assert db.counting_stop_channel(1, 2) is True
        assert db.counting_get_channel(1, 2) is None
        assert db.counting_get_stats(1, 2).attempt_count == 1
        assert db.counting_clear_channel(1, 2) is True
        assert db.counting_get_stats(1, 2).attempt_count == 0

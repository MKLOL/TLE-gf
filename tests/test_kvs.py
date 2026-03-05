"""Tests for the general key-value store (kvs) and its use by restart messages."""
import sqlite3

import pytest

from tle.util.db.user_db_conn import namedtuple_factory


class FakeKvsDb:
    """Minimal in-memory DB with kvs table for testing."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE kvs (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            )
        ''')
        self.conn.commit()

    # Import methods from UserDbConn
    from tle.util.db.user_db_conn import UserDbConn
    kvs_set = UserDbConn.kvs_set
    kvs_get = UserDbConn.kvs_get
    kvs_delete = UserDbConn.kvs_delete


@pytest.fixture
def db():
    return FakeKvsDb()


# ---------------------------------------------------------------------------
# Basic KVS operations
# ---------------------------------------------------------------------------

class TestKvsBasic:
    def test_get_missing_key_returns_none(self, db):
        assert db.kvs_get('nonexistent') is None

    def test_set_and_get(self, db):
        db.kvs_set('foo', 'bar')
        assert db.kvs_get('foo') == 'bar'

    def test_set_overwrites(self, db):
        db.kvs_set('key', 'value1')
        db.kvs_set('key', 'value2')
        assert db.kvs_get('key') == 'value2'

    def test_delete(self, db):
        db.kvs_set('key', 'value')
        db.kvs_delete('key')
        assert db.kvs_get('key') is None

    def test_delete_nonexistent_is_noop(self, db):
        db.kvs_delete('nonexistent')  # should not raise

    def test_multiple_keys(self, db):
        db.kvs_set('a', '1')
        db.kvs_set('b', '2')
        db.kvs_set('c', '3')
        assert db.kvs_get('a') == '1'
        assert db.kvs_get('b') == '2'
        assert db.kvs_get('c') == '3'

    def test_delete_one_preserves_others(self, db):
        db.kvs_set('a', '1')
        db.kvs_set('b', '2')
        db.kvs_delete('a')
        assert db.kvs_get('a') is None
        assert db.kvs_get('b') == '2'


# ---------------------------------------------------------------------------
# Restart message usage pattern
# ---------------------------------------------------------------------------

class TestRestartMessage:
    def test_store_and_retrieve(self, db):
        db.kvs_set('restart_message', '123456:789012')
        val = db.kvs_get('restart_message')
        channel_id, message_id = val.split(':')
        assert channel_id == '123456'
        assert message_id == '789012'

    def test_overwrite_stale_entry(self, db):
        db.kvs_set('restart_message', '111:222')
        db.kvs_set('restart_message', '333:444')
        val = db.kvs_get('restart_message')
        assert val == '333:444'

    def test_cleanup_after_restart(self, db):
        db.kvs_set('restart_message', '123:456')
        # Simulate bot startup: read then delete
        val = db.kvs_get('restart_message')
        assert val is not None
        db.kvs_delete('restart_message')
        assert db.kvs_get('restart_message') is None

    def test_no_restart_message_on_fresh_start(self, db):
        assert db.kvs_get('restart_message') is None


# ---------------------------------------------------------------------------
# Upgrade 1.6.0
# ---------------------------------------------------------------------------

class TestUpgrade160:
    def test_creates_kvs_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_6_0
        upgrade_1_6_0(conn)
        # Verify table exists by inserting and reading
        conn.execute("INSERT INTO kvs (key, value) VALUES ('test', 'val')")
        row = conn.execute("SELECT value FROM kvs WHERE key = 'test'").fetchone()
        assert row[0] == 'val'
        conn.close()

    def test_idempotent(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_6_0
        upgrade_1_6_0(conn)
        upgrade_1_6_0(conn)  # should not raise
        conn.close()

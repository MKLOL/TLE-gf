"""Migration tests for rpoll DB upgrades."""
import sqlite3
import time

from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import registry


class TestUpgrade150:
    def test_creates_tables(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert 'rpoll' in tables
        assert 'rpoll_option' in tables
        assert 'rpoll_vote' in tables
        conn.close()

    def test_idempotent(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        conn.close()


class TestUpgrade180:
    def test_adds_anonymous_column(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Q?', 'u', 1.0)
        )
        conn.commit()
        row = conn.execute('SELECT anonymous FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.anonymous == 0
        conn.close()

    def test_existing_polls_get_default_anonymous(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        conn.execute('UPDATE db_version SET version = ?', ('1.7.0',))
        conn.commit()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Old poll', 'u', 1.0)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT anonymous FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.anonymous == 0
        conn.close()


class TestUpgrade190:
    def _make_pre_190_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        conn.execute('UPDATE db_version SET version = ?', ('1.8.0',))
        conn.commit()
        return conn

    def test_adds_expires_at_and_closed_columns(self):
        conn = self._make_pre_190_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Q?', 'u', time.time())
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at > 0
        assert row.closed in (0, 1)
        conn.close()

    def test_old_expired_poll_gets_closed(self):
        conn = self._make_pre_190_db()
        old_time = time.time() - 200000
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, message_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            ('1', '2', '999', 'Old Q', 'u', old_time)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at == old_time + 86400
        assert row.closed == 1
        conn.close()

    def test_recent_poll_stays_open(self):
        conn = self._make_pre_190_db()
        now = time.time()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, message_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            ('1', '2', '999', 'Recent Q', 'u', now)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at == now + 86400
        assert row.closed == 0
        conn.close()

    def test_unposted_poll_gets_closed(self):
        conn = self._make_pre_190_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Unposted Q', 'u', time.time())
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.closed == 1
        conn.close()


class TestUpgrade1100:
    def _make_pre_1100_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        conn.execute('''
            CREATE TABLE rpoll (
                poll_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                message_id  TEXT,
                question    TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  REAL NOT NULL,
                anonymous   INTEGER NOT NULL DEFAULT 0,
                expires_at  REAL DEFAULT 0,
                closed      INTEGER NOT NULL DEFAULT 0
            )
        ''')
        conn.execute('''
            CREATE TABLE rpoll_option (
                poll_id       INTEGER NOT NULL,
                option_index  INTEGER NOT NULL,
                label         TEXT NOT NULL,
                PRIMARY KEY (poll_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')
        conn.execute('''
            CREATE TABLE rpoll_vote (
                poll_id       INTEGER NOT NULL,
                user_id       TEXT NOT NULL,
                option_index  INTEGER NOT NULL,
                rating        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (poll_id, user_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')
        registry.set_version(conn, '1.9.0')
        conn.commit()
        return conn

    def test_adds_formula_column(self):
        conn = self._make_pre_1100_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Q?', 'u', 1.0)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT formula FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.formula == 'sum'
        conn.close()

    def test_existing_polls_get_sum_default(self):
        conn = self._make_pre_1100_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, message_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            ('1', '2', '999', 'Old poll', 'u', 1.0)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT formula FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.formula == 'sum'
        conn.close()

    def test_idempotent(self):
        conn = self._make_pre_1100_db()
        registry.run(conn)
        conn.execute('UPDATE db_version SET version = ?', ('1.9.0',))
        conn.commit()
        registry.run(conn)
        conn.close()

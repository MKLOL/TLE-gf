"""Tests for the complaint feature — DB layer and upgrade."""
import sqlite3
import time

import pytest

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory


class FakeComplainDb:
    """Minimal in-memory DB with complaint table for testing."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE complaint (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                text        TEXT NOT NULL,
                created_at  REAL NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_complaint_guild
                ON complaint (guild_id, created_at DESC)
        ''')
        self.conn.commit()

    add_complaint = UserDbConn.add_complaint
    get_complaints = UserDbConn.get_complaints
    get_complaint = UserDbConn.get_complaint
    delete_complaint = UserDbConn.delete_complaint
    delete_complaints = UserDbConn.delete_complaints
    count_recent_complaints = UserDbConn.count_recent_complaints


@pytest.fixture
def db():
    return FakeComplainDb()


GUILD = '111'
USER_A = '100'
USER_B = '200'


class TestAddComplaint:
    def test_add_returns_id(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'test complaint')
        assert cid is not None and cid >= 1

    def test_add_multiple_increments_id(self, db):
        id1 = db.add_complaint(GUILD, USER_A, 'first')
        id2 = db.add_complaint(GUILD, USER_A, 'second')
        assert id2 > id1

    def test_add_stores_text(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'my complaint text')
        row = db.get_complaint(cid)
        assert row.text == 'my complaint text'
        assert row.user_id == USER_A
        assert row.guild_id == GUILD


class TestGetComplaints:
    def test_empty_guild(self, db):
        assert db.get_complaints(GUILD) == []

    def test_returns_newest_first(self, db):
        db.add_complaint(GUILD, USER_A, 'old')
        db.add_complaint(GUILD, USER_A, 'new')
        complaints = db.get_complaints(GUILD)
        assert len(complaints) == 2
        assert complaints[0].text == 'new'
        assert complaints[1].text == 'old'

    def test_guild_isolation(self, db):
        db.add_complaint('111', USER_A, 'guild1')
        db.add_complaint('222', USER_A, 'guild2')
        assert len(db.get_complaints('111')) == 1
        assert len(db.get_complaints('222')) == 1
        assert db.get_complaints('111')[0].text == 'guild1'


class TestGetComplaint:
    def test_existing(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'hello')
        row = db.get_complaint(cid)
        assert row is not None
        assert row.id == cid

    def test_nonexistent(self, db):
        assert db.get_complaint(9999) is None


class TestDeleteComplaint:
    def test_delete_existing(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'to delete')
        assert db.delete_complaint(cid) is True
        # Soft-deleted: not visible via get_complaint
        assert db.get_complaint(cid) is None
        # But row still exists in the DB
        row = db.conn.execute('SELECT active FROM complaint WHERE id = ?', (cid,)).fetchone()
        assert row is not None
        assert row[0] == 0

    def test_delete_nonexistent(self, db):
        assert db.delete_complaint(9999) is False

    def test_delete_idempotent(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'once')
        assert db.delete_complaint(cid) is True
        assert db.delete_complaint(cid) is False

    def test_delete_preserves_others(self, db):
        id1 = db.add_complaint(GUILD, USER_A, 'keep')
        id2 = db.add_complaint(GUILD, USER_A, 'remove')
        db.delete_complaint(id2)
        assert db.get_complaint(id1) is not None
        assert db.get_complaint(id2) is None


class TestDeleteComplaints:
    def test_bulk_delete(self, db):
        id1 = db.add_complaint(GUILD, USER_A, 'one')
        id2 = db.add_complaint(GUILD, USER_A, 'two')
        id3 = db.add_complaint(GUILD, USER_A, 'three')
        deleted = db.delete_complaints([id1, id3], GUILD)
        assert deleted == 2
        assert db.get_complaint(id1) is None
        assert db.get_complaint(id2) is not None
        assert db.get_complaint(id3) is None

    def test_bulk_delete_wrong_guild(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'complaint')
        deleted = db.delete_complaints([cid], '999')
        assert deleted == 0
        assert db.get_complaint(cid) is not None

    def test_bulk_delete_empty_list(self, db):
        assert db.delete_complaints([], GUILD) == 0

    def test_bulk_delete_nonexistent_ids(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'keep')
        deleted = db.delete_complaints([9998, 9999], GUILD)
        assert deleted == 0
        assert db.get_complaint(cid) is not None

    def test_bulk_delete_partial_match(self, db):
        id1 = db.add_complaint(GUILD, USER_A, 'exists')
        deleted = db.delete_complaints([id1, 9999], GUILD)
        assert deleted == 1
        assert db.get_complaint(id1) is None


class TestCountRecentComplaints:
    def test_no_complaints(self, db):
        assert db.count_recent_complaints(GUILD, USER_A, 0) == 0

    def test_counts_within_window(self, db):
        db.add_complaint(GUILD, USER_A, 'one')
        db.add_complaint(GUILD, USER_A, 'two')
        db.add_complaint(GUILD, USER_B, 'other user')
        since = time.time() - 10
        assert db.count_recent_complaints(GUILD, USER_A, since) == 2
        assert db.count_recent_complaints(GUILD, USER_B, since) == 1

    def test_excludes_soft_deleted(self, db):
        cid = db.add_complaint(GUILD, USER_A, 'will remove')
        db.add_complaint(GUILD, USER_A, 'stays')
        db.delete_complaint(cid)
        since = time.time() - 10
        assert db.count_recent_complaints(GUILD, USER_A, since) == 1

    def test_excludes_old(self, db):
        # Manually insert an old complaint
        old_time = time.time() - 99999
        db.conn.execute(
            'INSERT INTO complaint (guild_id, user_id, text, created_at) VALUES (?, ?, ?, ?)',
            (GUILD, USER_A, 'old', old_time)
        )
        db.conn.commit()
        db.add_complaint(GUILD, USER_A, 'recent')
        since = time.time() - 10
        assert db.count_recent_complaints(GUILD, USER_A, since) == 1


class TestUpgrade1170:
    def test_creates_complaint_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_17_0
        upgrade_1_17_0(conn)
        conn.execute(
            "INSERT INTO complaint (guild_id, user_id, text, created_at) VALUES ('1','2','t',0)"
        )
        row = conn.execute('SELECT text FROM complaint').fetchone()
        assert row[0] == 't'
        conn.close()

    def test_idempotent(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_17_0
        upgrade_1_17_0(conn)
        upgrade_1_17_0(conn)  # should not raise
        conn.close()

    def test_fresh_table_has_active_column(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_17_0
        upgrade_1_17_0(conn)
        conn.execute(
            "INSERT INTO complaint (guild_id, user_id, text, created_at) VALUES ('1','2','t',0)"
        )
        row = conn.execute('SELECT active FROM complaint').fetchone()
        assert row[0] == 1
        conn.close()


class TestUpgrade1190:
    def test_adds_active_column(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        # Create old schema without active column
        conn.execute('''
            CREATE TABLE complaint (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                text        TEXT NOT NULL,
                created_at  REAL NOT NULL
            )
        ''')
        conn.execute(
            "INSERT INTO complaint (guild_id, user_id, text, created_at) VALUES ('1','2','t',0)"
        )
        conn.commit()
        from tle.util.db.user_db_upgrades import upgrade_1_19_0
        upgrade_1_19_0(conn)
        row = conn.execute('SELECT active FROM complaint').fetchone()
        assert row[0] == 1  # existing rows default to active
        conn.close()

    def test_idempotent(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_17_0, upgrade_1_19_0
        upgrade_1_17_0(conn)
        upgrade_1_19_0(conn)  # already has active from fresh schema
        upgrade_1_19_0(conn)  # should not raise
        conn.close()

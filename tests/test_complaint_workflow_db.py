import hashlib
import sqlite3

import pytest

from tests.complaint_helpers import COMMIT, ComplaintDb
from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_59_0


def test_fresh_production_database_supports_complaints():
    db = UserDbConn(':memory:')
    cid = db.add_complaint(1, 2, 'broken')
    assert db.get_complaint(cid).notification_attempts == 0
    assert db.resolve_complaint(cid, 1, 3, 'fixed', COMMIT)
    assert db.get_complaints(1) == []
    assert db.get_complaints(1, 'resolved')[0].resolution == 'fixed'
    db.close()


def test_upgrade_preserves_existing_and_deleted_rows():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    conn.execute('''CREATE TABLE complaint (
        id INTEGER PRIMARY KEY, guild_id TEXT, user_id TEXT, text TEXT,
        created_at REAL, message_link TEXT, active INTEGER DEFAULT 1)''')
    conn.execute("INSERT INTO complaint (id, text, active) VALUES (1, 'kept', 1)")
    conn.execute("INSERT INTO complaint (id, text, active) VALUES (2, 'removed', 0)")
    upgrade_1_59_0(conn)
    upgrade_1_59_0(conn)
    rows = conn.execute('SELECT * FROM complaint ORDER BY id').fetchall()
    assert [row.text for row in rows] == ['kept', 'removed']
    assert [row.active for row in rows] == [1, 0]
    assert all(row.resolved_at is None for row in rows)
    conn.close()


def test_resolution_is_scoped_atomic_and_audited():
    db = ComplaintDb()
    cid = db.add_complaint(1, 2, 'broken')
    assert not db.resolve_complaint(cid, 9, 3, 'wrong server', COMMIT)
    assert db.resolve_complaint(cid, 1, 3, 'fixed', COMMIT, 7)
    assert not db.resolve_complaint(cid, 1, 4, 'overwritten', COMMIT)
    row = db.get_complaint(cid)
    assert (row.resolution, row.resolved_by, row.notification_status) == ('fixed', '3', 'pending')
    event, = db.get_complaint_events(cid, 1)
    assert (event.action, event.token_id, event.resolution) == ('resolve', 7, 'fixed')
    assert not db.get_complaint_events(cid, 9)
    assert db.count_recent_complaints(1, 2, 0) == 1
    assert not db.reopen_complaint(cid, 9, 3)
    assert db.reopen_complaint(cid, 1, 3)
    assert db.get_complaint(cid).resolution is None
    assert [e.action for e in db.get_complaint_events(cid, 1)] == ['reopen', 'resolve']
    db.delete_complaint(cid)
    assert not db.resolve_complaint(cid, 1, 3, 'deleted', COMMIT)


def test_notification_queue_persists_and_excludes_deleted(tmp_path):
    path = str(tmp_path / 'complaints.db')
    db = ComplaintDb(path)
    cid = db.add_complaint(1, 2, 'broken')
    db.resolve_complaint(cid, 1, 3, 'fixed', COMMIT)
    db.conn.close()
    db = ComplaintDb(path)
    assert db.pending_complaint_notifications()[0].id == cid
    db.record_complaint_notification(cid, 'failed')
    assert db.pending_complaint_notifications() == []  # Backoff is persistent.
    db.conn.execute('UPDATE complaint SET notification_attempt_at = 0')
    assert db.pending_complaint_notifications()[0].id == cid
    db.delete_complaint(cid)
    assert db.pending_complaint_notifications() == []


def test_tokens_are_hashed_scoped_revocable_and_persistent(tmp_path):
    path = str(tmp_path / 'tokens.db')
    db = ComplaintDb(path)
    token_id, raw = db.create_complaint_token(1, 10)
    row = db.conn.execute('SELECT * FROM complaint_api_token').fetchone()
    assert row.token_hash == hashlib.sha256(raw.encode()).hexdigest()
    assert raw not in repr(row)
    assert not db.revoke_complaint_token(token_id, 2)
    db.conn.close()
    assert raw.encode() not in (tmp_path / 'tokens.db').read_bytes()
    db = ComplaintDb(path)
    assert db.authenticate_complaint_token(raw).guild_id == '1'
    assert db.revoke_complaint_token(token_id, 1)
    assert db.authenticate_complaint_token(raw) is None
    _, expired = db.create_complaint_token(1, 10)
    db.conn.execute('UPDATE complaint_api_token SET expires_at = 0')
    assert db.authenticate_complaint_token(expired) is None


@pytest.mark.parametrize('token', ['', None, 'tlegf_' + 'x' * 42, "' OR 1=1 --", 'x' * 10000])
def test_invalid_token_rejected(token):
    assert ComplaintDb().authenticate_complaint_token(token) is None


@pytest.mark.parametrize('days', [0, -1, 366, True, '30'])
def test_invalid_token_lifetime(days):
    with pytest.raises(ValueError):
        ComplaintDb().create_complaint_token(1, 10, days)

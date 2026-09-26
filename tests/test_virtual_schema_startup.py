"""Exercise virtual schema repairs through the actual database startup path."""
import pytest

from tle.util.db.user_db_conn import UserDbConn
from tle.util.db.user_db_upgrades import registry

from tests.virtual_common import GUILD, HANDLE, NOW, USER


def _first_cut_schema(conn):
    conn.execute('DROP TABLE virtual_session')
    conn.execute('''CREATE TABLE virtual_session (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL, handle TEXT NOT NULL, contest_id INTEGER NOT NULL,
        contest_name TEXT NOT NULL, confirmed_at REAL NOT NULL,
        expires_at REAL NOT NULL, status TEXT NOT NULL,
        points INTEGER NOT NULL DEFAULT 0, credited TEXT NOT NULL DEFAULT '[]')''')
    conn.execute('''CREATE INDEX idx_virtual_session_user
                    ON virtual_session (guild_id, user_id, status)''')
    for guild, contest in ((GUILD, 1), (99, 3)):
        conn.execute(
            'INSERT INTO virtual_session (guild_id, user_id, handle, contest_id, '
            'contest_name, confirmed_at, expires_at, status) '
            "VALUES (?, ?, ?, ?, 'Round', ?, ?, 'active')",
            (str(guild), str(USER), HANDLE, contest, NOW, NOW + 7200))
    registry.set_version(conn, '1.62.0')
    conn.commit()


@pytest.mark.parametrize('cached_rating, expected', [
    (1849, 1800), (900, 1100), (None, 1100), (3400, 3000), ('missing', 3000)])
def test_startup_repairs_duplicate_legacy_sessions_before_creating_index(
        tmp_path, cached_rating, expected):
    path = str(tmp_path / 'user.db')
    db = UserDbConn(path)
    if cached_rating != 'missing':
        db.conn.execute('INSERT INTO cf_user_cache (handle, rating) VALUES (?, ?)',
                        (HANDLE.upper(), cached_rating))
    _first_cut_schema(db.conn)
    db.conn.close()

    db = UserDbConn(path)
    session = db.get_active_virtual_session(GUILD, USER)
    assert session.contest_id == 3
    assert session.base_rating == expected
    assert db.start_virtual_session(GUILD, USER, HANDLE, 1, 'Round', NOW,
                                    NOW + 7200, 1900) is None
    assert registry.get_current_version(db.conn) == registry.latest_version
    db.conn.close()


def test_startup_repairs_zero_rating_left_by_an_already_applied_upgrade(tmp_path):
    path = str(tmp_path / 'user.db')
    db = UserDbConn(path)
    db.conn.execute('INSERT INTO cf_user_cache (handle, rating) VALUES (?, 1900)',
                    (HANDLE,))
    db.start_virtual_session(GUILD, USER, HANDLE, 1, 'Round', NOW, NOW + 7200, 0)
    db.conn.close()

    db = UserDbConn(path)
    assert db.get_active_virtual_session(GUILD, USER).base_rating == 1900
    db.conn.execute('UPDATE cf_user_cache SET rating = 2200')
    db.conn.commit()
    db.conn.close()

    db = UserDbConn(path)
    assert db.get_active_virtual_session(GUILD, USER).base_rating == 1900
    db.conn.close()

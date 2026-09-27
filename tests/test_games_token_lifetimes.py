"""Permanent games credentials, optional TTLs, and the one-time conversion."""
import pytest

from tests.games_helpers import games
from tle.util.db import games_token_db
from tle.util.db.user_db_conn import UserDbConn
from tle.util.db.user_db_upgrades import registry


@pytest.mark.parametrize('days', [1, 365])
def test_explicit_lifetime_expires_at_exact_boundary(games, monkeypatch, days):
    issued = 2_000_000_000
    monkeypatch.setattr(games_token_db.time, 'time', lambda: issued)
    token_id, raw = games.db.create_games_token(1, 10, days)
    expiry = issued + days * 86400
    assert games.db.authenticate_games_token(raw).expires_at == expiry
    monkeypatch.setattr(games_token_db.time, 'time', lambda: expiry - 1)
    assert games.db.authenticate_games_token(raw)
    monkeypatch.setattr(games_token_db.time, 'time', lambda: expiry)
    assert games.db.authenticate_games_token(raw) is None
    assert token_id not in {row.id for row in games.db.list_games_tokens(1, 10)}


@pytest.mark.parametrize('expiry, active', [(-1, True), (0, False), (-2, False)])
def test_only_exact_permanent_sentinel_is_valid(games, expiry, active):
    games.db.conn.execute('UPDATE games_api_token SET expires_at = ?', (expiry,))
    assert bool(games.db.authenticate_games_token(games.raw)) is active
    assert bool(games.db.list_games_tokens(1, 10)) is active


def test_permanent_survives_future_restart_and_scoped_revocation(tmp_path, monkeypatch):
    issued = 2_000_000_000
    monkeypatch.setattr(games_token_db.time, 'time', lambda: issued)
    filename = str(tmp_path / 'permanent.db')
    db = UserDbConn(filename)
    token_id, raw = db.create_games_token(1, 10)
    assert db.authenticate_games_token(raw).expires_at == -1
    db.conn.close()
    monkeypatch.setattr(games_token_db.time, 'time', lambda: issued + 100 * 365 * 86400)
    db = UserDbConn(filename)
    assert db.authenticate_games_token(raw).user_id == '10'
    assert [row.id for row in db.list_games_tokens(1, 10)] == [token_id]
    assert not db.revoke_games_token(token_id, 2, 10)
    assert not db.revoke_games_token(token_id, 1, 11)
    assert db.authenticate_games_token(raw)
    assert db.revoke_games_token(token_id, 1, 10)
    assert db.authenticate_games_token(raw) is None
    assert not db.list_games_tokens(1, 10)
    db.conn.close()
    db = UserDbConn(filename)
    assert db.authenticate_games_token(raw) is None
    db.conn.close()


def test_upgrade_only_promotes_active_games_tokens_once(tmp_path, monkeypatch):
    now = 2_000_000_000
    monkeypatch.setattr(games_token_db.time, 'time', lambda: now)
    filename = str(tmp_path / 'legacy.db')
    db = UserDbConn(filename)
    cases = [
        (now + 1, None, True), (now + 365 * 86400, None, True),
        (now, None, False), (now - 1, None, False),
        (0, None, False), (-2, None, False), (-1, None, True),
        (now + 86400, now - 1, False), (-1, now - 1, False),
    ]
    originals = []
    for index, (expiry, revoked, active) in enumerate(cases):
        token_id, raw = db.create_games_token(index + 1, index + 10, 30)
        db.conn.execute('UPDATE games_api_token SET expires_at = ?, revoked_at = ? WHERE id = ?',
                        (expiry, revoked, token_id))
        before = db.conn.execute('SELECT * FROM games_api_token WHERE id = ?', (token_id,)).fetchone()
        originals.append((raw, before, active))
    _, complaint = db.create_complaint_token(1, 10)
    complaint_before = db.conn.execute('SELECT * FROM complaint_api_token').fetchall()
    registry.set_version(db.conn, '1.65.0')
    db.conn.commit()
    db.conn.close()

    db = UserDbConn(filename)
    assert registry.get_current_version(db.conn) == registry.latest_version
    for raw, before, active in originals:
        after = db.conn.execute('SELECT * FROM games_api_token WHERE id = ?', (before.id,)).fetchone()
        expected_expiry = -1 if active else before.expires_at
        assert after == before._replace(expires_at=expected_expiry)
        assert bool(db.authenticate_games_token(raw)) is active
        listed = db.list_games_tokens(before.guild_id, before.user_id)
        assert bool(listed) is active
    assert db.conn.execute('SELECT * FROM complaint_api_token').fetchall() == complaint_before
    assert db.authenticate_complaint_token(complaint).expires_at == now + 30 * 86400

    # Bounded credentials created after the upgrade must retain their lifetime.
    _, bounded = db.create_games_token(1, 10, 1)
    bounded_expiry = db.authenticate_games_token(bounded).expires_at
    db.conn.close()
    db = UserDbConn(filename)
    assert db.authenticate_games_token(bounded).expires_at == bounded_expiry
    db.conn.close()
    monkeypatch.setattr(games_token_db.time, 'time', lambda: now + 365 * 86400)
    db = UserDbConn(filename)
    assert db.authenticate_games_token(bounded) is None
    assert db.authenticate_complaint_token(complaint) is None
    assert db.authenticate_games_token(originals[0][0])
    db.conn.close()

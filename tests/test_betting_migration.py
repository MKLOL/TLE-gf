"""Betting schema-migration tests (upgrade_1_33_0 .. 1_37_0)."""
import sqlite3

import pytest  # noqa: F401

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory, bet_fixture_key
from tle.util.db.user_db_upgrades import (
    upgrade_1_33_0, upgrade_1_34_0, upgrade_1_35_0, upgrade_1_36_0,
    upgrade_1_37_0, upgrade_1_41_0,
)
from tests.betting_test_utils import GUILD, CH, THREAD, USER_A, USER_B  # noqa: F401


class TestMigration:
    def _wager_pk_cols(self, conn):
        cols = conn.execute('PRAGMA table_info(bet_wager)').fetchall()
        return [row.name for row in sorted(
            (row for row in cols if row.pk), key=lambda row: row.pk)]

    def _wager_rows(self, conn):
        return conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager ORDER BY market_id, user_id, pick'
        ).fetchall()

    def test_upgrade_creates_tables(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute(
            'INSERT INTO bet_wallet (guild_id, user_id, balance) VALUES (?, ?, ?)',
            ('1', '10', 1000))
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'A', 'B', 0.0, 2.0, 3.0, 4.0, '9', 0.0))
        # thread_id + thread_intro_id + bets_closed columns exist
        conn.execute('UPDATE bet_market SET thread_id = ?, bets_closed = 1 '
                     'WHERE event_id = ?', ('77', 'e'))
        conn.execute('UPDATE bet_market SET thread_intro_id = ? '
                     'WHERE event_id = ?', ('88', 'e'))
        market_cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_market)')]
        assert 'thread_intro_id' in market_cols
        # bet_wager has no odds/payout columns (derived from the frozen market)
        conn.execute(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', (1, '10', 'home', 100, 0.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wager').fetchone()[0] == 1
        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_wager)')]
        assert 'odds' not in cols and 'payout' not in cols
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_market)')]
        assert 'idx_bet_market_open_event' in indexes
        conn.execute(
            'INSERT INTO bet_wallet_txn '
            '(guild_id, user_id, actor_id, action, amount, balance_after, '
            'market_id, note, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '10', '99', 'mod_grant', 50, 1050, None, None, 1.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wallet_txn').fetchone()[0] == 1
        conn.close()

    def test_upgrade_134_creates_wallet_audit_for_existing_betting_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute('DROP TABLE bet_wallet_txn')
        upgrade_1_34_0(conn)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")]
        assert 'bet_wallet_txn' in tables
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_wallet_txn)')]
        assert 'idx_bet_wallet_txn_user' in indexes
        conn.close()

    def test_upgrade_135_backfills_fixture_key_and_index(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
             2.0, 3.0, 4.0, '9', 0.0))
        upgrade_1_35_0(conn)
        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_market)')]
        assert 'fixture_key' in cols
        row = conn.execute(
            'SELECT fixture_key FROM bet_market WHERE event_id = ?', ('e',)
        ).fetchone()
        assert row.fixture_key == bet_fixture_key(
            'soccer_epl', 'Spain', 'Cape Verde', 10_000.0)
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_market)')]
        assert 'idx_bet_market_open_fixture' in indexes
        conn.close()

    def test_upgrade_135_refuses_existing_duplicate_open_fixture(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        for event_id, home, away in [
                ('e1', 'Spain', 'Cape Verde'),
                ('e2', 'Cape Verde', 'Spain')]:
            conn.execute(
                'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
                'home_team, away_team, commence_time, odds_home, odds_draw, '
                'odds_away, created_by, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ('1', '2', event_id, 'soccer_epl', home, away, 10_000.0,
                 2.0, 3.0, 4.0, '9', 0.0))
        with pytest.raises(RuntimeError):
            upgrade_1_35_0(conn)
        conn.close()

    def test_upgrade_136_adds_thread_intro_column(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        conn.execute('''
            CREATE TABLE bet_market (
                market_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                message_id TEXT,
                thread_id TEXT
            )
        ''')
        upgrade_1_36_0(conn)
        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_market)')]
        assert 'thread_intro_id' in cols
        conn.close()

    def test_upgrade_141_adds_thread_lock_flag_and_stamps_terminal(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        # One open, one settled, one cancelled market.
        for event_id, status in [('open', 'open'), ('done', 'settled'),
                                  ('void', 'cancelled')]:
            conn.execute(
                'INSERT INTO bet_market (guild_id, channel_id, event_id, '
                'sport_key, home_team, away_team, commence_time, odds_home, '
                'odds_draw, odds_away, created_by, created_at, status) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ('1', '2', event_id, 'soccer_epl', 'A', 'B', 10_000.0,
                 2.0, 3.0, 4.0, '9', 0.0, status))

        upgrade_1_41_0(conn)

        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_market)')]
        assert 'thread_locked' in cols
        locked = {r.event_id: r.thread_locked for r in conn.execute(
            'SELECT event_id, thread_locked FROM bet_market')}
        # Already-terminal markets were locked under the old behavior → stamped.
        assert locked == {'open': 0, 'done': 1, 'void': 1}
        conn.close()

    def test_upgrade_137_migrates_wager_primary_key(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        rows = [
            (1, '10', 'home', 100, 7.0),
            (1, '11', 'away', 200, 8.0),
            (2, '10', 'draw', 300, 9.0),
        ]
        conn.executemany(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', rows)

        upgrade_1_37_0(conn)

        assert self._wager_pk_cols(conn) == ['market_id', 'user_id', 'pick']
        assert self._wager_rows(conn) == rows
        conn.execute(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', (1, '10', 'away', 50, 8.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wager').fetchone()[0] == 4
        conn.close()

    def test_upgrade_137_recovers_stranded_old_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        rows = [
            (1, '10', 'home', 100, 7.0),
            (2, '11', 'away', 200, 8.0),
        ]
        conn.executemany(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', rows)
        conn.execute('ALTER TABLE bet_wager RENAME TO bet_wager_old_137')
        conn.execute('''
            CREATE TABLE bet_wager (
                market_id   INTEGER NOT NULL,
                user_id     TEXT NOT NULL,
                pick        TEXT NOT NULL,
                stake       INTEGER NOT NULL,
                placed_at   REAL NOT NULL,
                PRIMARY KEY (market_id, user_id, pick)
            )
        ''')

        upgrade_1_37_0(conn)

        assert self._wager_pk_cols(conn) == ['market_id', 'user_id', 'pick']
        assert self._wager_rows(conn) == rows
        old_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name = 'bet_wager_old_137'"
        ).fetchone()
        assert old_table is None
        conn.close()

    def test_userdb_existing_134_runs_fixture_migration(self, tmp_path):
        from tle.util.db.user_db_upgrades import registry
        path = tmp_path / 'user.db'
        conn = sqlite3.connect(path)
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        upgrade_1_34_0(conn)
        conn.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        conn.execute('INSERT INTO db_version (version) VALUES (?)', ('1.34.0',))
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
             2.0, 3.0, 4.0, '9', 0.0))
        conn.execute(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', (1, '10', 'home', 100, 7.0))
        conn.commit()
        conn.close()

        db = UserDbConn(str(path))
        try:
            assert registry.get_current_version(db.conn) == registry.latest_version
            row = db.conn.execute(
                'SELECT fixture_key FROM bet_market WHERE event_id = ?', ('e',)
            ).fetchone()
            assert row.fixture_key == bet_fixture_key(
                'soccer_epl', 'Spain', 'Cape Verde', 10_000.0)
            cols = [r.name for r in db.conn.execute('PRAGMA table_info(bet_market)')]
            assert 'thread_intro_id' in cols
            assert self._wager_pk_cols(db.conn) == ['market_id', 'user_id', 'pick']
            assert self._wager_rows(db.conn) == [(1, '10', 'home', 100, 7.0)]
            indexes = [r.name for r in db.conn.execute('PRAGMA index_list(bet_market)')]
            assert 'idx_bet_market_open_fixture' in indexes
        finally:
            db.conn.close()

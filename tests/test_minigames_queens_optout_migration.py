"""Migration coverage for Queens' independent rating opt-out."""

import sqlite3

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import (
    registry, upgrade_1_42_0, upgrade_1_43_0, upgrade_1_44_0,
)


def _create_legacy_optout_table(conn):
    conn.execute('''
        CREATE TABLE minigame_optout (
            guild_id TEXT NOT NULL,
            game TEXT NOT NULL,
            user_id TEXT NOT NULL,
            opted_out_at REAL NOT NULL,
            PRIMARY KEY (guild_id, game, user_id)
        )
    ''')


def _create_and_seed_optouts(conn):
    _create_legacy_optout_table(conn)
    conn.executemany(
        'INSERT INTO minigame_optout '
        '(guild_id, game, user_id, opted_out_at) VALUES (?, ?, ?, ?)',
        [
            ('1', 'queens', '300', 1.0),
            ('1', 'akari', '301', 2.0),
        ],
    )


def _create_legacy_source_table(conn):
    conn.execute('''
        CREATE TABLE minigame_unresolved_result (
            guild_id TEXT NOT NULL,
            game TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            external_name TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            puzzle_number INTEGER NOT NULL,
            puzzle_date TEXT NOT NULL,
            accuracy INTEGER NOT NULL,
            time_seconds INTEGER NOT NULL,
            is_perfect INTEGER NOT NULL DEFAULT 0,
            raw_content TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (guild_id, game, normalized_name, puzzle_number)
        )
    ''')


def test_upgrade_142_clears_only_legacy_queens_rows():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    _create_and_seed_optouts(conn)

    upgrade_1_42_0(conn)
    upgrade_1_42_0(conn)

    rows = conn.execute(
        'SELECT guild_id, game, user_id FROM minigame_optout').fetchall()
    assert [(row.guild_id, row.game, row.user_id) for row in rows] == [
        ('1', 'akari', '301'),
    ]
    conn.close()


def test_opening_version_141_db_runs_queens_optout_conversion(tmp_path):
    dbfile = tmp_path / 'user.db'
    raw = sqlite3.connect(dbfile)
    _create_and_seed_optouts(raw)
    raw.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
    raw.execute(
        'INSERT INTO db_version (version) VALUES (?)',
        ('1.41.0',),
    )
    raw.commit()
    raw.close()

    conn = UserDbConn(str(dbfile))
    try:
        assert conn.is_minigame_opted_out('1', 'queens', '300') is False
        assert conn.is_minigame_opted_out('1', 'akari', '301') is True
        # Opening the DB runs every pending upgrade, so it lands on whatever
        # the newest registered version is — not a pinned number.
        assert registry.get_current_version(conn.conn) == registry.latest_version
    finally:
        conn.conn.close()


def test_upgrade_143_adds_rated_flag_without_changing_old_rows():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    _create_legacy_source_table(conn)
    _create_legacy_optout_table(conn)
    conn.execute(
        'INSERT INTO minigame_unresolved_result VALUES '
        '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('1', 'queens', 'alice', 'Alice', '2', 769, '2026-06-08',
         100, 5, 1, 'raw'),
    )

    upgrade_1_43_0(conn)
    upgrade_1_43_0(conn)

    row = conn.execute(
        'SELECT is_rated, stored_at, source_message_id '
        'FROM minigame_unresolved_result').fetchone()
    assert (row.is_rated, row.stored_at, row.source_message_id) == (
        1, 0, None)
    optout_columns = {
        row.name
        for row in conn.execute('PRAGMA table_info(minigame_optout)')
    }
    assert 'normalized_name' in optout_columns
    conn.close()


def test_upgrade_144_adds_nullable_rating_override():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    _create_legacy_source_table(conn)
    _create_legacy_optout_table(conn)
    upgrade_1_43_0(conn)

    upgrade_1_44_0(conn)
    upgrade_1_44_0(conn)

    columns = {
        row.name
        for row in conn.execute(
            'PRAGMA table_info(minigame_unresolved_result)')
    }
    assert 'rating_override' in columns
    conn.close()


def test_opening_version_142_db_runs_rated_flag_upgrade(tmp_path):
    dbfile = tmp_path / 'user.db'
    raw = sqlite3.connect(dbfile)
    _create_legacy_source_table(raw)
    raw.execute(
        'INSERT INTO minigame_unresolved_result VALUES '
        '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('1', 'queens', 'alice', 'Alice', '2', 769, '2026-06-08',
         100, 5, 1, 'raw'),
    )
    raw.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
    raw.execute(
        'INSERT INTO db_version (version) VALUES (?)',
        ('1.42.0',),
    )
    raw.commit()
    raw.close()

    conn = UserDbConn(str(dbfile))
    try:
        row = conn.get_minigame_unresolved_results_for_name(
            '1', 'queens', 'alice')[0]
        assert row.is_rated == 1
        assert row.rating_override is None
        assert registry.get_current_version(conn.conn) == registry.latest_version
    finally:
        conn.conn.close()

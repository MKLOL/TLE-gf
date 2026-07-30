"""Migration coverage for Queens' independent rating opt-out."""

import sqlite3

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import registry, upgrade_1_42_0


def _create_and_seed_optouts(conn):
    conn.execute('''
        CREATE TABLE minigame_optout (
            guild_id TEXT NOT NULL,
            game TEXT NOT NULL,
            user_id TEXT NOT NULL,
            opted_out_at REAL NOT NULL,
            PRIMARY KEY (guild_id, game, user_id)
        )
    ''')
    conn.executemany(
        'INSERT INTO minigame_optout '
        '(guild_id, game, user_id, opted_out_at) VALUES (?, ?, ?, ?)',
        [
            ('1', 'queens', '300', 1.0),
            ('1', 'akari', '301', 2.0),
        ],
    )


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
        assert registry.get_current_version(conn.conn) == '1.42.0'
        assert registry.latest_version == '1.42.0'
    finally:
        conn.conn.close()

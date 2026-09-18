"""Backfill selection, migration, and bounded SQLite work at startup."""
import sqlite3

import pytest

from tests.starboard_test_utils import GUILD_A, GUILD_B, STAR, FIRE
from tle.util.db.starboard_db import StarboardDbMixin
from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import (
    registry, upgrade_1_3_0, upgrade_1_58_0,
)


_INDEX = 'ix_starboard_message_v1_pending_backfill'


@pytest.fixture
def db():
    database = UserDbConn(':memory:')
    yield database
    database.close()


@pytest.mark.parametrize(('author', 'channel', 'pending'), [
    (None, None, True),
    (None, '500', True),
    ('42', None, True),
    ('42', '500', False),
    ('__UNKNOWN__', None, False),
    ('__UNKNOWN__', '500', False),
    ('', None, True),
    ('', '500', False),
])
def test_pending_selection_preserves_checkpoint_rules(db, author, channel, pending):
    # Include legacy empty strings without the insertion API normalizing them.
    db.conn.execute(
        'INSERT INTO starboard_message_v1 '
        '(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, channel_id) '
        'VALUES (?, ?, ?, ?, ?, ?)',
        ('101', '201', str(GUILD_A), STAR, author, channel))
    db.add_starboard_message_v1(102, 202, GUILD_B, STAR)

    rows = db.get_pending_starboard_messages_for_guild(GUILD_A)

    assert len(rows) == int(pending)
    if pending:
        assert rows[0].original_msg_id == '101'
        assert rows[0].guild_id == str(GUILD_A)
        assert rows[0].author_id == author
        assert rows[0].channel_id == channel


def test_pending_selection_keeps_separate_emojis_for_one_message(db):
    db.add_starboard_message_v1(101, 201, GUILD_A, STAR)
    db.add_starboard_message_v1(101, 202, GUILD_A, FIRE)
    assert {row.emoji for row in db.get_pending_starboard_messages_for_guild(
        str(GUILD_A))} == {STAR, FIRE}


def test_index_tracks_backfill_and_live_changes(db):
    db.add_starboard_message_v1(101, 201, GUILD_A, STAR)
    db.update_starboard_author_and_count(101, STAR, '42', 7)
    assert len(db.get_pending_starboard_messages_for_guild(GUILD_A)) == 1

    db.update_starboard_author_and_count(101, STAR, '42', 7, channel_id=500)
    assert db.get_pending_starboard_messages_for_guild(GUILD_A) == []
    db.add_starboard_message_v1(102, 202, GUILD_A, STAR)
    db.update_starboard_author_and_count(102, STAR, '__UNKNOWN__', 0)
    assert db.get_pending_starboard_messages_for_guild(GUILD_A) == []

    # Clearing metadata makes work pending again without rebuilding the index.
    db.conn.execute(
        "UPDATE starboard_message_v1 SET channel_id = NULL "
        "WHERE original_msg_id = '101'")
    rows = db.get_pending_starboard_messages_for_guild(GUILD_A)
    assert [(row.original_msg_id, row.author_id, row.star_count)
            for row in rows] == [('101', '42', 7)]


@pytest.mark.parametrize('history_size', [0, 20_000])
@pytest.mark.parametrize('has_pending', [False, True])
def test_query_work_does_not_grow_with_unrelated_history(
        db, history_size, has_pending):
    # Completed rows in this guild and pending rows in another guild must
    # both be skipped by the index, not scanned or converted to Python rows.
    with db.conn:
        db.conn.executemany(
            'INSERT INTO starboard_message_v1 '
            '(original_msg_id, guild_id, emoji, author_id, channel_id) '
            'VALUES (?, ?, ?, ?, ?)',
            ((f'done-{i}', str(GUILD_A), STAR, '42', '500')
             for i in range(history_size)))
        db.conn.executemany(
            'INSERT INTO starboard_message_v1 '
            '(original_msg_id, guild_id, emoji) VALUES (?, ?, ?)',
            ((f'other-{i}', str(GUILD_B), STAR) for i in range(history_size)))
    if has_pending:
        db.add_starboard_message_v1(101, 201, GUILD_A, STAR)

    steps = 0

    def enforce_work_budget():
        nonlocal steps
        steps += 100
        return int(steps > 2000)

    # A deterministic VM instruction budget catches a lost/mismatched index
    # or accidental full-history read without relying on machine speed.
    db.conn.set_progress_handler(enforce_work_budget, 100)
    try:
        rows = db.get_pending_starboard_messages_for_guild(GUILD_A)
    finally:
        db.conn.set_progress_handler(None, 0)
    assert [row.original_msg_id for row in rows] == (['101'] if has_pending else [])


def test_migration_is_idempotent_and_preserves_existing_rows(db):
    db.add_starboard_message_v1(101, 201, GUILD_A, STAR)
    db.add_starboard_message_v1(
        102, 202, GUILD_A, STAR, author_id='42', channel_id='500')
    before = db.get_all_starboard_messages_for_guild(GUILD_A)
    db.conn.execute(f'DROP INDEX {_INDEX}')
    registry.set_version(db.conn, '1.57.0')

    registry.run(db.conn)
    upgrade_1_58_0(db.conn)

    assert registry.get_current_version(db.conn) == registry.latest_version
    assert db.get_all_starboard_messages_for_guild(GUILD_A) == before
    index = next(row for row in db.conn.execute(
        'PRAGMA index_list(starboard_message_v1)') if row.name == _INDEX)
    assert index.partial == 1
    assert [row.original_msg_id for row in
            db.get_pending_starboard_messages_for_guild(GUILD_A)] == ['101']


def test_index_and_checkpoints_survive_database_reopen(tmp_path):
    path = str(tmp_path / 'user.db')
    db = UserDbConn(path)
    try:
        db.add_starboard_message_v1(101, 201, GUILD_A, STAR)
        db.add_starboard_message_v1(
            102, 202, GUILD_A, STAR, author_id='__UNKNOWN__')
        db.conn.execute(f'DROP INDEX {_INDEX}')
        registry.set_version(db.conn, '1.57.0')
    finally:
        db.close()

    for _ in range(2):
        db = UserDbConn(path)
        try:
            assert registry.get_current_version(db.conn) == registry.latest_version
            assert db.conn.execute(
                'SELECT 1 FROM sqlite_master WHERE name = ?', (_INDEX,)).fetchone()
            assert [row.original_msg_id for row in
                    db.get_pending_starboard_messages_for_guild(GUILD_A)] == ['101']
        finally:
            db.close()


def test_pre_author_schema_waits_for_column_upgrades():
    database = StarboardDbMixin()
    database.conn = sqlite3.connect(':memory:')
    database.conn.row_factory = namedtuple_factory
    try:
        database.conn.execute('''
            CREATE TABLE starboard_message_v1 (
                original_msg_id TEXT, starboard_msg_id TEXT, guild_id TEXT,
                emoji TEXT, PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        database.conn.execute(
            'INSERT INTO starboard_message_v1 VALUES (?, ?, ?, ?)',
            ('101', '201', str(GUILD_A), STAR))
        database._create_starboard_tables()
        assert database.conn.execute(
            'SELECT 1 FROM sqlite_master WHERE name = ?', (_INDEX,)).fetchone() is None

        upgrade_1_3_0(database.conn)
        upgrade_1_58_0(database.conn)

        rows = database.get_pending_starboard_messages_for_guild(GUILD_A)
        assert len(rows) == 1
        assert rows[0].original_msg_id == '101'
        assert rows[0].author_id is None
    finally:
        database.conn.close()

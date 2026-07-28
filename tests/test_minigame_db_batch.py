"""Transactional batch-write tests for minigame results."""

import sqlite3

import pytest

from tests.minigames_test_utils import db


def _row(message_id, *, game='queens', seconds=30):
    return (
        message_id, 100, game, 200, 300, 769,
        '2026-06-08', 100, seconds, True, 'raw',
    )


def _source_row(name, puzzle_number, *, seconds=30):
    return (
        name.casefold(), name, 200, puzzle_number, '2026-06-08',
        100, seconds, True, 'raw source',
    )


def test_batch_result_save_writes_every_row_in_one_call(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    saved = db.save_minigame_results([
        _row(1, seconds=30),
        _row(2, seconds=40),
    ])
    db.conn.set_trace_callback(None)

    assert saved == 2
    assert sum(item.startswith('BEGIN') for item in statements) == 1
    assert statements.count('COMMIT') == 1
    rows = db.get_live_minigame_results_for_guild(100, 'queens')
    assert sorted(
        (row.message_id, row.time_seconds) for row in rows
    ) == [
        ('1', 30),
        ('2', 40),
    ]


def test_batch_result_save_is_atomic_on_sql_error(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.save_minigame_results([
            _row(1),
            _row(2, game=None),
        ])

    assert db.get_live_minigame_results_for_guild(100, 'queens') == []


def test_empty_batch_result_save_starts_no_transaction(db):
    statements = []
    db.conn.set_trace_callback(statements.append)

    assert db.save_minigame_results(iter(())) == 0

    db.conn.set_trace_callback(None)
    assert not any(item.startswith('BEGIN') for item in statements)
    assert 'COMMIT' not in statements


def test_source_migration_batches_inserts_and_mixed_deletes(db):
    db.save_minigame_result(
        1, 100, 'queens', 200, 300, 769, '2026-06-08',
        100, 30, True, 'live')
    db.save_imported_minigame_result(
        2, 100, 'queens', 200, 301, 770, '2026-06-09',
        100, 40, True, 'imported')
    db.save_minigame_result(
        3, 100, 'queens', 200, 302, 771, '2026-06-10',
        100, 50, True, 'unrelated')
    statements = []
    db.conn.set_trace_callback(statements.append)

    applied = db.apply_minigame_source_migration(
        100,
        'queens',
        [_source_row('Alice', 769), _source_row('Bob', 770)],
        [('live', 1, 769), ('imported', 2, 770)],
    )

    db.conn.set_trace_callback(None)
    assert applied == (2, 2)
    assert sum(item.startswith('BEGIN') for item in statements) == 1
    assert statements.count('COMMIT') == 1
    assert {
        row.normalized_name
        for row in db.get_minigame_unresolved_results_for_guild(
            100, 'queens')
    } == {'alice', 'bob'}
    stored = db.get_stored_minigame_results_for_guild(100, 'queens')
    assert [(row.storage, row.message_id) for row in stored] == [('live', '3')]


def test_source_migration_rolls_back_insert_when_delete_fails(db):
    db.save_minigame_result(
        1, 100, 'queens', 200, 300, 769, '2026-06-08',
        100, 30, True, 'live')
    db.conn.execute('''
        CREATE TRIGGER fail_legacy_delete
        BEFORE DELETE ON minigame_result
        WHEN OLD.message_id = '1'
        BEGIN
            SELECT RAISE(ABORT, 'blocked');
        END
    ''')
    db.conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match='blocked'):
        db.apply_minigame_source_migration(
            100, 'queens', [_source_row('Alice', 769)],
            [('live', 1, 769)])

    assert db.get_minigame_unresolved_results_for_guild(
        100, 'queens') == []
    assert db.get_minigame_result(1) is not None


def test_source_migration_validates_storage_before_writing(db):
    with pytest.raises(ValueError, match='Unsupported'):
        db.apply_minigame_source_migration(
            100, 'queens', [_source_row('Alice', 769)],
            [('archive', 1, 769)])

    assert db.get_minigame_unresolved_results_for_guild(
        100, 'queens') == []


def test_empty_source_migration_starts_no_transaction(db):
    statements = []
    db.conn.set_trace_callback(statements.append)

    assert db.apply_minigame_source_migration(
        100, 'queens', iter(()), iter(())) == (0, 0)

    db.conn.set_trace_callback(None)
    assert not any(item.startswith('BEGIN') for item in statements)
    assert 'COMMIT' not in statements

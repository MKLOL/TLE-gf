"""Transactional batch-write tests for minigame results."""

import sqlite3

import pytest

from tests.minigames_test_utils import db


def _row(message_id, *, game='queens', seconds=30):
    return (
        message_id, 100, game, 200, 300, 769,
        '2026-06-08', 100, seconds, True, 'raw',
    )


def test_batch_result_save_writes_every_row_in_one_call(db):
    saved = db.save_minigame_results([
        _row(1, seconds=30),
        _row(2, seconds=40),
    ])

    assert saved == 2
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

"""Owner-scoped pending LinkedIn results claim atomically into named results."""

import sqlite3

import pytest

from tests.minigames_test_utils import db


SOURCE = '\x00tle:linkedin:you:300'
OTHER_OWNER = '\x00tle:linkedin:you:301'


def save(db, *, name=SOURCE, guild=100, game='queens', puzzle=769,
         rated=True, override=None, seconds=30, label='Pending player',
         channel=200, message='900', raw='synthetic source', stored_at=1234):
    db.save_minigame_unresolved_result(
        guild, game, name, label, channel, puzzle, '2026-06-08',
        100, seconds, True, raw, is_rated=rated, stored_at=stored_at,
        source_message_id=message, rating_override=override)


def claim(db):
    return db.claim_minigame_unassigned_results(
        100, 'queens', SOURCE, 'sample player', 'Sample Player')


def rows(db, name=SOURCE, guild=100, game='queens'):
    return db.get_minigame_unresolved_results_for_name(guild, game, name)


def test_claim_preserves_score_provenance_and_rating_decisions(db):
    save(db, rated=False, override=False)
    save(db, puzzle=770, seconds=42, override=True, message=None)
    originals = rows(db)

    assert claim(db) == 2

    claimed = rows(db, 'sample player')
    assert len(claimed) == 2
    for old, new in zip(originals, claimed):
        expected = old._asdict()
        expected.update(normalized_name='sample player', external_name='Sample Player')
        assert new._asdict() == expected
    assert rows(db) == []
    assert claim(db) == 0
    assert rows(db, 'sample player') == claimed


@pytest.mark.parametrize('source_rated', [False, True])
@pytest.mark.parametrize('target_rated', [False, True])
@pytest.mark.parametrize('source_override,target_override', [
    (None, None), (False, True), (True, False), (False, None),
])
def test_collision_keeps_target_except_conservative_unrated_state(
        db, source_rated, target_rated, source_override, target_override):
    save(db, rated=source_rated, override=source_override)
    save(db, name='sample player', rated=target_rated, override=target_override,
         seconds=75, label='Existing label', channel=201, message='901',
         raw='existing synthetic source', stored_at=1200)
    expected = rows(db, 'sample player')[0]._asdict()
    expected['is_rated'] = int(source_rated and target_rated)

    assert claim(db) == 1

    assert rows(db) == []
    assert rows(db, 'sample player')[0]._asdict() == expected


def test_claim_stays_within_guild_game_and_owner(db):
    save(db)
    save(db, guild=101)
    save(db, game='tango')
    save(db, name=OTHER_OWNER)
    untouched = (rows(db, guild=101), rows(db, game='tango'), rows(db, OTHER_OWNER))

    assert claim(db) == 1

    assert rows(db) == []
    assert (rows(db, guild=101), rows(db, game='tango'), rows(db, OTHER_OWNER)) == untouched


def test_same_identity_is_a_noop(db):
    save(db)
    original = rows(db)

    assert db.claim_minigame_unassigned_results(
        100, 'queens', SOURCE, SOURCE, 'Changed label') == 0

    assert rows(db) == original


def test_claim_and_source_deletion_share_one_transaction(db):
    save(db)
    save(db, puzzle=770)
    statements = []
    db.conn.set_trace_callback(statements.append)

    assert claim(db) == 2

    db.conn.set_trace_callback(None)
    assert sum(statement.startswith('BEGIN') for statement in statements) == 1
    assert statements.count('COMMIT') == 1


def test_delete_failure_rolls_back_new_rows_and_collision_changes(db):
    save(db, rated=False)
    save(db, puzzle=770)
    save(db, name='sample player', rated=True)
    original_sources = rows(db)
    original_target = rows(db, 'sample player')
    db.conn.execute('''
        CREATE TRIGGER block_pending_delete
        BEFORE DELETE ON minigame_unresolved_result
        WHEN OLD.puzzle_number = 770
        BEGIN SELECT RAISE(ABORT, 'blocked pending delete'); END
    ''')
    db.conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match='blocked pending delete'):
        claim(db)

    assert rows(db) == original_sources
    assert rows(db, 'sample player') == original_target

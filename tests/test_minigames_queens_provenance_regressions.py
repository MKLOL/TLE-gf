"""Regression coverage for Queens source provenance and rating ownership."""

import asyncio
from types import SimpleNamespace

from tle import constants
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
)
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeDiscordMember,
    _FakeGuild,
    _FakeMessage,
    _QueensCommandsBase,
    _queens_number,
    db,
)


_X_NAME = 'Identity X'
_X_NORM = normalize_queens_name(_X_NAME)


def _share(puzzle, seconds):
    return (
        f'Queens #{puzzle} | 0:{seconds:02d}\n'
        'No mistakes & no hints\n'
        'First crowns: green orange blue\n'
        'lnkd.in/queens.'
    )


def _entry(name, seconds):
    return SimpleNamespace(
        linkedin_name=name,
        time_seconds=seconds,
        no_hints=True,
        no_mistakes=True,
    )


def _enable_and_link(db, *, guild_id=1, user_id=999, channel_id=10,
                     name=_X_NAME):
    db.set_guild_config(guild_id, 'queens', '1')
    db.set_minigame_channel(guild_id, 'queens', channel_id)
    db.set_minigame_player_link(
        guild_id, 'linkedin', user_id, name, normalize_queens_name(name),
        None, 1.0, user_id)


def _ctx(guild, author, channel_id=10):
    ctx = _QueensCommandsBase._make_ctx(guild, author)
    ctx.channel.id = channel_id
    return ctx


def _run_reparse(cog, ctx):
    asyncio.run(cog._cmd_reparse(ctx, QUEENS_GAME))


def _generic_queens_puzzles(db, table):
    return [
        row.puzzle_number
        for row in db.conn.execute(
            f'''
            SELECT puzzle_number
            FROM {table}
            WHERE guild_id = ? AND game = ?
            ORDER BY puzzle_number
            ''',
            ('1', 'queens'),
        ).fetchall()
    ]


def test_history_import_keeps_message_provenance_for_edit_and_delete(
        db, monkeypatch):
    """A history row remains attached to its Discord message after migration."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    _enable_and_link(db)
    original_content = _share(774, 26)
    db.save_raw_message(
        123, 1, 10, 999, '2026-06-13T12:00:00', original_content)
    db.save_imported_minigame_result(
        123, 1, 'queens', 10, 999, 774, '2026-06-13',
        100, 26, True, original_content)
    cog = Minigames(bot=None)

    cog._recompute_game_ratings(1, QUEENS_GAME)

    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source is not None
    assert (source.normalized_name, source.time_seconds) == (_X_NORM, 26)

    before = _FakeMessage(123, 1, 10, 999, original_content)
    after = _FakeMessage(123, 1, 10, 999, _share(774, 30))
    asyncio.run(cog.on_message_edit(before, after))
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source is not None
    assert (source.normalized_name, source.time_seconds) == (_X_NORM, 30)
    assert db.get_minigame_result(123).time_seconds == 30

    asyncio.run(cog.on_raw_message_delete(SimpleNamespace(
        guild_id=1, message_id=123)))
    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is None
    assert db.get_minigame_result(123) is None
    assert db.conn.execute(
        'SELECT 1 FROM minigame_raw_message WHERE message_id = ?',
        ('123',),
    ).fetchone() is None


def test_reparse_correction_preserves_override_and_moves_source_without_stale(
        db, monkeypatch):
    """Reparse updates one canonical source, including a changed puzzle."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    _enable_and_link(db)
    guild = _FakeGuild(1)
    moderator = _FakeDiscordMember(
        999, 'mod', roles=[
            SimpleNamespace(name=constants.TLE_MODERATOR),
        ])
    ctx = _ctx(guild, moderator)
    cog = Minigames(bot=None)
    db.save_raw_message(
        123, 1, 10, 999, '2026-06-13T12:00:00', _share(774, 26))

    _run_reparse(cog, ctx)
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source is not None
    first_stored_at = source.stored_at
    db.set_minigame_unresolved_result_rating(
        1, 'queens', _X_NORM, 774, False)

    db.update_raw_message(123, _share(774, 30))
    _run_reparse(cog, ctx)
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source is not None
    assert (source.puzzle_number, source.time_seconds) == (774, 30)
    assert (source.is_rated, source.rating_override) == (0, 0)
    assert source.stored_at == first_stored_at
    assert _generic_queens_puzzles(db, 'minigame_result') == []
    assert _generic_queens_puzzles(db, 'minigame_import_result') == []
    assert db.get_minigame_ratings(1, 'queens') == []

    db.update_raw_message(123, _share(775, 31))
    _run_reparse(cog, ctx)
    sources = db.get_minigame_unresolved_results_for_guild(1, 'queens')
    assert len(sources) == 1
    source = sources[0]
    assert (
        source.puzzle_number,
        source.puzzle_date,
        source.time_seconds,
        source.source_message_id,
    ) == (775, '2026-06-14', 31, '123')
    assert (source.is_rated, source.rating_override) == (0, 0)
    assert source.stored_at == first_stored_at
    # The unrated canonical source is the sole representation of this
    # message: neither its old nor its new puzzle may leak into a generic
    # live/import table or the persisted rating projection.
    assert _generic_queens_puzzles(db, 'minigame_result') == []
    assert _generic_queens_puzzles(db, 'minigame_import_result') == []
    assert db.get_minigame_ratings(1, 'queens') == []


def test_unrated_history_source_stays_unrated_across_unchanged_reparse(
        db, monkeypatch):
    """Reparse cannot rematerialize an opted-out history result."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    _enable_and_link(db)
    db.optout_minigame_user(
        1, 'queens', 999, 1.0, _X_NORM)
    guild = _FakeGuild(1)
    moderator = _FakeDiscordMember(
        999, 'mod',
        roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
    ctx = _ctx(guild, moderator)
    cog = Minigames(bot=None)
    db.save_raw_message(
        123, 1, 10, 999, '2026-06-13T12:00:00', _share(774, 26))

    _run_reparse(cog, ctx)
    first = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert first is not None
    assert (first.is_rated, first.rating_override) == (0, None)

    _run_reparse(cog, ctx)
    unchanged = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert unchanged is not None
    assert (unchanged.puzzle_number, unchanged.time_seconds) == (774, 26)
    assert (unchanged.is_rated, unchanged.rating_override) == (0, None)
    assert unchanged.stored_at == first.stored_at
    assert _generic_queens_puzzles(db, 'minigame_result') == []
    assert _generic_queens_puzzles(db, 'minigame_import_result') == []
    assert db.get_minigame_ratings(1, 'queens') == []


def test_reparse_removes_source_when_raw_message_is_no_longer_a_result(
        db, monkeypatch):
    """A valid history result disappears when its raw message stops parsing."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    _enable_and_link(db)
    guild = _FakeGuild(1)
    moderator = _FakeDiscordMember(
        999, 'mod',
        roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
    ctx = _ctx(guild, moderator)
    cog = Minigames(bot=None)
    db.save_raw_message(
        123, 1, 10, 999, '2026-06-13T12:00:00', _share(774, 26))

    _run_reparse(cog, ctx)
    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is not None

    db.update_raw_message(123, 'This message is no longer a Queens result.')
    _run_reparse(cog, ctx)

    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is None
    assert db.get_minigame_unresolved_results_for_guild(
        1, 'queens') == []
    assert _generic_queens_puzzles(db, 'minigame_result') == []
    assert _generic_queens_puzzles(db, 'minigame_import_result') == []
    assert db.get_minigame_ratings(1, 'queens') == []


def test_moderator_override_survives_optout_identity_round_trip_and_correction(
        db, monkeypatch):
    """Changing X -> Y -> X cannot erase the moderator's rated override."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    alice = _FakeDiscordMember(300, 'alice', 'Alice')
    moderator = _FakeDiscordMember(
        999, 'mod', 'Mod',
        roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
    guild = _FakeGuild(100, members=[alice, moderator])
    db.set_guild_config(100, 'queens', '1')
    db.set_minigame_player_link(
        100, 'linkedin', alice.id, _X_NAME, _X_NORM, None, 1.0, moderator.id)
    cog = Minigames(bot=None)

    asyncio.run(cog._cmd_queens_optout(_ctx(guild, alice, 200), QUEENS_GAME))
    cog._save_queens_external_result(
        100, QUEENS_GAME, 200, _entry(_X_NAME, 5), '2026-06-08', 'first')
    moderator_ctx = _ctx(guild, moderator, 200)
    asyncio.run(cog._cmd_queens_set_result_rating(
        moderator_ctx, QUEENS_GAME, f'{_X_NAME} 2026-06-08', is_rated=True))
    initial = db.get_minigame_unresolved_results_for_name(
        100, 'queens', _X_NORM)[0]
    assert (initial.is_rated, initial.rating_override) == (1, 1)
    first_stored_at = initial.stored_at

    asyncio.run(cog._cmd_queens_set(
        moderator_ctx, QUEENS_GAME, alice, 'Identity Y'))
    asyncio.run(cog._cmd_queens_set(
        moderator_ctx, QUEENS_GAME, alice, _X_NAME))
    cog._save_queens_external_result(
        100, QUEENS_GAME, 200, _entry(_X_NAME, 4), '2026-06-08', 'corrected')
    cog._sync_queens_materialized_results(
        100, QUEENS_GAME, migrate_legacy=False)
    corrected = db.get_minigame_unresolved_results_for_name(
        100, 'queens', _X_NORM)[0]

    assert (corrected.time_seconds, corrected.is_rated) == (4, 1)
    assert corrected.rating_override == 1
    assert corrected.stored_at == first_stored_at
    assert db.is_minigame_opted_out(100, 'queens', alice.id) is True
    materialized = db.get_minigame_result_for_user_puzzle(
        100, 'queens', alice.id, _queens_number('2026-06-08'))
    assert materialized is not None
    assert materialized.time_seconds == 4


def test_stale_former_owner_optout_does_not_unrate_current_owner_import(
        db, monkeypatch):
    """A name snapshot is subordinate to the name's current linked owner."""
    monkeypatch.setattr(cf_common, 'user_db', db)
    former = _FakeDiscordMember(300, 'former', 'Former')
    current = _FakeDiscordMember(301, 'current', 'Current')
    guild = _FakeGuild(100, members=[former, current])
    db.set_guild_config(100, 'queens', '1')
    db.set_minigame_player_link(
        100, 'linkedin', former.id, _X_NAME, _X_NORM, None, 1.0, former.id)
    cog = Minigames(bot=None)

    former_ctx = _ctx(guild, former, 200)
    asyncio.run(cog._cmd_queens_optout(former_ctx, QUEENS_GAME))
    asyncio.run(cog._cmd_queens_unregister(former_ctx, QUEENS_GAME, None))
    current_ctx = _ctx(guild, current, 200)
    asyncio.run(cog._cmd_queens_register(
        current_ctx, QUEENS_GAME, current, _X_NAME))

    # Registration normally clears this stale identity snapshot. Reintroduce
    # the state an older database or interrupted transfer can contain: the
    # former user remains opted out and still names X, while X is now linked
    # to a different, opted-in user.
    db.set_minigame_optout_identity(
        100, 'queens', former.id, _X_NORM)
    stale = db.get_minigame_optout_by_name(100, 'queens', _X_NORM)
    assert stale is not None
    assert stale.user_id == str(former.id)
    assert db.is_minigame_opted_out(
        100, 'queens', current.id) is False

    preview = cog._make_queens_import_preview(
        current_ctx, QUEENS_GAME, '2026-06-08',
        'You\nNo hints & no mistakes!\n0:05\n')
    saved = cog._save_queens_import(current_ctx, QUEENS_GAME, preview)

    assert saved.resolved == 1
    source = db.get_minigame_unresolved_results_for_name(
        100, 'queens', _X_NORM)[0]
    assert (source.time_seconds, source.is_rated) == (5, 1)
    result = db.get_minigame_result_for_user_puzzle(
        100, 'queens', current.id, _queens_number('2026-06-08'))
    assert result is not None
    assert result.time_seconds == 5

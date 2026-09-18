"""Permanent per-result Queens rated/unrated behavior."""

import asyncio
from types import SimpleNamespace

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import QUEENS_GAME, normalize_queens_name
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common
from tle.util.minigame_rating import compute_ratings

from tests.minigames_test_utils import (
    _FakeDiscordMember, _FakeGuild, _FakeMessage, _QueensCommandsBase,
    _queens_number, _row, db,
)


_ALICE_NAME = 'Alice LinkedIn'
_BOB_NAME = 'Bob LinkedIn'
_ALICE_NORM = normalize_queens_name(_ALICE_NAME)
_BOB_NORM = normalize_queens_name(_BOB_NAME)


class TestQueensUnrated(_QueensCommandsBase):
    @staticmethod
    def _seed(db, cog):
        db.set_guild_config(100, 'queens', '1')
        puzzle = _queens_number('2026-06-08')
        for user_id, name, normalized, seconds in (
                (300, _ALICE_NAME, _ALICE_NORM, 5),
                (301, _BOB_NAME, _BOB_NORM, 6)):
            db.set_minigame_player_link(
                100, 'linkedin', user_id, name, normalized,
                None, 1.0, 999)
            db.save_minigame_unresolved_result(
                100, 'queens', normalized, name, 200, puzzle,
                '2026-06-08', 100, seconds, True, 'raw')
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

    def test_source_upsert_preserves_explicit_rating_status(self, db):
        puzzle = _queens_number('2026-06-08')
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 5, True, 'first', is_rated=False)
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 4, True, 'corrected', is_rated=True)

        row = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (row.time_seconds, row.raw_content, row.is_rated) == (
            4, 'corrected', 0)

        assert db.set_minigame_unresolved_result_rating(
            100, 'queens', _ALICE_NORM, puzzle, True) == 1
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0].is_rated == 1

        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 3, True, 'new correction',
            is_rated=False)
        row = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (row.time_seconds, row.is_rated) == (3, 1)

    def test_moderator_unrates_and_rerates_one_day(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(100, members=[alice, bob, mod])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)
        self._seed(db, cog)

        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Alice LinkedIn 2026-06-08', is_rated=False))

        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert source.is_rated == 0
        assert {row.user_id for row in
                db.get_minigame_results_for_guild(100, 'queens')} == {'301'}
        assert {row.user_id for row in
                db.get_minigame_ratings(100, 'queens')} == {'301'}

        captured = []

        def capture(_guild, rows, title, **kwargs):
            captured.append((
                {row.user_id for row in rows},
                set(kwargs['unrated_keys']),
                title,
            ))
            return object()

        monkeypatch.setattr(
            minigames_module, '_get_queens_results_table_image_file',
            capture)
        asyncio.run(Minigames.queens_results.__wrapped__(
            cog, ctx, '769'))
        assert captured[-1][0] == {'301'}

        asyncio.run(Minigames.queens_results.__wrapped__(
            cog, ctx, '769', '+unrated'))
        assert captured[-1][0] == {'300', '301'}
        assert captured[-1][1] == {('300', 769)}
        assert '+ Unrated' in captured[-1][2]

        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Alice LinkedIn #769', is_rated=True))
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0].is_rated == 1
        assert {row.user_id for row in
                db.get_minigame_results_for_guild(100, 'queens')} == {
                    '300', '301'}

    def test_live_post_during_optout_stays_unrated_after_optin(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_channel(100, 'queens', 200)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'linkedin', alice.id, _ALICE_NAME, _ALICE_NORM,
            None, 1.0, alice.id)
        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME))

        opted_out_post = _FakeMessage(
            10, 100, 200, alice.id,
            'Queens #774 | 0:05\nNo hints & no mistakes')
        asyncio.run(cog.on_message(opted_out_post))

        first = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert first.is_rated == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []

        captured = []
        monkeypatch.setattr(
            minigames_module, '_get_queens_results_table_image_file',
            lambda _guild, rows, _title, **kwargs: captured.append((
                [row.user_id for row in rows],
                set(kwargs['unrated_keys']),
            )) or object())
        asyncio.run(Minigames.queens_results.__wrapped__(
            cog, ctx, '#774', '+unrated'))
        assert captured[-1] == (['300'], {('300', 774)})

        asyncio.run(cog._cmd_queens_optin(ctx, QUEENS_GAME))
        assert db.get_minigame_results_for_guild(100, 'queens') == []

        rated_post = _FakeMessage(
            11, 100, 200, alice.id,
            'Queens #775 | 0:06\nNo hints & no mistakes')
        asyncio.run(cog.on_message(rated_post))

        sources = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)
        assert [row.is_rated for row in sources] == [0, 1]
        assert [row.puzzle_number for row in
                db.get_minigame_results_for_guild(100, 'queens')] == [775]

    def test_unrate_matches_replay_without_result_and_rerate_restores(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        members = [
            _FakeDiscordMember(300 + index, name.casefold(), name)
            for index, name in enumerate(('Alice', 'Bob', 'Cara'))
        ]
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(100, members=[*members, mod])
        puzzle = _queens_number('2026-06-08')
        for member, seconds in zip(members, (5, 6, 7)):
            external = f'{member.display_name} LinkedIn'
            normalized = normalize_queens_name(external)
            db.set_minigame_player_link(
                100, 'linkedin', member.id, external, normalized,
                None, 1.0, mod.id)
            db.save_minigame_unresolved_result(
                100, 'queens', normalized, external, 200, puzzle,
                '2026-06-08', 100, seconds, True, external)
        cog = Minigames(bot=None)
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        def snapshot():
            return {
                row.user_id: (
                    row.rating, row.games, row.peak, row.last_delta,
                    row.last_puzzle,
                )
                for row in db.get_minigame_ratings(100, 'queens')
            }

        original = snapshot()
        ctx = self._make_ctx(guild, mod)
        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Bob LinkedIn #769', is_rated=False))

        rows = db.get_minigame_results_for_guild(100, 'queens')
        expected = compute_ratings(
            rows, **cog._rating_compute_kwargs(QUEENS_GAME))
        actual = snapshot()
        assert set(actual) == {'300', '302'}
        assert {
            user_id: (
                state.rating, state.games, state.peak, state.last_delta,
                state.last_puzzle,
            )
            for user_id, state in expected.items()
        } == actual

        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Bob LinkedIn 2026-06-08', is_rated=True))
        assert snapshot() == original


def test_unrated_rows_do_not_change_rated_competition_ranks():
    guild = _FakeGuild(100, members=[
        _FakeDiscordMember(300, 'alice', 'Alice'),
        _FakeDiscordMember(301, 'bob', 'Bob'),
    ])
    rows = [
        _row(1, 300, '2026-06-08', True, 5, number=769),
        _row(2, 301, '2026-06-08', True, 6, number=769),
    ]
    table_rows = minigames_module._queens_results_table_rows(
        guild, rows,
        puzzle_info={
            '300': SimpleNamespace(
                pre_rating=1200, delta=10, performance=1400),
            '301': SimpleNamespace(
                pre_rating=1200, delta=-10, performance=1000),
        },
        registrants={'300', '301'},
        unrated_keys={('300', 769)})

    assert table_rows[0][0] == '\N{EM DASH}'
    assert str(table_rows[0][1]).endswith('(Unrated)')
    assert table_rows[0][-2:] == ('', '')
    assert table_rows[1][0] == 1
    assert table_rows[1][-2:] != ('', '')

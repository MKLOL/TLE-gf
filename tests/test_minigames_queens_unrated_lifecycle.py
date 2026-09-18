"""Queens unrated lifecycle and Discord-message provenance coverage."""

import asyncio
import datetime as dt
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
    _FakeFollowup,
    _FakeGuild,
    _FakeMessage,
    _FakeResponse,
    _QueensCommandsBase,
    _queens_number,
    db,
)


_ALICE_NAME = 'Alice LinkedIn'
_ALICE_NORM = normalize_queens_name(_ALICE_NAME)


def _queens_share(seconds, puzzle=774):
    return (
        f'Queens #{puzzle} | 0:{seconds:02d}\n'
        'No mistakes & no hints\n'
        'First crowns: green orange blue\n'
        'lnkd.in/queens.'
    )


def _setup_live(db, *, opted_out=False):
    db.set_guild_config(1, 'queens', '1')
    db.set_minigame_channel(1, 'queens', 10)
    db.set_minigame_player_link(
        1, 'linkedin', 999, _ALICE_NAME, _ALICE_NORM, None, 1.0, 999)
    if opted_out:
        db.optout_minigame_user(
            1, 'queens', 999, 1.0, _ALICE_NORM)


def test_rated_live_share_edit_and_delete_update_canonical_source(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    _setup_live(db)
    cog = Minigames(bot=None)

    original = _FakeMessage(123, 1, 10, 999, _queens_share(26))
    asyncio.run(cog.on_message(original))
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source.time_seconds == 26
    assert source.is_rated == 1
    assert db.get_minigame_result(123).time_seconds == 26

    edited = _FakeMessage(123, 1, 10, 999, _queens_share(30))
    asyncio.run(cog.on_message_edit(original, edited))
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert source.time_seconds == 30
    assert db.get_minigame_result(123).time_seconds == 30

    asyncio.run(cog.on_raw_message_delete(SimpleNamespace(
        guild_id=1, message_id=123)))
    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is None
    assert db.get_minigame_result(123) is None
    assert db.get_minigame_ratings(1, 'queens') == []


def test_unrated_live_share_edit_and_delete_stay_unrated(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    _setup_live(db, opted_out=True)
    cog = Minigames(bot=None)

    original = _FakeMessage(123, 1, 10, 999, _queens_share(26))
    asyncio.run(cog.on_message(original))
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert (source.time_seconds, source.is_rated) == (26, 0)
    assert db.get_minigame_result(123) is None

    edited = _FakeMessage(123, 1, 10, 999, _queens_share(30))
    asyncio.run(cog.on_message_edit(original, edited))
    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert (source.time_seconds, source.is_rated) == (30, 0)
    assert db.get_minigame_result(123) is None

    asyncio.run(cog.on_raw_message_delete(SimpleNamespace(
        guild_id=1, message_id=123)))
    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is None


def test_live_puzzle_move_preserves_moderator_override(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    _setup_live(db, opted_out=True)
    cog = Minigames(bot=None)
    moderator = _FakeDiscordMember(
        999, 'mod', roles=[
            SimpleNamespace(name=constants.TLE_MODERATOR),
        ])
    ctx = _QueensCommandsBase._make_ctx(_FakeGuild(1), moderator)
    original = _FakeMessage(123, 1, 10, 999, _queens_share(26, 774))
    asyncio.run(cog.on_message(original))
    asyncio.run(cog._cmd_queens_set_result_rating(
        ctx, QUEENS_GAME, 'Alice LinkedIn #774', is_rated=True))
    first = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)

    edited = _FakeMessage(123, 1, 10, 999, _queens_share(30, 775))
    asyncio.run(cog.on_message_edit(original, edited))

    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert (
        source.puzzle_number, source.time_seconds, source.is_rated,
        source.rating_override, source.stored_at,
    ) == (775, 30, 1, 1, first.stored_at)
    assert [row.puzzle_number for row in
            db.get_minigame_results_for_guild(1, 'queens')] == [775]


def test_live_puzzle_move_onto_existing_result_removes_old_source(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    _setup_live(db, opted_out=True)
    cog = Minigames(bot=None)
    moderator = _FakeDiscordMember(
        999, 'mod', roles=[
            SimpleNamespace(name=constants.TLE_MODERATOR),
        ])
    ctx = _QueensCommandsBase._make_ctx(_FakeGuild(1), moderator)
    old = _FakeMessage(123, 1, 10, 999, _queens_share(26, 774))
    existing = _FakeMessage(456, 1, 10, 999, _queens_share(20, 775))
    asyncio.run(cog.on_message(old))
    asyncio.run(cog.on_message(existing))
    asyncio.run(cog._cmd_queens_set_result_rating(
        ctx, QUEENS_GAME, 'Alice LinkedIn #775', is_rated=True))

    moved = _FakeMessage(123, 1, 10, 999, _queens_share(30, 775))
    asyncio.run(cog.on_message_edit(old, moved))

    assert db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123) is None
    surviving = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 456)
    assert (surviving.puzzle_number, surviving.time_seconds) == (775, 20)
    assert [row.message_id for row in
            db.get_minigame_results_for_guild(1, 'queens')] == ['456']


def test_live_share_edit_keeps_linkedin_source_after_unregister(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    _setup_live(db)
    cog = Minigames(bot=None)
    original = _FakeMessage(123, 1, 10, 999, _queens_share(26))
    asyncio.run(cog.on_message(original))
    link = db.get_minigame_player_link(1, 'linkedin', 999)
    cog._delete_queens_materialized_results_for_link(1, QUEENS_GAME, link)
    db.delete_minigame_player_link(1, 'linkedin', 999)

    edited = _FakeMessage(123, 1, 10, 999, _queens_share(30))
    asyncio.run(cog.on_message_edit(original, edited))

    source = db.get_minigame_unresolved_result_for_source_message(
        1, 'queens', 123)
    assert (source.normalized_name, source.time_seconds) == (
        _ALICE_NORM, 30)
    assert db.get_minigame_results_for_guild(1, 'queens') == []


class TestQueensUnratedLifecycle(_QueensCommandsBase):
    @staticmethod
    def _members():
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        return alice, mod, _FakeGuild(100, members=[alice, mod])

    @staticmethod
    def _link(db):
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_player_link(
            100, 'linkedin', 300, _ALICE_NAME, _ALICE_NORM,
            None, 1.0, 999)

    @staticmethod
    def _entry(seconds):
        return SimpleNamespace(
            linkedin_name=_ALICE_NAME,
            time_seconds=seconds,
            no_hints=True,
            no_mistakes=True,
        )

    @staticmethod
    def _interaction(guild, user):
        return SimpleNamespace(
            id=555,
            guild=guild,
            user=user,
            channel_id=200,
            client=None,
            response=_FakeResponse(),
            followup=_FakeFollowup(),
        )

    def test_moderator_rate_overrides_active_optout_for_one_day(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice, mod, guild = self._members()
        self._link(db)
        cog = Minigames(bot=None)
        asyncio.run(cog._cmd_queens_optout(self._make_ctx(guild, alice), QUEENS_GAME))
        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, self._entry(5), '2026-06-08', 'first')

        mod_ctx = self._make_ctx(guild, mod)
        asyncio.run(cog._cmd_queens_set_result_rating(
            mod_ctx, QUEENS_GAME, 'Alice LinkedIn #769', is_rated=True))
        assert db.is_minigame_opted_out(100, 'queens', 300) is True
        assert {row.user_id for row in
                db.get_minigame_results_for_guild(100, 'queens')} == {'300'}

        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, self._entry(6), '2026-06-09', 'next')
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        sources = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)
        assert [row.is_rated for row in sources] == [1, 0]
        assert [row.puzzle_date for row in
                db.get_minigame_results_for_guild(100, 'queens')] == [
                    '2026-06-08']

    def test_confirmed_import_and_correction_stay_unrated_during_optout(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice, mod, guild = self._members()
        self._link(db)
        db.set_minigame_player_link(
            100, 'linkedin', mod.id, 'Moderator LinkedIn',
            normalize_queens_name('Moderator LinkedIn'),
            None, 1.0, mod.id)
        cog = Minigames(bot=None)
        ctx = self._make_ctx(guild, mod)
        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME, alice))

        def leaderboard(seconds):
            return (
                'Alice LinkedIn\n'
                '\N{NERD FACE}\N{GEM STONE} '
                'No hints & no mistakes!\n'
                f'0:0{seconds}\n'
            )

        preview = cog._make_queens_import_preview(
            ctx, QUEENS_GAME, '2026-06-08', leaderboard(5))
        saved = cog._save_queens_import(ctx, QUEENS_GAME, preview)
        assert saved.resolved == 1
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (source.time_seconds, source.is_rated) == (5, 0)

        correction = cog._make_queens_import_preview(
            ctx, QUEENS_GAME, '2026-06-08', leaderboard(4))
        cog._save_queens_import(ctx, QUEENS_GAME, correction)
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (source.time_seconds, source.is_rated) == (4, 0)
        assert db.get_minigame_results_for_guild(100, 'queens') == []

    def test_unlink_import_reregister_optin_keeps_new_result_unrated(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice, _mod, guild = self._members()
        self._link(db)
        cog = Minigames(bot=None)
        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, self._entry(5), '2026-06-08', 'old')
        ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME))
        asyncio.run(cog._cmd_queens_unregister(ctx, QUEENS_GAME, None))

        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, self._entry(6), '2026-06-09', 'during optout')
        asyncio.run(cog._cmd_queens_register(
            ctx, QUEENS_GAME, alice, _ALICE_NAME))
        asyncio.run(cog._cmd_queens_optin(ctx, QUEENS_GAME))

        sources = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)
        assert [row.is_rated for row in sources] == [1, 0]
        assert [row.puzzle_date for row in
                db.get_minigame_results_for_guild(100, 'queens')] == [
                    '2026-06-08']

    def test_new_link_claims_optout_era_unresolved_result_as_unrated(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice, _mod, guild = self._members()
        self._link(db)
        cog = Minigames(bot=None)
        ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME))
        asyncio.run(cog._cmd_queens_unregister(ctx, QUEENS_GAME, None))

        new_name = 'Alice New LinkedIn'
        new_norm = normalize_queens_name(new_name)
        entry = self._entry(6)
        entry.linkedin_name = new_name
        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, entry, '2026-06-09', 'unresolved')
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', new_norm)[0].is_rated == 1

        asyncio.run(cog._cmd_queens_register(ctx, QUEENS_GAME, alice, new_name))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', new_norm)[0]
        assert source.is_rated == 0
        asyncio.run(cog._cmd_queens_optin(ctx, QUEENS_GAME))
        assert db.get_minigame_results_for_guild(100, 'queens') == []

    def test_manual_add_preserves_unrated_state(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _alice, mod, guild = self._members()
        self._link(db)
        puzzle = _queens_number('2026-06-08')
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 5, True, 'old', is_rated=False)
        cog = Minigames(bot=None)

        asyncio.run(cog._cmd_queens_add(
            self._make_ctx(guild, mod), QUEENS_GAME,
            'Alice LinkedIn 2026-06-08 0:04'))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (source.time_seconds, source.is_rated) == (4, 0)
        assert db.get_minigame_results_for_guild(100, 'queens') == []

    def test_manual_add_preserves_moderator_override_and_first_seen(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _alice, mod, guild = self._members()
        self._link(db)
        puzzle = _queens_number('2026-06-08')
        db.optout_minigame_user(
            100, 'queens', 300, 1.0, _ALICE_NORM)
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 5, True, 'old', is_rated=False,
            stored_at=2.0)
        cog = Minigames(bot=None)
        ctx = self._make_ctx(guild, mod)
        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Alice LinkedIn #769', is_rated=True))

        asyncio.run(cog._cmd_queens_add(
            ctx, QUEENS_GAME, 'Alice LinkedIn 2026-06-08 0:04'))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert (
            source.time_seconds, source.is_rated,
            source.rating_override, source.stored_at,
        ) == (4, 1, 1, 2.0)

        db.set_minigame_optout_identity(
            100, 'queens', 300, 'different identity')
        cog._claim_queens_unresolved_results(
            100, QUEENS_GAME, 300, _ALICE_NORM)
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0].is_rated == 1

    def test_rate_and_unrate_support_legacy_ordinal_source_key(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _alice, mod, guild = self._members()
        self._link(db)
        day = dt.date(2026, 6, 8)
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200,
            day.toordinal(), day.isoformat(), 100, 5, True, 'legacy')
        cog = Minigames(bot=None)
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        ctx = self._make_ctx(guild, mod)

        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Alice LinkedIn #769', is_rated=False))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert source.is_rated == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []

        asyncio.run(cog._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, 'Alice LinkedIn 2026-06-08', is_rated=True))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert source.is_rated == 1
        assert [row.puzzle_number for row in
                db.get_minigame_results_for_guild(100, 'queens')] == [769]

    def test_slash_moderator_routes_target_optout_unrate_and_rate(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice, mod, guild = self._members()
        self._link(db)
        puzzle = _queens_number('2026-06-08')
        db.save_minigame_unresolved_result(
            100, 'queens', _ALICE_NORM, _ALICE_NAME, 200, puzzle,
            '2026-06-08', 100, 5, True, 'raw')
        cog = Minigames(bot=None)
        cog._sync_queens_materialized_results(100, QUEENS_GAME)

        optout_interaction = self._interaction(guild, mod)
        asyncio.run(cog.slash_queens_optout(
            optout_interaction, alice))
        assert db.is_minigame_opted_out(100, 'queens', alice.id) is True

        unrate_interaction = self._interaction(guild, mod)
        asyncio.run(cog.slash_queens_result_unrate(
            unrate_interaction, alice, '#769'))
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0]
        assert source.is_rated == 0

        denied_interaction = self._interaction(guild, alice)
        asyncio.run(cog.slash_queens_result_rate(
            denied_interaction, alice, '#769'))
        assert denied_interaction.followup.sent
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0].is_rated == 0

        rate_interaction = self._interaction(guild, mod)
        asyncio.run(cog.slash_queens_result_rate(
            rate_interaction, alice, '#769'))
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _ALICE_NORM)[0].is_rated == 1
        assert {row.user_id for row in
                db.get_minigame_results_for_guild(100, 'queens')} == {'300'}

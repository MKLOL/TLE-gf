"""Queens identity unlinking and explicit rating opt-out behavior."""
import asyncio
from types import SimpleNamespace

import pytest

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.cogs._minigame_queens import QUEENS_GAME, normalize_queens_name
from tle.cogs.minigames import Minigames, MinigameCogError

from tests.minigames_test_utils import (
    _queens_number, db, _FakeFollowup, _FakeGuild, _FakeDiscordMember,
    _FakeResponse, _QueensCommandsBase,
)


_NORM_ALICE = normalize_queens_name('Alice LinkedIn')
_NORM_BOB = normalize_queens_name('Bob LinkedIn')


class TestQueensOptOutDb:
    def test_optout_round_trips(self, db):
        assert db.is_minigame_opted_out(100, 'queens', '300') is False
        assert db.optout_minigame_user(100, 'queens', '300', 1.0) == 1
        # Idempotent: a second opt-out keeps the original row.
        assert db.optout_minigame_user(100, 'queens', '300', 2.0) == 0
        assert db.is_minigame_opted_out(100, 'queens', '300') is True
        assert {row.user_id for row in db.get_minigame_optouts(100, 'queens')} == {
            '300'}

        assert db.clear_minigame_optout(100, 'queens', '300') == 1
        assert db.is_minigame_opted_out(100, 'queens', '300') is False
        assert db.clear_minigame_optout(100, 'queens', '300') == 0

    def test_optout_is_scoped_per_guild_and_game(self, db):
        db.optout_minigame_user(100, 'queens', '300', 1.0)
        assert db.is_minigame_opted_out(100, 'queens', '300') is True
        assert db.is_minigame_opted_out(101, 'queens', '300') is False
        assert db.is_minigame_opted_out(100, 'akari', '300') is False


class TestQueensOptOut(_QueensCommandsBase):
    def _seed_two_players(self, db, cog):
        """Two registered players, each with a stored source result + rating."""
        db.set_guild_config(100, 'queens', '1')
        num = _queens_number('2026-06-08')
        db.save_minigame_unresolved_result(
            100, 'queens', _NORM_ALICE, 'Alice LinkedIn', 200, num,
            '2026-06-08', 100, 5, 1, 'raw')
        db.save_minigame_unresolved_result(
            100, 'queens', _NORM_BOB, 'Bob LinkedIn', 200, num,
            '2026-06-08', 100, 6, 1, 'raw')
        db.set_minigame_player_link(
            100, 'linkedin', 300, 'Alice LinkedIn', _NORM_ALICE, None, 1.0, 300)
        db.set_minigame_player_link(
            100, 'linkedin', 301, 'Bob LinkedIn', _NORM_BOB, None, 1.0, 301)
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

    @staticmethod
    def _rated_ids(db):
        return {row.user_id for row in db.get_minigame_ratings(100, 'queens')}

    def test_unregister_unlinks_without_creating_rating_optout(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)
        assert self._rated_ids(db) == {'300', '301'}

        ctx = self._make_ctx(guild, alice)
        asyncio.run(Minigames.queens_unregister.__wrapped__(cog, ctx, None))

        # Link and projections are gone, but unlinking is not a sticky opt-out.
        assert db.get_minigame_player_link(100, 'linkedin', alice.id) is None
        assert db.is_minigame_opted_out(100, 'queens', alice.id) is False
        assert self._rated_ids(db) == {'301'}
        # The stored source data remains keyed by the LinkedIn name.
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _NORM_ALICE)

        asyncio.run(cog._cmd_queens_register(
            ctx, QUEENS_GAME, alice, 'Alice LinkedIn'))
        assert db.is_minigame_opted_out(100, 'queens', alice.id) is False
        assert db.get_minigame_player_link(100, 'linkedin', alice.id) is not None
        assert self._rated_ids(db) == {'300', '301'}

    def test_optout_keeps_already_rated_history(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)

        # Opt out while a link still exists (simulates an import/forced re-link).
        db.optout_minigame_user(100, 'queens', 300, 1.0)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)
        assert self._rated_ids(db) == {'300', '301'}

        rows = db.get_minigame_results_for_guild(100, 'queens')
        kept = cog._filter_minigame_banned_rows(100, QUEENS_GAME, rows)
        assert {row.user_id for row in kept} == {'300', '301'}

    def test_moderator_set_preserves_rating_optout(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(100, members=[alice, bob, mod])
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)

        ctx_alice = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_optout(ctx_alice, QUEENS_GAME))
        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id) is not None
        assert self._rated_ids(db) == {'300', '301'}

        ctx_mod = self._make_ctx(guild, mod)
        asyncio.run(cog._cmd_queens_set(
            ctx_mod, QUEENS_GAME, alice, 'Alice LinkedIn'))

        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id).external_name == 'Alice LinkedIn'
        assert db.is_minigame_opted_out(100, 'queens', alice.id) is True
        assert self._rated_ids(db) == {'300', '301'}
        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert {row.user_id for row in rows} == {'300', '301'}

    def test_moderator_can_opt_out_another_player(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(100, members=[alice, bob, mod])
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)

        ctx_bob = self._make_ctx(guild, bob)
        with pytest.raises(MinigameCogError, match='Only'):
            asyncio.run(cog._cmd_queens_optout(ctx_bob, QUEENS_GAME, alice))
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is False

        ctx_mod = self._make_ctx(guild, mod)
        asyncio.run(Minigames.queens_optout.__wrapped__(
            cog, ctx_mod, alice))
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is True
        assert self._rated_ids(db) == {'300', '301'}

    def test_prefix_optout_and_optin_keep_link_and_existing_rating(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)

        ctx = self._make_ctx(guild, alice)
        asyncio.run(Minigames.queens_optout.__wrapped__(cog, ctx))

        assert db.is_minigame_opted_out(100, 'queens', alice.id) is True
        assert db.get_minigame_player_link(100, 'linkedin', alice.id) is not None
        assert self._rated_ids(db) == {'300', '301'}
        with pytest.raises(MinigameCogError, match='already opted out'):
            asyncio.run(Minigames.queens_optout.__wrapped__(cog, ctx))

        asyncio.run(Minigames.queens_optin.__wrapped__(cog, ctx))

        assert db.is_minigame_opted_out(100, 'queens', alice.id) is False
        assert db.get_minigame_player_link(100, 'linkedin', alice.id) is not None
        assert self._rated_ids(db) == {'300', '301'}
        with pytest.raises(MinigameCogError, match='not opted out'):
            asyncio.run(Minigames.queens_optin.__wrapped__(cog, ctx))

    def test_unlink_and_reregister_do_not_change_explicit_optout(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        cog = Minigames(bot=None)
        self._seed_two_players(db, cog)
        ctx = self._make_ctx(guild, alice)

        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME))
        asyncio.run(cog._cmd_queens_unregister(ctx, QUEENS_GAME, None))
        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id) is None
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is True

        with pytest.raises(MinigameCogError, match='already linked'):
            asyncio.run(cog._cmd_queens_register(
                ctx, QUEENS_GAME, alice, 'Bob LinkedIn'))
        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id) is None
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is True

        asyncio.run(cog._cmd_queens_register(
            ctx, QUEENS_GAME, alice, 'Alice LinkedIn'))
        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id) is not None
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is True
        assert self._rated_ids(db) == {'300', '301'}

        asyncio.run(cog._cmd_queens_optin(ctx, QUEENS_GAME))
        assert self._rated_ids(db) == {'300', '301'}

    def test_import_while_opted_out_stays_stored_and_unrated(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'linkedin', alice.id, 'Alice LinkedIn', _NORM_ALICE,
            None, 1.0, alice.id)
        asyncio.run(cog._cmd_queens_optout(ctx, QUEENS_GAME))

        entry = SimpleNamespace(
            linkedin_name='Alice LinkedIn',
            time_seconds=5,
            no_hints=True,
            no_mistakes=True,
        )
        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, entry, '2026-06-08', 'raw')
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', _NORM_ALICE)[0].is_rated == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []
        assert db.get_minigame_ratings(100, 'queens') == []

        asyncio.run(cog._cmd_queens_optin(ctx, QUEENS_GAME))
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        assert db.get_minigame_results_for_guild(100, 'queens') == []

        cog._save_queens_external_result(
            100, QUEENS_GAME, 200, entry, '2026-06-09', 'next')
        next_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', _NORM_ALICE)[-1]
        assert next_source.is_rated == 1
        cog._sync_queens_materialized_results(100, QUEENS_GAME)
        assert [row.puzzle_date for row in
                db.get_minigame_results_for_guild(100, 'queens')] == [
                    '2026-06-09']

    def test_slash_optout_and_optin(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'linkedin', alice.id, 'Alice LinkedIn', _NORM_ALICE,
            None, 1.0, alice.id)

        def interaction():
            return SimpleNamespace(
                id=999,
                guild=guild,
                user=alice,
                channel_id=200,
                client=None,
                response=_FakeResponse(),
                followup=_FakeFollowup(),
            )

        optout_interaction = interaction()
        asyncio.run(cog.slash_queens_optout(optout_interaction))
        assert optout_interaction.response.deferred is True
        assert optout_interaction.followup.sent
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is True

        optin_interaction = interaction()
        asyncio.run(cog.slash_queens_optin(optin_interaction))
        assert optin_interaction.response.deferred is True
        assert optin_interaction.followup.sent
        assert db.is_minigame_opted_out(
            100, 'queens', alice.id) is False

    def test_sync_keeps_rated_source_during_active_optout(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        num = _queens_number('2026-06-08')
        db.save_minigame_unresolved_result(
            100, 'queens', _NORM_ALICE, 'Alice LinkedIn', 200, num,
            '2026-06-08', 100, 5, 1, 'raw')
        db.set_minigame_player_link(
            100, 'linkedin', 300, 'Alice LinkedIn', _NORM_ALICE, None, 1.0, 300)
        db.optout_minigame_user(100, 'queens', 300, 1.0)

        cog = Minigames(bot=None)
        cog._sync_queens_materialized_results(100, QUEENS_GAME)

        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert {row.user_id for row in rows} == {'300'}

"""Akari delegated-admin tier and bulk date deletion (delete / clean)."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.cogs._minigame_akari import puzzle_date_for
from tle.cogs.minigames import Minigames, MinigameCogError

from tests.minigames_test_utils import (
    _GAME, db, _FakeGuild, _FakeDiscordMember, _QueensCommandsBase,
)


def _save_akari_result(db, message_id, user_id, puzzle_number, *,
                       time_seconds=90, is_perfect=True, accuracy=100,
                       guild=1, channel=10):
    db.save_minigame_result(
        message_id, guild, _GAME, channel, user_id, puzzle_number,
        puzzle_date_for(puzzle_number).isoformat(), accuracy, time_seconds,
        is_perfect, 'raw')


def _patch_embeds(monkeypatch):
    for name in ('embed_success', 'embed_neutral', 'embed_alert'):
        monkeypatch.setattr(
            minigames_module.discord_common, name,
            lambda desc: SimpleNamespace(description=desc))


class TestAkariAdmins(_QueensCommandsBase):
    def test_akari_admin_allowlist_management(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        helper = _FakeDiscordMember(300, 'helper', 'Helper')
        guild = _FakeGuild(1, members=[mod, helper])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.akari_admins_add.__wrapped__(cog, ctx, helper))

        assert json.loads(db.get_guild_config(
            1, minigames_module._AKARI_ADMINS_KEY)) == ['300']
        assert cog._has_akari_mod_access(1, helper) is True
        assert cog._has_server_mod_role(helper) is False
        # The queens admin tier is a separate allowlist.
        assert cog._has_queens_mod_access(1, helper) is False

        helper_ctx = self._make_ctx(guild, helper)
        asyncio.run(Minigames.akari_admins.__wrapped__(cog, helper_ctx))
        assert 'Helper' in helper_ctx.sent['embed'].description

        with pytest.raises(MinigameCogError, match='can change'):
            asyncio.run(Minigames.akari_admins_add.__wrapped__(
                cog, helper_ctx, mod))
        with pytest.raises(MinigameCogError, match='can change'):
            asyncio.run(Minigames.akari_admins_remove.__wrapped__(
                cog, helper_ctx, mod))

        asyncio.run(Minigames.akari_admins_remove.__wrapped__(
            cog, ctx, helper))
        assert db.get_guild_config(
            1, minigames_module._AKARI_ADMINS_KEY) is None
        assert cog._has_akari_mod_access(1, helper) is False

    def test_akari_admins_add_is_idempotent(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_ADMIN)])
        helper = _FakeDiscordMember(300, 'helper', 'Helper')
        guild = _FakeGuild(1, members=[mod, helper])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.akari_admins_add.__wrapped__(cog, ctx, helper))
        asyncio.run(Minigames.akari_admins_add.__wrapped__(cog, ctx, helper))
        assert json.loads(db.get_guild_config(
            1, minigames_module._AKARI_ADMINS_KEY)) == ['300']
        assert 'already has' in ctx.sent['embed'].description

        asyncio.run(Minigames.akari_admins_remove.__wrapped__(
            cog, ctx, helper))
        asyncio.run(Minigames.akari_admins_remove.__wrapped__(
            cog, ctx, helper))
        assert 'was not an extra' in ctx.sent['embed'].description

    def test_akari_mod_role_error_mentions_akari_admin(self):
        message = Minigames._akari_mod_role_error_message()
        assert 'Akari admin access' in message


class TestAkariBulkDeletion(_QueensCommandsBase):
    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    def test_delete_removes_all_results_for_puzzle(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        self._enable(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        _save_akari_result(db, 1, alice.id, 446)
        _save_akari_result(db, 2, bob.id, 446)
        _save_akari_result(db, 3, alice.id, 447)
        # Imported rows for the same puzzle must be wiped too.
        db.save_imported_minigame_result(
            4, 1, _GAME, 10, bob.id, 446,
            puzzle_date_for(446).isoformat(), 100, 80, True, 'imported')

        asyncio.run(Minigames.akari_delete.__wrapped__(cog, ctx, '#446'))

        remaining = db.get_minigame_results_for_guild(1, _GAME)
        assert [(row.user_id, row.puzzle_number) for row in remaining] == [
            ('300', 447),
        ]
        # Snapshot recomputed: only alice still has results.
        assert [row.user_id
                for row in db.get_minigame_ratings(1, _GAME)] == ['300']
        assert '#446' in ctx.sent['embed'].description

    def test_delete_accepts_date_selector_and_requires_results(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        self._enable(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(1, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        _save_akari_result(db, 1, alice.id, 446)
        date_arg = puzzle_date_for(446).strftime('%d%m%Y')

        asyncio.run(Minigames.akari_delete.__wrapped__(cog, ctx, date_arg))
        assert db.get_minigame_results_for_guild(1, _GAME) == []

        with pytest.raises(MinigameCogError, match='No Daily Akari results'):
            asyncio.run(Minigames.akari_delete.__wrapped__(
                cog, ctx, date_arg))
        with pytest.raises(MinigameCogError, match='Could not parse'):
            asyncio.run(Minigames.akari_delete.__wrapped__(
                cog, ctx, 'not-a-date'))
        with pytest.raises(MinigameCogError, match='Usage'):
            asyncio.run(Minigames.akari_delete.__wrapped__(cog, ctx, None))

    def test_clean_removes_date_range(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        self._enable(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        _save_akari_result(db, 1, alice.id, 446)
        _save_akari_result(db, 2, bob.id, 447)
        _save_akari_result(db, 3, alice.id, 448)
        db.save_imported_minigame_result(
            4, 1, _GAME, 10, bob.id, 447,
            puzzle_date_for(447).isoformat(), 100, 80, True, 'imported')

        asyncio.run(Minigames.akari_clean.__wrapped__(
            cog, ctx, '#446', '#447'))

        remaining = db.get_minigame_results_for_guild(1, _GAME)
        assert [(row.user_id, row.puzzle_number) for row in remaining] == [
            ('300', 448),
        ]
        assert [row.user_id
                for row in db.get_minigame_ratings(1, _GAME)] == ['300']
        assert '2 day(s)' in ctx.sent['embed'].description

    def test_clean_single_date_and_validation(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _patch_embeds(monkeypatch)
        self._enable(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(1, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        _save_akari_result(db, 1, alice.id, 446)
        _save_akari_result(db, 2, alice.id, 447)

        with pytest.raises(MinigameCogError, match='cannot be before'):
            asyncio.run(Minigames.akari_clean.__wrapped__(
                cog, ctx, '#447', '#446'))
        with pytest.raises(MinigameCogError, match='Usage'):
            asyncio.run(Minigames.akari_clean.__wrapped__(
                cog, ctx, None))

        asyncio.run(Minigames.akari_clean.__wrapped__(cog, ctx, '#446'))
        remaining = db.get_minigame_results_for_guild(1, _GAME)
        assert [row.puzzle_number for row in remaining] == [447]

        with pytest.raises(MinigameCogError, match='No Daily Akari results'):
            asyncio.run(Minigames.akari_clean.__wrapped__(cog, ctx, '#446'))

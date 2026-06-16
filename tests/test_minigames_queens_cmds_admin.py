"""Queens commands: admin / backfill / add-remove."""
import asyncio
import datetime as dt
import json
import sqlite3
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_14_0, upgrade_1_15_0
from tle.util.db.minigame_db import (
    MinigameDbMixin, merged_minigame_winners, diff_merged_winners,
)
from tle.cogs._minigame_common import (
    compute_vs,
    compute_streak,
    compute_longest_streak,
    compute_top,
    parse_date_args,
    resolve_scoring,
    strip_codeblock,
)
from tle.cogs._minigame_akari import AKARI_GAME, parse_akari_message, puzzle_date_for
from tle.cogs._minigame_guessgame import (
    GUESSGAME_GAME,
    parse_guessgame_message,
    guessgame_score_matchup,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
    parse_queens_leaderboard,
    parse_queens_message,
    rank_queens_participants,
)
from tle.cogs.minigames import Minigames
from tle.cogs.minigames import (
    MinigameCogError,
    _SlashCtx,
    _akari_puzzle_table_rows,
    _akari_rating_table_rows,
    _format_akari_puzzle_table,
    _get_akari_puzzle_table_image_file,
    _get_akari_puzzle_table_image,
    _maybe_parse_puzzle_selector,
)
from tle.util.minigame_rating import RatingState

from tests.minigames_test_utils import (
    _GAME, _queens_number, _row, db, FakeMinigameDb,
    _FakeGuild, _FakeChannel, _FakeAttachment, _FakeAuthor, _FakeDiscordMember,
    _FakeMessage, _FakeMember, _FakeFollowup, _FakeResponse, _FakeInteraction,
    _FakeGroup, _QueensCommandsBase, _AkariRatingHelpers,
)


class TestQueensCommandsAdmin(_QueensCommandsBase):
    def test_register_anonymous_without_name_uses_private_modal(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+anon'))

        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        view = ctx.sent['kwargs']['view']
        assert view.requester_id == alice.id
        assert view.children

        captured = {}

        class Response:
            async def send_modal(self, modal):
                captured['modal'] = modal

            async def send_message(self, content=None, *, embed=None,
                                   ephemeral=False, **kwargs):
                captured['content'] = content
                captured['embed'] = embed
                captured['ephemeral'] = ephemeral
                captured['kwargs'] = kwargs

        interaction = SimpleNamespace(
            guild=guild,
            user=alice,
            channel_id=200,
            response=Response(),
        )
        asyncio.run(view.children[0].callback(interaction))

        modal = captured['modal']
        modal.linkedin_name.value = 'Alice LinkedIn'
        asyncio.run(modal.on_submit(interaction))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        assert cog._queens_pending_registrations[('100', '300')].name == (
            'Alice LinkedIn')
        assert captured['content'] is None
        assert captured['ephemeral'] is True
        assert 'Alice LinkedIn' in captured['embed'].description

    def test_connection_set_requires_and_stores_profile_url(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(999, 'mod', 'Mod')
        guild = _FakeGuild(100, members=[mod])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='profile URL'):
            asyncio.run(Minigames.queens_connection_set.__wrapped__(
                cog, ctx, linkedin='Linked User'))

        asyncio.run(Minigames.queens_connection_set.__wrapped__(
            cog, ctx,
            linkedin='Linked User https://www.linkedin.com/in/linked/'))

        account = cog._get_queens_connection_account(100)
        assert account == {
            'name': 'Linked User',
            'url': 'https://www.linkedin.com/in/linked/',
        }
        instruction = cog._queens_connection_instruction(100)
        assert 'https://www.linkedin.com/in/linked/' in instruction
        assert 'Linked User' not in instruction

    def test_queens_admin_allowlist_management(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        helper = _FakeDiscordMember(300, 'helper', 'Helper')
        guild = _FakeGuild(100, members=[mod, helper])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_admins_add.__wrapped__(
            cog, ctx, helper))

        assert json.loads(db.get_guild_config(
            100, minigames_module._QUEENS_ADMINS_KEY)) == ['300']
        assert cog._has_queens_mod_access(100, helper) is True
        assert cog._has_server_mod_role(helper) is False

        helper_ctx = self._make_ctx(guild, helper)
        asyncio.run(Minigames.queens_admins.__wrapped__(cog, helper_ctx))
        assert helper_ctx.sent['embed'] is not None

        with pytest.raises(MinigameCogError, match='can change'):
            asyncio.run(Minigames.queens_admins_add.__wrapped__(
                cog, helper_ctx, mod))

        asyncio.run(Minigames.queens_admins_remove.__wrapped__(
            cog, ctx, helper))
        assert db.get_guild_config(
            100, minigames_module._QUEENS_ADMINS_KEY) is None
        assert cog._has_queens_mod_access(100, helper) is False

    def test_queens_admin_can_register_other_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        helper = _FakeDiscordMember(300, 'helper', 'Helper')
        alice = _FakeDiscordMember(301, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[helper, alice])
        ctx = self._make_ctx(guild, helper)
        cog = Minigames(bot=None)
        db.set_guild_config(
            100, minigames_module._QUEENS_ADMINS_KEY,
            json.dumps(['300']))

        assert cog._resolve_queens_registrar_target(ctx, alice) is alice
        with pytest.raises(MinigameCogError, match='Only'):
            cog._resolve_registrar_target(ctx, alice)

    def test_backfill_single_user_from_attachment(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[mod, alice])
        ctx = self._make_ctx(guild, mod)
        ctx.message = SimpleNamespace(attachments=[
            _FakeAttachment('queens_history.json', json.dumps([
                {
                    'linkedin_name': 'Alice LinkedIn',
                    'puzzle_number': _queens_number('2026-06-08'),
                    'puzzle_date': '2026-06-08',
                    'time_seconds': 5,
                    'no_hints': True,
                    'no_mistakes': True,
                },
            ])),
        ])
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, mod.id)

        asyncio.run(Minigames.queens_backfill.__wrapped__(
            cog, ctx, 'alice'))

        row = db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08'))
        assert row is not None
        assert row.time_seconds == 5
        assert ctx.sent['embed'] is not None

    def test_backfill_all_registered_users_from_attachment(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        cara = _FakeDiscordMember(302, 'cara', 'Cara')
        unknown = _FakeDiscordMember(303, 'unknown', 'Unknown')
        guild = _FakeGuild(100, members=[mod, alice, bob, cara, unknown])
        ctx = self._make_ctx(guild, mod)
        ctx.message = SimpleNamespace(attachments=[
            _FakeAttachment('queens_history.json', json.dumps([
                {
                    'linkedin_name': 'Alice LinkedIn',
                    'puzzle_number': _queens_number('2026-06-08'),
                    'puzzle_date': '2026-06-08',
                    'time_seconds': 8,
                    'no_hints': True,
                    'no_mistakes': True,
                },
                {
                    'linkedin_name': 'Alice LinkedIn',
                    'puzzle_number': _queens_number('2026-06-09'),
                    'puzzle_date': '2026-06-09',
                    'time_seconds': 4,
                    'no_hints': True,
                    'no_mistakes': True,
                },
                {
                    'linkedin_name': 'Bob LinkedIn',
                    'puzzle_number': _queens_number('2026-06-09'),
                    'puzzle_date': '2026-06-09',
                    'time_seconds': 7,
                    'no_hints': True,
                    'no_mistakes': False,
                },
                {
                    'linkedin_name': 'Cara LinkedIn',
                    'puzzle_number': 'bad',
                    'time_seconds': 10,
                },
                {
                    'linkedin_name': 'Cara LinkedIn',
                    'puzzle_number': _queens_number('2026-06-09'),
                    'puzzle_date': '2026-06-08',
                    'time_seconds': 9,
                    'no_hints': True,
                    'no_mistakes': True,
                },
                {
                    'linkedin_name': 'Unknown LinkedIn',
                    'puzzle_number': _queens_number('2026-06-09'),
                    'puzzle_date': '2026-06-09',
                    'time_seconds': 3,
                    'no_hints': True,
                    'no_mistakes': True,
                },
            ])),
        ])
        cog = Minigames(bot=None)
        for member, name in (
                (alice, 'Alice LinkedIn'),
                (bob, 'Bob LinkedIn'),
                (cara, 'Cara LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', member.id, name, normalize_queens_name(name),
                None, 1.0, mod.id)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 8, True, 'existing')

        asyncio.run(Minigames.queens_backfill.__wrapped__(
            cog, ctx, '+all'))

        alice_saved = db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-09'))
        bob_saved = db.get_minigame_result_for_user_puzzle(
            100, 'queens', bob.id, _queens_number('2026-06-09'))
        assert alice_saved.time_seconds == 4
        assert bob_saved.time_seconds == 7
        assert bob_saved.accuracy == 0
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', cara.id, _queens_number('2026-06-09')) is None
        unknown_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Unknown LinkedIn'))
        assert [(row.external_name, row.time_seconds) for row in unknown_source] == [
            ('Unknown LinkedIn', 3),
        ]
        assert ctx.sent['embed'] is not None

        claimed = cog._cmd_queens_register_link(ctx, unknown, 'Unknown LinkedIn')
        assert claimed == 1
        unknown_saved = db.get_minigame_result_for_user_puzzle(
            100, 'queens', unknown.id, _queens_number('2026-06-09'))
        assert unknown_saved is not None
        assert unknown_saved.time_seconds == 3

    def test_register_rejects_url_input(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='Profile URLs'):
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, 'Alice',
                linkedin='https://www.linkedin.com/in/alice/'))

        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_rejects_duplicate_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, bob.id)

        with pytest.raises(MinigameCogError, match='already linked'):
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, 'alice', linkedin='linkedin'))

        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_duplicate_name_uses_discord_owner_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, bob)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Existing LinkedIn',
            normalize_queens_name('Existing LinkedIn'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER, 1.0, alice.id)

        with pytest.raises(MinigameCogError, match='already linked to Alice'):
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, 'Existing', linkedin='LinkedIn'))

    def test_queens_link_command_is_not_registered(self):
        assert not hasattr(Minigames, 'queens_link')

    def test_add_accepts_registered_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, alice.id)

        asyncio.run(Minigames.queens_add.__wrapped__(
            cog, ctx,
            args='Alice LinkedIn 769 0:05 No hints & no mistakes'))

        row = db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08'))
        assert row is not None
        assert row.time_seconds == 5
        assert row.is_perfect == 1
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn'))
        assert [(row.time_seconds, row.is_perfect) for row in source] == [(5, 1)]
        assert db.get_minigame_rating(100, 'queens', alice.id) is not None
        assert ctx.sent['embed'] is not None

    def test_remove_accepts_registered_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, alice.id)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 5, True, 'source')
        cog._sync_queens_materialized_results(100)

        asyncio.run(Minigames.queens_remove.__wrapped__(
            cog, ctx, args='Alice LinkedIn #769'))

        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn')) == []
        assert ctx.sent['embed'] is not None

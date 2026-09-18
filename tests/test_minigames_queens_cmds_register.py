"""Queens direct registration and moderator overwrite tests."""

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs._minigame_queens import QUEENS_GAME
from tle import constants
from tle.cogs import _mgimpl_queenscmdb as queens_cmd_impl
from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import normalize_queens_name
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _queens_number,
    db,  # noqa: F401 - imported pytest fixture
    _FakeDiscordMember,
    _FakeFollowup,
    _FakeGuild,
    _FakeResponse,
    _QueensCommandsBase,
)


class TestQueensCommandsRegister(_QueensCommandsBase):
    def test_stats_and_streak_use_queens_dates(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        rendered = []
        fake_file = SimpleNamespace(filename='queens-stats.png')

        def fake_queens_stats(results, display_name, *, title_suffix='',
                              weekdays=None, as_of_date=None,
                              game_label=None):
            rendered.append({
                'dates': [
                    minigames_module.normalize_puzzle_date(row.puzzle_date)
                    .isoformat()
                    for row in results
                ],
                'display_name': display_name,
                'title_suffix': title_suffix,
                'as_of_date': as_of_date,
            })
            return fake_file

        monkeypatch.setattr(
            minigames_module, 'plot_queens_stats', fake_queens_stats)
        logical_today = dt.date(2026, 6, 11)
        monkeypatch.setattr(
            queens_cmd_impl, '_queens_current_puzzle_date',
            lambda: logical_today)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5, True, 100)
        self._save_queens_result(db, 2, alice.id, '2026-06-09', 9, False, 0)
        self._save_queens_result(db, 3, alice.id, '2026-06-10', 4, True, 100)
        self._save_queens_result(db, 4, alice.id, '2026-06-11', 6, True, 100)

        asyncio.run(cog._cmd_queens_stats(ctx, QUEENS_GAME))
        assert rendered[-1]['dates'] == [
            '2026-06-08', '2026-06-09', '2026-06-10', '2026-06-11',
        ]
        assert rendered[-1]['as_of_date'] == logical_today
        asyncio.run(cog._cmd_queens_stats(ctx, QUEENS_GAME, 'week'))
        assert rendered[-1]['dates'] == [
            '2026-06-08', '2026-06-09', '2026-06-10', '2026-06-11',
        ]
        asyncio.run(cog._cmd_queens_stats(ctx, QUEENS_GAME, '+dow=mon,wed'))
        assert rendered[-1]['dates'] == ['2026-06-08', '2026-06-10']

        asyncio.run(cog._cmd_queens_streak(ctx, QUEENS_GAME, 'week'))
        assert '**2** consecutive clean day(s)' in ctx.sent['embed'].description
        assert 'Latest result: **2026-06-11**' in ctx.sent['embed'].description

    def test_register_self_saves_immediately_and_claims_results(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        name = 'Artsiom Savich'
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name(name), name, 200,
            _queens_number('2026-06-08'), '2026-06-08',
            100, 5, True, 'source')

        asyncio.run(Minigames.queens_register.__wrapped__(
            Minigames(bot=None), ctx, 'Artsiom', linkedin='Savich'))

        link = db.get_minigame_player_link(100, 'linkedin', alice.id)
        assert link.external_name == name
        assert link.normalized_name == normalize_queens_name(name)
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is not None
        assert db.get_minigame_rating(100, 'queens', alice.id) is not None
        assert 'Claimed 1 stored Queens result' in ctx.sent['embed'].description

    def test_register_other_saves_immediately(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        ctx = self._make_ctx(_FakeGuild(100, members=[mod, bob]), mod)

        asyncio.run(Minigames.queens_register.__wrapped__(
            Minigames(bot=None), ctx, '+username',
            linkedin='bob Bob LinkedIn'))

        link = db.get_minigame_player_link(100, 'linkedin', bob.id)
        assert link.external_name == 'Bob LinkedIn'

    def test_register_again_requires_unregister_even_for_same_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = self._make_ctx(_FakeGuild(100, members=[alice]), alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Alice', linkedin='LinkedIn'))
        with pytest.raises(MinigameCogError, match='unregister'):
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, 'Alice', linkedin='LinkedIn'))
        with pytest.raises(MinigameCogError, match='unregister'):
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, 'Different', linkedin='Name'))

        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id).external_name == 'Alice LinkedIn'

    def test_unregister_allows_self_to_register_again(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = self._make_ctx(_FakeGuild(100, members=[alice]), alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Old', linkedin='Name'))
        asyncio.run(Minigames.queens_unregister.__wrapped__(cog, ctx))
        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'New', linkedin='Name'))

        link = db.get_minigame_player_link(100, 'linkedin', alice.id)
        assert link.external_name == 'New Name'
        assert not db.is_minigame_opted_out(100, 'queens', alice.id)

    def test_set_remains_moderator_overwrite_path(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'linkedin', bob.id, 'Old Name',
            normalize_queens_name('Old Name'), None, 1.0, mod.id)

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, self._make_ctx(guild, mod), 'bob',
            linkedin='New Name +anon'))

        link = db.get_minigame_player_link(100, 'linkedin', bob.id)
        assert link.external_name == 'New Name'
        assert link.external_url == (
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER)

    @pytest.mark.parametrize(
        ('first', 'rest', 'expected'),
        [
            ('bob', 'Bob LinkedIn', 'bob Bob LinkedIn'),
            ('+bob', 'Bob LinkedIn', '+bob Bob LinkedIn'),
            ('<@301>', 'Bob LinkedIn', '<@301> Bob LinkedIn'),
        ],
    )
    def test_only_explicit_username_flag_selects_another_user(
            self, db, monkeypatch, first, rest, expected):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        ctx = self._make_ctx(_FakeGuild(100, members=[alice, bob]), alice)

        asyncio.run(Minigames.queens_register.__wrapped__(
            Minigames(bot=None), ctx, first, linkedin=rest))

        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id).external_name == expected
        assert db.get_minigame_player_link(100, 'linkedin', bob.id) is None

    def test_anonymous_registration_is_direct_and_private(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = self._make_ctx(_FakeGuild(100, members=[alice]), alice)

        asyncio.run(Minigames.queens_register.__wrapped__(
            Minigames(bot=None), ctx, '+anon', linkedin='Alice LinkedIn'))

        link = db.get_minigame_player_link(100, 'linkedin', alice.id)
        assert link.external_url == (
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER)
        assert 'Anonymous' in ctx.sent['embed'].description
        assert 'Alice LinkedIn' not in ctx.sent['embed'].description

    def test_slash_register_self_saves_immediately(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        interaction = SimpleNamespace(
            id=999,
            guild=guild,
            user=alice,
            channel_id=200,
            client=None,
            response=_FakeResponse(),
            followup=_FakeFollowup(),
        )

        asyncio.run(Minigames(bot=None).slash_queens_register(
            interaction, 'Alice LinkedIn'))

        assert db.get_minigame_player_link(
            100, 'linkedin', alice.id).external_name == 'Alice LinkedIn'
        assert interaction.response.deferred is True
        assert interaction.followup.sent

    def test_scraper_commands_and_helpers_are_removed(self):
        removed = (
            'queens_connection', 'queens_install', 'queens_login',
            'queens_play', 'queens_update', 'queens_settings',
            'queens_state_path', 'slash_queens_update',
            '_run_queens_scraper', '_run_queens_connect',
            '_queens_daily_update_check',
        )
        for name in removed:
            assert not hasattr(Minigames, name)

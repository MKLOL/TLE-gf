"""Privacy regressions for Queens registration and public link lists."""

import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import normalize_queens_name
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeDiscordMember,
    _FakeGuild,
    _QueensCommandsBase,
    db,  # noqa: F401 - imported pytest fixture
)


class TestQueensPrivacy(_QueensCommandsBase):
    def test_anonymous_registration_prompt_rejects_other_users(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)

        asyncio.run(Minigames.queens_register.__wrapped__(
            Minigames(bot=None), ctx, '+anon'))
        view = ctx.sent['kwargs']['view']
        response = SimpleNamespace(sent=None, modal=None)

        async def send_message(content=None, *, ephemeral=False, **_kwargs):
            response.sent = (content, ephemeral)

        async def send_modal(modal):
            response.modal = modal

        response.send_message = send_message
        response.send_modal = send_modal
        interaction = SimpleNamespace(
            user=bob, guild=guild, channel_id=200, response=response)

        asyncio.run(view.children[0].callback(interaction))

        assert response.modal is None
        assert response.sent == (
            'Only the requester can use this registration prompt.', True)

    def test_links_sort_anonymous_players_as_anonymous(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        low_id_anon = _FakeDiscordMember(100, 'low', 'Low ID')
        public = _FakeDiscordMember(300, 'public', 'Public')
        high_id_anon = _FakeDiscordMember(900, 'high', 'High ID')
        guild = _FakeGuild(
            100, members=[low_id_anon, public, high_id_anon])

        db.set_minigame_player_link(
            100, 'linkedin', low_id_anon.id, 'Aaron Secret',
            normalize_queens_name('Aaron Secret'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER,
            1.0, low_id_anon.id)
        db.set_minigame_player_link(
            100, 'linkedin', public.id, 'Bob LinkedIn',
            normalize_queens_name('Bob LinkedIn'), None, 1.0, public.id)
        db.set_minigame_player_link(
            100, 'linkedin', high_id_anon.id, 'Zulu Secret',
            normalize_queens_name('Zulu Secret'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER,
            1.0, high_id_anon.id)

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs:
                pages.extend(page_list))

        cog = Minigames(bot=None)
        ctx = self._make_ctx(guild, public)
        asyncio.run(cog._cmd_queens_links(ctx, QUEENS_GAME))

        assert len(pages) == 1
        assert pages[0][1].description.splitlines() == [
            '- Low ID: `Anonymous`',
            '- High ID: `Anonymous`',
            '- Public: `Bob LinkedIn`',
        ]
        assert 'Aaron Secret' not in pages[0][1].description
        assert 'Zulu Secret' not in pages[0][1].description

    def test_duplicate_anonymous_name_does_not_reveal_owner(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        secret_name = 'Hidden LinkedIn'
        db.set_minigame_player_link(
            100, 'linkedin', alice.id, secret_name,
            normalize_queens_name(secret_name),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER,
            1.0, alice.id)

        with pytest.raises(MinigameCogError) as exc_info:
            asyncio.run(Minigames.queens_register.__wrapped__(
                Minigames(bot=None), self._make_ctx(guild, bob),
                'Hidden', linkedin='LinkedIn'))

        message = str(exc_info.value)
        assert message == 'That Queens name is already taken.'
        assert 'Alice' not in message
        assert secret_name not in message

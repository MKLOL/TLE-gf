"""Tests for the self-service ``;akari giveup`` submission command."""

import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    db, _FakeDiscordMember, _FakeGuild, _QueensCommandsBase,
)


_GIVEUP_SECONDS = 67 * 3600 + 67 * 60 + 67


class TestAkariGiveup(_QueensCommandsBase):
    @staticmethod
    def _ctx(db, monkeypatch, *, message_id=900, user_id=300):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda description: SimpleNamespace(description=description))
        db.set_guild_config(1, 'akari', '1')
        author = _FakeDiscordMember(user_id, 'alice', 'Alice')
        ctx = TestAkariGiveup._make_ctx(
            _FakeGuild(1, members=[author]), author)
        ctx.message = SimpleNamespace(id=message_id)
        return ctx

    @pytest.mark.parametrize(
        'selector', ['446', '#446', '2026-03-27', '27032026'])
    def test_records_literal_zero_percent_submission(
            self, db, monkeypatch, selector):
        ctx = self._ctx(db, monkeypatch)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.akari_giveup.__wrapped__(cog, ctx, selector))

        row = db.get_minigame_result_for_user_puzzle(
            1, 'akari', ctx.author.id, 446)
        assert row is not None
        assert row.message_id == '900'
        assert row.channel_id == '200'
        assert row.user_id == str(ctx.author.id)
        assert row.puzzle_date == '2026-03-27'
        assert row.accuracy == 0
        assert row.is_perfect == 0
        assert row.time_seconds == _GIVEUP_SECONDS
        assert '\U0001f3af 0% \U0001f553 67:67:67' in row.raw_content
        assert '**0%**' in ctx.sent['embed'].description
        assert '**67:67:67**' in ctx.sent['embed'].description

    def test_rejects_duplicate_user_puzzle_result(self, db, monkeypatch):
        ctx = self._ctx(db, monkeypatch)
        db.save_minigame_result(
            1, 1, 'akari', 200, ctx.author.id, 446, '2026-03-27',
            100, 60, True, 'existing result')
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='already has a result'):
            asyncio.run(Minigames.akari_giveup.__wrapped__(
                cog, ctx, '#446'))

        row = db.get_minigame_result_for_user_puzzle(
            1, 'akari', ctx.author.id, 446)
        assert row.message_id == '1'
        assert row.accuracy == 100

    def test_rejects_banned_user(self, db, monkeypatch):
        ctx = self._ctx(db, monkeypatch)
        db.ban_akari_user(1, ctx.author.id, 1.0, 999, 'spam')
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='banned from posting'):
            asyncio.run(Minigames.akari_giveup.__wrapped__(
                cog, ctx, '#446'))

        assert db.get_minigame_result_for_user_puzzle(
            1, 'akari', ctx.author.id, 446) is None

    def test_requires_date_or_number(self, db, monkeypatch):
        ctx = self._ctx(db, monkeypatch)

        with pytest.raises(MinigameCogError, match='akari giveup'):
            asyncio.run(Minigames.akari_giveup.__wrapped__(
                Minigames(bot=None), ctx))

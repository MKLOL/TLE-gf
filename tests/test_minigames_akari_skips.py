"""Tests for ``;akari skips`` and its slash-command counterpart."""

import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.cogs._mgimpl_akaric import _akari_skipped_puzzles
from tle.cogs._minigame_akari import puzzle_date_for
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeDiscordMember, _FakeGuild, _QueensCommandsBase, db,
)


_GUILD = 1
_CHANNEL = 10
_USER = 300


def _save_result(db, message_id, user_id, puzzle_number, *,
                 guild_id=_GUILD, imported=False, accuracy=100,
                 is_perfect=True):
    args = (
        message_id, guild_id, 'akari', _CHANNEL, user_id, puzzle_number,
        puzzle_date_for(puzzle_number).isoformat(), accuracy, 90,
        is_perfect, 'raw',
    )
    if imported:
        db.save_imported_minigame_result(*args)
    else:
        db.save_minigame_result(*args)


def _enable(db):
    db.set_guild_config(_GUILD, 'akari', '1')
    db.set_minigame_channel(_GUILD, 'akari', _CHANNEL)


class TestAkariSkippedPuzzleHelper:
    def test_returns_only_missing_concluded_puzzles_newest_first(self):
        rows = [
            SimpleNamespace(puzzle_number=446),
            # An imperfect result is still a submission.
            SimpleNamespace(puzzle_number=448, accuracy=82, is_perfect=False),
            SimpleNamespace(puzzle_number=450),
            # Current and future rows are not skipped or used as gaps.
            SimpleNamespace(puzzle_number=451),
            SimpleNamespace(puzzle_number=452),
            SimpleNamespace(puzzle_number='bad'),
            SimpleNamespace(puzzle_number=0),
        ]

        first, skipped = _akari_skipped_puzzles(rows, current_puzzle=451)

        assert first == 446
        assert skipped == [449, 447]
        assert 445 not in skipped
        assert 451 not in skipped

    def test_current_submission_can_be_the_tracking_boundary(self):
        first, skipped = _akari_skipped_puzzles(
            [SimpleNamespace(puzzle_number=451)], current_puzzle=451)

        assert first == 451
        assert skipped == []

    def test_no_valid_submission_returns_empty_marker(self):
        assert _akari_skipped_puzzles(
            [SimpleNamespace(puzzle_number=452)], current_puzzle=451
        ) == (None, [])


class TestAkariSkipsCommand(_QueensCommandsBase):
    def _setup(self, db, monkeypatch, current_puzzle=451):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module, 'expected_puzzle_number',
            lambda _date: current_puzzle)
        _enable(db)
        alice = _FakeDiscordMember(_USER, 'alice', 'Alice')
        guild = _FakeGuild(_GUILD, members=[alice])
        return Minigames(bot=object()), self._make_ctx(guild, alice), alice

    @staticmethod
    def _capture_pages(monkeypatch):
        captured = {}

        def capture(bot, channel, pages, **kwargs):
            captured['bot'] = bot
            captured['channel'] = channel
            captured['pages'] = pages
            captured['kwargs'] = kwargs

        monkeypatch.setattr(
            minigames_module.paginator, 'paginate', capture)
        return captured

    def test_prefix_command_uses_merged_rows_and_lists_gaps(self, db, monkeypatch):
        cog, ctx, alice = self._setup(db, monkeypatch)
        # First result is deliberately imperfect: it still starts tracking.
        _save_result(
            db, 1, alice.id, 446, accuracy=82, is_perfect=False)
        _save_result(db, 2, alice.id, 448, imported=True)
        _save_result(db, 3, alice.id, 450)
        # Other users and guilds cannot fill Alice's missing days.
        _save_result(db, 4, 301, 447)
        _save_result(db, 5, alice.id, 449, guild_id=2)
        captured = self._capture_pages(monkeypatch)

        asyncio.run(Minigames.akari_skips.__wrapped__(cog, ctx, None))

        pages = captured['pages']
        assert len(pages) == 1
        embed = pages[0][1]
        assert embed.title == 'Daily Akari skipped days — Alice (2 days)'
        assert 'Since first submission: **#446**' in embed.description
        assert '**#449**' in embed.description
        assert '**#447**' in embed.description
        assert embed.description.index('**#449**') < embed.description.index(
            '**#447**')
        assert '**#448**' not in embed.description
        assert '**#451**' not in embed.description
        assert puzzle_date_for(449).isoformat() in embed.description
        assert puzzle_date_for(447).strftime('%A') in embed.description
        assert captured['kwargs']['author_id'] == alice.id

    def test_long_skip_history_paginates_like_rating_history(
            self, db, monkeypatch):
        cog, ctx, alice = self._setup(
            db, monkeypatch, current_puzzle=464)
        _save_result(db, 1, alice.id, 446)
        captured = self._capture_pages(monkeypatch)

        asyncio.run(cog._cmd_akari_skips(ctx, alice))

        pages = captured['pages']
        assert len(pages) == 2
        assert '**#463**' in pages[0][1].description
        assert '**#449**' in pages[0][1].description
        assert '**#448**' not in pages[0][1].description
        assert '**#448**' in pages[1][1].description
        assert '**#447**' in pages[1][1].description
        assert all(
            page[1].title.endswith('(17 days)') for page in pages)

    def test_no_skips_is_a_successful_empty_state(self, db, monkeypatch):
        cog, ctx, alice = self._setup(
            db, monkeypatch, current_puzzle=449)
        for message_id, puzzle_number in enumerate(
                (446, 447, 448), start=1):
            _save_result(db, message_id, alice.id, puzzle_number)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda description: SimpleNamespace(description=description))
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda *_args, **_kwargs: pytest.fail(
                'An empty skip history should not open the paginator.'))

        asyncio.run(cog._cmd_akari_skips(ctx, alice))

        description = ctx.sent['embed'].description
        assert 'has no skipped Daily Akari days' in description
        assert '**#446**' in description
        assert puzzle_date_for(446).isoformat() in description

    def test_no_results_and_visibility_guards(self, db, monkeypatch):
        cog, ctx, alice = self._setup(db, monkeypatch)

        with pytest.raises(MinigameCogError, match='No Daily Akari results'):
            asyncio.run(cog._cmd_akari_skips(ctx, alice))

        _save_result(db, 1, alice.id, 446)
        db.unregister_akari_user(_GUILD, alice.id, 1.0)
        with pytest.raises(MinigameCogError, match='has not opted in'):
            asyncio.run(cog._cmd_akari_skips(ctx, alice))

        db.register_akari_user(_GUILD, alice.id)
        db.ban_akari_user(_GUILD, alice.id, 2.0, 999, 'test')
        with pytest.raises(MinigameCogError, match='is banned'):
            asyncio.run(cog._cmd_akari_skips(ctx, alice))

    def test_slash_command_defaults_to_interaction_user(self, db, monkeypatch):
        cog, _ctx, alice = self._setup(db, monkeypatch)
        _save_result(db, 1, alice.id, 446)
        captured = self._capture_pages(monkeypatch)

        class Response:
            deferred = False

            async def defer(self):
                self.deferred = True

        interaction = SimpleNamespace(
            response=Response(),
            user=alice,
            guild=_FakeGuild(_GUILD, members=[alice]),
            channel_id=200,
            client=object(),
            id=9999,
        )

        asyncio.run(cog.slash_akari_skips(interaction))

        assert interaction.response.deferred is True
        assert captured['kwargs']['author_id'] == alice.id
        assert captured['pages'][0][1].title.startswith(
            'Daily Akari skipped days — Alice')

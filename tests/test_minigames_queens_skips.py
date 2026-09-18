"""Tests for the ``;queens skips`` prefix command."""

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs import _mgimpl_queenscmd as queens_cmd_module
from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import normalize_queens_name
from tle.cogs._minigame_queens_cog import (
    _queens_date_for_puzzle_number,
    _queens_puzzle_number_for_date,
)
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeDiscordMember, _FakeGuild, _QueensCommandsBase, db,
)


_GUILD = 100
_CHANNEL = 200
_USER = 300
_NAME = 'Alice LinkedIn'
_NORMALIZED_NAME = normalize_queens_name(_NAME)


def _puzzle_number(puzzle_date):
    return _queens_puzzle_number_for_date(
        dt.date.fromisoformat(puzzle_date))


class TestQueensSkipsCommand(_QueensCommandsBase):
    def _setup(self, db, monkeypatch, *, current_date='2026-06-13'):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            queens_cmd_module,
            '_queens_current_puzzle_date',
            lambda: dt.date.fromisoformat(current_date),
        )
        db.set_guild_config(_GUILD, 'queens', '1')
        alice = _FakeDiscordMember(_USER, 'alice', 'Alice')
        db.set_minigame_player_link(
            _GUILD, 'linkedin', alice.id, _NAME, _NORMALIZED_NAME,
            None, 1.0, alice.id)
        guild = _FakeGuild(_GUILD, members=[alice])
        return Minigames(bot=object()), self._make_ctx(guild, alice), alice

    @staticmethod
    def _save_source(
            db, puzzle_date, *, normalized_name=_NORMALIZED_NAME,
            external_name=_NAME, puzzle_number=None, is_rated=True):
        if puzzle_number is None:
            puzzle_number = _puzzle_number(puzzle_date)
        db.save_minigame_unresolved_result(
            _GUILD, 'queens', normalized_name, external_name, _CHANNEL,
            puzzle_number, puzzle_date, 100, 90, True, 'raw',
            is_rated=is_rated)

    @staticmethod
    def _capture_pages(monkeypatch):
        captured = {}

        def capture(bot, channel, pages, **kwargs):
            captured['bot'] = bot
            captured['channel'] = channel
            captured['pages'] = pages
            captured['kwargs'] = kwargs

        monkeypatch.setattr(minigames_module.paginator, 'paginate', capture)
        return captured

    def test_prefix_lists_linked_source_gaps_newest_first(
            self, db, monkeypatch):
        cog, ctx, alice = self._setup(db, monkeypatch)
        self._save_source(db, '2026-06-08')
        # Rating opt-out still means the player submitted that day. The stored
        # legacy number must be canonicalized from its source date.
        self._save_source(
            db, '2026-06-10',
            puzzle_number=dt.date(2026, 6, 10).toordinal(),
            is_rated=False)
        self._save_source(db, '2026-06-12')
        self._save_source(db, '2026-06-13')  # current/open boundary
        self._save_source(db, '2026-06-14')  # future row is ignored
        # Another LinkedIn identity cannot fill Alice's missing day.
        self._save_source(
            db, '2026-06-11', normalized_name='bob linkedin',
            external_name='Bob LinkedIn')
        captured = self._capture_pages(monkeypatch)

        asyncio.run(Minigames.queens_skips.__wrapped__(cog, ctx, None))

        pages = captured['pages']
        assert len(pages) == 1
        embed = pages[0][1]
        assert embed.title == 'LinkedIn Queens skipped days — Alice (2 days)'
        assert 'Since first submission: **#769**' in embed.description
        assert '**#772**' in embed.description
        assert '**#770**' in embed.description
        assert embed.description.index('**#772**') < embed.description.index(
            '**#770**')
        assert '**#771**' not in embed.description
        assert '**#773**' not in embed.description
        assert '**#774**' not in embed.description
        assert '2026-06-11' in embed.description
        assert dt.date(2026, 6, 9).strftime('%A') in embed.description
        assert captured['kwargs']['author_id'] == alice.id

    def test_legacy_materialized_rows_are_migrated_before_gap_detection(
            self, db, monkeypatch):
        cog, ctx, alice = self._setup(
            db, monkeypatch, current_date='2026-06-11')
        self._save_source(db, '2026-06-08')
        legacy_date = dt.date(2026, 6, 10)
        db.save_minigame_result(
            99, _GUILD, 'queens', _CHANNEL, alice.id,
            legacy_date.toordinal(), legacy_date.isoformat(),
            100, 80, True, 'legacy')
        captured = self._capture_pages(monkeypatch)

        asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        description = captured['pages'][0][1].description
        assert '**#770**' in description
        assert '**#771**' not in description
        sources = db.get_minigame_unresolved_results_for_name(
            _GUILD, 'queens', _NORMALIZED_NAME)
        assert any(
            row.puzzle_number == _puzzle_number('2026-06-10')
            for row in sources)

    def test_long_skip_history_paginates(self, db, monkeypatch):
        cog, ctx, alice = self._setup(
            db, monkeypatch, current_date='2026-06-27')
        self._save_source(db, '2026-06-08')
        captured = self._capture_pages(monkeypatch)

        asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        pages = captured['pages']
        assert len(pages) == 2
        assert '**#787**' in pages[0][1].description
        assert '**#773**' in pages[0][1].description
        assert '**#772**' in pages[1][1].description
        assert '**#770**' in pages[1][1].description
        assert all(page[1].title.endswith('(18 days)') for page in pages)

    def test_no_skips_is_a_successful_empty_state(self, db, monkeypatch):
        cog, ctx, alice = self._setup(db, monkeypatch)
        for day in range(8, 13):
            self._save_source(db, f'2026-06-{day:02d}')
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda description: SimpleNamespace(description=description))
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda *_args, **_kwargs: pytest.fail(
                'An empty skip history should not open the paginator.'))

        asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        description = ctx.sent['embed'].description
        assert 'has no skipped LinkedIn Queens days' in description
        assert '**#769**' in description
        assert '2026-06-08' in description

    def test_no_results_registration_ban_and_feature_guards(
            self, db, monkeypatch):
        cog, ctx, alice = self._setup(db, monkeypatch)
        with pytest.raises(
                MinigameCogError, match='No LinkedIn Queens results'):
            asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        db.delete_minigame_player_link(_GUILD, 'linkedin', alice.id)
        with pytest.raises(MinigameCogError, match='is not registered'):
            asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        db.set_minigame_player_link(
            _GUILD, 'linkedin', alice.id, _NAME, _NORMALIZED_NAME,
            None, 1.0, alice.id)
        self._save_source(db, '2026-06-08')
        db.ban_minigame_user(
            _GUILD, 'queens', alice.id, 2.0, 999, 'test')
        with pytest.raises(MinigameCogError, match='is banned'):
            asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))

        db.set_guild_config(_GUILD, 'queens', '0')
        with pytest.raises(MinigameCogError, match='is not enabled'):
            asyncio.run(cog._cmd_queens_skips(ctx, QUEENS_GAME, alice))


def test_queens_date_number_round_trip_for_skip_rendering():
    for puzzle_number in (769, 770, 1000):
        assert _queens_puzzle_number_for_date(
            _queens_date_for_puzzle_number(puzzle_number)
        ) == puzzle_number

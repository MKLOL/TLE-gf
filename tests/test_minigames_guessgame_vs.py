"""GuessGame vs-command tests."""
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
    _FakeGroup, _QueensCommandsBase,
)


class TestGuessGameVsCommand:
    def _make_ctx(self, guild_id, requester, members):
        guild = _FakeGuild(guild_id, members=members)
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        return SimpleNamespace(
            guild=guild,
            author=requester,
            channel=object(),
            send=send,
            sent=sent,
        )

    def _save_guessgame_result(self, db, message_id, user_id, puzzle_number,
                               puzzle_date, accuracy, time_seconds):
        db.save_minigame_result(
            message_id, 1, 'guessgame', 10, user_id, puzzle_number, puzzle_date,
            accuracy, time_seconds, int(accuracy == 6), f'#{puzzle_number}'
        )

    def test_vs_keeps_original_summary_embed(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        member1 = _FakeDiscordMember(10, 'alice', 'Alice')
        member2 = _FakeDiscordMember(20, 'bob', 'Bob')
        ctx = self._make_ctx(1, member1, [member1, member2])
        cog = Minigames(bot=object())

        self._save_guessgame_result(db, 1, member1.id, 1200, '2026-03-03', 6, 7)
        self._save_guessgame_result(db, 2, member2.id, 1200, '2026-03-05', 4, 7)
        self._save_guessgame_result(db, 3, member1.id, 1201, '2026-03-04', 3, 7)
        self._save_guessgame_result(db, 4, member2.id, 1201, '2026-03-06', 0, 5)

        asyncio.run(cog._cmd_vs(ctx, GUESSGAME_GAME, member1, member2, 'p>=1200'))

        embed = ctx.sent['embed']
        assert embed.title == 'GuessThe.Game Head to Head'
        assert 'Puzzles: **2**' in embed.description
        assert 'Alice' in embed.description
        assert 'Bob' in embed.description

    def test_results_uses_paginated_side_by_side_pages_with_links(self, db, monkeypatch):
        import tle.cogs.minigames as minigames_module

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        member1 = _FakeDiscordMember(10, 'alice', 'Alice')
        member2 = _FakeDiscordMember(20, 'bob', 'Bob')
        ctx = self._make_ctx(1, member1, [member1, member2])
        cog = Minigames(bot=object())

        for offset, puzzle in enumerate(range(1200, 1211), start=1):
            self._save_guessgame_result(
                db, 1000 + offset, member1.id, puzzle, '2026-03-26', 6, 7)
            self._save_guessgame_result(
                db, 2000 + offset, member2.id, puzzle, '2026-03-26', 3, 7)

        captured = {}

        def fake_paginate(bot, channel, pages, **kwargs):
            captured['bot'] = bot
            captured['channel'] = channel
            captured['pages'] = pages
            captured['kwargs'] = kwargs

        monkeypatch.setattr(minigames_module.paginator, 'paginate', fake_paginate)

        asyncio.run(cog._cmd_guessgame_matchups(ctx, member1, member2, 'p>=1200'))

        assert captured['bot'] is cog.bot
        assert captured['channel'] is ctx.channel
        assert captured['kwargs']['author_id'] == ctx.author.id
        assert captured['kwargs']['set_pagenum_footers'] is True
        assert len(captured['pages']) == 2

        first_embed = captured['pages'][0][1]
        second_embed = captured['pages'][1][1]
        assert first_embed.title == 'GuessThe.Game Head to Head'
        assert 'Puzzles: **11**' in first_embed.description
        assert len(first_embed.fields) == 2
        assert first_embed.fields[0]['name'] == 'Alice'
        assert first_embed.fields[1]['name'] == 'Bob'
        assert '[#1210](https://guessthe.game/p/1210)' in first_embed.fields[0]['value']
        assert '[#1210](https://guessthe.game/p/1210)' in first_embed.fields[1]['value']
        assert '[#1200](https://guessthe.game/p/1200)' in second_embed.fields[0]['value']

    def test_results_groups_historical_results_by_puzzle_number_and_filters_puzzles(self, db, monkeypatch):
        import tle.cogs.minigames as minigames_module

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        member1 = _FakeDiscordMember(10, 'alice', 'Alice')
        member2 = _FakeDiscordMember(20, 'bob', 'Bob')
        ctx = self._make_ctx(1, member1, [member1, member2])
        cog = Minigames(bot=object())

        self._save_guessgame_result(db, 1, member1.id, 1199, '2026-03-01', 6, 7)
        self._save_guessgame_result(db, 2, member2.id, 1199, '2026-03-02', 4, 7)
        self._save_guessgame_result(db, 3, member1.id, 1200, '2026-03-03', 6, 7)
        self._save_guessgame_result(db, 4, member2.id, 1200, '2026-03-05', 4, 7)
        self._save_guessgame_result(db, 5, member1.id, 1201, '2026-03-04', 3, 7)
        self._save_guessgame_result(db, 6, member2.id, 1201, '2026-03-06', 0, 5)

        captured = {}

        def fake_paginate(bot, channel, pages, **kwargs):
            captured['pages'] = pages

        monkeypatch.setattr(minigames_module.paginator, 'paginate', fake_paginate)

        asyncio.run(cog._cmd_guessgame_matchups(ctx, member1, member2, 'p>=1200'))

        assert len(captured['pages']) == 1
        embed = captured['pages'][0][1]
        assert 'Puzzles: **2**' in embed.description
        assert '[#1201](https://guessthe.game/p/1201)' in embed.fields[0]['value']
        assert '[#1200](https://guessthe.game/p/1200)' in embed.fields[1]['value']
        assert '[#1199](https://guessthe.game/p/1199)' not in embed.fields[0]['value']

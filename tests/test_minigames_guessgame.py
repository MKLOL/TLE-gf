"""GuessGame parsing / scoring tests."""
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


class TestGuessGameParsing:
    def test_parse_single_result_green(self):
        results = parse_guessgame_message(
            '<#123> #1412\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e8 \U0001f7e9 \u2b1c \u2b1c \u2b1c\n\n'
            '#Gamer\nhttps://GuessThe.Game/p/1412'
        )
        assert len(results) == 1
        r = results[0]
        assert r.puzzle_number == 1412
        assert r.is_perfect is False
        assert r.accuracy == 4   # 7 - green_pos(3) = 4
        assert r.time_seconds == 2  # yellow at pos 2

    def test_parse_perfect_first_guess(self):
        results = parse_guessgame_message(
            '<#123> #1412\n'
            '\U0001f3ae \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1412'
        )
        assert len(results) == 1
        assert results[0].is_perfect is True
        assert results[0].accuracy == 6  # 7 - 1

    def test_parse_no_green(self):
        results = parse_guessgame_message(
            '<#123> #1411\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e8 \U0001f7e8\n\n'
            '#ScreenshotSleuth\nhttps://GuessThe.Game/p/1411'
        )
        assert len(results) == 1
        r = results[0]
        assert r.accuracy == 0       # no green
        assert r.time_seconds == 5   # first yellow at pos 5
        assert r.is_perfect is False

    def test_parse_multi_result_message(self):
        content = (
            '<#123> #1407\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1407\n\n'
            '<#123> #1412\n'
            '\U0001f3ae \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1412'
        )
        results = parse_guessgame_message(content)
        assert len(results) == 2
        assert results[0].puzzle_number == 1407
        assert results[0].accuracy == 3  # 7-4
        assert results[1].puzzle_number == 1412
        assert results[1].accuracy == 6  # 7-1
        assert results[1].is_perfect is True

    def test_parse_channel_mention_dump_without_guessgame_text(self):
        content = (
            '<#1435360903137853652> #1427\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c\n\n'
            '#ScreenshotSleuth\n\n'
            '<#1435360903137853652> #1432\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e8 \U0001f7e9 \u2b1c \u2b1c\n\n'
            '#ScreenshotSleuth'
        )
        results = parse_guessgame_message(content)
        assert len(results) == 2
        assert results[0].puzzle_number == 1427
        assert results[0].accuracy == 5
        assert results[1].puzzle_number == 1432
        assert results[1].accuracy == 3
        assert results[1].time_seconds == 3

    def test_parse_no_url_hashtag_only(self):
        """Messages with #GuessTheGame (no dot, no URL) should still parse."""
        results = parse_guessgame_message(
            '#GuessTheGame #1197\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c\n\n'
            '#RookieGuesser'
        )
        assert len(results) == 1
        assert results[0].puzzle_number == 1197
        assert results[0].accuracy == 2  # 7 - green_pos(5)

    def test_parse_with_user_prefix(self):
        """User commentary before the GG content."""
        results = parse_guessgame_message(
            'f0lse \n\n'
            '#GuessTheGame #1197\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c\n\n'
            '#RookieGuesser\n'
            'https://guessthe.game/p/1197'
        )
        assert len(results) == 1
        assert results[0].puzzle_number == 1197

    def test_parse_rejects_non_guessgame(self):
        assert parse_guessgame_message('hello world') == []
        assert parse_guessgame_message('#1234\n\U0001f3ae \U0001f7e9') == []

    def test_no_yellow_gives_time_7(self):
        results = parse_guessgame_message(
            '<#123> #100\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/100'
        )
        assert len(results) == 1
        assert results[0].time_seconds == 7  # no yellow


class TestGuessGameScoring:
    def _row(self, accuracy, time_seconds):
        Row = namedtuple('Row', 'accuracy time_seconds is_perfect')
        return Row(accuracy, time_seconds, accuracy == 6)

    def _approx(self, val, expected, tol=0.001):
        return abs(val - expected) < tol

    def test_green_pos1_vs_all_red_is_max_blowout(self):
        # strength 12 vs 0 → margin 0.5 → 1.0/0.0
        s1, s2 = guessgame_score_matchup(self._row(6, 7), self._row(0, 7))
        assert self._approx(s1, 1.0) and self._approx(s2, 0.0)

    def test_green_pos6_vs_all_red_is_big_win(self):
        # strength 7 vs 0 → margin 7/24 ≈ 0.292 → 0.792/0.208
        s1, s2 = guessgame_score_matchup(self._row(1, 7), self._row(0, 7))
        assert s1 > 0.75 and s2 < 0.25

    def test_both_green_close_positions_is_tight(self):
        # green pos 1 (12) vs green pos 2 (11) → margin 1/24 ≈ 0.042
        s1, s2 = guessgame_score_matchup(self._row(6, 7), self._row(5, 7))
        assert 0.5 < s1 < 0.6 and 0.4 < s2 < 0.5
        assert self._approx(s1 + s2, 1.0)

    def test_both_green_far_apart_is_wider(self):
        # green pos 1 (12) vs green pos 6 (7) → margin 5/24 ≈ 0.208
        s1, s2 = guessgame_score_matchup(self._row(6, 7), self._row(1, 7))
        assert s1 > 0.7 and s2 < 0.3

    def test_green_beats_yellow_decisively(self):
        # green pos 6 (7) vs yellow pos 1 (3) → margin 4/24 ≈ 0.167
        s1, s2 = guessgame_score_matchup(self._row(1, 7), self._row(0, 1))
        assert s1 > 0.6 and s2 < 0.4

    def test_yellow_beats_all_red_modestly(self):
        # yellow pos 1 (3) vs all red (0) → margin 3/24 = 0.125
        s1, s2 = guessgame_score_matchup(self._row(0, 1), self._row(0, 7))
        assert 0.6 < s1 < 0.7 and 0.3 < s2 < 0.4

    def test_identical_results_tie(self):
        s1, s2 = guessgame_score_matchup(self._row(4, 2), self._row(4, 2))
        assert s1 == 0.5 and s2 == 0.5

    def test_all_red_vs_all_red_tie(self):
        s1, s2 = guessgame_score_matchup(self._row(0, 7), self._row(0, 7))
        assert s1 == 0.5 and s2 == 0.5

    def test_same_green_same_score_regardless_of_yellow(self):
        # Both green pos 3 — yellow shouldn't matter
        s1, s2 = guessgame_score_matchup(self._row(4, 1), self._row(4, 5))
        assert s1 == 0.5 and s2 == 0.5

    def test_points_always_sum_to_one(self):
        """Every matchup should distribute exactly 1.0 total points."""
        cases = [
            (self._row(6, 7), self._row(0, 7)),  # green 1 vs all red
            (self._row(3, 7), self._row(1, 7)),   # green 4 vs green 6
            (self._row(0, 2), self._row(0, 5)),    # yellow 2 vs yellow 5
            (self._row(5, 7), self._row(0, 3)),    # green 2 vs yellow 3
        ]
        for r1, r2 in cases:
            s1, s2 = guessgame_score_matchup(r1, r2)
            assert self._approx(s1 + s2, 1.0), f'{s1} + {s2} != 1.0'

    def test_green_vs_red_better_than_yellow_vs_red(self):
        """Green win over all-red should award more points than yellow win over all-red."""
        green_s1, _ = guessgame_score_matchup(self._row(1, 7), self._row(0, 7))
        yellow_s1, _ = guessgame_score_matchup(self._row(0, 1), self._row(0, 7))
        assert green_s1 > yellow_s1

    def test_missing_is_loss(self):
        """When missing_is_loss=True, absent player loses that puzzle."""
        Row = namedtuple('Row', 'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy')
        rows1 = [
            Row('1', '10', '2026-03-26', 1412, 1, 7, 6),
            Row('3', '10', '2026-03-27', 1413, 0, 7, 3),
        ]
        rows2 = [
            Row('2', '20', '2026-03-26', 1412, 0, 2, 4),
        ]
        # Without missing_is_loss: only puzzle 1412 compared
        stats = compute_vs(rows1, rows2, guessgame_score_matchup, missing_is_loss=False)
        assert stats['common_count'] == 1
        assert stats['wins1'] == 1

        # With missing_is_loss: puzzle 1413 counts as loss for player 2
        stats = compute_vs(rows1, rows2, guessgame_score_matchup, missing_is_loss=True)
        assert stats['common_count'] == 2
        assert stats['wins1'] == 2
        assert stats['wins2'] == 0

    def test_missing_with_missing_result_scores_as_all_red(self):
        """When missing_result is provided, missing player is scored as all-red, not auto-loss."""
        from tle.cogs._minigame_guessgame import _ALL_RED
        Row = namedtuple('Row', 'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy')
        # Player 1 has all-red (accuracy=0, time=7), player 2 is missing
        rows1 = [Row('1', '10', '2026-03-26', 1412, 0, 7, 0)]
        rows2 = []
        stats = compute_vs(
            rows1, rows2, guessgame_score_matchup,
            missing_is_loss=True, missing_result=_ALL_RED,
        )
        assert stats['common_count'] == 1
        # All-red vs all-red (missing) should tie, not give 1 point to player 1
        assert stats['ties'] == 1
        assert stats['wins1'] == 0
        assert stats['wins2'] == 0
        assert stats['score1'] == 0.5
        assert stats['score2'] == 0.5

    def test_missing_with_missing_result_green_still_wins(self):
        """Green result vs missing (treated as all-red) should win."""
        from tle.cogs._minigame_guessgame import _ALL_RED
        Row = namedtuple('Row', 'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy')
        rows1 = [Row('1', '10', '2026-03-26', 1412, 0, 7, 3)]  # green pos 4
        rows2 = []
        stats = compute_vs(
            rows1, rows2, guessgame_score_matchup,
            missing_is_loss=True, missing_result=_ALL_RED,
        )
        assert stats['common_count'] == 1
        assert stats['wins1'] == 1
        assert stats['wins2'] == 0

    def test_compute_vs_with_guessgame_scoring(self):
        Row = namedtuple('Row', 'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy')
        rows1 = [Row('1', '10', '2026-03-26', 1412, 1, 7, 6)]   # perfect (green pos 1)
        rows2 = [Row('2', '20', '2026-03-26', 1412, 0, 2, 4)]   # green pos 3
        stats = compute_vs(rows1, rows2, guessgame_score_matchup)
        assert stats['wins1'] == 1
        assert stats['wins2'] == 0

    def test_multi_result_ingestion(self, db, monkeypatch):
        """Multi-result message stores all results under the same message_id."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        content = (
            '<#123> #1407\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1407\n\n'
            '<#123> #1412\n'
            '\U0001f3ae \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1412'
        )
        cog = Minigames(bot=None)
        msg = _FakeMessage(500, 1, 10, 999, content)
        asyncio.run(cog.on_message(msg))

        rows = db.get_minigame_results_for_user(1, 'guessgame', 999)
        assert len(rows) == 2
        puzzles = {r.puzzle_number for r in rows}
        assert puzzles == {1407, 1412}

    def test_channel_mention_dump_ingestion(self, db, monkeypatch):
        """Controller-only GuessThe.Game dumps should ingest from the configured channel."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        content = (
            '<#1435360903137853652> #1427\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c\n\n'
            '#ScreenshotSleuth\n\n'
            '<#1435360903137853652> #1428\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9\n\n'
            '#ScreenshotSleuth'
        )
        cog = Minigames(bot=None)
        msg = _FakeMessage(500, 1, 10, 999, content)
        asyncio.run(cog.on_message(msg))

        rows = db.get_minigame_results_for_user(1, 'guessgame', 999)
        assert len(rows) == 2
        assert {r.puzzle_number for r in rows} == {1427, 1428}

    def test_reparse_picks_up_channel_mention_dump(self, db, monkeypatch):
        """Reparse should recover old raw GuessThe.Game dumps without site text or URLs."""
        monkeypatch.setattr(cf_common, 'user_db', db)

        content = (
            '<#1435360903137853652> #1429\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c\n\n'
            '#ScreenshotSleuth\n\n'
            '<#1435360903137853652> #1430\n\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c\n\n'
            '#ScreenshotSleuth'
        )
        db.save_raw_message(700, 1, 10, 999, '2026-04-05T12:00:00', content)

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['payload'] = embed if embed is not None else content

        ctx = SimpleNamespace(
            guild=_FakeGuild(1),
            send=send,
        )
        cog = Minigames(bot=None)
        asyncio.run(cog._cmd_reparse(ctx, GUESSGAME_GAME))

        rows = db.get_minigame_results_for_user(1, 'guessgame', 999)
        assert len(rows) == 2
        assert {r.puzzle_number for r in rows} == {1429, 1430}

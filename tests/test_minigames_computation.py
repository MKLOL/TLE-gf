"""Minigames score-computation, streak, strip-codeblock tests."""
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
    _GAME,
    _queens_number,
    _row,
    db,
    FakeMinigameDb,
    _FakeGuild,
    _FakeChannel,
    _FakeAttachment,
    _FakeAuthor,
    _FakeDiscordMember,
    _FakeMessage,
    _FakeMember,
    _FakeFollowup,
    _FakeResponse,
    _FakeInteraction,
    _FakeGroup,
    _QueensCommandsBase,
)


class TestComputation:
    def test_vs_scores_perfect_vs_partial(self):
        stats = compute_vs(
            [_row(1, 10, '2026-03-26', True, 80, 100, 445)],
            [_row(2, 20, '2026-03-26', False, 40, 96, 445)],
        )
        assert stats['common_count'] == 1
        assert stats['score1'] == 1.0
        assert stats['score2'] == 0.0
        assert stats['wins1'] == 1

    def test_vs_scores_both_partial_as_tie(self):
        stats = compute_vs(
            [_row(1, 10, '2026-03-26', False, 80, 96, 445)],
            [_row(2, 20, '2026-03-26', False, 40, 50, 445)],
        )
        assert stats['score1'] == 0.5
        assert stats['score2'] == 0.5
        assert stats['ties'] == 1

    def test_vs_uses_best_submission_per_puzzle(self):
        stats = compute_vs(
            [
                _row(1, 10, '2026-03-26', False, 80, 96, 445),
                _row(3, 10, '2026-03-26', True, 70, 100, 445),
            ],
            [_row(2, 20, '2026-03-26', True, 75, 100, 445)],
        )
        assert stats['score1'] == 1.0
        assert stats['score2'] == 0.0

    def test_streak_counts_latest_consecutive_perfect_days(self):
        rows = [
            _row(1, 10, '2026-03-24', True, 60, number=443),
            _row(2, 10, '2026-03-25', True, 70, number=444),
            _row(3, 10, '2026-03-26', True, 80, number=445),
        ]
        assert compute_streak(rows) == 3

    def test_streak_breaks_on_partial_or_missing_day(self):
        rows = [
            _row(1, 10, '2026-03-24', True, 60, number=443),
            _row(2, 10, '2026-03-25', False, 70, 96, 444),
            _row(3, 10, '2026-03-26', True, 80, number=445),
        ]
        assert compute_streak(rows) == 1

    def test_longest_streak_spans_entire_history(self):
        rows = [
            _row(1, 10, '2026-03-20', True, 60, number=439),
            _row(2, 10, '2026-03-21', True, 60, number=440),
            _row(3, 10, '2026-03-22', True, 60, number=441),
            _row(4, 10, '2026-03-23', False, 70, 96, 442),
            _row(5, 10, '2026-03-24', True, 60, number=443),
            _row(6, 10, '2026-03-25', True, 60, number=444),
        ]
        # Current streak is 2 (Mar 24-25), but longest is 3 (Mar 20-22)
        assert compute_streak(rows) == 2
        assert compute_longest_streak(rows) == 3

    def test_longest_streak_gap_breaks_run(self):
        rows = [
            _row(1, 10, '2026-03-20', True, 60, number=439),
            _row(2, 10, '2026-03-22', True, 60, number=441),
            _row(3, 10, '2026-03-23', True, 60, number=442),
        ]
        # Gap on Mar 21 breaks it: longest is 2 (Mar 22-23)
        assert compute_longest_streak(rows) == 2

    def test_longest_streak_equals_current_when_all_perfect(self):
        rows = [
            _row(1, 10, '2026-03-24', True, 60, number=443),
            _row(2, 10, '2026-03-25', True, 70, number=444),
            _row(3, 10, '2026-03-26', True, 80, number=445),
        ]
        assert compute_longest_streak(rows) == 3
        assert compute_streak(rows) == 3

    def test_longest_streak_empty_rows(self):
        assert compute_longest_streak([]) == 0

    def test_longest_streak_no_perfects(self):
        rows = [
            _row(1, 10, '2026-03-24', False, 60, 90, 443),
            _row(2, 10, '2026-03-25', False, 70, 85, 444),
        ]
        assert compute_longest_streak(rows) == 0

    def test_top_counts_shared_fastest_perfect_wins(self):
        rows = [
            _row(1, 10, '2026-03-26', True, 80, number=445),
            _row(2, 20, '2026-03-26', True, 80, number=445),
            _row(3, 30, '2026-03-26', True, 90, number=445),
            _row(4, 10, '2026-03-27', True, 75, number=446),
            _row(5, 20, '2026-03-27', False, 60, 99, 446),
        ]
        assert compute_top(rows) == [('10', 2), ('20', 1)]

    def test_top_with_custom_is_eligible(self):
        """compute_top with GuessGame-style eligibility: any green counts."""
        rows = [
            _row(1, 10, '2026-03-26', False, 7, accuracy=4, number=1412),  # has green
            _row(2, 20, '2026-03-26', False, 7, accuracy=0, number=1412),  # no green
        ]
        result = compute_top(rows, is_eligible=lambda row: row.accuracy > 0)
        assert result == [('10', 1)]

    def test_resolve_scoring_uses_akari_raw_variant(self):
        args, scoring_name, scoring = resolve_scoring(AKARI_GAME, ('week', 'raw'))
        assert args == ('week',)
        assert scoring_name == 'raw'
        assert scoring.score_matchup is not None
        assert scoring.is_eligible_winner is not None
        assert scoring.best_result_sort_key is not None
        assert scoring.winner_result_sort_key is not None

    def test_akari_raw_vs_ignores_accuracy_and_uses_time(self):
        _, _, scoring = resolve_scoring(AKARI_GAME, ('raw',))
        stats = compute_vs(
            [_row(1, 10, '2026-03-26', False, 60, 50, 445)],
            [_row(2, 20, '2026-03-26', True, 90, 100, 445)],
            score_fn=scoring.score_matchup,
            best_result_sort_key_fn=scoring.best_result_sort_key,
        )
        assert stats['common_count'] == 1
        assert stats['score1'] == 1.0
        assert stats['score2'] == 0.0
        assert stats['wins1'] == 1

    def test_akari_raw_top_counts_fastest_time_even_if_not_perfect(self):
        _, _, scoring = resolve_scoring(AKARI_GAME, ('raw',))
        rows = [
            _row(1, 10, '2026-03-26', False, 60, 50, 445),
            _row(2, 20, '2026-03-26', True, 90, 100, 445),
            _row(3, 10, '2026-03-27', False, 70, 80, 446),
            _row(4, 20, '2026-03-27', True, 70, 100, 446),
        ]
        result = compute_top(
            rows,
            is_eligible=scoring.is_eligible_winner,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            winner_result_sort_key_fn=scoring.winner_result_sort_key,
        )
        assert result == [('10', 2), ('20', 1)]

    def test_resolve_scoring_uses_akari_all_variant(self):
        args, scoring_name, scoring = resolve_scoring(AKARI_GAME, ('week', 'all'))
        assert args == ('week',)
        assert scoring_name == 'all'
        assert scoring.score_matchup is not None
        assert scoring.is_eligible_winner is not None
        assert scoring.missing_is_loss is True
        assert scoring.missing_result is not None

    def test_akari_all_vs_counts_unshared_puzzles(self):
        _, _, scoring = resolve_scoring(AKARI_GAME, ('all',))
        stats = compute_vs(
            [
                _row(1, 10, '2026-03-26', True, 60, 100, 445),
                _row(2, 10, '2026-03-27', False, 75, 95, 446),
            ],
            [
                _row(3, 20, '2026-03-26', False, 80, 96, 445),
            ],
            score_fn=scoring.score_matchup,
            missing_is_loss=scoring.missing_is_loss,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            missing_result=scoring.missing_result,
        )
        assert stats['common_count'] == 2
        assert stats['score1'] == 2.0
        assert stats['score2'] == 0.0
        assert stats['wins1'] == 2

    def test_akari_all_top_counts_single_partial_completion(self):
        _, _, scoring = resolve_scoring(AKARI_GAME, ('all',))
        rows = [
            _row(1, 10, '2026-03-26', False, 60, 70, 445),
            _row(2, 20, '2026-03-27', True, 80, 100, 446),
        ]
        result = compute_top(
            rows,
            is_eligible=scoring.is_eligible_winner,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            winner_result_sort_key_fn=scoring.winner_result_sort_key,
        )
        assert result == [('10', 1), ('20', 1)]


class TestStripCodeblock:
    def test_plain_text_unchanged(self):
        text = 'Daily Akari 436\n2026-03-17\n\U0001f31f Perfect!   \U0001f553 1:26'
        assert strip_codeblock(text) == text

    def test_triple_backtick_block(self):
        text = '```\nDaily Akari 436\n2026-03-17\n\U0001f31f Perfect!   \U0001f553 1:26\n```'
        assert '`' not in strip_codeblock(text)
        assert 'Daily Akari 436' in strip_codeblock(text)

    def test_triple_backtick_with_language_tag(self):
        text = '```txt\nDaily Akari 436\n```'
        result = strip_codeblock(text)
        assert '`' not in result
        assert 'Daily Akari 436' in result

    def test_single_backtick_per_line(self):
        text = '`Daily Akari 436`\n`2026-03-17`\n`\U0001f31f Perfect!   \U0001f553 1:26`'
        result = strip_codeblock(text)
        assert '`' not in result
        assert 'Daily Akari 436' in result

    def test_single_backtick_whole_message(self):
        text = '`Daily Akari 436\n2026-03-17\n\U0001f31f Perfect!   \U0001f553 1:26`'
        result = strip_codeblock(text)
        assert '`' not in result
        assert 'Daily Akari 436' in result


class TestAkariCodeblockParsing:
    """Akari parser should handle messages wrapped in Discord monospace."""

    _PLAIN = 'Daily Akari \U0001f60a 436\n\u2705 2026-03-17 (Tue)\u2705\n\U0001f31f Perfect!   \U0001f553 1:26'

    def _parse(self, text):
        return parse_akari_message(strip_codeblock(text))

    def test_plain(self):
        r = self._parse(self._PLAIN)
        assert len(r) == 1
        assert r[0].puzzle_number == 436
        assert r[0].is_perfect

    def test_triple_backtick(self):
        r = self._parse('```\n' + self._PLAIN + '\n```')
        assert len(r) == 1
        assert r[0].puzzle_number == 436

    def test_single_backtick_per_line(self):
        wrapped = '\n'.join(f'`{line}`' for line in self._PLAIN.splitlines())
        r = self._parse(wrapped)
        assert len(r) == 1
        assert r[0].puzzle_number == 436

    def test_single_backtick_whole(self):
        r = self._parse('`' + self._PLAIN + '`')
        assert len(r) == 1
        assert r[0].puzzle_number == 436

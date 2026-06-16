"""Tests for the minigames system (Daily Akari, etc.) - core parsing/args."""
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


class TestParsing:
    def test_parse_perfect_result(self):
        results = parse_akari_message(
            'Daily Akari 😊 445\n'
            '✅2026-03-26 (Thu)✅\n'
            '🌟 Perfect!   🕓 1:29\n'
            'https://dailyakari.com/'
        )
        assert len(results) == 1
        parsed = results[0]
        assert parsed.puzzle_number == 445
        assert parsed.puzzle_date == dt.date(2026, 3, 26)
        assert parsed.is_perfect is True
        assert parsed.accuracy == 100
        assert parsed.time_seconds == 89

    def test_parse_partial_result(self):
        results = parse_akari_message(
            'Daily Akari 445\n'
            '✅03/26/2026✅\n'
            '🎯 96%   🕓 1:00\n'
            'https://dailyakari.com/'
        )
        assert len(results) == 1
        parsed = results[0]
        assert parsed.puzzle_date == dt.date(2026, 3, 26)
        assert parsed.is_perfect is False
        assert parsed.accuracy == 96
        assert parsed.time_seconds == 60

    def test_parse_perfect_word(self):
        results = parse_akari_message(
            'Daily Akari 445\n'
            '✅March 26, 2026✅\n'
            'Perfect   🕓 2:15\n'
            'https://dailyakari.com/'
        )
        assert len(results) == 1
        assert results[0].is_perfect is True
        assert results[0].accuracy == 100

    def test_parse_url_before_header(self):
        """Akari share text may have the URL before the header line."""
        results = parse_akari_message(
            'https://dailyakari.com/\n'
            'Daily Akari 😊 445\n'
            '✅2026-03-26 (Thu)✅\n'
            '🌟 Perfect!   🕓 1:29'
        )
        assert len(results) == 1
        assert results[0].puzzle_number == 445
        assert results[0].is_perfect is True

    def test_parse_commentary_before_header(self):
        """Users may add commentary before their Akari result."""
        results = parse_akari_message(
            'got it!\n'
            'Daily Akari 😊 445\n'
            '✅2026-03-26 (Thu)✅\n'
            '🌟 Perfect!   🕓 1:29'
        )
        assert len(results) == 1
        assert results[0].puzzle_number == 445

    def test_parse_no_puzzle_number(self):
        """Older share text omits the puzzle number; infer from the date."""
        results = parse_akari_message(
            'Daily Akari 😊\n'
            '✅Fri Oct 17, 2025✅\n'
            '🌟 Perfect!   🕓 4:17'
        )
        assert len(results) == 1
        parsed = results[0]
        assert parsed.puzzle_date == dt.date(2025, 10, 17)
        # 446 + (2025-10-17 - 2026-03-27).days = 446 + (-161) = 285
        assert parsed.puzzle_number == 285
        assert parsed.is_perfect is True
        assert parsed.time_seconds == 257

    def test_parse_no_number_partial(self):
        results = parse_akari_message(
            'Daily Akari\n'
            '✅2025-12-25✅\n'
            '🎯 90%   🕓 3:00'
        )
        assert len(results) == 1
        parsed = results[0]
        # 446 + (2025-12-25 - 2026-03-27).days = 446 + (-92) = 354
        assert parsed.puzzle_number == 354
        assert parsed.accuracy == 90
        assert parsed.is_perfect is False

    def test_parse_rejects_invalid_message(self):
        assert parse_akari_message('hello world') == []

    def test_parse_rejects_mismatched_number_and_date(self):
        assert parse_akari_message(
            'Daily Akari 446\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29'
        ) == []

    def test_parse_rejects_out_of_range_puzzle_number(self):
        assert parse_akari_message(
            'Daily Akari 4000000\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29'
        ) == []

    def test_parse_rejects_non_pro_mode(self):
        # Non-pro dailyakari.com share format: header + date + time + ✅ Solved,
        # but no accuracy % / 🌟 / "perfect" — the real parser must drop it so
        # the cog can route to the non-pro notice instead of counting a result.
        results = parse_akari_message(
            'Daily Akari \U0001f60a 514\n'
            '2026-06-03 (Wed)\n'
            '✅ Solved!   \U0001f553 2:49\n'
            'https://dailyakari.com/'
        )
        assert results == []

    def test_looks_like_non_pro_akari_detects_solved_form(self):
        from tle.cogs._minigame_akari import looks_like_non_pro_akari
        assert looks_like_non_pro_akari(
            'Daily Akari \U0001f60a 514\n'
            '2026-06-03 (Wed)\n'
            '✅ Solved!   \U0001f553 2:49\n'
            'https://dailyakari.com/'
        ) is True

    def test_looks_like_non_pro_akari_rejects_perfect(self):
        # A real perfect result must not be misclassified as non-pro.
        from tle.cogs._minigame_akari import looks_like_non_pro_akari
        assert looks_like_non_pro_akari(
            'Daily Akari 445\n'
            '✅2026-03-26 (Thu)✅\n'
            '\U0001f31f Perfect!   \U0001f553 1:29'
        ) is False

    def test_looks_like_non_pro_akari_rejects_accuracy(self):
        # Same for a partial result with an accuracy percentage.
        from tle.cogs._minigame_akari import looks_like_non_pro_akari
        assert looks_like_non_pro_akari(
            'Daily Akari 445\n'
            '2026-03-26\n'
            '\U0001f3af 92%   \U0001f553 2:11'
        ) is False

    def test_looks_like_non_pro_akari_rejects_non_akari(self):
        from tle.cogs._minigame_akari import looks_like_non_pro_akari
        assert looks_like_non_pro_akari('just chatting in the channel') is False


class TestQueensParsing:
    def test_parse_copied_linkedin_leaderboard(self):
        results = parse_queens_leaderboard(
            'Ali Farhat\n'
            'Ali Farhat\n'
            'Ali Farhat\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'Robert Kocharyan\n'
            'Robert Kocharyan\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
            '4\n'
            'Zepur Jokaklian\n'
            'Zepur Jokaklian\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:07\n'
        )

        assert [r.linkedin_name for r in results] == [
            'Ali Farhat',
            'Robert Kocharyan',
            'Zepur Jokaklian',
        ]
        assert [r.is_you for r in results] == [False, True, False]
        assert [r.time_seconds for r in results] == [4, 6, 7]
        assert all(r.no_hints and r.no_mistakes for r in results)

    def test_parse_you_when_no_name_exists(self):
        results = parse_queens_leaderboard(
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
        )
        assert len(results) == 1
        assert results[0].linkedin_name == 'You'

    def test_parse_shared_queens_result(self):
        results = parse_queens_message(
            'Queens #774 | 1:26\n'
            'No mistakes & no hints\n'
            'First \U0001f451s: \U0001f7eb \U0001f7e7 \U0001f7e6\n'
            'lnkd.in/queens.'
        )

        assert len(results) == 1
        assert results[0].puzzle_number == 774
        assert results[0].puzzle_date == dt.date(2026, 6, 13)
        assert results[0].time_seconds == 86
        assert results[0].accuracy == 100
        assert results[0].is_perfect is True

    def test_queens_rating_ranks_by_time_only(self):
        rows = [
            _row(1, 10, '2026-06-08', False, 10, 0, 20260608),
            _row(2, 20, '2026-06-08', True, 10, 100, 20260608),
            _row(3, 30, '2026-06-08', False, 8, 0, 20260608),
        ]
        ranks = rank_queens_participants(rows)
        assert ranks == {'30': 1, '10': 2, '20': 2}
        assert (
            QUEENS_GAME.winner_result_sort_key(rows[0])
            == QUEENS_GAME.winner_result_sort_key(rows[1])
        )


class TestRatingDefinitions:
    def test_games_declare_shared_rating_configs(self):
        assert AKARI_GAME.rating is not None
        assert AKARI_GAME.rating.damping == constants.AKARI_RATING_DAMPING
        assert AKARI_GAME.rating.decay_base == constants.AKARI_DECAY_BASE
        assert AKARI_GAME.rating.max_puzzle_lookahead == constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        assert callable(AKARI_GAME.rating.current_puzzle_number_fn)

        assert QUEENS_GAME.rating is not None
        assert QUEENS_GAME.rating.rank_fn is rank_queens_participants
        assert QUEENS_GAME.rating.decay_base == 0.0
        assert QUEENS_GAME.rating.decay_max == 0.0
        assert QUEENS_GAME.rating.decay_grace == 0

        assert GUESSGAME_GAME.rating is None


class TestArgs:
    def test_parse_date_filters(self):
        dlo, dhi, plo, phi = parse_date_args(('d>=26032026', 'd<28032026'))
        assert dt.datetime.fromtimestamp(dlo).date() == dt.date(2026, 3, 26)
        assert dt.datetime.fromtimestamp(dhi).date() == dt.date(2026, 3, 28)
        assert plo == 0
        assert phi == 0

    def test_parse_puzzle_number_filters(self):
        dlo, dhi, plo, phi = parse_date_args(('p>=1300', 'p<1500'))
        assert plo == 1300
        assert phi == 1500

    def test_parse_exact_puzzle_selector_number(self):
        assert _maybe_parse_puzzle_selector('445') == ('puzzle', 445)

    def test_parse_exact_puzzle_selector_day(self):
        assert _maybe_parse_puzzle_selector('26032026') == ('day', dt.date(2026, 3, 26))

    def test_parse_exact_puzzle_selector_rejects_filters(self):
        assert _maybe_parse_puzzle_selector('week') is None
        assert _maybe_parse_puzzle_selector('p>=445') is None

    def test_bare_four_digit_number_is_a_year_not_a_puzzle(self):
        # Back-compat: a bare 4/6/8-digit value keeps parsing as a date.
        assert _maybe_parse_puzzle_selector('2026') == ('day', dt.date(2026, 1, 1))
        assert _maybe_parse_puzzle_selector('032026') == ('day', dt.date(2026, 3, 1))

    def test_hash_prefix_forces_puzzle_number(self):
        # The unambiguous way to reach a puzzle whose number collides with a
        # date format once daily puzzle numbers reach four digits.
        assert _maybe_parse_puzzle_selector('#1000') == ('puzzle', 1000)
        assert _maybe_parse_puzzle_selector('#2026') == ('puzzle', 2026)
        assert _maybe_parse_puzzle_selector('#112024') == ('puzzle', 112024)

    def test_p_equals_prefix_forces_puzzle_number(self):
        assert _maybe_parse_puzzle_selector('p=1000') == ('puzzle', 1000)
        assert _maybe_parse_puzzle_selector('P=2026') == ('puzzle', 2026)

    def test_explicit_prefix_still_works_for_small_numbers(self):
        # #N is consistent for every puzzle, not just the colliding ones.
        assert _maybe_parse_puzzle_selector('#445') == ('puzzle', 445)
        assert _maybe_parse_puzzle_selector('p=445') == ('puzzle', 445)

    def test_explicit_prefix_with_non_digit_is_rejected(self):
        assert _maybe_parse_puzzle_selector('#abc') is None
        assert _maybe_parse_puzzle_selector('#') is None
        assert _maybe_parse_puzzle_selector('p=') is None

    def test_bare_small_numbers_remain_puzzles(self):
        # Lengths that are not valid date formats stay puzzle numbers.
        assert _maybe_parse_puzzle_selector('5') == ('puzzle', 5)
        assert _maybe_parse_puzzle_selector('445') == ('puzzle', 445)
        assert _maybe_parse_puzzle_selector('99999') == ('puzzle', 99999)

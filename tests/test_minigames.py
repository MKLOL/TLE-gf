"""Tests for the minigames system (Daily Akari, etc.)."""
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
from tle.util.db.minigame_db import MinigameDbMixin
from tle.cogs._minigame_common import (
    compute_vs,
    compute_streak,
    compute_longest_streak,
    compute_top,
    parse_date_args,
    resolve_scoring,
    strip_codeblock,
)
from tle.cogs._minigame_akari import AKARI_GAME, parse_akari_message
from tle.cogs._minigame_guessgame import (
    GUESSGAME_GAME,
    parse_guessgame_message,
    guessgame_score_matchup,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
    parse_queens_leaderboard,
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


_GAME = 'akari'


def _queens_number(value):
    if isinstance(value, str):
        value = dt.date.fromisoformat(value)
    return minigames_module._queens_puzzle_number_for_date(value)


class FakeMinigameDb(MinigameDbMixin):
    """In-memory SQLite with the minigame schema, reusing the real DB mixin."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_config (
                guild_id   TEXT NOT NULL,
                game       TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                PRIMARY KEY (guild_id, game)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_result (
                message_id     TEXT NOT NULL,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0,
                raw_content    TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (message_id, game, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_import_result (
                message_id     TEXT NOT NULL,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0,
                raw_content    TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (message_id, game, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_raw_message (
                message_id  TEXT NOT NULL PRIMARY KEY,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                raw_content TEXT NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id    TEXT,
                key         TEXT,
                value       TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_player_link (
                guild_id        TEXT NOT NULL,
                game            TEXT NOT NULL,
                user_id         TEXT NOT NULL,
                external_name   TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                external_url    TEXT,
                linked_at       REAL NOT NULL,
                linked_by       TEXT NOT NULL,
                PRIMARY KEY (guild_id, game, user_id),
                UNIQUE (guild_id, game, normalized_name)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_unresolved_result (
                guild_id        TEXT NOT NULL,
                game            TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                external_name   TEXT NOT NULL,
                channel_id      TEXT NOT NULL,
                puzzle_number   INTEGER NOT NULL,
                puzzle_date     TEXT NOT NULL,
                accuracy        INTEGER NOT NULL,
                time_seconds    INTEGER NOT NULL,
                is_perfect      INTEGER NOT NULL DEFAULT 0,
                raw_content     TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, game, normalized_name, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_rating (
                guild_id    TEXT NOT NULL,
                game        TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, game, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_ban (
                guild_id   TEXT NOT NULL,
                game       TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                banned_at  REAL NOT NULL,
                banned_by  TEXT NOT NULL,
                reason     TEXT,
                PRIMARY KEY (guild_id, game, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_registrant (
                guild_id      TEXT NOT NULL,
                user_id       TEXT NOT NULL,
                registered_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_optout (
                guild_id     TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                opted_out_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_ban (
                guild_id   TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                banned_at  REAL NOT NULL,
                banned_by  TEXT NOT NULL,
                reason     TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_rating (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.commit()

    def get_guild_config(self, guild_id, key):
        row = self.conn.execute(
            'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)
        ).fetchone()
        return row.value if row else None

    def set_guild_config(self, guild_id, key, value):
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (str(guild_id), key, value)
        )
        self.conn.commit()

    def delete_guild_config(self, guild_id, key):
        rc = self.conn.execute(
            'DELETE FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)
        ).rowcount
        self.conn.commit()
        return rc

    def close(self):
        self.conn.close()


@pytest.fixture
def db():
    d = FakeMinigameDb()
    yield d
    d.close()


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
            'Daily Akari 500\n'
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


def _row(message_id, user_id, puzzle_date, is_perfect, time_seconds, accuracy=100, number=1):
    Row = namedtuple(
        'Row',
        'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy'
    )
    return Row(str(message_id), str(user_id), puzzle_date, number, is_perfect, time_seconds, accuracy)


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


class TestDbMixin:
    def test_channel_crud(self, db):
        assert db.get_minigame_channel(123, _GAME) is None
        db.set_minigame_channel(123, _GAME, 456)
        assert db.get_minigame_channel(123, _GAME) == '456'
        db.clear_minigame_channel(123, _GAME)
        assert db.get_minigame_channel(123, _GAME) is None

    def test_result_storage(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'raw msg')
        row = db.get_minigame_result(1)
        assert row is not None
        assert row.user_id == '300'
        assert row.puzzle_number == 445
        assert row.is_perfect == 1
        assert row.time_seconds == 89
        assert row.raw_content == 'raw msg'

    def test_results_for_user(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_minigame_result(2, 100, _GAME, 200, 301, 446, '2026-03-27', 90, 99, False, 'c2')
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '1'

    def test_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c')
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '1'

    def test_imported_results_are_included_in_queries(self, db):
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c')
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_first_message_across_live_and_imported_wins(self, db):
        db.save_minigame_result(20, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'c1')
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False, 'c2')
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '10'
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_delete_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_imported_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 90, True, 'c2')
        rc = db.delete_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert rc == 2
        assert db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445) is None

    def test_delete_results_for_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_minigame_result(2, 100, _GAME, 200, 301, 445, '2026-03-26', 100, 90, True, 'c2')
        db.save_imported_minigame_result(3, 100, _GAME, 200, 302, 445, '2026-03-26', 100, 91, True, 'c3')
        db.save_minigame_result(4, 100, _GAME, 200, 300, 446, '2026-03-27', 100, 92, True, 'c4')

        assert db.delete_minigame_results_for_puzzle(100, _GAME, 445) == 3

        rows = db.get_minigame_results_for_guild(100, _GAME)
        assert [(row.user_id, row.puzzle_number) for row in rows] == [('300', 446)]

    def test_puzzle_number_filtering(self, db):
        """plo/phi should filter results by puzzle_number at the DB level."""
        db.save_minigame_result(1, 100, _GAME, 200, 300, 440, '2026-03-20', 100, 60, True, 'c')
        db.save_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-25', 100, 70, True, 'c')
        db.save_minigame_result(3, 100, _GAME, 200, 300, 450, '2026-03-30', 100, 80, True, 'c')
        rows = db.get_minigame_results_for_user(100, _GAME, 300, plo=445, phi=450)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_date_filtering(self, db):
        """dlo/dhi should filter results by puzzle_date at the DB level."""
        import time
        db.save_minigame_result(1, 100, _GAME, 200, 300, 440, '2026-03-20', 100, 60, True, 'c')
        db.save_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-25', 100, 70, True, 'c')
        db.save_minigame_result(3, 100, _GAME, 200, 300, 450, '2026-03-30', 100, 80, True, 'c')
        dlo = time.mktime(dt.datetime(2026, 3, 24).timetuple())
        dhi = time.mktime(dt.datetime(2026, 3, 26).timetuple())
        rows = db.get_minigame_results_for_user(100, _GAME, 300, dlo=dlo, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_raw_content_updated_on_replace(self, db):
        """INSERT OR REPLACE should update raw_content when re-saving same message_id."""
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'original')
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'edited')
        row = db.get_minigame_result(1)
        assert row.raw_content == 'edited'

    def test_raw_message_storage_and_reparse(self, db):
        """Raw messages are stored and can be reparsed into import results."""
        content = (
            'Daily Akari \U0001f60a 445\n'
            '\u27052026-03-26 (Thu)\u2705\n'
            '\U0001f31f Perfect!   \U0001f553 1:29\n'
            'https://dailyakari.com/'
        )
        db.save_raw_message(1, 100, 200, 300, '2026-03-26T12:00:00', content)
        db.save_raw_message(2, 100, 200, 301, '2026-03-26T12:05:00', 'not a game msg')

        raws = db.get_raw_messages_for_guild(100)
        assert len(raws) == 2

        # Simulate reparse: parse raw content and save matches
        from tle.cogs._minigame_akari import parse_akari_message
        from tle.cogs._minigame_common import strip_codeblock
        parsed_count = 0
        for row in raws:
            results = parse_akari_message(strip_codeblock(row.raw_content))
            for r in results:
                db.save_imported_minigame_result(
                    row.message_id, row.guild_id, _GAME, row.channel_id,
                    row.user_id, r.puzzle_number,
                    r.puzzle_date.isoformat(), r.accuracy,
                    r.time_seconds, r.is_perfect, row.raw_content,
                )
                parsed_count += 1

        assert parsed_count == 1
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_clear_imported_per_channel(self, db):
        """Import clear with channel_id only removes that channel's rows."""
        db.save_imported_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_imported_minigame_result(2, 100, _GAME, 201, 301, 446, '2026-03-27', 100, 90, True, 'c2')
        deleted = db.clear_imported_minigame_results(100, _GAME, channel_id=200)
        assert deleted == 1
        # Channel 201's result should survive
        rows = db.get_minigame_results_for_guild(100, _GAME)
        assert len(rows) == 1
        assert rows[0].channel_id == '201'

    def test_minigame_player_link_crud_and_unique_name(self, db):
        db.set_minigame_player_link(
            100, 'queens', 300, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'),
            'https://www.linkedin.com/in/robert/', 1.0, 999)
        row = db.get_minigame_player_link(100, 'queens', 300)
        assert row.external_name == 'Robert Kocharyan'
        assert row.external_url == 'https://www.linkedin.com/in/robert/'

        by_name = db.get_minigame_player_link_by_name(
            100, 'queens', normalize_queens_name('  robert   kocharyan '))
        assert by_name.user_id == '300'

        with pytest.raises(sqlite3.IntegrityError):
            db.set_minigame_player_link(
                100, 'queens', 301, 'Robert   Kocharyan',
                normalize_queens_name('Robert   Kocharyan'),
                None, 2.0, 999)

    def test_minigame_unresolved_result_crud(self, db):
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, 123, '2026-06-08',
            100, 5, True, 'raw')
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Bob LinkedIn'),
            'Bob LinkedIn', 200, 123, '2026-06-08',
            100, 7, True, 'raw')

        by_name = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn'))
        assert [(row.external_name, row.time_seconds) for row in by_name] == [
            ('Alice LinkedIn', 5),
        ]
        by_puzzle = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123)
        assert [(row.external_name, row.time_seconds) for row in by_puzzle] == [
            ('Alice LinkedIn', 5),
            ('Bob LinkedIn', 7),
        ]
        assert db.delete_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn')) == 1
        assert db.delete_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123) == 1
        assert db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123) == []

    def test_minigame_rating_snapshot_is_game_keyed(self, db):
        states = [
            RatingState('300', 1210.5, 2, 1210.5, 5.0),
            RatingState('301', 1190.0, 2, 1200.0, -5.0),
        ]
        db.replace_minigame_ratings(100, 'queens', states, 12.0)
        db.replace_minigame_ratings(100, 'akari', [RatingState('300', 1500, 1, 1500, 0)], 13.0)

        queens = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in queens] == ['300', '301']
        assert db.get_minigame_rating(100, 'queens', 300).rating == 1210.5
        assert db.get_minigame_rating(100, 'akari', 300).rating == 1500

    def test_minigame_ban_roundtrip(self, db):
        assert db.is_minigame_banned(100, 'queens', 300) is False
        assert db.ban_minigame_user(
            100, 'queens', 300, 12.0, 999, 'spam') == 1
        assert db.ban_minigame_user(
            100, 'queens', 300, 13.0, 999, 'again') == 0
        assert db.is_minigame_banned(100, 'queens', 300) is True
        row = db.get_minigame_ban(100, 'queens', 300)
        assert row.reason == 'spam'
        rows = db.get_minigame_bans(100, 'queens')
        assert [r.user_id for r in rows] == ['300']
        assert db.unban_minigame_user(100, 'queens', 300) == 1
        assert db.is_minigame_banned(100, 'queens', 300) is False


class _FakeGuild:
    def __init__(self, guild_id, members=None):
        self.id = guild_id
        self.members = members or []

    def get_member(self, user_id):
        for member in self.members:
            if getattr(member, 'id', None) == user_id:
                return member
        return None


class _FakeChannel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.mention = f'<#{channel_id}>'


class _FakeAttachment:
    def __init__(self, filename, payload):
        self.filename = filename
        self._payload = (
            payload if isinstance(payload, bytes) else payload.encode('utf-8'))
        self.size = len(self._payload)

    async def read(self):
        return self._payload


class _FakeAuthor:
    def __init__(self, user_id, bot=False):
        self.id = user_id
        self.bot = bot


class _FakeDiscordMember(_FakeAuthor):
    def __init__(self, user_id, name, display_name=None, bot=False, roles=None):
        super().__init__(user_id, bot=bot)
        self.name = name
        self.display_name = display_name or name
        self.roles = roles or []


class _FakeMessage:
    def __init__(self, msg_id, guild_id, channel_id, user_id, content):
        self.id = msg_id
        self.guild = _FakeGuild(guild_id)
        self.channel = _FakeChannel(channel_id)
        self.author = _FakeAuthor(user_id)
        self.content = content
        self.created_at = dt.datetime(2026, 3, 26, tzinfo=dt.timezone.utc)
        self.replies = []  # captures notice / reply embeds for assertions

    async def reply(self, *args, **kwargs):
        self.replies.append({'args': args, 'kwargs': kwargs})


class TestQueensImport:
    def test_queens_date_number_mapping_uses_linkedin_anchor(self):
        assert _queens_number('2026-06-08') == 769
        assert _queens_number('2026-06-09') == 770
        assert minigames_module._queens_date_for_puzzle_number(769) == (
            dt.date(2026, 6, 8))
        assert minigames_module._parse_queens_date_or_number('#770') == (
            dt.date(2026, 6, 9))

    def test_importer_must_be_linked(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'ali', 'Ali'),
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
        )

        cog = Minigames(bot=None)
        with pytest.raises(MinigameCogError, match='Register the importer'):
            cog._make_queens_import_preview(ctx, '2026-06-08', content)

    def test_importer_must_be_linked_even_for_unresolved_only_board(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        cog = Minigames(bot=None)
        with pytest.raises(MinigameCogError, match='Register the importer'):
            cog._make_queens_import_preview(ctx, '2026-06-08', (
                'Alice LinkedIn\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:04\n'
            ))

    def test_preview_resolves_linked_names_and_you_then_saves_ratings(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_player_link(
            100, 'queens', 300, 'Ali Farhat',
            normalize_queens_name('Ali Farhat'), None, 1.0, 999)
        db.set_minigame_player_link(
            100, 'queens', 301, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'),
            'https://www.linkedin.com/in/robert/', 1.0, 999)

        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'ali', 'Ali'),
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'Ali Farhat\n'
            'Ali Farhat\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
            'Unknown Person\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:07\n'
        )

        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', content)

        assert preview.puzzle_date == dt.date(2026, 6, 8)
        assert preview.puzzle_number == 769
        assert [entry.user_id for entry in preview.resolved] == ['300', '301']
        assert [entry.linkedin_name for entry in preview.unresolved] == [
            'Unknown Person',
        ]
        assert '2026-06-08' in cog._format_queens_import_preview(ctx, preview)
        assert '#769' in cog._format_queens_import_preview(ctx, preview)
        assert 'Robert Kocharyan' in cog._format_queens_import_preview(ctx, preview)

        saved = cog._save_queens_import(ctx, preview)

        assert saved.resolved == 2
        assert saved.unresolved == 1
        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert sorted((row.user_id, row.time_seconds) for row in rows) == [
            ('300', 4),
            ('301', 6),
        ]
        unresolved = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', _queens_number('2026-06-08'))
        assert [(row.external_name, row.time_seconds) for row in unresolved] == [
            ('Ali Farhat', 4),
            ('Robert Kocharyan', 6),
            ('Unknown Person', 7),
        ]
        assert {row.puzzle_number for row in rows} == {_queens_number('2026-06-08')}
        assert {row.puzzle_date for row in rows} == {'2026-06-08'}
        ratings = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in ratings] == ['300', '301']
        assert ratings[0].rating > ratings[1].rating

        reimport = cog._make_queens_import_preview(ctx, '08/06/2026', (
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:05\n'
        ))
        saved = cog._save_queens_import(ctx, reimport)
        assert saved.resolved == 0
        assert saved.unresolved == 0
        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert sorted((row.user_id, row.time_seconds) for row in rows) == [
            ('300', 4),
            ('301', 6),
        ]
        source_rows = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', _queens_number('2026-06-08'))
        assert [(row.external_name, row.time_seconds) for row in source_rows] == [
            ('Ali Farhat', 4),
            ('Robert Kocharyan', 6),
            ('Unknown Person', 7),
        ]
        ratings = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in ratings] == ['300', '301']
        assert ratings[0].rating > ratings[1].rating

    def test_register_claims_previously_unresolved_import_rows(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_player_link(
            100, 'queens', 300, 'Importer Name',
            normalize_queens_name('Importer Name'), None, 1.0, 999)
        importer = _FakeDiscordMember(300, 'importer', 'Importer')
        alice = _FakeDiscordMember(301, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[importer, alice])
        ctx = SimpleNamespace(
            guild=guild,
            author=importer,
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
            send=lambda *args, **kwargs: None,
        )
        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', (
            'Alice LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
        ))
        saved = cog._save_queens_import(ctx, preview)
        assert saved.resolved == 1
        assert saved.unresolved == 1
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        register_ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        claimed_count = cog._cmd_queens_register_link(
            register_ctx, alice, 'Alice LinkedIn')

        claimed = db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08'))
        assert claimed is not None
        assert claimed.time_seconds == 4
        assert [
            row.time_seconds for row in db.get_minigame_unresolved_results_for_name(
                100, 'queens', normalize_queens_name('Alice LinkedIn'))
        ] == [4]
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == [
            '301', '300',
        ]
        assert claimed_count == 1

    def test_linkedin_source_result_moves_when_name_is_reclaimed(
            self, db, monkeypatch):
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
        guild = _FakeGuild(100, members=[mod, alice, bob])
        cog = Minigames(bot=None)

        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Shared LinkedIn'),
            'Shared LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        alice_ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        cog._cmd_queens_register_link(alice_ctx, alice, 'Shared LinkedIn')
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is not None

        asyncio.run(cog._cmd_queens_unregister(alice_ctx, alice))
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Shared LinkedIn'))
        assert db.get_minigame_ratings(100, 'queens') == []

        mod_ctx = SimpleNamespace(
            guild=guild,
            author=mod,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, mod_ctx, 'bob', linkedin='Shared LinkedIn'))
        moved = db.get_minigame_result_for_user_puzzle(
            100, 'queens', bob.id, _queens_number('2026-06-08'))
        assert moved is not None
        assert moved.time_seconds == 4
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None
        assert [
            (row.user_id, row.time_seconds)
            for row in db.get_minigame_results_for_guild(100, 'queens')
        ] == [('301', 4)]
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == [
            '301',
        ]

    def test_register_normalizes_legacy_unresolved_puzzle_number(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(301, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, dt.date(2026, 6, 8).toordinal(),
            '2026-06-08', 100, 4, True, 'legacy')
        cog = Minigames(bot=None)

        claimed_count = cog._cmd_queens_register_link(
            ctx, alice, 'Alice LinkedIn')

        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is not None
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, dt.date(2026, 6, 8).toordinal()) is None
        assert claimed_count == 1

    def test_you_row_prefers_importer_even_when_name_is_copied(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'), None, 1.0, 999)
        db.set_minigame_player_link(
            100, 'queens', 301, 'Importer Name',
            normalize_queens_name('Importer Name'), None, 1.0, 999)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'robert', 'Robert'),
            _FakeDiscordMember(301, 'importer', 'Importer'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'importer', 'Importer'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'Robert Kocharyan\n'
            'Robert Kocharyan\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
        )

        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', content)

        assert [entry.user_id for entry in preview.resolved] == ['301']

    def test_legacy_live_and_imported_rows_migrate_to_linkedin_source_exactly(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 0, 8, False, 'alice raw')
        db.save_imported_minigame_result(
            12, 100, 'queens', 202, 301, _queens_number('2026-06-09'),
            '2026-06-09', 100, 5, True, 'bob raw')
        db.save_imported_minigame_result(
            13, 100, 'akari', 203, 302, 1, '2026-06-10',
            100, 9, True, 'akari raw')

        cog = Minigames(bot=None)
        saved = cog._sync_queens_materialized_results(100)

        source = {
            row.external_name: row
            for row in db.get_minigame_unresolved_results_for_guild(
                100, 'queens')
        }
        assert set(source) == {'Alice LinkedIn', 'Bob LinkedIn'}
        assert source['Alice LinkedIn'].normalized_name == 'alice linkedin'
        assert source['Alice LinkedIn'].channel_id == '201'
        assert source['Alice LinkedIn'].puzzle_number == _queens_number('2026-06-08')
        assert source['Alice LinkedIn'].puzzle_date == '2026-06-08'
        assert source['Alice LinkedIn'].accuracy == 0
        assert source['Alice LinkedIn'].time_seconds == 8
        assert source['Alice LinkedIn'].is_perfect == 0
        assert source['Alice LinkedIn'].raw_content == 'alice raw'
        assert source['Bob LinkedIn'].channel_id == '202'
        assert source['Bob LinkedIn'].puzzle_number == _queens_number('2026-06-09')
        assert source['Bob LinkedIn'].accuracy == 100
        assert source['Bob LinkedIn'].time_seconds == 5
        assert source['Bob LinkedIn'].is_perfect == 1
        assert source['Bob LinkedIn'].raw_content == 'bob raw'

        materialized = {
            row.user_id: row
            for row in db.get_minigame_results_for_guild(100, 'queens')
        }
        assert set(materialized) == {'300', '301'}
        assert materialized['300'].time_seconds == 8
        assert materialized['301'].time_seconds == 5
        assert saved == 2
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'queens'"
        ).fetchone()[0] == 0
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'akari'"
        ).fetchone()[0] == 1

    def test_additive_filter_migrates_legacy_rows_before_checking_new_entries(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 100, 8, True, 'alice legacy raw')
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'alice', 'Alice'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(999, 'bot', 'Bot'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(
            ctx, '2026-06-08', (
                'Alice LinkedIn\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:07\n'
                'Unknown Person\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:05\n'
            ), skip_importer=True)

        new_resolved, new_unresolved = cog._filter_new_queens_entries(
            100, preview)
        preview = preview._replace(
            resolved=new_resolved, unresolved=new_unresolved)
        saved = cog._save_queens_import(ctx, preview, skip_wipe=True)

        assert saved.resolved == 0
        assert saved.unresolved == 1
        alice_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn'))
        assert [row.time_seconds for row in alice_source] == [8]
        unknown_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Unknown Person'))
        assert [row.time_seconds for row in unknown_source] == [5]

    def test_unlinked_legacy_row_migrates_from_unique_raw_leaderboard_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        raw = (
            'Charlie LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Charlie LinkedIn'))
        assert len(source) == 1
        assert source[0].external_name == 'Charlie LinkedIn'
        assert source[0].time_seconds == 9

        db.set_minigame_player_link(
            100, 'queens', 300, 'Charlie LinkedIn',
            normalize_queens_name('Charlie LinkedIn'), None, 1.0, 999)
        saved = cog._sync_queens_materialized_results(100)

        materialized = db.get_minigame_result_for_user_puzzle(
            100, 'queens', 300, _queens_number('2026-06-08'))
        assert saved == 1
        assert materialized is not None
        assert materialized.time_seconds == 9

    def test_unlinked_imported_legacy_row_migrates_from_unique_raw_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        raw = (
            'Charlie LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_imported_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'queens'"
        ).fetchone()[0] == 0
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Charlie LinkedIn'))
        assert [row.time_seconds for row in source] == [9]

        db.set_minigame_player_link(
            100, 'queens', 300, 'Charlie LinkedIn',
            normalize_queens_name('Charlie LinkedIn'), None, 1.0, 999)
        saved = cog._sync_queens_materialized_results(100)

        materialized = db.get_minigame_result_for_user_puzzle(
            100, 'queens', 300, _queens_number('2026-06-08'))
        assert saved == 1
        assert materialized is not None
        assert materialized.time_seconds == 9

    def test_legacy_row_prefers_unique_raw_name_over_current_link(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Current LinkedIn',
            normalize_queens_name('Current LinkedIn'), None, 1.0, 999)
        raw = (
            'Original LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []
        original_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Original LinkedIn'))
        current_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Current LinkedIn'))
        assert [row.time_seconds for row in original_source] == [9]
        assert current_source == []

    def test_unmapped_legacy_row_is_preserved_when_identity_is_unknown(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.save_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, 'not a copied leaderboard')
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        assert saved == 0
        assert db.get_minigame_result(11) is not None
        assert db.get_minigame_unresolved_results_for_guild(100, 'queens') == []
        assert db.get_minigame_ratings(100, 'queens') == []

    def test_sync_does_not_remigrate_current_materialized_rows(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')
        cog = Minigames(bot=None)

        assert cog._sync_queens_materialized_results(100) == 1
        rows_before = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.time_seconds) for row in rows_before] == [
            ('300', 4),
        ]

        assert cog._migrate_legacy_queens_results_to_external(100) == 0
        rows_after = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.time_seconds) for row in rows_after] == [
            ('300', 4),
        ]

    def test_sync_does_not_rewrite_current_materialized_rows(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')
        cog = Minigames(bot=None)

        assert cog._sync_queens_materialized_results(100) == 1
        writes = []

        def record_write(*args, **kwargs):
            writes.append(args)
            raise AssertionError('current projection row was rewritten')

        monkeypatch.setattr(db, 'save_minigame_result', record_write)

        assert cog._sync_queens_materialized_results(
            100, migrate_legacy=False) == 0
        assert writes == []

    def test_generic_recompute_writes_queens_snapshot_only(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.replace_minigame_ratings(
            100, 'akari', [RatingState('999', 1500, 1, 1500, 0)], 1.0)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn'),
                (302, 'Cara LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            1, 100, 'queens', 200, 300, _queens_number('2026-06-08'), '2026-06-08',
            0, 8, False, 'fast no badges')
        db.save_minigame_result(
            2, 100, 'queens', 200, 301, _queens_number('2026-06-08'), '2026-06-08',
            100, 10, True, 'slow perfect')
        db.save_minigame_result(
            3, 100, 'queens', 200, 302, _queens_number('2026-06-08'), '2026-06-08',
            0, 10, False, 'slow imperfect')
        db.save_minigame_result(
            4, 100, 'queens', 200, 300, _queens_number('2026-06-09'), '2026-06-09',
            100, 5, True, 'alice solo')

        cog = Minigames(bot=None)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        queens = {
            row.user_id: row
            for row in db.get_minigame_ratings(100, 'queens')
        }
        assert set(queens) == {'300', '301', '302'}
        assert queens['300'].rating > queens['301'].rating
        assert abs(queens['301'].rating - queens['302'].rating) < 1e-9
        assert queens['300'].games == 2
        assert queens['301'].games == 1
        assert queens['302'].games == 1

        akari = db.get_minigame_rating(100, 'akari', 999)
        assert akari.rating == 1500
        assert akari.games == 1

    def test_queens_rating_does_not_decay_absent_players(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn'),
                (302, 'Cara LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            1, 100, 'queens', 200, 300, _queens_number('2026-06-08'), '2026-06-08',
            100, 5, True, 'alice fast')
        db.save_minigame_result(
            2, 100, 'queens', 200, 301, _queens_number('2026-06-08'), '2026-06-08',
            100, 10, True, 'bob slow')

        cog = Minigames(bot=None)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)
        alice_before = db.get_minigame_rating(100, 'queens', 300)

        db.save_minigame_result(
            3, 100, 'queens', 200, 301, _queens_number('2026-06-09'), '2026-06-09',
            100, 5, True, 'bob fast')
        db.save_minigame_result(
            4, 100, 'queens', 200, 302, _queens_number('2026-06-09'), '2026-06-09',
            100, 10, True, 'cara slow')
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        alice_after = db.get_minigame_rating(100, 'queens', 300)
        assert abs(alice_after.rating - alice_before.rating) < 1e-9
        assert alice_after.skip_streak == 1


class TestQueensCommands:
    @staticmethod
    def _make_ctx(guild, author):
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        return SimpleNamespace(
            guild=guild,
            author=author,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )

    @staticmethod
    def _save_queens_result(db, message_id, user_id, puzzle_date, time_seconds,
                            is_perfect=True, accuracy=100):
        day = dt.date.fromisoformat(puzzle_date)
        db.save_minigame_result(
            message_id, 100, 'queens', 200, user_id, _queens_number(day),
            puzzle_date, accuracy, time_seconds, is_perfect, puzzle_date)

    def test_stats_and_streak_use_queens_dates(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5, True, 100)
        self._save_queens_result(db, 2, alice.id, '2026-06-09', 9, False, 0)
        self._save_queens_result(db, 3, alice.id, '2026-06-10', 4, True, 100)
        self._save_queens_result(db, 4, alice.id, '2026-06-11', 6, True, 100)

        asyncio.run(cog._cmd_queens_stats(ctx))
        stats = ctx.sent['embed']
        assert stats.title == 'LinkedIn Queens Stats'
        assert 'Queens days: **4**' in stats.description
        assert 'Clean: **3**' in stats.description
        assert 'Current clean streak: **2**' in stats.description
        assert 'Latest: **2026-06-11**' in stats.description

        asyncio.run(cog._cmd_queens_stats(ctx, 'd>=10062026'))
        filtered = ctx.sent['embed']
        assert 'Queens days: **2**' in filtered.description

        asyncio.run(cog._cmd_queens_stats(ctx, '+dow=mon,wed'))
        weekday_filtered = ctx.sent['embed']
        assert weekday_filtered.title == 'LinkedIn Queens Stats (Mon/Wed)'
        assert 'Queens days: **2**' in weekday_filtered.description
        assert 'Clean: **2**' in weekday_filtered.description
        assert 'Latest: **2026-06-10**' in weekday_filtered.description

        asyncio.run(cog._cmd_queens_streak(ctx))
        streak = ctx.sent['embed']
        assert streak.title == 'LinkedIn Queens Streak'
        assert '**2** consecutive clean day(s)' in streak.description
        assert 'Latest result: **2026-06-11**' in streak.description

        asyncio.run(cog._cmd_queens_streak(ctx, '+dow=wed,thu'))
        weekday_streak = ctx.sent['embed']
        assert weekday_streak.title == 'LinkedIn Queens Streak (Wed/Thu)'
        assert '**2** consecutive clean day(s)' in weekday_streak.description

    def test_register_self_queues_connection_check(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        cog._set_queens_connection_account(
            100, 'Linked User', 'https://www.linkedin.com/in/linked/')

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Alice', linkedin='LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == 'Alice LinkedIn'
        instruction = cog._queens_connection_instruction(100)
        assert 'https://www.linkedin.com/in/linked/' in instruction
        assert 'Linked User' not in instruction
        assert 'registration is pending as `Alice LinkedIn`' in (
            ctx.sent['embed'].description)

    def test_register_other_accepts_after_linkedin_match(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+username', linkedin='bob Bob LinkedIn'))

        assert db.get_minigame_player_link(100, 'queens', bob.id) is None
        pending = list(cog._queens_pending_registrations.values())
        assert pending[0].name == 'Bob LinkedIn'
        assert '`Bob`' in ctx.sent['embed'].description

        async def fake_connect(guild_id, names):
            assert str(guild_id) == '100'
            assert names == ['Bob LinkedIn']
            return {
                'status': 'ok',
                'accepted': ['Bob LinkedIn'],
                'accepted_normalized': [normalize_queens_name('Bob LinkedIn')],
            }, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert cog._queens_pending_registrations == {}

    def test_set_registers_other_without_linkedin_match_and_clears_pending(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, alice, bob])
        cog = Minigames(bot=None)

        alice_ctx = self._make_ctx(guild, alice)
        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, alice_ctx, 'Bob', linkedin='LinkedIn'))
        assert cog._queens_pending_registrations[('100', '300')].name == (
            'Bob LinkedIn')

        mod_ctx = self._make_ctx(guild, mod)
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, mod_ctx, 'bob', linkedin='Bob LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert cog._queens_pending_registrations == {}
        assert '`Bob` is registered for LinkedIn Queens as `Bob LinkedIn`' in (
            mod_ctx.sent['embed'].description)

    def test_set_accepts_anonymous_flag(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, 'bob', linkedin='Bob LinkedIn +anon'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert row.external_url == minigames_module._QUEENS_ANONYMOUS_LINK_MARKER
        assert '`Bob` is registered for LinkedIn Queens as `Anonymous`' in (
            ctx.sent['embed'].description)
        assert 'Bob LinkedIn' not in ctx.sent['embed'].description

    def test_set_accepts_prefix_anonymous_flag(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='bob Bob LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert row.external_url == minigames_module._QUEENS_ANONYMOUS_LINK_MARKER
        assert '`Bob` is registered for LinkedIn Queens as `Anonymous`' in (
            ctx.sent['embed'].description)
        assert 'Bob LinkedIn' not in ctx.sent['embed'].description

    def test_set_does_not_report_claimed_count_on_repeat(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'dontdefense', 'dontdefense')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)
        for offset, seconds in enumerate((4, 5)):
            puzzle_date = dt.date(2026, 6, 8) + dt.timedelta(days=offset)
            db.save_minigame_unresolved_result(
                100, 'queens', normalize_queens_name('Dragos Ristache'),
                'Dragos Ristache', 200, _queens_number(puzzle_date),
                puzzle_date.isoformat(), 100, seconds, True, 'source')

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='dontdefense Dragos Ristache'))
        assert 'Claimed' not in ctx.sent['embed'].description
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='dontdefense Dragos Ristache'))
        assert 'Claimed' not in ctx.sent['embed'].description
        assert 'Dragos Ristache' not in ctx.sent['embed'].description
        assert len(db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Dragos Ristache'))) == 2

    def test_pending_register_expires_after_linkedin_scan_without_match(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_alert',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Alice', linkedin='LinkedIn'))
        pending = list(cog._queens_pending_registrations.values())

        async def fake_connect(_guild_id, _names):
            return {'status': 'ok', 'accepted': [], 'accepted_normalized': []}, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        assert cog._queens_pending_registrations == {}
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_plain_username_stays_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'bob', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == 'bob Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        assert db.get_minigame_player_link(100, 'queens', bob.id) is None

    def test_register_non_username_plus_token_stays_linkedin_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+bob', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == '+bob Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_plain_mention_stays_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '<@301>', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == '<@301> Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        assert db.get_minigame_player_link(100, 'queens', bob.id) is None

    def test_slash_register_self_does_not_require_mod_role(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
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
        cog = Minigames(bot=None)

        asyncio.run(cog.slash_queens_register(
            interaction, 'Alice LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        assert cog._queens_pending_registrations[('100', '300')].name == (
            'Alice LinkedIn')
        assert interaction.response.deferred is True
        assert 'registration is pending as `Alice LinkedIn`' in (
            interaction.followup.sent[0]['embed'].description)

    def test_update_sends_slow_notice_when_not_rate_limited(
            self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.kvs_get = lambda _key: None
        kvs_updates = []
        db.kvs_set = lambda key, value: kvs_updates.append((key, value))
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        async def send(content=None, *, embed=None, **kwargs):
            sent.append({'content': content, 'embed': embed, 'kwargs': kwargs})

        ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
        )
        state_path = tmp_path / 'queens_state.json'
        state_path.write_text('{}')
        cog = Minigames(bot=None)
        monkeypatch.setattr(cog, '_queens_state_path', lambda _guild_id: state_path)

        async def fake_scraper(_guild_id, *, auto_play):
            assert auto_play is False
            return {'status': 'ok', 'raw_text': ''}, None

        imports = []

        async def fake_import(_ctx, payload, *, source_label):
            imports.append((payload, source_label))

        monkeypatch.setattr(cog, '_run_queens_scraper', fake_scraper)
        monkeypatch.setattr(cog, '_do_queens_import', fake_import)

        asyncio.run(Minigames.queens_update.__wrapped__(cog, ctx))

        assert sent[0]['content'] == 'This will take a while'
        assert len(kvs_updates) == 1
        assert imports == [({'status': 'ok', 'raw_text': ''}, 'Update')]

    def test_update_rate_limit_skips_slow_notice(self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.kvs_get = lambda _key: str(time.time())
        db.kvs_set = lambda _key, _value: None
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        async def send(content=None, *, embed=None, **kwargs):
            sent.append({'content': content, 'embed': embed, 'kwargs': kwargs})

        ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
        )
        state_path = tmp_path / 'queens_state.json'
        state_path.write_text('{}')
        cog = Minigames(bot=None)
        monkeypatch.setattr(cog, '_queens_state_path', lambda _guild_id: state_path)

        with pytest.raises(MinigameCogError, match='rate-limited'):
            asyncio.run(Minigames.queens_update.__wrapped__(cog, ctx))

        assert sent == []

    def test_register_anonymous_keeps_linkedin_name_private(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+anon', linkedin='Alice LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        pending = list(cog._queens_pending_registrations.values())
        assert pending[0].name == 'Alice LinkedIn'
        assert 'Anonymous' in ctx.sent['embed'].description
        assert 'Alice LinkedIn' not in ctx.sent['embed'].description

        async def fake_connect(_guild_id, _names):
            return {
                'status': 'ok',
                'accepted': ['Alice LinkedIn'],
                'accepted_normalized': [normalize_queens_name('Alice LinkedIn')],
            }, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row.external_name == 'Alice LinkedIn'
        assert row.normalized_name == normalize_queens_name('Alice LinkedIn')
        assert row.external_url == (
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER)

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))

        asyncio.run(Minigames.queens_links.__wrapped__(cog, ctx))

        assert pages
        assert 'Alice: `Anonymous`' in pages[0][1].description
        assert 'Alice LinkedIn' not in pages[0][1].description

    def test_anonymous_modal_response_shows_private_name_without_context_object(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        class Response:
            async def send_message(self, content=None, *, embed=None,
                                   ephemeral=False, **kwargs):
                sent.append({
                    'content': content,
                    'embed': embed,
                    'ephemeral': ephemeral,
                    'kwargs': kwargs,
                })

        interaction = SimpleNamespace(
            guild=guild,
            user=alice,
            channel_id=200,
            response=Response(),
        )
        cog = Minigames(bot=None)
        modal = minigames_module._QueensAnonymousRegisterModal(cog)
        modal.linkedin_name.value = 'Alice LinkedIn'

        asyncio.run(modal.on_submit(interaction))

        assert len(sent) == 1
        assert sent[0]['content'] is None
        assert sent[0]['ephemeral'] is True
        assert 'registration is pending as `Alice LinkedIn`' in (
            sent[0]['embed'].description)
        assert '_QueensModalCtx object' not in sent[0]['embed'].description
        assert cog._queens_pending_registrations[('100', '300')].anonymous is True

    def test_anonymous_pending_expiry_hides_linkedin_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_alert',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        class Channel:
            async def send(self, *, embed=None, **kwargs):
                sent.append(embed)

        class Bot:
            def get_channel(self, channel_id):
                assert channel_id == 200
                return Channel()

        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=Bot())

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+anon', linkedin='Alice LinkedIn'))
        pending = list(cog._queens_pending_registrations.values())

        async def fake_connect(_guild_id, _names):
            return {'status': 'ok', 'accepted': [], 'accepted_normalized': []}, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        assert sent
        assert 'Anonymous' in sent[-1].description
        assert 'Alice LinkedIn' not in sent[-1].description

    def test_anonymous_duplicate_registration_hides_linkedin_name(
            self, db, monkeypatch):
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

        with pytest.raises(MinigameCogError) as exc_info:
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, '+anon', linkedin='Alice LinkedIn'))

        assert 'Anonymous' in str(exc_info.value)
        assert 'Alice LinkedIn' not in str(exc_info.value)

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
        assert 'LinkedIn name will not be posted' in (
            ctx.sent['embed'].description)
        view = ctx.sent['kwargs']['view']
        assert view.requester_id == alice.id
        assert view.children[0].label == 'Enter LinkedIn name'

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
        assert 'registration is pending as `Alice LinkedIn`' in (
            captured['embed'].description)

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
        assert 'Backfilled **1** result(s) for `Alice`' in (
            ctx.sent['embed'].description)

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
        description = ctx.sent['embed'].description
        assert 'Backfilled **3** LinkedIn-name result(s)' in description
        assert 'Parsed **4** valid JSON result(s)' in description
        assert '**2** registered LinkedIn name(s)' in description
        assert '**1** unregistered LinkedIn name(s)' in description
        assert 'Skipped **1** already-saved result(s)' in description
        assert 'Ignored **1** malformed entry/entries' in description

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
        assert 'Added LinkedIn Queens result for `Alice`' in ctx.sent['embed'].description

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
        assert 'Removed LinkedIn Queens result for `Alice`' in ctx.sent['embed'].description

    def test_clear_removes_all_results_for_queens_date(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        for member, name in ((alice, 'Alice LinkedIn'), (bob, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', member.id, name, normalize_queens_name(name),
                None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 6)
        self._save_queens_result(db, 3, alice.id, '2026-06-09', 4)
        # Compatibility check for rows stored before Queens got real numbers.
        db.save_imported_minigame_result(
            4, 100, 'queens', 200, bob.id,
            dt.date(2026, 6, 8).toordinal(), '2026-06-08',
            100, 7, True, 'imported')
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Unknown Person'),
            'Unknown Person', 200, dt.date(2026, 6, 8).toordinal(),
            '2026-06-08', 100, 9, True, 'raw')

        asyncio.run(Minigames.queens_clear.__wrapped__(
            cog, ctx, '2026-06-08'))

        remaining = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.puzzle_date) for row in remaining] == [
            ('300', '2026-06-09'),
        ]
        assert db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', dt.date(2026, 6, 8).toordinal()) == []
        assert ('Removed 3 registered and 1 unresolved LinkedIn Queens result(s) '
                'for #769 2026-06-08') in (
            ctx.sent['embed'].description)
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == ['300']

    def test_clean_removes_queens_date_range(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        for member, name in ((alice, 'Alice LinkedIn'), (bob, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', member.id, name, normalize_queens_name(name),
                None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-09', 6)
        self._save_queens_result(db, 3, alice.id, '2026-06-10', 4)
        db.save_imported_minigame_result(
            4, 100, 'queens', 200, bob.id, _queens_number('2026-06-09'),
            '2026-06-09', 100, 7, True, 'imported')
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Unknown Person'),
            'Unknown Person', 200, _queens_number('2026-06-09'),
            '2026-06-09', 100, 9, True, 'raw')

        asyncio.run(Minigames.queens_clean.__wrapped__(
            cog, ctx, '2026-06-08', '2026-06-09'))

        remaining = db.get_minigame_results_for_guild(100, 'queens')
        assert sorted((row.user_id, row.puzzle_date) for row in remaining) == [
            ('300', '2026-06-10'),
        ]
        assert db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', _queens_number('2026-06-09')) == []
        assert ('Removed 3 registered and 1 unresolved LinkedIn Queens result(s) '
                'from 2026-06-08 to 2026-06-09 (2 day(s))') in (
            ctx.sent['embed'].description)
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == ['300']

    def test_ratings_use_image_and_default_to_registered_players(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 10)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        captured = []

        def _capture(guild, rating_rows, registrants, **kwargs):
            captured.append({
                'user_ids': [row.user_id for row in rating_rows],
                'games': [row.games for row in rating_rows],
                'registrants': set(registrants),
                'identity_label': kwargs['identity_label'],
                'mark_registered': kwargs['mark_registered'],
            })
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', _capture)

        asyncio.run(cog._cmd_queens_ratings(ctx))
        assert captured[-1]['user_ids'] == ['300']
        assert captured[-1]['games'] == [1]
        assert captured[-1]['identity_label'] == 'LinkedIn'
        assert captured[-1]['mark_registered'] is False
        assert 'file' in ctx.sent['kwargs']

        asyncio.run(cog._cmd_queens_ratings(ctx, show_all=True))
        assert set(captured[-1]['user_ids']) == {'300'}
        assert captured[-1]['mark_registered'] is True

    def test_queens_ratings_requires_enabled(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='not enabled'):
            asyncio.run(cog._cmd_queens_ratings(ctx))

    def test_anonymous_registration_hides_linkedin_identity_only(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER, 1.0, alice.id)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Bob LinkedIn',
            normalize_queens_name('Bob LinkedIn'), None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 10)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        captured_results = {}

        def _capture_results(guild, rows, title, **kwargs):
            captured_results['names'] = [
                kwargs['name_fn'](guild, row) for row in rows]
            captured_results['identities'] = [
                kwargs['identity_fn'](guild, row) for row in rows]
            captured_results['title'] = title
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_queens_results_table_image_file',
            _capture_results)

        asyncio.run(Minigames.queens_results.__wrapped__(
            cog, ctx, '2026-06-08'))

        assert captured_results['names'] == ['Alice', 'Bob']
        assert captured_results['identities'] == ['Anonymous', 'Bob LinkedIn']

        captured_ratings = {}

        def _capture_ratings(guild, rating_rows, registrants, **kwargs):
            captured_ratings['names'] = [
                kwargs['name_fn'](guild, row) for row in rating_rows]
            captured_ratings['identities'] = [
                kwargs['identity_fn'](guild, row) for row in rating_rows]
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file',
            _capture_ratings)

        asyncio.run(cog._cmd_queens_ratings(ctx, show_all=True))

        assert captured_ratings['names'][0] == 'Alice'
        assert captured_ratings['identities'][0] == 'Anonymous'

    def test_queens_rating_performance_and_history_views(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())

        for member, name in ((alice, 'Alice LinkedIn'), (bob, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', member.id, name,
                normalize_queens_name(name), None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 10)
        self._save_queens_result(db, 3, alice.id, '2026-06-09', 9)
        self._save_queens_result(db, 4, bob.id, '2026-06-09', 4)
        self._save_queens_result(db, 5, alice.id, '2026-06-10', 7)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        rating_series = {}
        perf_series = {}
        fake_file = SimpleNamespace(filename='plot.png')

        def _rating(series):
            rating_series['names'] = [name for _history, name in series]
            rating_series['dates'] = [
                [str(point.puzzle_date) for point in history]
                for history, _name in series
            ]
            return fake_file

        def _performance(series):
            perf_series['names'] = [name for _history, name, _rating in series]
            return fake_file

        monkeypatch.setattr(minigames_module, 'plot_akari_rating', _rating)
        monkeypatch.setattr(minigames_module, 'plot_akari_performance', _performance)

        asyncio.run(cog._cmd_queens_rating(ctx, [alice, bob]))
        assert rating_series['names'] == ['Alice LinkedIn', 'Bob LinkedIn']
        assert ctx.sent['embed'].title == 'LinkedIn Queens ratings — 2 players'
        assert ctx.sent['kwargs']['file'] is fake_file

        asyncio.run(cog._cmd_queens_rating(ctx, [alice], weekdays={0, 2}))
        assert rating_series['dates'] == [['2026-06-08', '2026-06-10']]

        asyncio.run(cog._cmd_queens_performance(ctx, [alice]))
        assert perf_series['names'] == ['Alice LinkedIn']
        assert ctx.sent['embed'].title == 'LinkedIn Queens performance — Alice'

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))
        asyncio.run(cog._cmd_queens_history(ctx, alice))
        assert pages
        assert pages[0][1].title.endswith('(3 days)')
        assert '2026-06-10' in pages[0][1].description
        assert '2026-06-09' in pages[0][1].description
        assert 'solo' in pages[0][1].description
        assert '**#' not in pages[0][1].description

    def test_queens_history_shows_solo_only_days(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())
        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))

        asyncio.run(cog._cmd_queens_history(ctx, alice))

        assert pages
        assert pages[0][1].title.endswith('(1 day)')
        assert '2026-06-08' in pages[0][1].description
        assert 'solo' in pages[0][1].description

    def test_anonymous_registration_hides_graph_identity_only(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER, 1.0, alice.id)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Bob LinkedIn',
            normalize_queens_name('Bob LinkedIn'), None, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 10)
        self._save_queens_result(db, 3, alice.id, '2026-06-09', 9)
        self._save_queens_result(db, 4, bob.id, '2026-06-09', 4)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        rating_series = {}
        perf_series = {}
        fake_file = SimpleNamespace(filename='plot.png')
        monkeypatch.setattr(
            minigames_module, 'plot_akari_rating',
            lambda series: rating_series.update(
                names=[name for _history, name in series]) or fake_file)
        monkeypatch.setattr(
            minigames_module, 'plot_akari_performance',
            lambda series: perf_series.update(
                names=[name for _history, name, _rating in series]) or fake_file)

        asyncio.run(cog._cmd_queens_rating(ctx, [alice]))
        assert rating_series['names'] == ['Anonymous']
        assert ctx.sent['embed'].title == 'LinkedIn Queens rating — Alice'

        asyncio.run(cog._cmd_queens_performance(ctx, [alice]))
        assert perf_series['names'] == ['Anonymous']
        assert ctx.sent['embed'].title == (
            'LinkedIn Queens performance — Alice')

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))
        asyncio.run(cog._cmd_queens_history(ctx, alice))
        assert pages
        assert pages[0][1].title.startswith(
            'LinkedIn Queens rating history — Alice')

    def test_queens_rating_filters_reject_decay(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='do not use decay'):
            asyncio.run(cog._extract_queens_rating_filters(ctx, ['+decay']))

        remaining, excluded_ids, included_ids, weekdays = asyncio.run(
            cog._extract_queens_rating_filters(
                ctx, ['+dow=mon,wed', '+include=alice']))
        assert remaining == []
        assert excluded_ids == set()
        assert included_ids == {'300'}
        assert weekdays == {0, 2}

        with pytest.raises(MinigameCogError, match='Unknown Queens weekday'):
            asyncio.run(cog._extract_queens_rating_filters(ctx, ['+dow=funday']))

    def test_queens_results_renders_date_results_image(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, alice.id)
        self._save_queens_result(db, 1, bob.id, '2026-06-08', 8)
        self._save_queens_result(db, 2, alice.id, '2026-06-08', 5)

        captured = []

        def _capture(guild, rows, title, **kwargs):
            captured.append({
                'user_ids': [row.user_id for row in rows],
                'title': title,
                'identity_label': kwargs['identity_label'],
                'registrants': set(kwargs['registrants']),
            })
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_queens_results_table_image_file', _capture)

        asyncio.run(Minigames.queens_results.__wrapped__(cog, ctx, '769'))

        assert captured[-1]['title'] == 'LinkedIn Queens #769 2026-06-08 Results'
        assert captured[-1]['identity_label'] == 'LinkedIn'
        assert captured[-1]['user_ids'] == ['300']
        assert set(captured[-1]['registrants']) == {'300'}
        assert 'file' in ctx.sent['kwargs']

        asyncio.run(cog._cmd_queens_stats_date(ctx, '769', show_all=True))

        assert set(captured[-1]['user_ids']) == {'300', '301'}
        assert set(captured[-1]['registrants']) == {'300'}

    def test_queens_results_table_omits_accuracy_result_column(
            self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image',
            lambda rows, **kwargs: captured.update(rows=rows, **kwargs) or object())
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'alice', 'Alice'),
        ])
        row = _row(1, 300, '2026-06-08', True, 5, 100, 769)

        minigames_module._get_queens_results_table_image_file(
            guild, [row], 'Queens Results',
            identity_fn=lambda _guild, _row: 'Alice LinkedIn')

        assert captured['header'] == ('#', 'Name', 'LinkedIn', 'Time')
        assert captured['rows'] == [(1, 'Alice', 'Alice LinkedIn', '0:05')]

        minigames_module._get_queens_results_table_image_file(
            guild, [row], 'Queens Results',
            puzzle_info={
                '300': minigames_module._PuzzlePlayerInfo(
                    pre_rating=1200.0, delta=10.0),
            },
            registrants={'300'},
            identity_fn=lambda _guild, _row: 'Alice LinkedIn')

        assert captured['header'] == (
            '#', 'Name', 'LinkedIn', 'Time', '\N{INCREMENT}')
        assert captured['rows'] == [
            (1, 'Alice (1200 E)', 'Alice LinkedIn', '0:05', '+10')]

    def test_queens_stats_keeps_number_args_as_personal_filters(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)

        with pytest.raises(MinigameCogError, match='Unrecognized filter'):
            asyncio.run(Minigames.queens_stats.__wrapped__(cog, ctx, '769'))

    def test_ban_removes_link_and_excludes_queens_rating(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(100, members=[alice, bob, mod])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, mod.id)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Bob LinkedIn',
            normalize_queens_name('Bob LinkedIn'), None, 1.0, mod.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 6)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)
        assert {row.user_id for row in db.get_minigame_ratings(100, 'queens')} == {
            '300', '301',
        }

        asyncio.run(Minigames.queens_ban.__wrapped__(
            cog, ctx, alice, reason='duplicate account'))

        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        assert db.is_minigame_banned(100, 'queens', alice.id) is True
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == ['301']
        assert db.get_minigame_ban(100, 'queens', alice.id).reason == 'duplicate account'

    def test_import_skips_banned_linked_user(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, bob)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, bob.id)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Bob LinkedIn',
            normalize_queens_name('Bob LinkedIn'), None, 1.0, bob.id)
        db.ban_minigame_user(
            100, 'queens', alice.id, 1.0, bob.id, 'duplicate account')

        preview = cog._make_queens_import_preview(ctx, '2026-06-08', (
            'Alice LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'Bob LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:05\n'
        ))

        assert [entry.user_id for entry in preview.resolved] == ['301']

    def test_vs_uses_time_only_scoring(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5, False, 0)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 7, True, 100)
        self._save_queens_result(db, 3, alice.id, '2026-06-09', 8, True, 100)
        self._save_queens_result(db, 4, bob.id, '2026-06-09', 8, False, 0)

        asyncio.run(cog._cmd_vs(ctx, QUEENS_GAME, alice, bob))

        embed = ctx.sent['embed']
        assert embed.title == 'LinkedIn Queens Head to Head'
        assert '`Alice`: **1.5** points, **1** wins' in embed.description
        assert '`Bob`: **0.5** points, **0** wins' in embed.description
        assert 'Ties: **1**' in embed.description

    def test_top_counts_fastest_winners(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())

        db.set_minigame_player_link(
            100, 'queens', alice.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'),
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER, 1.0, alice.id)
        self._save_queens_result(db, 1, alice.id, '2026-06-08', 10, False, 0)
        self._save_queens_result(db, 2, bob.id, '2026-06-08', 5, True, 100)
        self._save_queens_result(db, 3, alice.id, '2026-06-09', 12, True, 100)
        self._save_queens_result(db, 4, bob.id, '2026-06-09', 4, False, 0)

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))

        asyncio.run(cog._cmd_top(ctx, QUEENS_GAME))

        assert len(pages) == 1
        embed = pages[0][1]
        assert embed.title == 'LinkedIn Queens Winners'
        assert '`Alice` — **2** wins' in embed.description
        assert '`Bob`' not in embed.description


class TestCogIngest:
    def test_ingests_only_enabled_configured_channel(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        message = _FakeMessage(
            123, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🌟 🕓 1:29\nhttps://dailyakari.com/'
        )
        asyncio.run(cog.on_message(message))

        row = db.get_minigame_result(123)
        assert row is not None
        assert row.user_id == '999'
        assert row.guild_id == '1'
        assert row.channel_id == '10'
        assert row.game == _GAME

    def test_ignores_disabled_feature(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        message = _FakeMessage(
            123, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🌟 🕓 1:29\nhttps://dailyakari.com/'
        )
        asyncio.run(cog.on_message(message))

        assert db.get_minigame_result(123) is None

    def test_only_first_message_counts_for_user_puzzle(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        first = _FakeMessage(
            123, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🌟 Perfect! 🕓 1:29\nhttps://dailyakari.com/'
        )
        second = _FakeMessage(
            124, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🎯 96% 🕓 1:00\nhttps://dailyakari.com/'
        )

        async def _inner():
            await cog.on_message(first)
            await cog.on_message(second)
        asyncio.run(_inner())

        row = db.get_minigame_result_for_user_puzzle(1, _GAME, 999, 445)
        assert row is not None
        assert row.message_id == '123'

    def test_edit_updates_raw_content(self, db, monkeypatch):
        """Editing a message should update the stored raw_content."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        original = 'Daily Akari 445\n\u27052026-03-26\u2705\n\U0001f31f \U0001f553 1:29\nhttps://dailyakari.com/'
        edited = 'Daily Akari 445\n\u27052026-03-26\u2705\n\U0001f31f \U0001f553 2:00\nhttps://dailyakari.com/'

        msg = _FakeMessage(123, 1, 10, 999, original)
        asyncio.run(cog.on_message(msg))
        row = db.get_minigame_result(123)
        assert row.raw_content == original

        before = _FakeMessage(123, 1, 10, 999, original)
        after = _FakeMessage(123, 1, 10, 999, edited)
        asyncio.run(cog.on_message_edit(before, after))
        row = db.get_minigame_result(123)
        assert row.raw_content == edited

    def test_edit_in_non_configured_channel_is_ignored(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        # Edit a message in channel 99 (not the configured channel 10)
        before = _FakeMessage(50, 1, 99, 999, 'old content')
        after = _FakeMessage(50, 1, 99, 999, 'new content')
        asyncio.run(cog.on_message_edit(before, after))
        # Should not trigger any DB writes — no result to find or delete
        assert db.get_minigame_result(50) is None

    def test_edit_removes_result_from_multi_result_message(self, db, monkeypatch):
        """Editing a multi-result message to have fewer results should delete removed ones."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        two_results = (
            '<#123> #1407\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1407\n\n'
            '<#123> #1412\n'
            '\U0001f3ae \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1412'
        )
        one_result = (
            '<#123> #1407\n'
            '\U0001f3ae \U0001f7e5 \U0001f7e5 \U0001f7e5 \U0001f7e9 \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1407'
        )

        cog = Minigames(bot=None)
        msg = _FakeMessage(500, 1, 10, 999, two_results)
        asyncio.run(cog.on_message(msg))
        assert len(db.get_minigame_results_for_user(1, 'guessgame', 999)) == 2

        before = _FakeMessage(500, 1, 10, 999, two_results)
        after = _FakeMessage(500, 1, 10, 999, one_result)
        asyncio.run(cog.on_message_edit(before, after))
        rows = db.get_minigame_results_for_user(1, 'guessgame', 999)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 1407

    def test_on_raw_message_delete_removes_results(self, db, monkeypatch):
        """on_raw_message_delete should remove results from both tables."""
        import types
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.save_minigame_result(500, 1, 'guessgame', 10, 999, 1407, '2026-03-26', 3, 7, 0, 'c')
        db.save_imported_minigame_result(500, 1, 'guessgame', 10, 999, 1412, '2026-03-26', 6, 7, 1, 'c')

        cog = Minigames(bot=None)
        payload = types.SimpleNamespace(guild_id=1, message_id=500)
        asyncio.run(cog.on_raw_message_delete(payload))

        assert db.get_minigame_result(500) is None
        rows = db.conn.execute(
            'SELECT * FROM minigame_import_result WHERE message_id = ?', ('500',)
        ).fetchall()
        assert len(rows) == 0

    def test_date_fallback_uses_message_created_at(self, db, monkeypatch):
        """When parser returns puzzle_date=None, cog should use message.created_at."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'guessgame', '1')
        db.set_minigame_channel(1, 'guessgame', 10)

        content = (
            '<#123> #1412\n'
            '\U0001f3ae \U0001f7e9 \u2b1c \u2b1c \u2b1c \u2b1c \u2b1c\n'
            'https://GuessThe.Game/p/1412'
        )
        cog = Minigames(bot=None)
        msg = _FakeMessage(500, 1, 10, 999, content)
        msg.created_at = dt.datetime(2025, 12, 25, tzinfo=dt.timezone.utc)
        asyncio.run(cog.on_message(msg))

        row = db.get_minigame_result(500)
        assert row.puzzle_date == '2025-12-25'


class TestFormatting:
    def test_format_akari_puzzle_table_orders_best_results_first(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: {
                '10': 'alice_cf',
                '20': 'bob_cf',
                '30': 'cara_cf',
            }.get(str(user_id))
        ))
        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'alice', 'Alice'),
            _FakeDiscordMember(20, 'bob', 'Bob'),
            _FakeDiscordMember(30, 'cara', 'Cara'),
        ])
        table_str = _format_akari_puzzle_table(guild, [
            _row(3, 30, '2026-03-26', False, 50, 97, 445),
            _row(2, 20, '2026-03-26', True, 80, 100, 445),
            _row(1, 10, '2026-03-26', True, 60, 100, 445),
        ])

        assert '#  Name' in table_str
        assert 'Handle' in table_str
        assert '1  Alice  alice_cf  100%    1:00' in table_str
        assert '2  Bob    bob_cf    100%    1:20' in table_str
        assert '3  Cara   cara_cf   97%     0:50' in table_str

    def test_get_akari_puzzle_table_image_returns_png_file(self, monkeypatch):
        class _Surface:
            def __init__(self, *args):
                pass

            def write_to_png(self, fp):
                fp.write(b'png-data')

        class _Context:
            def __init__(self, *args):
                pass

            def set_source_rgb(self, *args):
                pass

            def rectangle(self, *args):
                pass

            def fill(self):
                pass

            def move_to(self, *args):
                pass

            def rel_move_to(self, *args):
                pass

        class _Layout:
            def set_font_description(self, *args):
                pass

            def set_ellipsize(self, *args):
                pass

            def set_width(self, *args):
                pass

            def set_alignment(self, *args):
                pass

            def set_markup(self, *args):
                pass

        monkeypatch.setattr(minigames_module.cairo, 'FORMAT_ARGB32', 0, raising=False)
        monkeypatch.setattr(minigames_module.cairo, 'ImageSurface', _Surface, raising=False)
        monkeypatch.setattr(minigames_module.cairo, 'Context', _Context, raising=False)
        monkeypatch.setattr(minigames_module.Pango, 'SCALE', 1000, raising=False)
        monkeypatch.setattr(
            minigames_module.Pango, 'Alignment',
            SimpleNamespace(LEFT=0, RIGHT=1), raising=False)
        monkeypatch.setattr(
            minigames_module.PangoCairo, 'create_layout',
            lambda _context: _Layout(), raising=False)
        monkeypatch.setattr(
            minigames_module.PangoCairo, 'show_layout',
            lambda *_args, **_kw: None, raising=False)
        monkeypatch.setattr(
            minigames_module.discord, 'File',
            lambda fp, filename: SimpleNamespace(fp=fp, filename=filename), raising=False)
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: {
                '10': 'alice_cf',
                '20': 'emoji_cf',
            }.get(str(user_id))
        ))

        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'alice', 'Alice' * 200),
            _FakeDiscordMember(20, 'emoji', '🧶'),
        ])
        rows = [
            _row(1, 10, '2026-03-26', True, 60, 100, 445),
            _row(2, 20, '2026-03-26', False, 80, 98, 445),
        ]

        discord_file = _get_akari_puzzle_table_image(
            _akari_puzzle_table_rows(guild, rows))

        assert discord_file.filename == 'akari-results.png'
        assert discord_file.fp.getbuffer().nbytes > 0

    def test_akari_puzzle_table_image_file_is_bounded(self, monkeypatch):
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image',
            lambda rows, *, title=None, footer=None, **_: SimpleNamespace(
                rows=rows, title=title, footer=footer, filename='akari-results.png'))
        handle_lookups = []
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: handle_lookups.append(user_id) or f'h{user_id}'
        ))

        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(user_id, f'user{user_id}', f'Player {user_id:03d}')
            for user_id in range(1, 46)
        ])
        rows = [
            _row(user_id, user_id, '2026-03-26', False, user_id, 90, 445)
            for user_id in range(1, 46)
        ]

        discord_file = _get_akari_puzzle_table_image_file(
            guild, rows, 'Akari Results')

        assert len(discord_file.rows) == 40
        assert discord_file.filename == 'akari-results.png'
        assert discord_file.title == 'Akari Results'
        assert discord_file.footer == 'Showing top 40 of 45 results'
        assert handle_lookups == [str(user_id) for user_id in range(1, 41)]

    def test_akari_puzzle_selector_sends_image_file_without_embed(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.save_minigame_result(
            1, 1, 'akari', 10, 20, 445, '2026-03-26', 100, 60, True, 'raw')

        image_file = SimpleNamespace(filename='akari-results.png')
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image_file',
            lambda guild, rows, title, **_: image_file)

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=_FakeGuild(1, members=[_FakeDiscordMember(20, 'alice', 'Alice')]),
            author=_FakeDiscordMember(20, 'alice', 'Alice'),
            send=send,
        )

        asyncio.run(Minigames(bot=None)._cmd_stats(ctx, AKARI_GAME, '445'))

        assert sent['content'] is None
        assert sent['embed'] is None
        assert sent['kwargs']['file'] is image_file


def test_slash_context_forwards_file_kwarg():
    captured = {}

    class _Followup:
        async def send(self, content=None, *, embed=None, wait=False, **kw):
            captured['content'] = content
            captured['embed'] = embed
            captured['wait'] = wait
            captured['kw'] = kw

    interaction = SimpleNamespace(
        id=999,
        guild=object(),
        user=object(),
        channel_id=123,
        client=object(),
        followup=_Followup(),
    )
    ctx = _SlashCtx(interaction)

    asyncio.run(ctx.send(embed='embed', file='file'))

    assert captured['embed'] == 'embed'
    assert captured['wait'] is True
    assert captured['kw']['file'] == 'file'


class TestCogSafety:
    """Tests for cog robustness: exception handling, cancellation, cleanup."""

    def test_import_cancellation_rolls_back_partial_batch(self, db, monkeypatch):
        """Cancelling an import mid-batch should not leave orphan rows
        that get committed by a later DB operation."""
        monkeypatch.setattr(cf_common, 'user_db', db)

        akari_fmt = 'Daily Akari {n}\n\u27052026-03-26\u2705\n\U0001f31f \U0001f553 1:29\nhttps://dailyakari.com/'
        messages = [
            _FakeMessage(i, 1, 10, 999, akari_fmt.format(n=i))
            for i in range(1, 6)
        ]

        class _CancelAfterN:
            """Async iterator that yields n items then raises CancelledError."""
            def __init__(self, msgs, n):
                self._msgs = iter(msgs)
                self._n = n
                self._count = 0
            def __aiter__(self):
                return self
            async def __anext__(self):
                if self._count >= self._n:
                    raise asyncio.CancelledError()
                try:
                    msg = next(self._msgs)
                except StopIteration:
                    raise StopAsyncIteration
                self._count += 1
                return msg

        class _FakeChan:
            id = 10
            def history(self, **kw):
                return _CancelAfterN(messages, 3)

        class _FakeBot:
            def get_channel(self, cid):
                return _FakeChan()

        from tle.cogs._minigame_akari import AKARI_GAME
        cog = Minigames(bot=_FakeBot())
        key = (1, 'akari')
        cog._import_status[key] = {
            'state': 'running', 'scanned': 0, 'done': 0,
            'latest_message_id': None, 'error': None,
        }

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(cog._run_import(1, 10, AKARI_GAME))

        # A subsequent operation that calls commit() should NOT leak the orphan rows
        db.save_minigame_result(999, 1, 'akari', 10, 888, 999, '2026-04-01', 100, 50, 1, 'c')

        rows = db.conn.execute('SELECT * FROM minigame_import_result').fetchall()
        assert len(rows) == 0

    def test_cog_unload_cancels_import_tasks(self):
        """cog_unload should cancel all running import tasks."""
        cog = Minigames(bot=None)

        async def _test():
            async def long_task():
                await asyncio.sleep(10000)

            task = asyncio.create_task(long_task())
            cog._import_tasks[(1, 'akari')] = task
            await cog.cog_unload()
            assert task.cancelled()

        asyncio.run(_test())

    def test_on_message_catches_exceptions(self, db, monkeypatch):
        """on_message should not propagate exceptions from _ingest_message."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)

        async def bad_ingest(msg, game):
            raise RuntimeError('DB exploded')
        monkeypatch.setattr(cog, '_ingest_message', bad_ingest)

        message = _FakeMessage(123, 1, 10, 999, 'anything')
        # Should not raise
        asyncio.run(cog.on_message(message))

    def test_on_message_edit_catches_exceptions(self, db, monkeypatch):
        """on_message_edit should not propagate exceptions."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)

        async def bad_ingest(msg, game):
            raise RuntimeError('DB exploded')
        monkeypatch.setattr(cog, '_ingest_message', bad_ingest)

        content = 'Daily Akari 445\n\u27052026-03-26\u2705\n\U0001f31f \U0001f553 1:29\nhttps://dailyakari.com/'
        before = _FakeMessage(50, 1, 10, 999, 'old content')
        after = _FakeMessage(50, 1, 10, 999, content)
        # Should not raise
        asyncio.run(cog.on_message_edit(before, after))


class TestUpgrade:
    def test_upgrade_1_14_0_creates_tables(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_14_0(conn)
        conn.execute('SELECT * FROM minigame_config').fetchall()
        conn.execute('SELECT * FROM minigame_result').fetchall()
        conn.close()

    def test_upgrade_1_15_0_creates_import_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_15_0(conn)
        conn.execute('SELECT * FROM minigame_import_result').fetchall()
        conn.close()


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


class _FakeMember:
    def __init__(self, name, display_name=None):
        self.name = name
        self.display_name = display_name or name


class TestCaseInsensitiveMember:
    """CaseInsensitiveMember falls back to case-insensitive name matching."""

    def _make_ctx(self, members):
        class Ctx:
            pass
        ctx = Ctx()
        guild = _FakeGuild(1)
        guild.members = members
        ctx.guild = guild
        ctx.bot = type('Bot', (), {'get_guild': lambda self, gid: guild})()
        return ctx

    def test_exact_case_matches(self):
        m = _FakeMember('Alice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'Alice'))
        assert result is m

    def test_different_case_matches(self):
        m = _FakeMember('Alice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'alice'))
        assert result is m

    def test_display_name_case_insensitive(self):
        m = _FakeMember('alice123', display_name='BigAlice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'bigalice'))
        assert result is m

    def test_no_match_raises(self):
        ctx = self._make_ctx([_FakeMember('Bob')])
        from tle.cogs.minigames import CaseInsensitiveMember
        with pytest.raises(Exception):
            asyncio.run(CaseInsensitiveMember().convert(ctx, 'alice'))


# ── Slash command adapter tests ────────────────────────────────────────

class _FakeFollowup:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, *, embed=None, view=None, wait=False, **kw):
        msg = type('Msg', (), {'id': len(self.sent) + 1})()
        self.sent.append({'content': content, 'embed': embed, 'view': view})
        return msg


class _FakeResponse:
    def __init__(self):
        self.deferred = False

    async def defer(self, **kw):
        self.deferred = True


class _FakeInteraction:
    def __init__(self, guild_id=1, user_id=10, channel_id=100):
        self.guild = _FakeGuild(guild_id)
        self.user = _FakeAuthor(user_id)
        self.channel_id = channel_id
        self.client = None
        self.id = 999
        self.response = _FakeResponse()
        self.followup = _FakeFollowup()


class TestSlashCtx:
    """_SlashCtx adapter maps Interaction to a ctx-like object."""

    def test_maps_guild_author_channel(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction(guild_id=42, user_id=7, channel_id=99)
        ctx = _SlashCtx(inter)
        assert ctx.guild.id == 42
        assert ctx.author.id == 7
        assert ctx.channel.id == 99
        assert ctx.channel.mention == '<#99>'

    def test_send_uses_followup(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction()
        ctx = _SlashCtx(inter)
        asyncio.run(ctx.send('hello', embed='test_embed'))
        assert len(inter.followup.sent) == 1
        assert inter.followup.sent[0]['content'] == 'hello'
        assert inter.followup.sent[0]['embed'] == 'test_embed'

    def test_channel_send_uses_followup(self):
        from tle.cogs.minigames import _FollowupChannel
        inter = _FakeInteraction()
        ch = _FollowupChannel(inter)
        asyncio.run(ch.send('msg', embed='e', view='v'))
        assert len(inter.followup.sent) == 1
        assert inter.followup.sent[0]['embed'] == 'e'
        assert inter.followup.sent[0]['view'] == 'v'

    def test_author_override_for_streak(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction(user_id=10)
        ctx = _SlashCtx(inter)
        assert ctx.author.id == 10
        other = _FakeAuthor(20)
        ctx.author = other
        assert ctx.author.id == 20

    def test_channel_send_returns_message(self):
        from tle.cogs.minigames import _FollowupChannel
        inter = _FakeInteraction()
        ch = _FollowupChannel(inter)
        msg = asyncio.run(ch.send('hi'))
        assert hasattr(msg, 'id')


class TestRatingDb:
    def test_default_registered_for_anyone_not_opted_out(self, db):
        # Default-opt-in: even a user we've never heard of is "registered" —
        # is_akari_registered is the inverse of is_akari_opted_out.
        assert db.is_akari_registered(1, 999) is True
        # register on a user with no opt-out is a no-op.
        assert db.register_akari_user(1, 999) is False

    def test_unregister_adds_optout_then_register_lifts_it(self, db):
        # First unregister adds the opt-out; second is a no-op.
        assert db.unregister_akari_user(1, 999, 1.0) is True
        assert db.unregister_akari_user(1, 999, 2.0) is False
        assert db.is_akari_registered(1, 999) is False
        assert db.is_akari_opted_out(1, 999) is True
        # register lifts the opt-out.
        assert db.register_akari_user(1, 999) is True
        assert db.is_akari_opted_out(1, 999) is False
        assert db.is_akari_registered(1, 999) is True

    def test_registrants_lists_users_with_results_minus_optouts(self, db):
        # Only users with any result show up; opt-outs are excluded.
        db.save_minigame_result(
            'm1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_minigame_result(
            'm2', 1, 'akari', 10, 888, 2,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999', '888'}
        db.unregister_akari_user(1, 888, 1.0)
        assert db.get_akari_registrants(1) == {'999'}

    def test_registrants_are_guild_scoped(self, db):
        # Results in guild 1 don't surface in guild 2's registrants list.
        db.save_minigame_result(
            'a', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_minigame_result(
            'b', 2, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.unregister_akari_user(1, 999, 2.0)
        assert db.get_akari_registrants(1) == set()
        assert db.get_akari_registrants(2) == {'999'}

    def test_registrants_dedupe_live_and_imported(self, db):
        # The same user appearing in both tables is listed once.
        db.save_minigame_result(
            'l1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_imported_minigame_result(
            'i1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999'}

    def test_imported_results_make_user_visible(self, db):
        # An imported-only user (no live results) still appears in registrants.
        db.save_imported_minigame_result(
            'i1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999'}

    def test_replace_and_get_ratings_sorted_desc(self, db):
        states = [
            RatingState('a', 1300.5, 4, 1320.0, 5.0),
            RatingState('b', 1100.25, 4, 1200.0, -3.0),
        ]
        assert db.replace_akari_ratings(1, states, 1000.0) == 2
        rows = db.get_akari_ratings(1)
        assert [r.user_id for r in rows] == ['a', 'b']
        assert abs(rows[0].rating - 1300.5) < 1e-9
        b = db.get_akari_rating(1, 'b')
        assert b.games == 4
        assert abs(b.last_delta + 3.0) < 1e-9

    def test_replace_overwrites_not_appends(self, db):
        db.replace_akari_ratings(1, [RatingState('a', 1300.0, 1, 1300.0, 0.0)], 1.0)
        db.replace_akari_ratings(1, [RatingState('a', 1250.0, 2, 1300.0, -50.0)], 2.0)
        rows = db.get_akari_ratings(1)
        assert len(rows) == 1
        assert abs(rows[0].rating - 1250.0) < 1e-9

    def test_replace_is_guild_scoped(self, db):
        db.replace_akari_ratings(1, [RatingState('a', 1300.0, 1, 1300.0, 0.0)], 1.0)
        db.replace_akari_ratings(2, [RatingState('b', 1400.0, 1, 1400.0, 0.0)], 1.0)
        # Rebuilding guild 1 must leave guild 2 untouched.
        db.replace_akari_ratings(1, [RatingState('a', 1290.0, 2, 1300.0, -10.0)], 3.0)
        assert len(db.get_akari_ratings(2)) == 1
        assert db.get_akari_rating(2, 'b').rating == 1400.0

    def test_replace_persists_decay_fields(self, db):
        state = RatingState('a', 1300.0, 4, 1320.0, -2.5, 7, 612)
        db.replace_akari_ratings(1, [state], 1000.0)
        row = db.get_akari_rating(1, 'a')
        assert row.skip_streak == 7
        assert row.last_puzzle == 612

    def test_akari_rating_reads_legacy_snapshot_if_generic_missing(self, db):
        db.conn.execute(
            '''
            INSERT INTO akari_rating
                (guild_id, user_id, rating, games, peak, last_delta,
                 skip_streak, last_puzzle, updated_at)
            VALUES ('1', 'a', 1300.0, 2, 1310.0, 5.0, 1, 445, 1000.0)
            '''
        )
        db.conn.commit()

        rows = db.get_akari_ratings(1)
        assert [row.user_id for row in rows] == ['a']
        assert db.get_akari_rating(1, 'a').rating == 1300.0

    def test_akari_rating_prefers_generic_snapshot_when_present(self, db):
        db.conn.execute(
            '''
            INSERT INTO akari_rating
                (guild_id, user_id, rating, games, peak, last_delta,
                 skip_streak, last_puzzle, updated_at)
            VALUES ('1', 'a', 1300.0, 2, 1310.0, 5.0, 1, 445, 1000.0)
            '''
        )
        db.replace_minigame_ratings(
            1, 'akari',
            [RatingState('a', 1400.0, 3, 1400.0, 10.0, 0, 446)],
            1001.0,
        )

        rows = db.get_akari_ratings(1)
        assert rows[0].rating == 1400.0
        assert db.get_akari_rating(1, 'a').games == 3


class TestCogRating:
    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    @staticmethod
    def _akari_msg(msg_id, user_id, body, guild=1, channel=10):
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari 445\n✅2026-03-26✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _akari_msg_n(msg_id, user_id, puzzle, body, guild=1, channel=10):
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari {puzzle}\n✅2026-03-26✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _no_puzzle_filter(monkeypatch):
        # Make recompute clock-independent: don't drop the test's puzzle numbers
        # as "far ahead of today" regardless of the machine's date.
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: 10 ** 9)

    def test_results_persist_rating_snapshot(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)
        perfect = self._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        partial = self._akari_msg(2, 888, '\U0001f3af 96% \U0001f553 1:00')

        async def _inner():
            await cog.on_message(perfect)
            await cog.on_message(partial)
        asyncio.run(_inner())

        rows = db.get_akari_ratings(1)
        by_user = {r.user_id: r for r in rows}
        assert set(by_user) == {'999', '888'}
        # Perfect beats partial -> the perfect solver is rated above 1200.
        assert by_user['999'].rating > 1200 > by_user['888'].rating
        assert by_user['999'].games == 1
        assert rows[0].user_id == '999'  # strongest first

    def test_generic_minigame_ban_cannot_hide_akari_ratings(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)
        asyncio.run(cog.on_message(
            self._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')))
        asyncio.run(cog.on_message(
            self._akari_msg(2, 888, '\U0001f3af 96% \U0001f553 1:00')))

        db.ban_minigame_user(1, 'akari', 999, 1.0, 7, 'wrong table')
        cog._recompute_akari_ratings(1)

        assert {row.user_id for row in db.get_akari_ratings(1)} == {'999', '888'}

    def test_generic_minigame_ban_cannot_hide_akari_vs_or_top(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        cog = Minigames(bot=object())
        alice = _FakeDiscordMember(999, 'Alice')
        bob = _FakeDiscordMember(888, 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])

        db.save_minigame_result(
            1, 1, 'akari', 10, alice.id, 445,
            '2026-03-26', 100, 60, True, 'raw')
        db.save_minigame_result(
            2, 1, 'akari', 10, bob.id, 445,
            '2026-03-26', 100, 90, True, 'raw')
        db.ban_minigame_user(1, 'akari', alice.id, 1.0, 7, 'wrong table')

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['embed'] = embed

        ctx = SimpleNamespace(
            guild=guild,
            channel=_FakeChannel(10),
            author=alice,
            send=send,
        )
        asyncio.run(cog._cmd_vs(ctx, AKARI_GAME, alice, bob))
        assert 'Puzzles: **1**' in sent['embed'].description

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))
        asyncio.run(cog._cmd_top(ctx, AKARI_GAME))
        assert '`Alice` — **1** wins' in pages[0][1].description

    def test_recompute_runs_after_admin_remove(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29'))
            await cog.on_message(self._akari_msg(2, 888, '\U0001f3af 96% \U0001f553 1:00'))
        asyncio.run(_inner())
        assert len(db.get_akari_ratings(1)) == 2

        member = _FakeDiscordMember(888, 'Bob')

        async def _send(content=None, *, embed=None, **kwargs):
            return None
        ctx = SimpleNamespace(guild=_FakeGuild(1), send=_send)
        asyncio.run(cog._cmd_remove(ctx, AKARI_GAME, member, 445))
        # 888's only result is gone -> they fall out of the rebuilt snapshot.
        users = {r.user_id for r in db.get_akari_ratings(1)}
        assert users == {'999'}

    def test_absent_user_decays_in_snapshot(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            # Day 500: A (perfect) beats B (partial) -> A above 1200.
            await cog.on_message(self._akari_msg_n(1, 999, 500, '\U0001f31f Perfect! \U0001f553 1:29'))
            await cog.on_message(self._akari_msg_n(2, 888, 500, '\U0001f3af 96% \U0001f553 1:00'))
            # Days 501-515: B and C play; A is absent for 15 community days.
            mid = 3
            for puzzle in range(501, 516):
                await cog.on_message(self._akari_msg_n(mid, 888, puzzle, '\U0001f31f Perfect! \U0001f553 0:40'))
                mid += 1
                await cog.on_message(self._akari_msg_n(mid, 777, puzzle, '\U0001f3af 50% \U0001f553 3:00'))
                mid += 1
        asyncio.run(_inner())

        a = db.get_akari_rating(1, '999')
        assert a is not None
        assert a.skip_streak == 15   # missed puzzles 501..515
        assert a.last_puzzle == 500  # last day actually played
        assert a.rating > 1200       # decayed toward, but never past, the default

    def test_debug_leaderboard_includes_opted_out(self, db, monkeypatch):
        # ;mg akari ratings debug is the admin variant: it must include users
        # who explicitly opted out — they're filtered out of the public
        # ratings view but still appear here so admins can see everyone.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        # Bob opts out explicitly; Alice stays at the default (opted-in).
        db.unregister_akari_user(1, 888, 1.0)
        cog = Minigames(bot=None)

        async def _seed():
            await cog.on_message(self._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29'))
            await cog.on_message(self._akari_msg(2, 888, '\U0001f3af 96% \U0001f553 1:00'))
        asyncio.run(_seed())

        # Inactivity filter compares last_puzzle against today's real puzzle
        # number; the test's puzzle numbers are 1/2, so disable filtering for
        # this assertion.
        monkeypatch.setattr(
            Minigames, '_active_ranking_rows',
            staticmethod(lambda rows, *, include_inactive=False: list(rows)))

        captured = {}

        def _capture(guild, rating_rows, registrants, *, title='', mark_registered=True):
            captured['user_ids'] = [r.user_id for r in rating_rows]
            captured['mark_registered'] = mark_registered
            return object()  # stand-in for the discord.File
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', _capture)

        sent = {}

        async def _send(*a, **k):
            sent.update(k)
        ctx = SimpleNamespace(
            guild=_FakeGuild(1, members=[
                _FakeDiscordMember(999, 'Alice'), _FakeDiscordMember(888, 'Bob')]),
            channel=SimpleNamespace(id=10),
            author=SimpleNamespace(id=999),
            send=_send,
        )
        asyncio.run(cog._cmd_akari_ratings_debug(ctx))

        assert set(captured['user_ids']) == {'999', '888'}  # both users shown
        assert captured['mark_registered'] is True           # ✓ kept in debug view
        assert 'file' in sent                                # sent as an image

    def test_recompute_never_raises_without_rating_table(self, monkeypatch):
        # Ingestion must survive even if the rating recompute fails internally.
        class _NoRatingDb(FakeMinigameDb):
            def replace_akari_ratings(self, *a, **k):
                raise sqlite3.OperationalError('boom')
        bad = _NoRatingDb()
        monkeypatch.setattr(cf_common, 'user_db', bad)
        self._enable(bad)
        cog = Minigames(bot=None)
        asyncio.run(cog.on_message(
            self._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')))
        # The result still saved despite the rating failure.
        assert bad.get_minigame_result(1) is not None
        bad.close()


class TestAkariExcludeFilter:
    """`+exclude=user1,user2,...` reshapes ratings without disturbing the cache."""

    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    @staticmethod
    def _akari_msg_n(msg_id, user_id, puzzle, body, guild=1, channel=10):
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari {puzzle}\n✅2026-03-26✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _no_puzzle_filter(monkeypatch):
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: 10 ** 9)

    def test_extract_filters_parses_decay_and_exclude(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+decay', '+exclude=alice,cara', 'remaining'])
        (remaining, include_decay, excluded, included, _inactive,
         _test) = asyncio.run(_go())
        assert include_decay is True
        assert excluded == {'101', '303'}
        assert included == set()
        assert remaining == ['remaining']

    def test_extract_filters_ignores_empty_exclude_entries(self):
        # `+exclude=alice,,,bob` should split cleanly without resolving an
        # empty member name.
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+exclude=alice,,bob,'])
        (_remaining, _include_decay, excluded, _included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == {'101', '202'}

    def test_extract_filters_parses_include(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+include=alice,bob'])
        (_remaining, _include_decay, excluded, included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == set()
        assert included == {'101', '202'}

    def test_extract_filters_include_and_exclude_compose(self):
        # Include narrows the universe; exclude trims from there.
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+include=alice,bob,cara', '+exclude=cara'])
        (_remaining, _include_decay, excluded, included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == {'303'}
        assert included == {'101', '202', '303'}

    def test_filtered_rating_rows_keeps_only_included_users(self, db, monkeypatch):
        # The mirror of the exclude case: only the listed users count, every
        # other row is dropped before the replay.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        rows = cog._akari_filtered_rating_rows(
            1, included_ids={'100', '300'})
        assert {r.user_id for r in rows} == {'100', '300'}

    def test_include_and_exclude_compose_in_replay(self):
        # Plain row-filter behaviour: include narrows, exclude trims.
        Row = namedtuple('Row', 'user_id puzzle_number')
        rows = [Row(str(u), 1) for u in (100, 200, 300, 400)]
        filtered = Minigames._filter_akari_rows(
            rows, included_ids={'100', '200', '300'}, excluded_ids={'200'})
        assert {r.user_id for r in filtered} == {'100', '300'}

    def test_filter_row_helper_is_pass_through_with_no_filters(self):
        # Cheap sanity check: when both filter sets are empty, the helper is
        # a no-op (and notably doesn't copy the list either).
        Row = namedtuple('Row', 'user_id puzzle_number')
        rows = [Row('100', 1), Row('200', 1)]
        assert Minigames._filter_akari_rows(rows) is rows

    def test_filtered_rating_rows_drops_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        assert {r.user_id for r in db.get_akari_ratings(1)} == {'100', '200', '300'}
        rows = cog._akari_filtered_rating_rows(1, excluded_ids={'200'})
        assert {r.user_id for r in rows} == {'100', '300'}

    def test_filtered_rating_rows_does_not_touch_cache(self, db, monkeypatch):
        # The whole point of the ``+exclude`` design: the persisted snapshot
        # stays canonical so subsequent un-filtered queries are still fast and
        # consistent.  This pins that invariant.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        before = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        cog._akari_filtered_rating_rows(1, excluded_ids={'200'})
        after = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        assert before == after

    def test_exclude_changes_remaining_players_rating(self, db, monkeypatch):
        # Excluding a player shrinks the contest field; the surviving players'
        # CF deltas change accordingly.  Without this, the feature would be a
        # display-only hide — but it really replays the math.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f31f Perfect! \U0001f553 2:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 50% \U0001f553 5:00'))
        asyncio.run(_inner())
        baseline = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        filtered = {r.user_id: r.rating
                    for r in cog._akari_filtered_rating_rows(1, excluded_ids={'200'})}
        assert '200' not in filtered
        # 100 and 300 are both still in, but their ratings differ from the
        # 3-player snapshot because the contest math is now binary.
        assert filtered['100'] != baseline['100']
        assert filtered['300'] != baseline['300']

    def test_akari_user_data_replays_without_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
        asyncio.run(_inner())
        # Without exclude: a 2-player contest, so the played day has a
        # ``performance`` (the field exists).
        _state, history = cog._akari_user_data(1, 100)
        assert len(history) == 1
        assert history[0].performance is not None
        # Excluding 200 leaves 100 alone on the day → solo, no performance.
        _state, history = cog._akari_user_data(1, 100, excluded_ids={'200'})
        assert len(history) == 1
        assert history[0].performance is None

    def test_akari_puzzle_change_info_omits_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        full = cog._akari_puzzle_change_info(1, 500)
        assert set(full) == {'100', '200', '300'}
        partial = cog._akari_puzzle_change_info(1, 500, excluded_ids={'200'})
        assert set(partial) == {'100', '300'}


class TestAkariMultiMember:
    """``;mg akari rating @a @b ...`` and ``performance @a @b ...`` plot many."""

    def _ctx(self, members):
        guild = _FakeGuild(1, members=members)
        return SimpleNamespace(
            guild=guild,
            author=members[0] if members else None,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )

    def test_parse_returns_list_of_resolved_members(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        ctx = self._ctx([alice, bob, cara])
        (members, include_decay, excluded, included,
         _inactive, _test) = asyncio.run(
            cog._parse_akari_rating_args(ctx, ['alice', 'bob']))
        assert [m.id for m in members] == [101, 202]
        assert include_decay is False
        assert excluded == set()
        assert included == set()

    def test_parse_with_decay_and_exclude_alongside_members(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        ctx = self._ctx([alice, bob, cara])
        (members, include_decay, excluded, included,
         _inactive, _test) = asyncio.run(
            cog._parse_akari_rating_args(
                ctx, ['alice', '+decay', 'bob', '+exclude=cara',
                      '+include=alice,bob,cara']))
        assert [m.id for m in members] == [101, 202]
        assert include_decay is True
        assert excluded == {'303'}
        assert included == {'101', '202', '303'}

    def test_parse_falls_back_to_ctx_author_when_no_member(self):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(999, 'author')
        ctx = self._ctx([author])
        members, _decay, _excl, _incl, _inactive, _test = asyncio.run(
            cog._parse_akari_rating_args(ctx, []))
        assert members == [author]

    def test_parse_member_required_errors_when_empty(self):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(999, 'author')
        ctx = self._ctx([author])
        from tle.cogs.minigames import MinigameCogError
        with pytest.raises(MinigameCogError):
            asyncio.run(cog._parse_akari_rating_args(
                ctx, [], member_required=True))


class TestRegisterTarget:
    """`;mg akari register [@user]` — anyone can self-register; only mods can
    pass a different @user."""

    @staticmethod
    def _ctx(author_id, author_roles):
        roles = [SimpleNamespace(name=r) for r in author_roles]
        return SimpleNamespace(
            author=SimpleNamespace(id=author_id, roles=roles))

    def test_non_mod_can_self_register_without_arg(self):
        ctx = self._ctx(999, author_roles=[])
        target = Minigames._resolve_registrar_target(ctx, member=None)
        assert target is ctx.author

    def test_non_mod_can_pass_self_explicitly(self):
        ctx = self._ctx(999, author_roles=[])
        target = Minigames._resolve_registrar_target(
            ctx, member=SimpleNamespace(id=999))
        # When the explicit member matches the author, we collapse to the
        # author (so message logic sees a "self" registration).
        assert target.id == ctx.author.id

    def test_non_mod_blocked_from_registering_other(self):
        ctx = self._ctx(999, author_roles=['Member'])
        with pytest.raises(Exception, match='Only.*can register'):
            Minigames._resolve_registrar_target(
                ctx, member=SimpleNamespace(id=888))

    def test_admin_can_register_other(self):
        ctx = self._ctx(999, author_roles=['Admin'])
        other = SimpleNamespace(id=888)
        assert Minigames._resolve_registrar_target(ctx, member=other) is other

    def test_moderator_can_register_other(self):
        ctx = self._ctx(999, author_roles=['Moderator'])
        other = SimpleNamespace(id=888)
        assert Minigames._resolve_registrar_target(ctx, member=other) is other


class TestAkariBan:
    """`;mg akari ban @user` blocks the user's future Akari ingest path.

    Verifies the four ingest entry points all short-circuit on the banlist,
    and that the DB methods round-trip cleanly.
    """

    def test_ban_db_methods_roundtrip(self, db):
        assert db.is_akari_banned(1, 999) is False
        assert db.ban_akari_user(1, 999, 100.0, 7, 'spam') == 1
        assert db.ban_akari_user(1, 999, 200.0, 7, 'spam') == 0   # idempotent
        assert db.is_akari_banned(1, 999) is True
        rows = db.get_akari_bans(1)
        assert len(rows) == 1
        assert rows[0].user_id == '999'
        assert rows[0].reason == 'spam'
        # Original ban metadata preserved (re-ban kept the first banned_at).
        assert rows[0].banned_at == 100.0
        assert db.unban_akari_user(1, 999) == 1
        assert db.is_akari_banned(1, 999) is False

    def test_get_akari_bans_sorted_newest_first(self, db):
        db.ban_akari_user(1, 'a', 100.0, 7, None)
        db.ban_akari_user(1, 'b', 300.0, 7, None)
        db.ban_akari_user(1, 'c', 200.0, 7, None)
        order = [r.user_id for r in db.get_akari_bans(1)]
        assert order == ['b', 'c', 'a']

    def test_on_message_drops_banned_user(self, db, monkeypatch):
        # A banned user's Akari message is fully ignored: no raw store, no
        # result row, no rating recompute side-effect.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        db.ban_akari_user(1, 999, 1.0, 7, 'leak')
        cog = Minigames(bot=None)
        msg = TestCogRating._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(1) is None
        # No raw row stored either.
        raws = db.conn.execute(
            'SELECT 1 FROM minigame_raw_message WHERE message_id = ?',
            ('1',)).fetchall()
        assert raws == []

    def test_on_message_passes_non_banned_user(self, db, monkeypatch):
        # Sanity: the ingest path itself still works when the user isn't banned.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        cog = Minigames(bot=None)
        msg = TestCogRating._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(1) is not None

    def test_reparse_skips_banned_user(self, db, monkeypatch):
        # Even if a banned user's raw message is in the store from before the
        # ban, reparse must not produce a result row for them.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        # Stash a pre-ban raw message authored by the soon-to-be-banned user.
        msg = TestCogRating._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        db.save_raw_message(
            msg.id, msg.guild.id, msg.channel.id, msg.author.id,
            msg.created_at.isoformat(), msg.content)
        db.ban_akari_user(1, 999, 200.0, 7, None)
        # Also clear out any imported rows that an earlier setup might have
        # left lying around for this guild.
        db.clear_imported_minigame_results(1, 'akari')

        sent_messages = []

        async def _send(*a, **k):
            sent_messages.append((a, k))

        cog = Minigames(bot=None)
        ctx = SimpleNamespace(
            guild=_FakeGuild(1, members=[_FakeDiscordMember(999, 'Alice')]),
            channel=SimpleNamespace(id=10),
            author=SimpleNamespace(id=7),
            send=_send,
        )
        asyncio.run(cog._cmd_reparse(ctx, AKARI_GAME))
        # No imported row created for the banned author.
        imported = db.conn.execute(
            'SELECT 1 FROM minigame_import_result WHERE user_id = ?',
            ('999',)).fetchall()
        assert imported == []


class TestAkariNonProMode:
    """Non-pro Daily Akari submissions get a notice and aren't ingested."""

    _NON_PRO_BODY = (
        'Daily Akari \U0001f60a 514\n'
        '2026-06-03 (Wed)\n'
        '✅ Solved!   \U0001f553 2:49\n'
        'https://dailyakari.com/'
    )

    @staticmethod
    def _capture_embed_text(monkeypatch):
        """Make embed_alert return its description string so tests can inspect it."""
        from tle.util import discord_common as _dc
        monkeypatch.setattr(_dc, 'embed_alert', lambda desc: desc)

    def test_on_message_skips_save_and_replies(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        self._capture_embed_text(monkeypatch)
        cog = Minigames(bot=None)
        msg = _FakeMessage(1, 1, 10, 999, self._NON_PRO_BODY)
        asyncio.run(cog.on_message(msg))
        # No result row was created.
        assert db.get_minigame_result(1) is None
        # A reply was sent to the message.
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Pro Mode' in body

    def test_on_message_keeps_raw_for_future_reparse(self, db, monkeypatch):
        # Non-pro messages are stored in the raw cache so we can reparse them
        # later if the format becomes supported.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        cog = Minigames(bot=None)
        msg = _FakeMessage(2, 1, 10, 999, self._NON_PRO_BODY)
        asyncio.run(cog.on_message(msg))
        raws = db.conn.execute(
            'SELECT raw_content FROM minigame_raw_message WHERE message_id = ?',
            ('2',)).fetchall()
        assert len(raws) == 1

    def test_banned_user_non_pro_still_gets_ban_notice(self, db, monkeypatch):
        # A banned user posting a non-pro submission should hit the ban notice,
        # not the Pro Mode notice — bans take precedence.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        self._capture_embed_text(monkeypatch)
        db.ban_akari_user(1, 999, 1.0, 7, 'spam')
        cog = Minigames(bot=None)
        msg = _FakeMessage(3, 1, 10, 999, self._NON_PRO_BODY)
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(3) is None
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'banned' in body.lower()

    def test_on_message_edit_to_non_pro_deletes_old_result(self, db, monkeypatch):
        # If a previously-saved real result is edited into a non-pro shape, the
        # old row must be dropped and the user notified.
        monkeypatch.setattr(cf_common, 'user_db', db)
        TestCogRating._enable(db)
        cog = Minigames(bot=None)

        # First: real perfect submission saves a row.
        msg = _FakeMessage(4, 1, 10, 999,
                           'Daily Akari 514\n'
                           '2026-06-03\n'
                           '\U0001f31f Perfect! \U0001f553 2:49')
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(4) is not None

        # Then: edit into a non-pro shape removes the row + notifies.
        edited = _FakeMessage(4, 1, 10, 999, self._NON_PRO_BODY)
        asyncio.run(cog.on_message_edit(msg, edited))
        assert db.get_minigame_result(4) is None
        assert len(edited.replies) == 1


class TestRatingDisplayNoLeak:
    def test_rating_table_rows_mark_registered_and_round(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', None)  # handles render as '-'
        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(999, 'Alice'),
            _FakeDiscordMember(888, 'Bob'),
        ])
        rating_rows = [
            SimpleNamespace(user_id='999', rating=1316.1, games=5, peak=1316.1, last_delta=2.0),
            SimpleNamespace(user_id='888', rating=1090.4, games=5, peak=1200.0, last_delta=-3.0),
        ]
        out = _akari_rating_table_rows(guild, rating_rows, registrants={'999'})
        # columns: (#, name, handle, "rating · rank", games)
        # Per-row colouring is applied by the image renderer (not in the row
        # tuple), so the cell is plain text here.
        assert out[0][0] == 1
        assert '\N{CHECK MARK}' in out[0][1]       # registered marked
        assert '\N{CHECK MARK}' not in out[1][1]   # shadow-rated, not marked
        assert out[0][3] == '1316 · CM'            # rounded for display + tier abbr
        assert out[1][3] == '1090 · P'
        assert out[0][4] == '5'

    def test_puzzle_result_rows_carry_no_rating(self, monkeypatch):
        # The public per-puzzle table must never surface a rating value.
        monkeypatch.setattr(cf_common, 'user_db', None)
        guild = _FakeGuild(1, members=[_FakeDiscordMember(999, 'Alice')])
        result_row = SimpleNamespace(
            user_id='999', is_perfect=True, accuracy=100,
            time_seconds=89, message_id=1)
        out = _akari_puzzle_table_rows(guild, [result_row])
        # (#, name, handle, result, time) — no rating/tier leaked.
        assert out[0][3] == '100%'
        assert out[0][4] == '1:29'
        assert '1200' not in ' '.join(str(c) for c in out[0])

    def test_annotated_puzzle_rows_include_pre_rating_and_delta(self, monkeypatch):
        # When puzzle_info + registrants are supplied (the user-facing per-puzzle
        # path), opted-in users get a 5-tuple row with pre-rating tier in the
        # name cell and a signed delta in the 5th cell.
        from tle.cogs.minigames import (
            _PuzzlePlayerInfo, _akari_puzzle_table_rows as _rows_fn)
        monkeypatch.setattr(cf_common, 'user_db', None)
        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'alice', 'Alice'),
            _FakeDiscordMember(20, 'bob', 'Bob'),
        ])
        result_rows = [
            SimpleNamespace(user_id='10', is_perfect=True, accuracy=100,
                            time_seconds=60, message_id=1),
            SimpleNamespace(user_id='20', is_perfect=False, accuracy=88,
                            time_seconds=145, message_id=2),
        ]
        puzzle_info = {
            '10': _PuzzlePlayerInfo(pre_rating=1304.0, delta=12.4),
            '20': _PuzzlePlayerInfo(pre_rating=1190.7, delta=-8.6),
        }
        registrants = {'10', '20'}
        out = _rows_fn(guild, result_rows,
                       puzzle_info=puzzle_info, registrants=registrants)
        assert len(out[0]) == 6
        # Alice — opted in, rated 1304 (CM tier), gained ~12.
        assert '1304 CM' in out[0][1]
        assert out[0][3] == '100%'
        assert out[0][4] == '1:00'
        assert out[0][5] == '+12'
        # Bob — opted in, rated 1191 (Specialist tier), lost ~9.
        assert '1191 S' in out[1][1]
        assert out[1][3] == '88%'
        assert out[1][4] == '2:25'
        assert out[1][5] == '-9'

    def test_unregistered_users_have_empty_delta_in_annotated_table(self, monkeypatch):
        # Privacy: a user who isn't in the registrants set shows neither
        # pre-rating annotation nor delta, even if puzzle_info has their entry.
        from tle.cogs.minigames import (
            _PuzzlePlayerInfo, _akari_puzzle_table_rows as _rows_fn)
        monkeypatch.setattr(cf_common, 'user_db', None)
        guild = _FakeGuild(1, members=[_FakeDiscordMember(99, 'hidden', 'Hidden')])
        result_rows = [
            SimpleNamespace(user_id='99', is_perfect=True, accuracy=100,
                            time_seconds=60, message_id=1),
        ]
        puzzle_info = {'99': _PuzzlePlayerInfo(pre_rating=1700.0, delta=22.0)}
        registrants = set()  # hidden user is not opted in
        out = _rows_fn(guild, result_rows,
                       puzzle_info=puzzle_info, registrants=registrants)
        # Annotated mode still emits 6 cells (so the renderer has them all),
        # but the rating/delta surface is empty for the opted-out user.
        assert len(out[0]) == 6
        assert '1700' not in out[0][1]
        assert out[0][5] == ''

    def test_active_ranking_hides_inactive_and_garbage(self):
        import datetime as _dt
        from tle.cogs._minigame_akari import expected_puzzle_number
        current = expected_puzzle_number(_dt.date.today())
        rows = [
            SimpleNamespace(user_id='today', last_puzzle=current),
            SimpleNamespace(user_id='week', last_puzzle=current - 7),
            SimpleNamespace(user_id='month', last_puzzle=current - 40),       # >30d -> hidden
            SimpleNamespace(user_id='troll', last_puzzle=9223372036854775806),  # garbage -> hidden
        ]
        kept = {r.user_id for r in Minigames._active_ranking_rows(rows)}
        assert kept == {'today', 'week'}


class _FakeGroup:
    """Minimal Group stand-in for testing cog_load's backcompat aliasing.

    Real discord.py Groups expose all_commands + get_command; the conftest
    stub doesn't, so we build a real one here.
    """
    def __init__(self, name='stub', aliases=()):
        self.name = name
        self.aliases = list(aliases)
        self.all_commands = {}

    def add(self, cmd):
        self.all_commands[cmd.name] = cmd
        for alias in cmd.aliases:
            self.all_commands[alias] = cmd

    def get_command(self, name):
        return self.all_commands.get(name)


class TestAkariMgBackcompat:
    """`;akari …` is the canonical group; `;mg akari …` keeps working via
    a same-object mirror that cog_load installs on the ;mg group."""

    def _run_cog_load(self, cog, mg, akari):
        """Substitute fake groups onto the cog and run cog_load."""
        # The real cog has .minigames / .akari attributes that resolve to
        # the decorator-built groups; the stubbed harness leaves those as
        # opaque objects.  Patch them with our fakes so cog_load's logic
        # actually runs end-to-end.
        cog.minigames = mg
        cog.akari = akari
        asyncio.run(cog.cog_load())

    def test_mg_resolves_akari_to_same_object(self):
        cog = Minigames(bot=None)
        mg = _FakeGroup(name='minigames', aliases=['mg'])
        akari = _FakeGroup(name='akari', aliases=['dailyakari'])
        self._run_cog_load(cog, mg, akari)
        # ;mg akari and ;mg dailyakari both point at the canonical akari group
        assert mg.get_command('akari') is akari
        assert mg.get_command('dailyakari') is akari

    def test_existing_mg_subcommand_not_clobbered(self):
        """If ;mg.akari somehow already exists, leave it alone."""
        cog = Minigames(bot=None)
        mg = _FakeGroup(name='minigames', aliases=['mg'])
        akari = _FakeGroup(name='akari', aliases=['dailyakari'])
        original = _FakeGroup(name='akari')
        mg.add(original)
        self._run_cog_load(cog, mg, akari)
        assert mg.get_command('akari') is original

    def test_cog_load_no_crash_with_stubbed_group(self):
        """The conftest stub leaves the cog's groups without all_commands;
        cog_load must silently no-op rather than crash."""
        cog = Minigames(bot=None)
        asyncio.run(cog.cog_load())  # must not raise


class TestActiveRankingRowsInactiveFlag:
    """`include_inactive=True` should drop the day-cutoff but keep the
    garbage-future-puzzle filter."""

    def _rows(self):
        from tle.cogs._minigame_akari import expected_puzzle_number
        current = expected_puzzle_number(dt.date.today())
        return [
            SimpleNamespace(user_id='today', last_puzzle=current),
            SimpleNamespace(user_id='week', last_puzzle=current - 7),
            SimpleNamespace(user_id='month', last_puzzle=current - 40),       # >30d
            SimpleNamespace(user_id='year', last_puzzle=current - 400),       # >>30d
            SimpleNamespace(user_id='troll', last_puzzle=9223372036854775806),  # garbage
        ]

    def test_default_hides_inactive_and_garbage(self):
        kept = {r.user_id for r in Minigames._active_ranking_rows(self._rows())}
        assert kept == {'today', 'week'}

    def test_include_inactive_keeps_dormant_but_drops_garbage(self):
        kept = {
            r.user_id for r in
            Minigames._active_ranking_rows(self._rows(), include_inactive=True)
        }
        assert kept == {'today', 'week', 'month', 'year'}
        assert 'troll' not in kept


class TestExtractAkariFiltersInactive:
    """`+inactive` should land as a 5th return value, default False."""

    def _ctx_stub(self):
        # _extract_akari_filters only touches ctx for +include / +exclude.
        return SimpleNamespace()

    def _run(self, args):
        cog = Minigames(bot=None)
        return asyncio.run(cog._extract_akari_filters(self._ctx_stub(), args))

    def test_default_false(self):
        (remaining, include_decay, ex, inc, include_inactive,
         test_decay) = self._run(())
        assert include_inactive is False
        assert remaining == []
        assert include_decay is False
        assert ex == set()
        assert inc == set()
        assert test_decay is False

    def test_flag_sets_true(self):
        (remaining, _decay, _ex, _inc, include_inactive,
         _test) = self._run(('+inactive',))
        assert include_inactive is True

    def test_test_flag_sets_test_decay(self):
        (remaining, _decay, _ex, _inc, _inactive,
         test_decay) = self._run(('+test',))
        assert test_decay is True
        assert remaining == []
        assert remaining == []  # the flag is consumed, not passed through

    def test_flag_composes_with_decay(self):
        remaining, decay, _ex, _inc, inactive, _test = self._run(
            ('+inactive', '+decay'))
        assert decay is True
        assert inactive is True
        assert remaining == []

    def test_unknown_flag_passes_through(self):
        (remaining, _decay, _ex, _inc, _inactive,
         _test) = self._run(('+inactive', 'foo'))
        assert remaining == ['foo']

"""Tests for the minigames system (Daily Akari, etc.)."""
import asyncio
import datetime as dt
import sqlite3
from collections import namedtuple
from types import SimpleNamespace

import pytest

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
from tle.cogs.minigames import Minigames


_GAME = 'akari'


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


class _FakeAuthor:
    def __init__(self, user_id, bot=False):
        self.id = user_id
        self.bot = bot


class _FakeDiscordMember(_FakeAuthor):
    def __init__(self, user_id, name, display_name=None, bot=False):
        super().__init__(user_id, bot=bot)
        self.name = name
        self.display_name = display_name or name


class _FakeMessage:
    def __init__(self, msg_id, guild_id, channel_id, user_id, content):
        self.id = msg_id
        self.guild = _FakeGuild(guild_id)
        self.channel = _FakeChannel(channel_id)
        self.author = _FakeAuthor(user_id)
        self.content = content
        self.created_at = dt.datetime(2026, 3, 26, tzinfo=dt.timezone.utc)


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

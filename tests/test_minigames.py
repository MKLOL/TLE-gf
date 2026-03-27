"""Tests for the minigames system (Daily Akari, etc.)."""
import datetime as dt
import sqlite3
from collections import namedtuple

import pytest

from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_14_0, upgrade_1_15_0
from tle.util.db.minigame_db import MinigameDbMixin
from tle.cogs._minigame_common import (
    compute_vs,
    compute_streak,
    compute_top,
    parse_date_args,
)
from tle.cogs._minigame_akari import parse_akari_message
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
                message_id     TEXT PRIMARY KEY,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_import_result (
                message_id     TEXT PRIMARY KEY,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0
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
        parsed = parse_akari_message(
            'Daily Akari 😊 445\n'
            '✅2026-03-26 (Thu)✅\n'
            '🌟 Perfect!   🕓 1:29\n'
            'https://dailyakari.com/'
        )
        assert parsed is not None
        assert parsed.puzzle_number == 445
        assert parsed.puzzle_date == dt.date(2026, 3, 26)
        assert parsed.is_perfect is True
        assert parsed.accuracy == 100
        assert parsed.time_seconds == 89

    def test_parse_partial_result(self):
        parsed = parse_akari_message(
            'Daily Akari 445\n'
            '✅03/26/2026✅\n'
            '🎯 96%   🕓 1:00\n'
            'https://dailyakari.com/'
        )
        assert parsed is not None
        assert parsed.puzzle_date == dt.date(2026, 3, 26)
        assert parsed.is_perfect is False
        assert parsed.accuracy == 96
        assert parsed.time_seconds == 60

    def test_parse_perfect_word(self):
        parsed = parse_akari_message(
            'Daily Akari 500\n'
            '✅March 26, 2026✅\n'
            'Perfect   🕓 2:15\n'
            'https://dailyakari.com/'
        )
        assert parsed is not None
        assert parsed.is_perfect is True
        assert parsed.accuracy == 100

    def test_parse_rejects_invalid_message(self):
        assert parse_akari_message('hello world') is None


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

    def test_top_counts_shared_fastest_perfect_wins(self):
        rows = [
            _row(1, 10, '2026-03-26', True, 80, number=445),
            _row(2, 20, '2026-03-26', True, 80, number=445),
            _row(3, 30, '2026-03-26', True, 90, number=445),
            _row(4, 10, '2026-03-27', True, 75, number=446),
            _row(5, 20, '2026-03-27', False, 60, 99, 446),
        ]
        assert compute_top(rows) == [('10', 2), ('20', 1)]


class TestArgs:
    def test_parse_date_filters(self):
        dlo, dhi = parse_date_args(('d>=26032026', 'd<28032026'))
        assert dt.datetime.fromtimestamp(dlo).date() == dt.date(2026, 3, 26)
        assert dt.datetime.fromtimestamp(dhi).date() == dt.date(2026, 3, 28)


class TestDbMixin:
    def test_channel_crud(self, db):
        assert db.get_minigame_channel(123, _GAME) is None
        db.set_minigame_channel(123, _GAME, 456)
        assert db.get_minigame_channel(123, _GAME) == '456'
        db.clear_minigame_channel(123, _GAME)
        assert db.get_minigame_channel(123, _GAME) is None

    def test_result_storage(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True)
        row = db.get_minigame_result(1)
        assert row is not None
        assert row.user_id == '300'
        assert row.puzzle_number == 445
        assert row.is_perfect == 1
        assert row.time_seconds == 89

    def test_results_for_user(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True)
        db.save_minigame_result(2, 100, _GAME, 200, 301, 446, '2026-03-27', 90, 99, False)
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '1'

    def test_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True)
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '1'

    def test_imported_results_are_included_in_queries(self, db):
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True)
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_first_message_across_live_and_imported_wins(self, db):
        db.save_minigame_result(20, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True)
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False)
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '10'
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_delete_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True)
        db.save_imported_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 90, True)
        rc = db.delete_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert rc == 2
        assert db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445) is None


class _FakeGuild:
    def __init__(self, guild_id):
        self.id = guild_id


class _FakeChannel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.mention = f'<#{channel_id}>'


class _FakeAuthor:
    def __init__(self, user_id, bot=False):
        self.id = user_id
        self.bot = bot


class _FakeMessage:
    def __init__(self, msg_id, guild_id, channel_id, user_id, content):
        self.id = msg_id
        self.guild = _FakeGuild(guild_id)
        self.channel = _FakeChannel(channel_id)
        self.author = _FakeAuthor(user_id)
        self.content = content


class TestCogIngest:
    @pytest.mark.asyncio
    async def test_ingests_only_enabled_configured_channel(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        message = _FakeMessage(
            123, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🌟 🕓 1:29\nhttps://dailyakari.com/'
        )
        await cog.on_message(message)

        row = db.get_minigame_result(123)
        assert row is not None
        assert row.user_id == '999'
        assert row.guild_id == '1'
        assert row.channel_id == '10'
        assert row.game == _GAME

    @pytest.mark.asyncio
    async def test_ignores_disabled_feature(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        message = _FakeMessage(
            123, 1, 10, 999,
            'Daily Akari 445\n✅2026-03-26✅\n🌟 🕓 1:29\nhttps://dailyakari.com/'
        )
        await cog.on_message(message)

        assert db.get_minigame_result(123) is None

    @pytest.mark.asyncio
    async def test_only_first_message_counts_for_user_puzzle(self, db, monkeypatch):
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
        await cog.on_message(first)
        await cog.on_message(second)

        row = db.get_minigame_result_for_user_puzzle(1, _GAME, 999, 445)
        assert row is not None
        assert row.message_id == '123'

    @pytest.mark.asyncio
    async def test_edit_in_non_configured_channel_is_ignored(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.set_minigame_channel(1, _GAME, 10)

        cog = Minigames(bot=None)
        # Edit a message in channel 99 (not the configured channel 10)
        before = _FakeMessage(50, 1, 99, 999, 'old content')
        after = _FakeMessage(50, 1, 99, 999, 'new content')
        await cog.on_message_edit(before, after)
        # Should not trigger any DB writes — no result to find or delete
        assert db.get_minigame_result(50) is None


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

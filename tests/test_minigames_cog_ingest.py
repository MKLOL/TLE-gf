"""Cog ingest / safety / upgrade tests."""
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

    def test_ingests_queens_share_from_configured_channel(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'queens', '1')
        db.set_minigame_channel(1, 'queens', 10)
        db.set_minigame_player_link(
            1, 'queens', 999, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)

        cog = Minigames(bot=None)
        message = _FakeMessage(
            123, 1, 10, 999,
            'Queens #774 | 1:26\n'
            'No mistakes & no hints\n'
            'First \U0001f451s: \U0001f7eb \U0001f7e7 \U0001f7e6\n'
            'lnkd.in/queens.'
        )
        asyncio.run(cog.on_message(message))

        row = db.get_minigame_result(123)
        assert row is not None
        assert row.game == 'queens'
        assert row.user_id == '999'
        assert row.puzzle_number == 774
        assert row.puzzle_date == '2026-06-13'
        assert row.accuracy == 100
        assert row.time_seconds == 86
        assert row.is_perfect == 1
        rating = db.get_minigame_rating(1, 'queens', 999)
        assert rating is not None
        # A lone share is a solo day — rated snapshot exists but games stays
        # 0 (contested-days semantics, same as Akari).
        assert rating.games == 0

    def test_queens_reparse_uses_raw_share_messages(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_minigame_player_link(
            1, 'queens', 999, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        content = (
            'Queens #774 | 1:26\n'
            'No mistakes & no hints\n'
            'First \U0001f451s: \U0001f7eb \U0001f7e7 \U0001f7e6\n'
            'lnkd.in/queens.'
        )
        db.save_raw_message(123, 1, 10, 999, '2026-06-13T12:00:00', content)
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=_FakeGuild(1),
            channel=_FakeChannel(10),
            author=_FakeDiscordMember(
                999, 'mod', roles=[SimpleNamespace(name=constants.TLE_MODERATOR)]),
            send=send,
        )
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_reparse.__wrapped__(cog, ctx))

        rows = db.get_minigame_results_for_user(1, 'queens', 999)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 774
        assert rows[0].time_seconds == 86
        assert sent['embed'] is not None
        rating = db.get_minigame_rating(1, 'queens', 999)
        assert rating is not None
        # A lone share is a solo day — rated snapshot exists but games stays
        # 0 (contested-days semantics, same as Akari).
        assert rating.games == 0

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

        messages = [
            _FakeMessage(
                i, 1, 10, 999,
                f'Daily Akari {444 + i}\n'
                f'\u2705{puzzle_date_for(444 + i).isoformat()}\u2705\n'
                '\U0001f31f \U0001f553 1:29\nhttps://dailyakari.com/')
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

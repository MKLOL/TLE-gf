"""Akari non-pro import/reparse mismatch tests."""
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
    _FakeGroup, _QueensCommandsBase, _AkariRatingHelpers,
)


class TestAkariNonProModeImport:
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

    def test_import_replies_to_historical_date_number_mismatch(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._capture_embed_text(monkeypatch)
        msg = _FakeMessage(
            12, 1, 10, 999,
            'Daily Akari 446\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')

        class _HistoryChannel(_FakeChannel):
            def history(self, **_kwargs):
                async def _gen():
                    yield msg
                return _gen()

        class _Bot:
            def __init__(self, channel):
                self._channel = channel

            def get_channel(self, _channel_id):
                return self._channel

        cog = Minigames(bot=_Bot(_HistoryChannel(10)))
        cog._import_status[(1, 'akari')] = {
            'state': 'running',
            'channel_id': 10,
            'scanned': 0,
            'done': 0,
            'skipped': [],
            'error': None,
            'latest_message_id': None,
            'started_at': dt.datetime.now(),
        }

        asyncio.run(cog._run_import(1, 10, AKARI_GAME))

        assert db.get_minigame_result(12) is None
        raws = db.conn.execute(
            'SELECT raw_content FROM minigame_raw_message WHERE message_id = ?',
            ('12',)).fetchall()
        assert raws == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'never play this game again' in body

    def test_import_replies_to_out_of_range_puzzle_number(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._capture_embed_text(monkeypatch)
        msg = _FakeMessage(
            15, 1, 10, 999,
            'Daily Akari 4000000\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')

        class _HistoryChannel(_FakeChannel):
            def history(self, **_kwargs):
                async def _gen():
                    yield msg
                return _gen()

        class _Bot:
            def __init__(self, channel):
                self._channel = channel

            def get_channel(self, _channel_id):
                return self._channel

        cog = Minigames(bot=_Bot(_HistoryChannel(10)))
        status = {
            'state': 'running',
            'channel_id': 10,
            'scanned': 0,
            'done': 0,
            'skipped': [],
            'error': None,
            'latest_message_id': None,
            'started_at': dt.datetime.now(),
        }
        cog._import_status[(1, 'akari')] = status

        asyncio.run(cog._run_import(1, 10, AKARI_GAME))

        assert status['state'] == 'done'
        assert status['error'] is None
        assert status['done'] == 0
        assert status['skipped'] == ['15']
        assert db.get_minigame_result(15) is None
        raws = db.conn.execute(
            'SELECT raw_content FROM minigame_raw_message WHERE message_id = ?',
            ('15',)).fetchall()
        assert raws == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'outside the supported Daily Akari date range' in body

    def test_reparse_replies_to_stored_date_number_mismatch(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._capture_embed_text(monkeypatch)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        msg = _FakeMessage(
            13, 1, 10, 999,
            'Daily Akari 446\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')
        db.save_raw_message(
            msg.id, msg.guild.id, msg.channel.id, msg.author.id,
            msg.created_at.isoformat(), msg.content)

        class _FetchChannel(_FakeChannel):
            async def fetch_message(self, message_id):
                assert int(message_id) == msg.id
                return msg

        class _Bot:
            def __init__(self, channel):
                self._channel = channel

            def get_channel(self, _channel_id):
                return self._channel

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
        cog = Minigames(bot=_Bot(_FetchChannel(10)))

        asyncio.run(cog._cmd_reparse(ctx, AKARI_GAME))

        imported = db.conn.execute(
            'SELECT 1 FROM minigame_import_result WHERE message_id = ?',
            ('13',)).fetchall()
        assert imported == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'never play this game again' in body

    def test_reparse_replies_to_stored_out_of_range_puzzle_number(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._capture_embed_text(monkeypatch)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        msg = _FakeMessage(
            16, 1, 10, 999,
            'Daily Akari 4000000\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')
        db.save_raw_message(
            msg.id, msg.guild.id, msg.channel.id, msg.author.id,
            msg.created_at.isoformat(), msg.content)

        class _FetchChannel(_FakeChannel):
            async def fetch_message(self, message_id):
                assert int(message_id) == msg.id
                return msg

        class _Bot:
            def __init__(self, channel):
                self._channel = channel

            def get_channel(self, _channel_id):
                return self._channel

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
        cog = Minigames(bot=_Bot(_FetchChannel(10)))

        asyncio.run(cog._cmd_reparse(ctx, AKARI_GAME))

        imported = db.conn.execute(
            'SELECT 1 FROM minigame_import_result WHERE message_id = ?',
            ('16',)).fetchall()
        assert imported == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'outside the supported Daily Akari date range' in body

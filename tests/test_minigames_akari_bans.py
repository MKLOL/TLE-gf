"""Akari ban + non-pro on_message/edit tests."""
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
        _AkariRatingHelpers._enable(db)
        db.ban_akari_user(1, 999, 1.0, 7, 'leak')
        cog = Minigames(bot=None)
        msg = _AkariRatingHelpers._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
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
        _AkariRatingHelpers._enable(db)
        cog = Minigames(bot=None)
        msg = _AkariRatingHelpers._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(1) is not None

    def test_reparse_skips_banned_user_post_ban_rows(self, db, monkeypatch):
        # Bans are forward-only: a raw message sent AFTER the ban took effect
        # must not produce a result row on reparse.
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
        msg = _AkariRatingHelpers._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        db.save_raw_message(
            msg.id, msg.guild.id, msg.channel.id, msg.author.id,
            msg.created_at.isoformat(), msg.content)
        # banned_at=200.0 (1970 epoch) predates the 2026 message — post-ban.
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

    def test_reparse_keeps_banned_user_pre_ban_rows(self, db, monkeypatch):
        # Forward-only ban: raw messages sent BEFORE the ban keep
        # materializing on reparse — 'existing results stay rated'.
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
        msg = _AkariRatingHelpers._akari_msg(1, 999, '\U0001f31f Perfect! \U0001f553 1:29')
        db.save_raw_message(
            msg.id, msg.guild.id, msg.channel.id, msg.author.id,
            msg.created_at.isoformat(), msg.content)
        # Ban a day after the message — the row is pre-ban.
        db.ban_akari_user(1, 999, msg.created_at.timestamp() + 86400, 7, None)
        db.clear_imported_minigame_results(1, 'akari')

        async def _send(*a, **k):
            pass

        cog = Minigames(bot=None)
        ctx = SimpleNamespace(
            guild=_FakeGuild(1, members=[_FakeDiscordMember(999, 'Alice')]),
            channel=SimpleNamespace(id=10),
            author=SimpleNamespace(id=7),
            send=_send,
        )
        asyncio.run(cog._cmd_reparse(ctx, AKARI_GAME))
        imported = db.conn.execute(
            'SELECT 1 FROM minigame_import_result WHERE user_id = ?',
            ('999',)).fetchall()
        assert imported != []


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
        _AkariRatingHelpers._enable(db)
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

    def test_on_message_replies_to_date_number_mismatch(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
        self._capture_embed_text(monkeypatch)
        cog = Minigames(bot=None)
        msg = _FakeMessage(
            10, 1, 10, 999,
            'Daily Akari 446\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')

        asyncio.run(cog.on_message(msg))

        assert db.get_minigame_result(10) is None
        raws = db.conn.execute(
            'SELECT raw_content FROM minigame_raw_message WHERE message_id = ?',
            ('10',)).fetchall()
        assert raws == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'mismatch' in body.lower()
        assert 'Result not counted' in body
        assert 'never play this game again' in body

    def test_on_message_replies_to_out_of_range_puzzle_number(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
        self._capture_embed_text(monkeypatch)
        cog = Minigames(bot=None)
        msg = _FakeMessage(
            14, 1, 10, 999,
            'Daily Akari 4000000\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')

        asyncio.run(cog.on_message(msg))

        assert db.get_minigame_result(14) is None
        raws = db.conn.execute(
            'SELECT raw_content FROM minigame_raw_message WHERE message_id = ?',
            ('14',)).fetchall()
        assert raws == []
        assert len(msg.replies) == 1
        body = msg.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'outside the supported Daily Akari date range' in body
        assert 'Daily Akari #4000000' in body
        assert 'never play this game again' in body

    def test_on_message_keeps_raw_for_future_reparse(self, db, monkeypatch):
        # Non-pro messages are stored in the raw cache so we can reparse them
        # later if the format becomes supported.
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
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
        _AkariRatingHelpers._enable(db)
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
        _AkariRatingHelpers._enable(db)
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

    def test_on_message_edit_to_mismatch_deletes_old_result(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        _AkariRatingHelpers._enable(db)
        self._capture_embed_text(monkeypatch)
        cog = Minigames(bot=None)

        msg = _FakeMessage(
            11, 1, 10, 999,
            'Daily Akari 445\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')
        asyncio.run(cog.on_message(msg))
        assert db.get_minigame_result(11) is not None

        edited = _FakeMessage(
            11, 1, 10, 999,
            'Daily Akari 446\n'
            '✅2026-03-26✅\n'
            '🌟 Perfect!   🕓 1:29')
        asyncio.run(cog.on_message_edit(msg, edited))

        assert db.get_minigame_result(11) is None
        assert len(edited.replies) == 1
        body = edited.replies[0]['kwargs'].get('embed', '')
        assert 'Invalid submission' in body
        assert 'mismatch' in body.lower()
        assert 'never play this game again' in body

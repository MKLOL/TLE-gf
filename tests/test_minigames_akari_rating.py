"""Akari cog rating + rating-display privacy tests."""
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
        puzzle_date = puzzle_date_for(puzzle).isoformat()
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari {puzzle}\n✅{puzzle_date}✅\n{body}\n'
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

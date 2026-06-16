"""Queens commands: rating filters, results, vs/top, bans."""
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


class TestQueensCommandsResults(_QueensCommandsBase):
    def test_queens_rating_filters_reject_decay(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='do not use decay'):
            asyncio.run(cog._extract_queens_rating_filters(ctx, ['+decay']))

        (members, excluded_ids, included_ids, weekdays, date_bounds,
         recalculate) = asyncio.run(cog._parse_queens_rating_args(
            ctx, ['+recalculate'], allow_recalculate=True))
        assert members == [alice]
        assert recalculate is True
        assert not (excluded_ids or included_ids or weekdays or date_bounds)

        with pytest.raises(MinigameCogError, match='only supported'):
            asyncio.run(cog._parse_queens_rating_args(ctx, ['+recalculate']))

        (remaining, excluded_ids, included_ids, weekdays, date_bounds) = asyncio.run(
            cog._extract_queens_rating_filters(
                ctx, [
                    '+dow=mon,wed', '+include=alice',
                    'd>=08062026', 'd<10062026',
                ]))
        assert remaining == []
        assert excluded_ids == set()
        assert included_ids == {'300'}
        assert weekdays == {0, 2}
        assert date_bounds is not None

        (remaining, excluded_ids, included_ids, weekdays, date_bounds) = asyncio.run(
            cog._extract_queens_rating_filters(
                ctx, ['+weekday=monday,wednesday,saturday']))
        assert remaining == []
        assert weekdays == {0, 2, 5}
        assert not (excluded_ids or included_ids or date_bounds)

        with pytest.raises(MinigameCogError, match='Unknown Queens weekday'):
            asyncio.run(cog._extract_queens_rating_filters(ctx, ['+dow=funday']))

        with pytest.raises(MinigameCogError, match='invalid date'):
            asyncio.run(cog._extract_queens_rating_filters(ctx, ['d>=bad']))

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
        assert '`Alice` — **2** wins' in embed.description
        assert '`Bob`' not in embed.description

"""Queens commands: clear/clean/ratings/history views."""
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


class TestQueensCommandsRatings(_QueensCommandsBase):
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

        asyncio.run(Minigames.queens_delete.__wrapped__(
            cog, ctx, '2026-06-08'))

        remaining = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.puzzle_date) for row in remaining] == [
            ('300', '2026-06-09'),
        ]
        assert db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', dt.date(2026, 6, 8).toordinal()) == []
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
        # Bob is unregistered, so alice's only day has no opponent in the
        # rating pool — a solo day is not a game (contested-days semantics).
        assert captured[-1]['games'] == [0]
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
            rating_series['ratings'] = [
                [point.rating for point in history]
                for history, _name in series
            ]
            rating_series['hidden_markers'] = [
                [getattr(point, 'is_decay', False) for point in history]
                for history, _name in series
            ]
            return fake_file

        def _performance(series):
            perf_series['names'] = [name for _history, name, _rating in series]
            perf_series['dates'] = [
                [str(point.puzzle_date) for point in history]
                for history, _name, _rating in series
            ]
            perf_series['ratings'] = [
                rating for _history, _name, rating in series
            ]
            return fake_file

        monkeypatch.setattr(minigames_module, 'plot_akari_rating', _rating)
        monkeypatch.setattr(minigames_module, 'plot_akari_performance', _performance)

        asyncio.run(cog._cmd_queens_rating(ctx, [alice, bob]))
        assert rating_series['names'] == ['Alice LinkedIn', 'Bob LinkedIn']
        full_alice_rating_dates = rating_series['dates'][0]
        full_alice_rating_values = rating_series['ratings'][0]
        assert full_alice_rating_dates == ['2026-06-08', '2026-06-09']
        assert rating_series['hidden_markers'][0] == [False, False]
        assert rating_series['hidden_markers'][1] == [False, False]
        assert ctx.sent['kwargs']['file'] is fake_file

        asyncio.run(cog._cmd_queens_rating(ctx, [alice], weekdays={0, 2}))
        assert rating_series['dates'] == [['2026-06-08']]

        date_bounds = parse_date_args(('d>=09062026', 'd<10062026'))
        date_start_index = full_alice_rating_dates.index('2026-06-09')
        asyncio.run(cog._cmd_queens_rating(
            ctx, [alice], date_bounds=date_bounds))
        assert rating_series['dates'] == [['2026-06-09']]
        assert rating_series['ratings'] == [
            [full_alice_rating_values[date_start_index]]
        ]

        _expected_row, expected_recalculated_history = cog._minigame_user_data(
            100, QUEENS_GAME, alice.id, date_bounds=date_bounds)
        asyncio.run(cog._cmd_queens_rating(
            ctx, [alice], date_bounds=date_bounds, recalculate=True))
        assert rating_series['dates'] == [['2026-06-09']]
        assert rating_series['ratings'] == [[
            point.rating for point in expected_recalculated_history
        ]]
        assert rating_series['ratings'] != [
            [full_alice_rating_values[date_start_index]]
        ]

        asyncio.run(cog._cmd_queens_performance(ctx, [alice]))
        assert perf_series['names'] == ['Alice LinkedIn']

        asyncio.run(cog._cmd_queens_performance(
            ctx, [alice], date_bounds=date_bounds))
        assert perf_series['dates'] == [['2026-06-09']]
        assert perf_series['ratings'] == [
            round(full_alice_rating_values[date_start_index])]

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))
        asyncio.run(cog._cmd_queens_history(ctx, alice))
        assert pages
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

        asyncio.run(cog._cmd_queens_performance(ctx, [alice]))
        assert perf_series['names'] == ['Anonymous']

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))
        asyncio.run(cog._cmd_queens_history(ctx, alice))
        assert pages
        assert pages[0][1].description

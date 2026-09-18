"""Command integration tests for the opt-in Queens rating beta."""

import asyncio
from types import SimpleNamespace

from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
)
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _queens_number,
    db,
    _FakeDiscordMember,
    _FakeGuild,
    _QueensCommandsBase,
)


def _rating_snapshot(db):
    return [
        tuple(row)
        for row in db.get_minigame_ratings(100, QUEENS_GAME.name)
    ]


class _ImprovedQueensBase(_QueensCommandsBase):
    @classmethod
    def _seed(cls, db):
        db.set_guild_config(100, 'queens', '1')
        members = [
            _FakeDiscordMember(300, 'alice', 'Alice'),
            _FakeDiscordMember(301, 'bob', 'Bob'),
            _FakeDiscordMember(302, 'cara', 'Cara'),
        ]
        for member in members:
            linkedin_name = f'{member.display_name} LinkedIn'
            db.set_minigame_player_link(
                100,
                QUEENS_GAME.link_key,
                member.id,
                linkedin_name,
                normalize_queens_name(linkedin_name),
                None,
                1.0,
                members[0].id,
            )

        daily_times = (
            ('2026-06-08', (10, 20, 30)),
            ('2026-06-09', (30, 20, 10)),
            ('2026-06-10', (10, 30, 20)),
        )
        message_id = 1
        for puzzle_date, times in daily_times:
            for member, time_seconds in zip(members, times):
                cls._save_queens_result(
                    db, message_id, member.id, puzzle_date, time_seconds)
                message_id += 1

        guild = _FakeGuild(100, members=members)
        cog = Minigames(bot=object())
        cog._recompute_minigame_ratings(100, QUEENS_GAME)
        return cog, guild, members


class TestQueensImprovedLeaderboard(_ImprovedQueensBase):
    def test_improved_leaderboard_replays_without_touching_cache(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog, guild, members = self._seed(db)
        ctx = self._make_ctx(guild, members[0])
        canonical_before = _rating_snapshot(db)
        captured = {}
        calls = []
        original_rating_rows = cog._minigame_rating_rows

        def rating_rows(guild_id, game, **kwargs):
            calls.append(kwargs)
            return original_rating_rows(guild_id, game, **kwargs)

        def render(_guild, rows, _registrants, **kwargs):
            captured['rows'] = list(rows)
            captured['title'] = kwargs['title']
            return object()

        def forbid_recompute(*_args, **_kwargs):
            raise AssertionError('beta view must not rewrite canonical ratings')

        monkeypatch.setattr(cog, '_minigame_rating_rows', rating_rows)
        monkeypatch.setattr(
            cog, '_recompute_minigame_ratings', forbid_recompute)
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', render)

        asyncio.run(cog._cmd_queens_ratings(ctx, QUEENS_GAME, improved=True))

        assert calls and calls[-1]['improved'] is True
        assert captured['rows']
        assert '(beta testing)' in captured['title']
        assert _rating_snapshot(db) == canonical_before
        assert [row.rating for row in captured['rows']] != [
            row[1] for row in canonical_before
        ]

    def test_standard_leaderboard_still_reads_canonical_snapshot(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog, guild, members = self._seed(db)
        ctx = self._make_ctx(guild, members[0])
        captured = {}

        def forbid_replay(*_args, **_kwargs):
            raise AssertionError('unfiltered standard view should use the cache')

        def render(_guild, rows, _registrants, **kwargs):
            captured['rows'] = list(rows)
            captured['title'] = kwargs['title']
            return object()

        monkeypatch.setattr(cog, '_minigame_rating_rows', forbid_replay)
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', render)

        asyncio.run(cog._cmd_queens_ratings(ctx, QUEENS_GAME))

        assert [tuple(row) for row in captured['rows']] == _rating_snapshot(db)
        assert '(beta testing)' not in captured['title']


class TestQueensImprovedViews(_ImprovedQueensBase):
    def test_rating_performance_and_history_use_beta_history_and_labels(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog, guild, members = self._seed(db)
        alice = members[0]
        _canonical_row, canonical_history = cog._minigame_user_data(
            100, QUEENS_GAME, alice.id)
        _beta_row, beta_history = cog._minigame_user_data(
            100, QUEENS_GAME, alice.id, improved=True)
        assert [point.rating for point in beta_history] != [
            point.rating for point in canonical_history
        ]

        data_calls = []
        history_calls = []
        original_user_data = cog._minigame_user_data
        original_user_history = cog._minigame_user_history

        def user_data(guild_id, game, user_id, **kwargs):
            data_calls.append(kwargs)
            return original_user_data(guild_id, game, user_id, **kwargs)

        def user_history(guild_id, game, user_id, **kwargs):
            history_calls.append(kwargs)
            return original_user_history(guild_id, game, user_id, **kwargs)

        def forbid_recompute(*_args, **_kwargs):
            raise AssertionError('beta views must not recompute canonical cache')

        fake_file = SimpleNamespace(filename='beta.png')
        plotted_rating = {}
        plotted_performance = {}
        pages = []
        monkeypatch.setattr(cog, '_minigame_user_data', user_data)
        monkeypatch.setattr(cog, '_minigame_user_history', user_history)
        monkeypatch.setattr(
            cog, '_recompute_minigame_ratings', forbid_recompute)
        monkeypatch.setattr(
            minigames_module,
            'plot_akari_rating',
            lambda series: plotted_rating.update(series=series) or fake_file,
        )
        monkeypatch.setattr(
            minigames_module,
            'plot_akari_performance',
            lambda series:
                plotted_performance.update(series=series) or fake_file,
        )
        monkeypatch.setattr(
            minigames_module.paginator,
            'paginate',
            lambda _bot, _channel, page_list, **_kwargs:
                pages.extend(page_list),
        )

        rating_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_rating(
            rating_ctx, QUEENS_GAME, [alice], improved=True))
        assert data_calls[-1]['improved'] is True
        assert plotted_rating['series'][0][0] == beta_history
        assert '(beta testing)' in rating_ctx.sent['embed'].title

        performance_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_performance(
            performance_ctx, QUEENS_GAME, [alice], improved=True))
        assert data_calls[-1]['improved'] is True
        assert plotted_performance['series'][0][0] == beta_history
        assert '(beta testing)' in performance_ctx.sent['embed'].title

        history_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_queens_history(
            history_ctx, QUEENS_GAME, alice, improved=True))
        assert history_calls[-1]['improved'] is True
        assert pages
        assert '(beta testing)' in pages[0][1].title
        assert str(round(beta_history[-1].rating)) in pages[0][1].description

    def test_improved_results_use_beta_change_info_and_title(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog, guild, members = self._seed(db)
        puzzle_number = _queens_number('2026-06-09')
        canonical_info = cog._minigame_puzzle_change_info(
            100, QUEENS_GAME, puzzle_number)
        beta_info = cog._minigame_puzzle_change_info(
            100, QUEENS_GAME, puzzle_number, improved=True)
        assert any(
            abs(beta_info[user_id].delta - canonical_info[user_id].delta)
            > 1e-6
            for user_id in beta_info
        )

        calls = []
        captured = {}
        original_change_info = cog._minigame_puzzle_change_info

        def change_info(guild_id, game, puzzle, **kwargs):
            calls.append(kwargs)
            return original_change_info(guild_id, game, puzzle, **kwargs)

        def render(_guild, _rows, title, **kwargs):
            captured['title'] = title
            captured['puzzle_info'] = kwargs['puzzle_info']
            return object()

        monkeypatch.setattr(
            cog, '_minigame_puzzle_change_info', change_info)
        monkeypatch.setattr(
            minigames_module, '_get_queens_results_table_image_file', render)
        ctx = self._make_ctx(guild, members[0])

        asyncio.run(cog._cmd_queens_stats_date(
            ctx, QUEENS_GAME, '2026-06-09', improved=True))

        assert calls and calls[-1]['improved'] is True
        assert captured['puzzle_info'] == beta_info
        assert all(
            info.performance is not None
            for info in captured['puzzle_info'].values()
        )
        assert '(beta testing)' in captured['title']


class TestQueensImprovedPrefixRouting:
    def test_ratings_and_perf_prefix_flags_pass_through(
            self, monkeypatch):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = SimpleNamespace(
            guild=SimpleNamespace(id=100),
            author=author,
        )
        captured = {}

        monkeypatch.setattr(
            cog, '_require_enabled', lambda *_args, **_kwargs: None)

        async def ratings(_ctx, _game, **kwargs):
            captured['ratings'] = kwargs

        async def performance(_ctx, _game, members, **kwargs):
            captured['performance'] = (members, kwargs)

        monkeypatch.setattr(cog, '_cmd_queens_ratings', ratings)
        asyncio.run(Minigames.queens_ratings.__wrapped__(
            cog, ctx, '+beta'))

        monkeypatch.setattr(cog, '_cmd_queens_performance', performance)
        asyncio.run(Minigames.queens_performance.__wrapped__(
            cog, ctx, '+beta'))

        assert captured['ratings']['improved'] is True
        members, kwargs = captured['performance']
        assert members == [author]
        assert kwargs['improved'] is True

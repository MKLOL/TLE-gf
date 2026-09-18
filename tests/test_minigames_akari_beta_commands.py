"""Command integration tests for the opt-in Akari beta rating."""

import asyncio
import datetime as dt
import inspect
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_akari import (
    AKARI_GAME,
    expected_puzzle_number,
    puzzle_date_for,
)
from tle.cogs._mgimpl_akarid import _akari_beta_performance_keys
from tle.cogs._minigame_result_rows import _ranked_result_rows
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeDiscordMember,
    _FakeGuild,
    _QueensCommandsBase,
    db,
)


_GUILD_ID = 100


def _rating_snapshot(database):
    return [tuple(row) for row in database.get_akari_ratings(_GUILD_ID)]


class _AkariBetaBase(_QueensCommandsBase):
    @classmethod
    def _seed(cls, database):
        database.set_guild_config(_GUILD_ID, AKARI_GAME.feature_flag, '1')
        members = [
            _FakeDiscordMember(300, 'alice', 'Alice'),
            _FakeDiscordMember(301, 'bob', 'Bob'),
            _FakeDiscordMember(302, 'cara', 'Cara'),
        ]
        for member in members:
            database.register_akari_user(_GUILD_ID, member.id)

        current = expected_puzzle_number(dt.date.today())
        results = (
            ((True, 100, 30), (True, 100, 31), (False, 99, 5)),
            ((False, 98, 20), (True, 100, 80), (False, 98, 25)),
            ((True, 100, 40), (False, 99, 10), (True, 100, 42)),
        )
        message_id = 1
        for offset, day_results in enumerate(results, start=-2):
            puzzle = current + offset
            puzzle_date = puzzle_date_for(puzzle).isoformat()
            for member, (perfect, accuracy, seconds) in zip(
                    members, day_results):
                database.save_minigame_result(
                    message_id, _GUILD_ID, AKARI_GAME.name, 200,
                    member.id, puzzle, puzzle_date, accuracy, seconds,
                    perfect, puzzle_date)
                message_id += 1

        cog = Minigames(bot=object())
        cog._recompute_akari_ratings(_GUILD_ID)
        guild = _FakeGuild(_GUILD_ID, members=members)
        return cog, guild, members, current


class TestAkariBetaViews(_AkariBetaBase):
    def test_beta_leaderboard_replays_without_touching_cache(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog, guild, members, _current = self._seed(db)
        canonical_before = _rating_snapshot(db)
        captured = {}
        calls = []
        original = cog._minigame_rating_rows

        def rating_rows(guild_id, game, **kwargs):
            calls.append(kwargs)
            return original(guild_id, game, **kwargs)

        def render(_guild, rows, _registrants, **kwargs):
            captured['rows'] = list(rows)
            captured['title'] = kwargs['title']
            return object()

        monkeypatch.setattr(cog, '_minigame_rating_rows', rating_rows)
        monkeypatch.setattr(
            cog, '_recompute_akari_ratings',
            lambda *_args: pytest.fail('beta must not rewrite the cache'),
        )
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', render)

        ctx = self._make_ctx(guild, members[0])
        asyncio.run(cog._cmd_akari_ratings(ctx, beta=True))

        assert calls[-1]['improved'] is True
        assert '(beta testing)' in captured['title']
        assert _rating_snapshot(db) == canonical_before
        assert [row.rating for row in captured['rows']] != [
            row[1] for row in canonical_before
        ]

    def test_rating_performance_history_and_results_show_beta(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            db, 'get_handle', lambda _user_id, _guild_id: None,
            raising=False)
        cog, guild, members, current = self._seed(db)
        alice = members[0]
        fake_file = SimpleNamespace(filename='beta.png')
        pages = []
        monkeypatch.setattr(
            minigames_module, 'plot_akari_rating',
            lambda _series: fake_file)
        monkeypatch.setattr(
            minigames_module, 'plot_akari_performance',
            lambda _series: fake_file)
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs:
                pages.extend(page_list))

        rating_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_akari_rating(
            rating_ctx, [alice], beta=True))
        assert '(beta testing)' in rating_ctx.sent['embed'].title

        performance_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_akari_performance(
            performance_ctx, [alice], beta=True))
        assert '(beta testing)' in performance_ctx.sent['embed'].title

        history_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_akari_history(
            history_ctx, alice, beta=True))
        assert '(beta testing)' in pages[0][1].title

        captured = {}

        def render_results(_guild, _rows, title, **kwargs):
            captured['title'] = title
            captured['rows'] = list(_rows)
            captured['puzzle_info'] = kwargs['puzzle_info']
            captured['sort_key_fn'] = kwargs['sort_key_fn']
            captured['rank_key_fn'] = kwargs['rank_key_fn']
            return object()

        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image_file',
            render_results)
        results_ctx = self._make_ctx(guild, alice)
        asyncio.run(cog._cmd_akari_results(
            results_ctx, [f'#{current}', '+beta']))
        assert '(beta testing)' in captured['title']
        assert all(
            info.performance is not None
            for info in captured['puzzle_info'].values()
        )
        ordered = sorted(
            captured['rows'], key=captured['sort_key_fn'])
        shown_performances = [
            captured['puzzle_info'][str(row.user_id)].performance
            for row in ordered
        ]
        assert shown_performances == sorted(
            shown_performances, reverse=True)
        # Accuracy is a hard tier: Bob's much faster 99% result stays below
        # both 100% results, then equal accuracy is ordered by time.
        assert [str(row.user_id) for row in ordered] == [
            str(members[0].id), str(members[2].id), str(members[1].id)]

    def test_beta_result_sort_is_accuracy_then_time_with_exact_ties(self):
        rows = [
            SimpleNamespace(
                user_id=str(user_id), is_perfect=perfect, accuracy=accuracy,
                time_seconds=time_seconds, message_id=user_id,
            )
            for user_id, perfect, accuracy, time_seconds in (
                (1, True, 100, 30),
                (2, False, 100, 20),
                (3, False, 99, 1),
                (4, False, 100, 30),
            )
        ]
        puzzle_info = {
            '1': SimpleNamespace(performance=1500.40),
            '2': SimpleNamespace(performance=1500.49),
            '3': SimpleNamespace(performance=9999.0),
            '4': SimpleNamespace(performance=1200.0),
        }
        sort_key, rank_key = _akari_beta_performance_keys(puzzle_info)

        ranked = _ranked_result_rows(
            rows, sort_key_fn=sort_key, rank_key_fn=rank_key)

        assert [str(row.user_id) for _rank, row in ranked] == [
            '2', '1', '4', '3']
        assert [rank for rank, _row in ranked] == [1, 2, 2, 4]


class TestAkariBetaRouting:
    def test_prefix_flags_route_to_rating_commands(self, monkeypatch):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = SimpleNamespace(
            guild=SimpleNamespace(id=_GUILD_ID),
            author=author,
        )
        captured = {}

        async def ratings(_ctx, **kwargs):
            captured['ratings'] = kwargs

        async def performance(_ctx, members, **kwargs):
            captured['performance'] = (members, kwargs)

        monkeypatch.setattr(cog, '_cmd_akari_ratings', ratings)
        asyncio.run(Minigames.akari_ratings.__wrapped__(
            cog, ctx, '+beta'))
        monkeypatch.setattr(cog, '_cmd_akari_performance', performance)
        asyncio.run(Minigames.akari_performance.__wrapped__(
            cog, ctx, '+beta'))

        assert captured['ratings']['beta'] is True
        members, kwargs = captured['performance']
        assert members == [author]
        assert kwargs['beta'] is True

    @pytest.mark.parametrize(
        'kwargs, conflict',
        [
            ({'beta': True, 'test_decay': True}, '+test'),
            ({'beta': True, 'weekly': True}, '+weekly'),
        ],
    )
    def test_incompatible_beta_modes_are_rejected(self, kwargs, conflict):
        with pytest.raises(MinigameCogError) as exc_info:
            Minigames._validate_akari_beta(**kwargs)
        assert conflict in str(exc_info.value)

    def test_beta_accepts_decay_history_display(self):
        Minigames._validate_akari_beta(
            beta=True, include_decay=True)

    def test_every_akari_rating_slash_view_exposes_beta_and_time_only(self):
        methods = (
            Minigames.slash_akari_results,
            Minigames.slash_akari_ratings,
            Minigames.slash_akari_rating,
            Minigames.slash_akari_performance,
            Minigames.slash_akari_history,
        )
        assert all(
            {'beta', 'time_only'} <= set(inspect.signature(method).parameters)
            for method in methods
        )

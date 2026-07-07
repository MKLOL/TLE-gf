"""Akari weekday / date-bound filters, +recalculate, and ;akari results."""
import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.cogs._minigame_akari import AKARI_GAME, puzzle_date_for
from tle.cogs._minigame_common import parse_date_args
from tle.cogs.minigames import Minigames, MinigameCogError

from tests.minigames_test_utils import (
    _GAME, db, _FakeGuild, _FakeDiscordMember, _QueensCommandsBase,
)

# Puzzle 446 = 2026-03-27 (Fri); 447 = Sat; 448 = Sun.
_FRI, _SAT, _SUN = 446, 447, 448


def _save_akari_result(db, message_id, user_id, puzzle_number, *,
                       time_seconds=90, is_perfect=True, accuracy=100,
                       guild=1, channel=10):
    db.save_minigame_result(
        message_id, guild, _GAME, channel, user_id, puzzle_number,
        puzzle_date_for(puzzle_number).isoformat(), accuracy, time_seconds,
        is_perfect, 'raw')


def _ddmmyyyy(puzzle_number):
    return puzzle_date_for(puzzle_number).strftime('%d%m%Y')


class _AkariFilterBase(_QueensCommandsBase):
    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    @staticmethod
    def _pin_today(monkeypatch, puzzle_number=_SUN + 1):
        # Fixed "today" for replay bounds and the inactivity cutoff.
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: puzzle_number)


class TestAkariExtendedFilterParsing(_AkariFilterBase):
    def test_extended_filters_parse_weekday_date_and_test(self):
        cog = Minigames(bot=None)
        ctx = SimpleNamespace()

        async def _go():
            return await cog._extract_akari_extended_filters(
                ctx, ['+dow=mon,wed', 'd>=01062026', '+test', 'rest'])
        (remaining, _decay, excluded, included, _inactive, test_decay,
         weekdays, date_bounds, recalculate) = asyncio.run(_go())
        assert remaining == ['rest']
        assert weekdays == {0, 2}
        assert date_bounds is not None and date_bounds[0] > 0
        assert test_decay is True
        assert recalculate is False
        assert not (excluded or included)

        with pytest.raises(MinigameCogError, match='Unknown Queens weekday'):
            asyncio.run(cog._extract_akari_extended_filters(
                ctx, ['+dow=funday']))
        with pytest.raises(MinigameCogError, match='invalid date'):
            asyncio.run(cog._extract_akari_extended_filters(ctx, ['d>=bad']))

    def test_recalculate_only_where_allowed(self):
        cog = Minigames(bot=None)
        ctx = SimpleNamespace()
        with pytest.raises(MinigameCogError, match='only supported'):
            asyncio.run(cog._extract_akari_extended_filters(
                ctx, ['+recalculate']))
        (_remaining, _decay, _ex, _in, _inactive, _test, _weekdays,
         _bounds, recalculate) = asyncio.run(
            cog._extract_akari_extended_filters(
                ctx, ['+recalculate'], allow_recalculate=True))
        assert recalculate is True


class TestAkariWeekdayFilters(_AkariFilterBase):
    def _seed(self, db):
        # Fri: alice perfect 1:30, bob perfect 1:40 (alice wins).
        # Sat: alice perfect, bob imperfect (alice wins).
        # Sun: bob perfect solo (bob wins).
        _save_akari_result(db, 1, 300, _FRI, time_seconds=90)
        _save_akari_result(db, 2, 301, _FRI, time_seconds=100)
        _save_akari_result(db, 3, 300, _SAT, time_seconds=95)
        _save_akari_result(db, 4, 301, _SAT, is_perfect=False, accuracy=90)
        _save_akari_result(db, 5, 301, _SUN, time_seconds=80)

    def test_vs_weekday_filter_restricts_common_puzzles(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._seed(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(cog._cmd_vs(ctx, AKARI_GAME, alice, bob))
        assert 'Puzzles: **2**' in ctx.sent['embed'].description

        asyncio.run(cog._cmd_vs(ctx, AKARI_GAME, alice, bob, '+dow=fri'))
        assert 'Puzzles: **1**' in ctx.sent['embed'].description
        assert '(Fri)' in ctx.sent['embed'].title

    def test_top_weekday_filter_restricts_winners(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._seed(db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kw: pages.extend(page_list))

        asyncio.run(cog._cmd_top(ctx, AKARI_GAME, '+dow=sun'))
        description = pages[-1][1].description
        assert 'Bob' in description and 'Alice' not in description
        assert '(Sun)' in pages[-1][1].title

        pages.clear()
        asyncio.run(cog._cmd_top(ctx, AKARI_GAME, '+dow=fri,sat'))
        description = pages[-1][1].description
        assert 'Alice' in description and 'Bob' not in description

    def test_streak_weekday_filter(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        # Alice: perfect Fri, imperfect Sat — full streak is 0, Fri-only is 1.
        _save_akari_result(db, 1, 300, _FRI)
        _save_akari_result(db, 2, 300, _SAT, is_perfect=False, accuracy=90)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(1, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(cog._cmd_streak(ctx, AKARI_GAME))
        assert '**0** consecutive' in ctx.sent['embed'].description

        asyncio.run(cog._cmd_streak(ctx, AKARI_GAME, '+dow=fri'))
        assert '**1** consecutive' in ctx.sent['embed'].description
        assert '(Fri)' in ctx.sent['embed'].title

    def test_ratings_weekday_filter_uses_adhoc_replay(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._seed(db)
        self._pin_today(monkeypatch)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        cog._recompute_akari_ratings(1)
        snapshot = [(row.user_id, row.rating)
                    for row in db.get_minigame_ratings(1, _GAME)]

        captured = []

        def _capture(guild, rating_rows, registrants, **kwargs):
            captured.append({
                'user_ids': [row.user_id for row in rating_rows],
                'title': kwargs.get('title', ''),
            })
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file', _capture)

        asyncio.run(cog._cmd_akari_ratings(ctx))
        assert set(captured[-1]['user_ids']) == {'300', '301'}

        # Sunday only: bob played solo, alice vanishes from the board.
        asyncio.run(cog._cmd_akari_ratings(ctx, weekdays={6}))
        assert captured[-1]['user_ids'] == ['301']
        assert '(Sun)' in captured[-1]['title']

        # Ad-hoc view never touches the persisted snapshot.
        assert [(row.user_id, row.rating)
                for row in db.get_minigame_ratings(1, _GAME)] == snapshot


class TestAkariRatingDateBounds(_AkariFilterBase):
    def _seed_views(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._pin_today(monkeypatch)
        db.get_handle = lambda user_id, guild_id: None
        _save_akari_result(db, 1, 300, _FRI, time_seconds=90)
        _save_akari_result(db, 2, 301, _FRI, time_seconds=100)
        _save_akari_result(db, 3, 300, _SAT, time_seconds=95)
        _save_akari_result(db, 4, 301, _SAT, time_seconds=85)
        _save_akari_result(db, 5, 300, _SUN, time_seconds=70)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        cog = Minigames(bot=object())
        cog._recompute_akari_ratings(1)
        return cog, guild, alice, bob

    def test_rating_date_bounds_display_and_recalculate(
            self, db, monkeypatch):
        cog, guild, alice, bob = self._seed_views(db, monkeypatch)
        ctx = self._make_ctx(guild, alice)

        series = {}
        fake_file = SimpleNamespace(filename='plot.png')

        def _rating(plotted):
            series['dates'] = [
                [str(point.puzzle_date) for point in history]
                for history, _name in plotted
            ]
            series['ratings'] = [
                [point.rating for point in history]
                for history, _name in plotted
            ]
            return fake_file
        monkeypatch.setattr(minigames_module, 'plot_akari_rating', _rating)

        asyncio.run(cog._cmd_akari_rating(ctx, [alice]))
        full_dates = series['dates'][0]
        full_ratings = series['ratings'][0]
        assert full_dates == [puzzle_date_for(n).isoformat()
                              for n in (_FRI, _SAT, _SUN)]

        # Weekday filter forces a fresh replay on the surviving rows.
        asyncio.run(cog._cmd_akari_rating(ctx, [alice], weekdays={4}))
        assert series['dates'] == [[puzzle_date_for(_FRI).isoformat()]]

        # Date bounds alone display-filter the full history.
        date_bounds = parse_date_args(
            (f'd>={_ddmmyyyy(_SAT)}', f'd<{_ddmmyyyy(_SUN)}'))
        asyncio.run(cog._cmd_akari_rating(
            ctx, [alice], date_bounds=date_bounds))
        assert series['dates'] == [[puzzle_date_for(_SAT).isoformat()]]
        sat_index = full_dates.index(puzzle_date_for(_SAT).isoformat())
        assert series['ratings'] == [[full_ratings[sat_index]]]

        # +recalculate replays on only the filtered rows instead.
        _state, expected = cog._akari_user_data(
            1, alice.id, date_bounds=date_bounds)
        asyncio.run(cog._cmd_akari_rating(
            ctx, [alice], date_bounds=date_bounds, recalculate=True))
        assert series['dates'] == [[puzzle_date_for(_SAT).isoformat()]]
        assert series['ratings'] == [
            [point.rating for point in expected]]
        assert series['ratings'] != [[full_ratings[sat_index]]]

    def test_performance_and_history_date_bounds(self, db, monkeypatch):
        cog, guild, alice, bob = self._seed_views(db, monkeypatch)
        ctx = self._make_ctx(guild, alice)

        perf = {}
        fake_file = SimpleNamespace(filename='plot.png')

        def _performance(plotted):
            perf['dates'] = [
                [str(point.puzzle_date) for point in history
                 if point.performance is not None]
                for history, _name, _rating in plotted
            ]
            return fake_file
        monkeypatch.setattr(
            minigames_module, 'plot_akari_performance', _performance)

        asyncio.run(cog._cmd_akari_performance(ctx, [alice]))
        assert perf['dates'] == [[puzzle_date_for(n).isoformat()
                                  for n in (_FRI, _SAT)]]

        date_bounds = parse_date_args((f'd>={_ddmmyyyy(_SAT)}',))
        asyncio.run(cog._cmd_akari_performance(
            ctx, [alice], date_bounds=date_bounds))
        assert perf['dates'] == [[puzzle_date_for(_SAT).isoformat()]]

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kw: pages.extend(page_list))
        asyncio.run(cog._cmd_akari_history(
            ctx, alice, date_bounds=date_bounds))
        assert pages
        description = pages[0][1].description
        assert f'#{_SAT}' in description
        assert f'#{_FRI}' not in description


class TestAkariResultsCommand(_AkariFilterBase):
    def test_results_defaults_to_today_and_annotates_registrants(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._pin_today(monkeypatch, puzzle_number=_FRI)
        _save_akari_result(db, 1, 300, _FRI, time_seconds=90)
        _save_akari_result(db, 2, 301, _FRI, time_seconds=100)
        # Bob opted out — public view keeps his row but drops the annotation.
        db.unregister_akari_user(1, 301, 1.0)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        captured = []

        def _capture(guild, rows, title, **kwargs):
            captured.append({
                'user_ids': [row.user_id for row in rows],
                'title': title,
                'registrants': kwargs.get('registrants'),
            })
            return object()
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image_file', _capture)

        asyncio.run(Minigames.akari_results.__wrapped__(cog, ctx))
        assert f'#{_FRI}' in captured[-1]['title']
        assert set(captured[-1]['user_ids']) == {'300', '301'}
        assert set(captured[-1]['registrants']) == {'300'}
        assert 'file' in ctx.sent['kwargs']

        asyncio.run(Minigames.akari_results_debug.__wrapped__(cog, ctx))
        assert set(captured[-1]['registrants']) == {'300', '301'}

        with pytest.raises(MinigameCogError, match='Usage'):
            asyncio.run(Minigames.akari_results.__wrapped__(
                cog, ctx, '#446', 'extra'))

    def test_results_accepts_selector_and_weekday_filter(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable(db)
        self._pin_today(monkeypatch)
        _save_akari_result(db, 1, 300, _FRI)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(1, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        captured = []
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image_file',
            lambda guild, rows, title, **kwargs: captured.append(
                {'title': title,
                 'user_ids': [row.user_id for row in rows]}) or object())

        asyncio.run(Minigames.akari_results.__wrapped__(
            cog, ctx, f'#{_FRI}'))
        assert captured[-1]['user_ids'] == ['300']

        # A Friday result filtered to weekends only yields nothing.
        with pytest.raises(MinigameCogError, match='No Daily Akari results'):
            asyncio.run(Minigames.akari_results.__wrapped__(
                cog, ctx, f'#{_FRI}', '+dow=weekend'))

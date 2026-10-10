"""Weekly results and first-days behavior shared by Queens and Tango."""

import asyncio
import datetime as dt
import inspect
from types import SimpleNamespace

import pytest

from tests.minigames_test_utils import _FakeDiscordMember, _FakeGuild, _FakeInteraction, db
from tests.test_akari_weekly_results_commands import _prefix
from tests.test_queens_weekly import _logical_today, _install_render_spies, _capture_ctx
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.util import codeforces_common as cf_common


@pytest.fixture(params=[QUEENS_GAME, TANGO_GAME], ids=['queens', 'tango'])
def game(request):
    return request.param


def _ctx():
    members = [_FakeDiscordMember(10, 'Alice'), _FakeDiscordMember(20, 'Bob')]
    return SimpleNamespace(guild=_FakeGuild(100, members=members), author=members[0])


@pytest.mark.parametrize('debug', [False, True])
def test_prefix_weekly_results_forward_filters_and_visibility(game, monkeypatch, debug):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, actual_game, **kwargs):
        calls.append((actual_game, kwargs))

    monkeypatch.setattr(cog, '_cmd_queens_current_week_results', capture)
    name = f'{game.name}_results' + ('_debug' if debug else '')
    asyncio.run(_prefix(
        cog, name, _ctx(), '+weekly', '+days=3', '+include=Alice,Bob',
        '+exclude=Bob', '+dow=mon,wed', 'd>=01062026', 'd<08062026'))
    assert len(calls) == 1
    actual_game, options = calls[0]
    assert actual_game is game
    assert options['first_days'] == 3
    assert options['show_all'] is debug
    assert options['included_ids'] == {'10', '20'}
    assert options['excluded_ids'] == {'20'}
    assert options['weekdays'] == frozenset({0, 2})
    assert options['date_bounds'] is not None


@pytest.mark.parametrize('args', [
    ('+days=3',), ('+weekly', '+days=0'), ('+weekly', '+days=8'),
    ('+weekly', '+days=bad'), ('+weekly', '+days=2', '+days=3'),
    ('+weekly', '2026-06-15'), ('+weekly', '+beta'),
    ('+weekly', '+unrated'), ('+current',),
])
def test_invalid_weekly_results_fail_before_render(game, monkeypatch, args):
    cog = Minigames(bot=None)

    async def unexpected(*_args, **_kwargs):
        pytest.fail('Invalid weekly options must not reach a renderer')

    monkeypatch.setattr(cog, '_cmd_queens_stats_date', unexpected)
    monkeypatch.setattr(cog, '_cmd_queens_current_week_results', unexpected)
    with pytest.raises(MinigameCogError):
        asyncio.run(cog._cmd_queens_results(_ctx(), game, args))


@pytest.mark.parametrize('debug', [False, True])
@pytest.mark.parametrize('args', [('+current',), ('+current', '+days=3'), ('+days=3',)])
def test_ratings_direct_current_options_to_results(game, monkeypatch, debug, args):
    cog = Minigames(bot=None)
    monkeypatch.setattr(cog, '_require_enabled', lambda *_args: None)
    name = f'{game.name}_ratings' + ('_debug' if debug else '')
    target = 'results debug' if debug else 'results'
    with pytest.raises(MinigameCogError, match=rf'{target} \+weekly'):
        asyncio.run(_prefix(cog, name, _ctx(), *args))


def test_slash_weekly_results_route_day_and_date_filters(game, monkeypatch):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, actual_game, **kwargs):
        calls.append((actual_game, kwargs))

    monkeypatch.setattr(cog, '_cmd_queens_current_week_results', capture)
    interaction = _FakeInteraction(guild_id=100)
    command = getattr(cog, f'slash_{game.name}_results')
    asyncio.run(command(
        interaction, weekly=True, first_days=3, weekdays='mon,wed',
        date_filter='d>=01062026 d<08062026'))
    assert interaction.response.deferred
    assert len(calls) == 1
    assert calls[0][0] is game
    assert calls[0][1]['first_days'] == 3
    assert calls[0][1]['weekdays'] == frozenset({0, 2})
    assert calls[0][1]['date_bounds'] is not None
    for view in ('ratings', 'results'):
        assert 'current' not in inspect.signature(
            getattr(cog, f'slash_{game.name}_{view}')).parameters


@pytest.mark.parametrize('options', [
    {'first_days': 3}, {'weekly': True, 'first_days': 8},
    {'weekly': True, 'date': '2026-06-15'}, {'weekly': True, 'beta': True},
    {'weekly': True, 'unrated': True},
])
def test_invalid_slash_weekly_results_report_errors(game, monkeypatch, options):
    cog = Minigames(bot=None)
    errors = []

    async def capture_error(_interaction, error):
        errors.append(error)

    monkeypatch.setattr(cog, '_slash_handle_error', capture_error)
    asyncio.run(getattr(cog, f'slash_{game.name}_results')(_FakeInteraction(), **options))
    assert len(errors) == 1
    assert isinstance(errors[0], MinigameCogError)


def _seed(database, game, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', database)
    database.set_guild_config(100, game.feature_flag, '1')
    monday = dt.date(2030, 1, 7)
    _logical_today(monkeypatch, monday + dt.timedelta(days=6))
    ctx = _ctx()
    for member in ctx.guild.members:
        database.set_minigame_player_link(
            100, game.link_key, member.id, member.name, member.name.lower(),
            None, 1.0, member.id)
    dates = [monday - dt.timedelta(days=7), monday,
             monday + dt.timedelta(days=2), monday + dt.timedelta(days=3)]
    for index, day in enumerate(dates):
        for user, seconds in [(10, 60), (20, 120)]:
            database.save_minigame_result(
                index * 100 + user, 100, game.name, 200, user,
                game.linkedin.number_for_date(day), day.isoformat(),
                100, seconds, True, day.isoformat())
    return Minigames(bot=None), _capture_ctx(ctx.guild, ctx.author)[0]


def test_days_filter_changes_only_live_results_and_keeps_ratings(game, db, monkeypatch):
    cog, ctx = _seed(db, game, monkeypatch)
    full_ratings, full = cog._queens_weekly_preview(100, game)
    limited_ratings, limited = cog._queens_weekly_preview(100, game, first_days=3)
    assert limited_ratings == full_ratings
    assert {row.days_played for row in full} == {3}
    assert {row.days_played for row in limited} == {2}
    rendered = _install_render_spies(monkeypatch)
    asyncio.run(cog._cmd_queens_results(ctx, game, ('+weekly', '+days=3')))
    assert not rendered['ratings']
    assert 'Current Weekly Results' in rendered['scores'][0]['title']
    assert 'first 3 days' in rendered['scores'][0]['title']
    assert [row.rating for row in rendered['scores'][0]['standings']] == [
        row.rating for row in limited_ratings]
    assert all(row.performance is not None for row in rendered['scores'][0]['standings'])


def test_weekly_results_keep_public_ban_and_debug_visibility(game, db, monkeypatch):
    cog, ctx = _seed(db, game, monkeypatch)
    db.ban_minigame_user(100, game.name, 20, 1.0, 10, 'test')
    rendered = _install_render_spies(monkeypatch)
    asyncio.run(cog._cmd_queens_results(ctx, game, ('+weekly', '+days=3')))
    asyncio.run(cog._cmd_queens_results(ctx, game, ('+weekly', '+days=3'), show_all=True))
    assert [row.user_id for row in rendered['scores'][0]['standings']] == ['10']
    assert {row.user_id for row in rendered['scores'][1]['standings']} == {'10', '20'}


def test_no_live_scores_leak_through_weekly_ratings(game, db, monkeypatch):
    cog, ctx = _seed(db, game, monkeypatch)
    rendered = _install_render_spies(monkeypatch)
    asyncio.run(cog._cmd_queens_ratings(ctx, game, weekly=True))
    assert len(rendered['ratings']) == 1
    assert not rendered['scores']


def test_current_results_work_before_any_week_is_completed(game, db, monkeypatch):
    cog, ctx = _seed(db, game, monkeypatch)
    db.delete_minigame_results_for_date_range(
        100, game.name, '2029-12-31', '2030-01-01')
    rendered = _install_render_spies(monkeypatch)
    asyncio.run(cog._cmd_queens_results(ctx, game, ('+weekly', '+days=3')))
    assert {row.rating for row in rendered['scores'][0]['standings']} == {1200}
    with pytest.raises(MinigameCogError, match='ratings yet'):
        asyncio.run(cog._cmd_queens_ratings(ctx, game, weekly=True))


def test_daily_results_keep_beta_and_unrated_options(game, monkeypatch):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, actual_game, date, **kwargs):
        calls.append((actual_game, date, kwargs))

    monkeypatch.setattr(cog, '_cmd_queens_stats_date', capture)
    asyncio.run(cog._cmd_queens_results(_ctx(), game, ('2026-06-15', '+beta', '+unrated')))
    assert calls[0][0] is game
    assert calls[0][1] == '2026-06-15'
    assert calls[0][2]['improved'] is True
    assert calls[0][2]['show_unrated'] is True


def test_weekly_results_require_enabled_feature(game, monkeypatch):
    cog = Minigames(bot=None)

    def disabled(*_args):
        raise MinigameCogError('disabled')

    monkeypatch.setattr(cog, '_require_enabled', disabled)
    with pytest.raises(MinigameCogError, match='disabled'):
        asyncio.run(cog._cmd_queens_results(_ctx(), game, ('+weekly',)))

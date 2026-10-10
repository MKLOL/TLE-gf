"""Prefix/slash weekly results routing and invalid option combinations."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from tests.minigames_test_utils import _FakeDiscordMember, _FakeGuild, _FakeInteraction
from tle.cogs.minigames import Minigames, MinigameCogError


def _ctx():
    members = [_FakeDiscordMember(10, 'Alice'), _FakeDiscordMember(20, 'Bob')]
    return SimpleNamespace(guild=_FakeGuild(1, members=members), author=members[0])


def _prefix(cog, name, ctx, *args):
    command = getattr(cog, name)
    if hasattr(command, 'callback'):
        return command.callback(cog, ctx, *args)
    return command.__wrapped__(cog, ctx, *args)


@pytest.mark.parametrize('debug', [False, True])
def test_weekly_results_preserve_filters_and_debug_visibility(monkeypatch, debug):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(cog, '_cmd_akari_current_week_results', capture)
    name = 'akari_results_debug' if debug else 'akari_results'
    asyncio.run(_prefix(
        cog, name, _ctx(), '+weekly', '+days=3', '+include=Alice,Bob',
        '+exclude=Bob', '+dow=mon,wed', 'd>=01062026', 'd<08062026'))

    assert len(calls) == 1
    assert calls[0]['first_days'] == 3
    assert calls[0]['show_all'] is debug
    assert calls[0]['included_ids'] == {'10', '20'}
    assert calls[0]['excluded_ids'] == {'20'}
    assert calls[0]['weekdays'] == frozenset({0, 2})
    assert calls[0]['date_bounds'] is not None


@pytest.mark.parametrize('args,message', [
    (('+days=3',), 'requires'),
    (('+weekly', '+days=0'), '1 to 7'),
    (('+weekly', '+days=8'), '1 to 7'),
    (('+weekly', '+days=no'), '1 to 7'),
    (('+weekly', '+days=2', '+days=3'), 'once'),
    (('+weekly', '#526'), 'puzzle/date selector'),
    (('+weekly', '2026-06-15'), 'puzzle/date selector'),
    (('+weekly', '+beta'), r'\+beta'),
    (('+weekly', '+time'), r'\+time'),
])
def test_invalid_weekly_results_options_fail_before_render(monkeypatch, args, message):
    cog = Minigames(bot=None)

    async def unexpected(*_args, **_kwargs):
        pytest.fail('Invalid weekly options must not render results')

    monkeypatch.setattr(cog, '_cmd_akari_current_week_results', unexpected)
    monkeypatch.setattr(cog, '_cmd_akari_stats_puzzle', unexpected)
    with pytest.raises(MinigameCogError, match=message):
        asyncio.run(cog._cmd_akari_results(_ctx(), args))


@pytest.mark.parametrize('debug', [False, True])
@pytest.mark.parametrize('args', [('+current',), ('+current', '+days=3'), ('+days=3',)])
def test_old_ratings_options_point_to_results(debug, args):
    cog = Minigames(bot=None)
    name = 'akari_ratings_debug' if debug else 'akari_ratings'
    target = 'results debug' if debug else 'results'
    with pytest.raises(MinigameCogError, match=rf'{target} \+weekly'):
        asyncio.run(_prefix(cog, name, _ctx(), *args))


def test_slash_weekly_results_keep_day_and_date_filters(monkeypatch):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(cog, '_cmd_akari_current_week_results', capture)
    interaction = _FakeInteraction()
    asyncio.run(cog.slash_akari_results(
        interaction, weekly=True, first_days=3, weekdays='mon,wed',
        date_filter='d>=01062026 d<08062026'))

    assert interaction.response.deferred is True
    assert len(calls) == 1
    assert calls[0]['first_days'] == 3
    assert calls[0]['weekdays'] == frozenset({0, 2})
    assert calls[0]['date_bounds'] is not None


@pytest.mark.parametrize('options', [
    {'first_days': 3}, {'weekly': True, 'first_days': 8},
    {'weekly': True, 'selector': '#526'}, {'weekly': True, 'beta': True},
    {'weekly': True, 'time_only': True},
])
def test_invalid_slash_weekly_results_are_reported(monkeypatch, options):
    cog = Minigames(bot=None)
    errors = []

    async def capture_error(_interaction, error):
        errors.append(error)

    monkeypatch.setattr(cog, '_slash_handle_error', capture_error)
    interaction = _FakeInteraction()
    asyncio.run(cog.slash_akari_results(interaction, **options))
    assert interaction.response.deferred is True
    assert len(errors) == 1
    assert isinstance(errors[0], MinigameCogError)


def test_completed_weekly_ratings_keep_prefix_and_slash_routes(monkeypatch):
    cog = Minigames(bot=None)
    calls = []

    async def capture(_ctx, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(cog, '_cmd_akari_ratings', capture)
    asyncio.run(_prefix(cog, 'akari_ratings', _ctx(), '+weekly'))
    asyncio.run(cog.slash_akari_ratings(_FakeInteraction(), weekly=True))
    assert len(calls) == 2
    assert all(call['weekly'] is True for call in calls)
    assert 'current' not in inspect.signature(Minigames.slash_akari_ratings).parameters
    assert 'first_days' not in inspect.signature(Minigames.slash_akari_ratings).parameters


def test_weekly_results_require_enabled_feature(monkeypatch):
    cog = Minigames(bot=None)

    def disabled(*_args):
        raise MinigameCogError('Akari is disabled')

    monkeypatch.setattr(cog, '_require_enabled', disabled)
    with pytest.raises(MinigameCogError, match='disabled'):
        asyncio.run(cog._cmd_akari_results(_ctx(), ('+weekly',)))

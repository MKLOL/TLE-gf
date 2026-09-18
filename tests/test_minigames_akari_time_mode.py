"""Time-only Akari rating replay and command integration tests."""

import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_akari import AKARI_GAME, puzzle_date_for
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common
from tle.util.akari_beta_rating import compute_akari_beta_ratings

from tests.minigames_test_utils import (
    _FakeDiscordMember,
    _FakeGuild,
    db,
)


_GUILD = 1
_PUZZLE = 446


def _result(user_id, time_seconds, accuracy, is_perfect, message_id):
    return SimpleNamespace(
        user_id=str(user_id),
        puzzle_number=_PUZZLE,
        puzzle_date=puzzle_date_for(_PUZZLE).isoformat(),
        time_seconds=time_seconds,
        accuracy=accuracy,
        is_perfect=is_perfect,
        message_id=message_id,
        raw_content='raw',
    )


def _beta_outcome(rows):
    histories = {}
    states = compute_akari_beta_ratings(
        rows,
        time_only=True,
        histories=histories,
        current_puzzle_number=_PUZZLE + 1,
    )
    points = {
        user_id: [
            (point.rating, point.delta, point.performance)
            for point in history
        ]
        for user_id, history in histories.items()
    }
    return states, points


def test_beta_time_only_replay_is_invariant_to_accuracy_and_perfect_flags():
    fast_imperfect = [
        _result(300, 60, 1, False, 1),
        _result(301, 120, 100, True, 2),
    ]
    fast_perfect = [
        _result(300, 60, 100, True, 1),
        _result(301, 120, 1, False, 2),
    ]

    first_states, first_points = _beta_outcome(fast_imperfect)
    second_states, second_points = _beta_outcome(fast_perfect)

    assert first_states == second_states
    assert first_points == second_points
    assert first_states['300'].rating > first_states['301'].rating
    assert first_points['300'][0][2] > first_points['301'][0][2]

    accuracy_states = compute_akari_beta_ratings(
        fast_imperfect, current_puzzle_number=_PUZZLE + 1)
    assert accuracy_states['300'].rating < accuracy_states['301'].rating


def test_normal_time_only_replay_ranks_only_by_elapsed_time(db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(
        minigames_module, 'expected_puzzle_number',
        lambda _date: _PUZZLE + 1)
    for row in (
            _result(300, 60, 1, False, 1),
            _result(301, 120, 100, True, 2)):
        db.save_minigame_result(
            row.message_id, _GUILD, AKARI_GAME.name, 10, row.user_id,
            row.puzzle_number, row.puzzle_date, row.accuracy,
            row.time_seconds, row.is_perfect, row.raw_content)

    cog = Minigames(bot=None)
    default = {
        row.user_id: row
        for row in cog._akari_filtered_rating_rows(_GUILD)
    }
    time_only = {
        row.user_id: row
        for row in cog._akari_filtered_rating_rows(
            _GUILD, time_only=True)
    }

    assert default['300'].rating < default['301'].rating
    assert time_only['300'].rating > time_only['301'].rating


def test_prefix_rating_views_strip_and_forward_beta_time_mode(monkeypatch):
    author = _FakeDiscordMember(300, 'alice', 'Alice')
    ctx = SimpleNamespace(
        guild=_FakeGuild(_GUILD, members=[author]),
        author=author,
    )
    cog = Minigames(bot=None)
    captured = {}

    async def ratings(_ctx, **kwargs):
        captured['ratings'] = kwargs

    async def rating(_ctx, members, **kwargs):
        captured['rating'] = (members, kwargs)

    async def performance(_ctx, members, **kwargs):
        captured['performance'] = (members, kwargs)

    async def history(_ctx, member, **kwargs):
        captured['history'] = (member, kwargs)

    monkeypatch.setattr(cog, '_cmd_akari_ratings', ratings)
    monkeypatch.setattr(cog, '_cmd_akari_rating', rating)
    monkeypatch.setattr(cog, '_cmd_akari_performance', performance)
    monkeypatch.setattr(cog, '_cmd_akari_history', history)

    asyncio.run(Minigames.akari_ratings.__wrapped__(
        cog, ctx, '+BETA', '+TIME'))
    asyncio.run(Minigames.akari_rating.__wrapped__(
        cog, ctx, '+TIME', '+BETA'))
    asyncio.run(Minigames.akari_performance.__wrapped__(
        cog, ctx, '+beta', '+time'))
    asyncio.run(Minigames.akari_history.__wrapped__(
        cog, ctx, '+time', '+beta'))

    assert captured['ratings']['beta'] is True
    assert captured['ratings']['time_only'] is True
    for command in ('rating', 'performance'):
        members, kwargs = captured[command]
        assert members == [author]
        assert kwargs['beta'] is True
        assert kwargs['time_only'] is True
    member, kwargs = captured['history']
    assert member is author
    assert kwargs['beta'] is True
    assert kwargs['time_only'] is True


def test_results_beta_time_mode_drives_replay_and_display_order(monkeypatch):
    author = _FakeDiscordMember(300, 'alice', 'Alice')
    ctx = SimpleNamespace(
        guild=_FakeGuild(_GUILD, members=[author]),
        author=author,
    )
    cog = Minigames(bot=None)
    captured = {}

    async def puzzle(_ctx, selector, **kwargs):
        captured['selector'] = selector
        captured['kwargs'] = kwargs

    monkeypatch.setattr(cog, '_cmd_akari_stats_puzzle', puzzle)

    asyncio.run(cog._cmd_akari_results(
        ctx, (f'#{_PUZZLE}', '+BETA', '+TIME')))

    assert captured['selector'] == f'#{_PUZZLE}'
    kwargs = captured['kwargs']
    assert kwargs['beta'] is True
    assert kwargs['time_only'] is True
    fast = _result(300, 60, 1, False, 1)
    slow = _result(301, 120, 100, True, 2)
    assert kwargs['sort_key_fn'](fast) < kwargs['sort_key_fn'](slow)
    assert kwargs['rank_key_fn'](fast) < kwargs['rank_key_fn'](slow)


def test_weekly_and_time_only_rating_modes_are_rejected():
    author = _FakeDiscordMember(300, 'alice', 'Alice')
    ctx = SimpleNamespace(
        guild=_FakeGuild(_GUILD, members=[author]),
        author=author,
    )
    cog = Minigames(bot=None)

    with pytest.raises(MinigameCogError) as exc_info:
        asyncio.run(Minigames.akari_ratings.__wrapped__(
            cog, ctx, '+weekly', '+time'))

    message = str(exc_info.value)
    assert '+weekly' in message
    assert '+time' in message

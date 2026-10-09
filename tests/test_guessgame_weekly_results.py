"""First-days weekly filters for GuessThe.Game's pairwise results."""

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tests.minigames_test_utils import _FakeDiscordMember, _FakeGuild, db
from tests.test_akari_weekly_results_commands import _prefix
from tle.cogs import _mgimpl_sharedcmd as shared
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.util import codeforces_common as cf_common


def _seed(database, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', database)
    database.set_guild_config(1, 'guessgame', '1')
    monday = dt.date(2030, 1, 7)

    class Date(dt.date):
        @classmethod
        def today(cls):
            return monday + dt.timedelta(days=6)

    monkeypatch.setattr(shared, 'dt', SimpleNamespace(
        date=Date, datetime=dt.datetime, time=dt.time, timedelta=dt.timedelta))
    members = [_FakeDiscordMember(10, 'Alice'), _FakeDiscordMember(20, 'Bob')]
    ctx = SimpleNamespace(guild=_FakeGuild(1, members=members), author=members[0])
    for puzzle, day_offset in [(1199, -1), (1200, 0), (1202, 2), (1203, 3), (1207, 7)]:
        for member in members:
            day = monday + dt.timedelta(days=day_offset)
            database.save_minigame_result(
                puzzle * 100 + member.id, 1, 'guessgame', 200, member.id,
                puzzle, day.isoformat(), 6, 7, True, day.isoformat())
    cog = Minigames(bot=None)
    calls = []

    def capture(_ctx, _game, _a, _b, stats, matchups, scoring_name):
        calls.append((stats, matchups, scoring_name))

    monkeypatch.setattr(cog, '_make_guessgame_vs_pages', capture)
    return cog, ctx, members, calls


@pytest.mark.parametrize('days,expected', [
    (1, {1200}), (3, {1200, 1202}),
    (7, {1200, 1202, 1203}), (None, {1200, 1202, 1203}),
])
def test_weekly_days_filter_includes_only_first_n_days(db, monkeypatch, days, expected):
    cog, ctx, members, calls = _seed(db, monkeypatch)
    args = ['+weekly'] + ([f'+days={days}'] if days is not None else [])
    asyncio.run(_prefix(cog, 'gg_results', ctx, *members, *args))
    assert len(calls) == 1
    stats, matchups, _mode = calls[0]
    assert stats['common_count'] == len(expected)
    assert {matchup['row1'].puzzle_number for matchup in matchups} == expected


def test_weekly_days_intersect_existing_puzzle_and_date_filters(db, monkeypatch):
    cog, ctx, members, calls = _seed(db, monkeypatch)
    asyncio.run(cog._cmd_guessgame_matchups(
        ctx, *members, '+weekly', '+days=3', 'p>=1202',
        'd>=09012030', 'd<10012030'))
    assert calls[0][0]['common_count'] == 1
    assert calls[0][1][0]['row1'].puzzle_number == 1202
    assert calls[0][2] is None


@pytest.mark.parametrize('args', [
    ('+days=3',), ('+weekly', '+days=0'), ('+weekly', '+days=8'),
    ('+weekly', '+days=bad'), ('+weekly', '+days=2', '+days=3'), ('+current',),
])
def test_invalid_weekly_options_are_rejected(db, monkeypatch, args):
    cog, ctx, members, calls = _seed(db, monkeypatch)
    with pytest.raises(MinigameCogError):
        asyncio.run(cog._cmd_guessgame_matchups(ctx, *members, *args))
    assert not calls


def test_results_without_weekly_keep_all_existing_results(db, monkeypatch):
    cog, ctx, members, calls = _seed(db, monkeypatch)
    asyncio.run(cog._cmd_guessgame_matchups(ctx, *members, 'p>=1200'))
    assert calls[0][0]['common_count'] == 4

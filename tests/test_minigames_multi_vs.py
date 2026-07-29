"""Akari/Queens comparisons with more than two players."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_multi_vs import compute_multi_vs
from tle.cogs._minigame_queens import QUEENS_GAME, queens_time_score_matchup
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeChannel,
    _FakeDiscordMember,
    _FakeGuild,
    _queens_number,
    _row,
    db,  # noqa: F401 - imported pytest fixture
)


def test_multi_vs_scores_every_pair_on_shared_puzzles():
    rows_by_user = {
        '10': [_row(1, 10, '2026-03-26', True, 60, number=445)],
        '20': [_row(2, 20, '2026-03-26', True, 70, number=445)],
        '30': [_row(3, 30, '2026-03-26', True, 80, number=445)],
    }

    stats = compute_multi_vs(
        rows_by_user, score_fn=queens_time_score_matchup)

    assert stats['puzzle_count'] == 1
    assert stats['pair_count'] == 3
    assert stats['players']['10'] == {
        'user_id': '10', 'score': 2.0, 'wins': 2, 'losses': 0, 'ties': 0,
    }
    assert stats['players']['20']['score'] == 1.0
    assert stats['players']['20']['wins'] == 1
    assert stats['players']['20']['losses'] == 1
    assert stats['players']['30']['losses'] == 2


def test_multi_vs_uses_each_pairs_shared_puzzles():
    rows_by_user = {
        '10': [
            _row(1, 10, '2026-03-26', True, 60, number=445),
            _row(4, 10, '2026-03-27', True, 60, number=446),
        ],
        '20': [
            _row(2, 20, '2026-03-26', True, 70, number=445),
            _row(5, 20, '2026-03-27', True, 70, number=446),
        ],
        '30': [_row(3, 30, '2026-03-26', True, 80, number=445)],
    }

    stats = compute_multi_vs(
        rows_by_user, score_fn=queens_time_score_matchup)

    assert stats['puzzle_count'] == 2
    assert stats['comparison_count'] == 4
    assert stats['players']['10']['wins'] == 3


def test_multi_vs_works_without_one_puzzle_shared_by_every_player():
    rows_by_user = {
        '10': [_row(1, 10, '2026-03-26', True, 60, number=445)],
        '20': [
            _row(2, 20, '2026-03-26', True, 70, number=445),
            _row(3, 20, '2026-03-27', True, 70, number=446),
        ],
        '30': [_row(4, 30, '2026-03-27', True, 80, number=446)],
    }

    stats = compute_multi_vs(
        rows_by_user, score_fn=queens_time_score_matchup)

    assert stats['puzzle_count'] == 2
    assert stats['comparison_count'] == 2
    assert stats['players']['10']['wins'] == 1
    assert stats['players']['20']['wins'] == 1
    assert stats['players']['30']['losses'] == 1


def test_multi_vs_missing_is_loss_uses_union():
    rows_by_user = {
        '10': [_row(1, 10, '2026-03-26', True, 60, number=445)],
        '20': [_row(2, 20, '2026-03-27', True, 70, number=446)],
        '30': [],
    }

    stats = compute_multi_vs(
        rows_by_user,
        score_fn=queens_time_score_matchup,
        missing_is_loss=True,
    )

    assert stats['puzzle_count'] == 2
    assert stats['players']['10']['wins'] == 2
    assert stats['players']['20']['wins'] == 2
    assert stats['players']['30']['losses'] == 2


def test_vs_argument_parser_accepts_three_members_then_filters(monkeypatch):
    cog = Minigames(bot=None)
    members_by_text = {
        name: _FakeDiscordMember(user_id, name)
        for user_id, name in enumerate(('alice', 'bob', 'cara'), start=10)
    }

    async def resolve(_ctx, text):
        try:
            return members_by_text[text]
        except KeyError as exc:
            raise MinigameCogError(f'Unknown member: {text}') from exc

    monkeypatch.setattr(cog, '_resolve_member', resolve)
    members, filters = asyncio.run(cog._resolve_vs_arguments(
        object(), AKARI_GAME,
        ('alice', 'bob', 'cara', 'week', '+dow=fri')))

    assert [member.name for member in members] == ['alice', 'bob', 'cara']
    assert filters == ['week', '+dow=fri']


def test_vs_argument_parser_rejects_duplicate_member_objects():
    cog = Minigames(bot=None)
    alice = _FakeDiscordMember(10, 'alice')

    with pytest.raises(MinigameCogError, match='only appear once'):
        asyncio.run(cog._resolve_vs_arguments(
            object(), AKARI_GAME, (alice, alice)))


def test_vs_argument_parser_keeps_legacy_filter_when_member_has_same_name(
        monkeypatch):
    cog = Minigames(bot=None)
    members_by_text = {
        name: _FakeDiscordMember(user_id, name)
        for user_id, name in enumerate(('alice', 'bob', 'week'), start=10)
    }

    async def resolve(_ctx, text):
        return members_by_text[text]

    monkeypatch.setattr(cog, '_resolve_member', resolve)
    members, filters = asyncio.run(cog._resolve_vs_arguments(
        object(), AKARI_GAME, ('alice', 'bob', 'week')))

    assert [member.name for member in members] == ['alice', 'bob']
    assert filters == ['week']


def test_vs_argument_parser_reports_unknown_third_member(monkeypatch):
    cog = Minigames(bot=None)
    members_by_text = {
        name: _FakeDiscordMember(user_id, name)
        for user_id, name in enumerate(('alice', 'bob'), start=10)
    }

    async def resolve(_ctx, text):
        try:
            return members_by_text[text]
        except KeyError as exc:
            raise MinigameCogError(f'Unknown member: {text}') from exc

    monkeypatch.setattr(cog, '_resolve_member', resolve)
    with pytest.raises(MinigameCogError, match='Unknown member: cara'):
        asyncio.run(cog._resolve_vs_arguments(
            object(), AKARI_GAME, ('alice', 'bob', 'cara')))


def test_queens_slash_vs_does_not_offer_unsupported_scoring_modes():
    parameters = inspect.signature(Minigames.slash_queens_vs).parameters

    assert 'mode' not in parameters


def test_multi_vs_command_renders_three_player_standings(db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    db.set_guild_config(1, 'akari', '1')
    members = [
        _FakeDiscordMember(10, 'Alice'),
        _FakeDiscordMember(20, 'Bob'),
        _FakeDiscordMember(30, 'Cara'),
    ]
    for message_id, member, seconds in zip(range(1, 4), members, (60, 70, 80)):
        db.save_minigame_result(
            message_id, 1, 'akari', 100, member.id, 445,
            '2026-03-26', 100, seconds, True, 'raw')

    sent = {}

    async def send(content=None, *, embed=None, **kwargs):
        sent['embed'] = embed

    ctx = SimpleNamespace(
        guild=_FakeGuild(1, members=members),
        channel=_FakeChannel(100),
        author=members[0],
        send=send,
    )
    asyncio.run(Minigames(bot=None)._cmd_vs_members(
        ctx, AKARI_GAME, members, 'raw', '+dow=thu'))

    embed = sent['embed']
    assert embed.title == 'Daily Akari Head to Head (Raw, Thu)'
    assert '**#1** `Alice`' in embed.description
    assert '**2** wins' in embed.description
    assert 'Comparisons: **3**' in embed.description
    assert 'multi' not in embed.title.casefold()
    assert 'round-robin' not in embed.description.casefold()


def test_queens_vs_compares_pairwise_days_without_a_universal_day(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    db.set_guild_config(1, 'queens', '1')
    members = [
        _FakeDiscordMember(10, 'Alice'),
        _FakeDiscordMember(20, 'Bob'),
        _FakeDiscordMember(30, 'Cara'),
    ]
    results = [
        (1, members[0], '2026-06-08', 50),
        (2, members[1], '2026-06-08', 60),
        (3, members[1], '2026-06-09', 50),
        (4, members[2], '2026-06-09', 60),
    ]
    for message_id, member, puzzle_date, seconds in results:
        db.save_minigame_result(
            message_id, 1, 'queens', 100, member.id,
            _queens_number(puzzle_date), puzzle_date,
            100, seconds, True, 'raw')

    sent = {}

    async def send(content=None, *, embed=None, **kwargs):
        sent['embed'] = embed

    ctx = SimpleNamespace(
        guild=_FakeGuild(1, members=members),
        channel=_FakeChannel(100),
        author=members[0],
        send=send,
    )
    asyncio.run(Minigames(bot=None)._cmd_vs_members(
        ctx, QUEENS_GAME, members))

    embed = sent['embed']
    assert embed.title == 'LinkedIn Queens Head to Head'
    assert 'Puzzles: **2**' in embed.description
    assert 'Comparisons: **2**' in embed.description

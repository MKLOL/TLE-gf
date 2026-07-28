"""Round-robin Akari/Queens comparison tests."""

import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_multi_vs import compute_multi_vs
from tle.cogs._minigame_queens import queens_time_score_matchup
from tle.cogs.minigames import MinigameCogError, Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeChannel,
    _FakeDiscordMember,
    _FakeGuild,
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


def test_multi_vs_default_requires_every_player_to_share_puzzle():
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

    assert stats['puzzle_count'] == 1
    assert stats['players']['10']['wins'] == 2


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
        ctx, AKARI_GAME, members))

    embed = sent['embed']
    assert embed.title == 'Daily Akari Multi-player VS'
    assert '**#1** `Alice`' in embed.description
    assert '**2W 0L 0T**' in embed.description
    assert 'Round-robin pairs: **3**' in embed.description

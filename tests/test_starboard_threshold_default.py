"""Four-reaction starboard default, migration and fallback admission."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.starboard_fallback_utils import board, GASP, STAR_CHANNEL
from tests.starboard_test_utils import GUILD_A
from tle.util.db.user_db_conn import UserDbConn
from tle.util.db.user_db_upgrades import registry, upgrade_1_67_0
from tle.util.starboard_routing import STAR, PILL


@pytest.mark.parametrize('emoji,threshold,expected', [
    (STAR, None, 4), (STAR, 3, 3), (STAR, 7, 7), ('🔥', None, 3),
])
def test_add_default_and_explicit_thresholds(board, emoji, threshold, expected):
    h, db = board
    db.remove_starboard_emoji(GUILD_A, emoji)
    ctx = SimpleNamespace(guild=SimpleNamespace(id=GUILD_A),
                          author=SimpleNamespace(id=42), send=AsyncMock())
    if threshold is None:
        asyncio.run(h.cog.add.callback(h.cog, ctx, emoji))
    else:
        asyncio.run(h.cog.add.callback(h.cog, ctx, emoji, threshold))
    assert db.get_starboard_entry(GUILD_A, emoji).threshold == expected


def test_upgrade_changes_only_three_star_boards_and_runs_once(tmp_path):
    path = str(tmp_path / 'user.db')
    db = UserDbConn(path)
    cases = [(1, STAR, 3, 4), (2, STAR, 4, 4), (3, STAR, 7, 7),
             (1, PILL, 3, 3), (2, PILL, 4, 4), (1, '🔥', 3, 3)]
    for guild, emoji, before, _ in cases:
        db.add_starboard_emoji(guild, emoji, before, 123)
        db.set_starboard_channel(guild, emoji, 456)
    db.add_starboard_alias(1, GASP, STAR)
    db.add_starboard_message_v1(100, 200, 1, STAR, author_id=42, channel_id=456)
    db.add_reactor(100, STAR, 42)
    db.update_starboard_star_count(100, STAR, 3)
    registry.set_version(db.conn, '1.66.0')
    db.close()

    db = UserDbConn(path)
    for guild, emoji, _, after in cases:
        entry = db.get_starboard_entry(guild, emoji)
        assert (entry.threshold, entry.color, entry.channel_id) == (after, 123, '456')
    assert db.get_starboard_message_v1(100, STAR).star_count == 3
    assert db.get_reactors(100, STAR) == ['42']
    assert db.resolve_alias(1, GASP) == STAR
    assert registry.get_current_version(db.conn) == registry.latest_version
    # A subsequent administrator override to 3 must survive future startups.
    db.update_starboard_threshold(1, STAR, 3)
    db.close()
    db = UserDbConn(path)
    assert db.get_starboard_entry(1, STAR).threshold == 3
    db.close()


@pytest.mark.parametrize('emoji', [STAR, GASP])
def test_upgraded_board_requires_four_for_main_and_fallback(board, emoji):
    h, db = board
    upgrade_1_67_0(db.conn)
    h.react(emoji, [1, 2, 3])
    asyncio.run(h.add(emoji, 3))
    assert not h.channels[STAR_CHANNEL].sent
    h.react(emoji, [1, 2, 3, 4])
    asyncio.run(h.add(emoji, 4))
    assert h.channels[STAR_CHANNEL].sent[0].content.startswith(f'{emoji} **4** |')

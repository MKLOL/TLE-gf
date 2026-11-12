"""Once-qualified counts survive unreacting, restarts and renderer failures."""
import asyncio
import sqlite3
from unittest.mock import AsyncMock

import pytest

from tests.starboard_fallback_utils import (
    board, GASP, MESSAGE, STAR_CHANNEL, PILL_CHANNEL,
)
from tests.starboard_test_utils import GUILD_A
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.starboard_routing import STAR, PILL


@pytest.mark.parametrize('full_render', [True, False])
@pytest.mark.parametrize('main,raw,channel', [
    (STAR, STAR, STAR_CHANNEL), (STAR, GASP, STAR_CHANNEL),
    (PILL, PILL, PILL_CHANNEL), (PILL, '💀', PILL_CHANNEL),
])
def test_qualified_group_remains_visible_through_zero(
        board, monkeypatch, full_render, main, raw, channel):
    from tle.cogs import _starboard_core
    monkeypatch.setattr(_starboard_core, 'FULL_RE_RENDER', full_render)
    h, db = board
    db.update_starboard_threshold(GUILD_A, main, 4)
    h.react(raw, [1, 2, 3, 4])
    asyncio.run(h.add(raw, 4))
    post = h.channels[channel].sent[0]
    for count in (3, 2, 1, 0):
        h.react(raw, range(1, count + 1))
        asyncio.run(h.remove(raw, count + 1))
        assert post.content == f'{raw} **{count}** | {h.message.jump_url}'
        assert db.get_starboard_message_v1(MESSAGE, main).star_count == count
    assert db.get_starboard_qualified_emojis(MESSAGE, main) == {raw}
    assert len(h.channels[channel].sent) == 1


def test_zero_fallback_count_survives_database_reopen(board, tmp_path):
    h, db = board
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    h.react(GASP, [])
    asyncio.run(h.cog._sync_board_reactions(h.message, GUILD_A, STAR, force=True))
    # Reopen the actual stored schema rather than only recreating the cog.
    path = tmp_path / 'user.db'
    destination = sqlite3.connect(path)
    db.conn.backup(destination)
    destination.close()
    db.conn.close()
    db.conn = sqlite3.connect(path)
    db.conn.row_factory = namedtuple_factory
    h.restart()
    asyncio.run(h.cog._refresh_starboard_display(GUILD_A, MESSAGE, h.message.channel.id, STAR))
    assert h.channels[STAR_CHANNEL].sent[0].content == f'{GASP} **0** | {h.message.jump_url}'


def test_never_qualified_main_stays_hidden_after_fallback_drops(board):
    h, db = board
    db.update_starboard_threshold(GUILD_A, STAR, 4)
    h.react(STAR, [1, 2, 3])
    h.react(GASP, [10, 11, 12, 13])
    asyncio.run(h.add(GASP, 13))
    h.react(GASP, [10, 11])
    asyncio.run(h.cog._sync_board_reactions(h.message, GUILD_A, STAR, force=True))
    asyncio.run(h.add(STAR, 3))
    assert h.channels[STAR_CHANNEL].sent[0].content == f'{GASP} **2** | {h.message.jump_url}'
    assert db.get_starboard_qualified_emojis(MESSAGE, STAR) == {GASP}


def test_never_qualified_fallback_stays_hidden_when_main_reaches_zero(board):
    h, db = board
    fake = '<:pillfake:123>'
    h.react(PILL, [1, 2, 3, 4])
    h.react(fake, [10, 11])
    asyncio.run(h.add(PILL, 4))
    for user in (4, 3, 2, 1):
        h.react(PILL, range(1, user))
        asyncio.run(h.remove(PILL, user))
    assert h.channels[PILL_CHANNEL].sent[0].content == f'{PILL} **0** | {h.message.jump_url}'
    assert db.get_starboard_qualified_emojis(MESSAGE, PILL) == {PILL}


def test_failed_edit_does_not_lose_threshold_crossing(board, monkeypatch):
    h, db = board
    h.react(PILL, [1, 2, 3, 4])
    asyncio.run(h.add(PILL, 4))
    post = h.channels[PILL_CHANNEL].sent[0]
    with monkeypatch.context() as patch:
        patch.setattr(post, 'edit', AsyncMock(side_effect=RuntimeError('temporary Discord error')))
        h.react('💀', [10, 11, 12, 13])
        asyncio.run(h.add('💀', 13))
    h.restart()
    h.react('💀', [10, 11, 12])
    asyncio.run(h.remove('💀', 13))
    assert post.content == f'{PILL} **4** 💀 **3** | {h.message.jump_url}'


def test_failed_initial_send_does_not_grant_qualification(board, monkeypatch):
    h, db = board
    channel = h.channels[PILL_CHANNEL]
    with monkeypatch.context() as patch:
        patch.setattr(channel, 'send', AsyncMock(side_effect=RuntimeError('send failed')))
        h.react('💀', [1, 2, 3, 4])
        asyncio.run(h.add('💀', 4))
    assert db.get_starboard_qualified_emojis(MESSAGE, PILL) == set()
    h.react('💀', [1, 2, 3])
    asyncio.run(h.add('💀', 3))
    assert channel.sent == []


def test_unreact_during_initial_send_preserves_qualification(board, monkeypatch):
    h, db = board
    channel = h.channels[PILL_CHANNEL]
    send = channel.send

    async def delayed_send(**kwargs):
        h.react('💀', [1, 2, 3])
        await h.remove('💀', 4)
        return await send(**kwargs)

    monkeypatch.setattr(channel, 'send', delayed_send)
    h.react('💀', [1, 2, 3, 4])
    asyncio.run(h.add('💀', 4))
    assert db.get_starboard_qualified_emojis(MESSAGE, PILL) == {'💀'}
    asyncio.run(h.cog._refresh_starboard_display(GUILD_A, MESSAGE, h.message.channel.id, PILL))
    assert channel.sent[0].content == f'💀 **3** | {h.message.jump_url}'


@pytest.mark.parametrize('deletion', ['post', 'entry', 'original', 'board'])
def test_deleting_entry_clears_qualification_history(board, deletion):
    h, db = board
    h.react(GASP, [1, 2, 3, 4])
    h.react(PILL, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    asyncio.run(h.add(PILL, 4))
    post = h.channels[STAR_CHANNEL].sent[0]
    if deletion == 'post':
        db.remove_starboard_message(starboard_msg_id=post.id)
    elif deletion == 'entry':
        db.remove_starboard_message(original_msg_id=MESSAGE, emoji=STAR)
    elif deletion == 'original':
        db.remove_starboard_message(original_msg_id=MESSAGE)
    else:
        db.remove_starboard_emoji(GUILD_A, STAR)
    assert db.get_starboard_qualified_emojis(MESSAGE, STAR) == set()
    assert db.get_starboard_qualified_emojis(MESSAGE, PILL) == (
        set() if deletion == 'original' else {PILL})

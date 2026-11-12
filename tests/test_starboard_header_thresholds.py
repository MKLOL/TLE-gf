"""Board symbols disappear below threshold without removing a valid post."""
import asyncio

import pytest

from tests.starboard_fallback_utils import (
    board, GASP, ELEPHANT, MESSAGE, STAR_CHANNEL, PILL_CHANNEL,
)
from tests.starboard_test_utils import GUILD_A, _FakeAttachment
from tle.util.starboard_routing import STAR, PILL


def test_reported_header_hides_two_stars(board):
    h, db = board
    h.react(STAR, [1, 2])
    h.react(GASP, range(1, 6))
    h.react(ELEPHANT, range(1, 8))
    asyncio.run(h.add(GASP, 5))
    post = h.channels[STAR_CHANNEL].sent[0]
    assert post.content == f'{GASP} **5** {ELEPHANT} **7** | {h.message.jump_url}'
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 7


@pytest.mark.parametrize('full_render', [True, False])
@pytest.mark.parametrize('main,fallback,threshold,channel', [
    (STAR, GASP, 3, STAR_CHANNEL), (PILL, '💀', 4, PILL_CHANNEL),
])
def test_symbol_appears_at_threshold_and_disappears_below_it(
        board, monkeypatch, full_render, main, fallback, threshold, channel):
    from tle.cogs import _starboard_core
    monkeypatch.setattr(_starboard_core, 'FULL_RE_RENDER', full_render)
    h, db = board
    h.react(fallback, range(10, 10 + threshold))
    h.react(main, range(1, threshold))
    asyncio.run(h.add(fallback, 10))
    post = h.channels[channel].sent[0]
    assert main not in post.content

    h.react(main, range(1, threshold + 1))
    asyncio.run(h.add(main, threshold))
    assert post.content.startswith(f'{main} **{threshold}** {fallback} **{threshold}** |')

    h.react(main, range(1, threshold))
    asyncio.run(h.remove(main, threshold))
    assert post.content == f'{fallback} **{threshold}** | {h.message.jump_url}'
    assert len(h.channels[channel].sent) == 1
    # Hidden symbols still contribute to distinct-user statistics.
    assert db.get_starboard_message_v1(MESSAGE, main).star_count == threshold * 2 - 1


def test_header_uses_configured_threshold_and_alias_union(board):
    h, db = board
    db.update_starboard_threshold(GUILD_A, STAR, 5)
    db.add_starboard_alias(GUILD_A, '👍', STAR)
    h.react(GASP, range(10, 15))
    h.react(STAR, [1, 2])
    h.react('👍', [2, 3, 4])
    asyncio.run(h.add(GASP, 14))
    post = h.channels[STAR_CHANNEL].sent[0]
    assert STAR not in post.content
    h.react('👍', [2, 3, 4, 5])
    asyncio.run(h.add('👍', 5))
    assert post.content.startswith(f'{STAR} **5** {GASP} **5** |')


@pytest.mark.parametrize('full_render', [True, False])
def test_existing_post_with_no_visible_counts_keeps_only_jump_url(
        board, monkeypatch, full_render):
    from tle.cogs import _starboard_core
    monkeypatch.setattr(_starboard_core, 'FULL_RE_RENDER', full_render)
    h, db = board
    h.react(STAR, [1, 2, 3])
    asyncio.run(h.add(STAR, 3))
    post = h.channels[STAR_CHANNEL].sent[0]
    h.react(STAR, [1, 2])
    asyncio.run(h.remove(STAR, 3))
    assert post.content == h.message.jump_url
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 2
    h.react(STAR, [1, 2, 3])
    asyncio.run(h.add(STAR, 3))
    assert post.content == f'{STAR} **3** | {h.message.jump_url}'


def test_video_keeps_author_when_symbol_is_hidden(board):
    h, _ = board
    h.message.attachments = [_FakeAttachment('clip.mp4', 'https://example.test/clip.mp4')]
    h.react(STAR, [1, 2, 3])
    asyncio.run(h.add(STAR, 3))
    h.react(STAR, [1, 2])
    asyncio.run(h.remove(STAR, 3))
    assert h.channels[STAR_CHANNEL].sent[0].content == (
        f'**{h.message.author.display_name}** | {h.message.jump_url}')

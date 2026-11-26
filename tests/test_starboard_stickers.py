"""Sticker-only messages qualify and retain their media on either board."""
from types import SimpleNamespace

import pytest

from tests.starboard_fallback_utils import (
    MESSAGE, PILL_CHANNEL, STAR_CHANNEL, board,
)
from tests.starboard_test_utils import (
    _FakeAttachment, _FakeMessage, _FakeReference, _run,
)
from tle.cogs._starboard_render import (
    _is_starboard_eligible, build_starboard_message, resolve_forward_snapshot,
)
from tle.util.starboard_routing import PILL, STAR


def sticker(format_value=1, sticker_id=1575539679346950334):
    extension = {1: 'png', 2: 'png', 3: 'json', 4: 'gif'}[format_value]
    host = 'media.discordapp.net' if format_value == 4 else 'cdn.discordapp.com'
    return SimpleNamespace(
        id=sticker_id, name='plz join', format=SimpleNamespace(value=format_value),
        url=f'https://{host}/stickers/{sticker_id}.{extension}')


def sticker_message(*stickers, **kwargs):
    message = _FakeMessage(**kwargs)
    message.stickers = list(stickers)
    return message


def render(message):
    return _run(build_starboard_message(message, PILL, 10, 0xffaa10))


@pytest.mark.parametrize('format_value', [1, 2, 4])
def test_sticker_only_message_is_eligible_and_displays_image(format_value):
    item = sticker(format_value)
    message = sticker_message(item, content='')
    assert _is_starboard_eligible(message)
    content, embeds, files = render(message)
    assert content == f'{PILL} **10** | {message.jump_url}'
    assert len(embeds) == 1
    assert embeds[0].image_url == item.url
    assert embeds[0].author_data['name'] == message.author.display_name
    assert files == []


def test_empty_message_still_rejected():
    assert not _is_starboard_eligible(sticker_message(content=''))


def test_stickers_join_attachment_gallery_without_losing_text():
    items = [sticker(), sticker(4, 123)]
    attachment = _FakeAttachment('photo.png', 'https://example.com/photo.png')
    message = sticker_message(*items, content='Look', attachments=[attachment])
    _, embeds, _ = render(message)
    assert embeds[0].description == 'Look'
    assert [e.image_url for e in embeds] == [attachment.url, *(s.url for s in items)]


@pytest.mark.parametrize('with_video', [False, True])
def test_lottie_retains_name_without_embedding_json(with_video):
    attachments = [_FakeAttachment('clip.mp4')] if with_video else []
    message = sticker_message(sticker(3), content='', attachments=attachments)
    assert _is_starboard_eligible(message)
    _, embeds, files = render(message)
    assert len(embeds) == 1
    assert embeds[0].image_url is None
    assert embeds[0].fields == [
        {'name': 'Sticker', 'value': '`plz join`', 'inline': False}]
    assert len(files) == int(with_video)


@pytest.mark.parametrize('format_value', [1, 3])
def test_reply_to_sticker_only_message_has_context(format_value):
    source = sticker_message(sticker(format_value), content='')
    message = _FakeMessage(reference=_FakeReference(message_id=123, resolved=source))
    _, embeds, _ = render(message)
    assert len(embeds) == 2
    assert embeds[0].author_data['name'] == 'Replying to TestUser'
    if format_value == 1:
        assert embeds[0].image_url == source.stickers[0].url
    else:
        assert embeds[0].fields[0]['value'] == '`plz join`'


def test_forwarded_sticker_snapshot_is_rendered():
    source = sticker_message(sticker(), content='')
    message = _FakeMessage(content='')
    message.message_snapshots = [source]
    assert _is_starboard_eligible(message, source)
    _, embeds, _ = render(message)
    assert embeds[0].image_url == source.stickers[0].url
    assert embeds[0].author_data['name'] == 'Forwarded by TestUser'


def test_legacy_forward_accepts_resolved_sticker_source():
    source = sticker_message(sticker(), content='')
    message = _FakeMessage(content='', reference=_FakeReference(123, source))
    message.reference.type = 1
    assert _run(resolve_forward_snapshot(message, None)) is source


@pytest.mark.parametrize('emoji,channel', [(PILL, PILL_CHANNEL), (STAR, STAR_CHANNEL)])
def test_sticker_only_message_posts_and_updates_on_reactions(board, emoji, channel):
    harness, db = board
    harness.message.content = ''
    item = sticker()
    harness.message.stickers = [item]
    harness.react(emoji, range(1, 11))
    _run(harness.add(emoji, 10))
    posts = harness.channels[channel].sent
    assert len(posts) == 1
    assert posts[0].embeds[0].image_url == item.url
    assert db.get_starboard_message_v1(MESSAGE, emoji).star_count == 10

    for user in range(10, 2, -1):
        harness.react(emoji, range(1, user))
        _run(harness.remove(emoji, user))
    assert len(posts) == 1
    assert posts[0].content.startswith(f'{emoji} **2** |')
    assert posts[0].embeds[0].image_url == item.url

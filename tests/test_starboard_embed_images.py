"""Image deduplication for the shared starboard and pillboard renderer."""

from types import SimpleNamespace

import pytest

from tests.starboard_test_utils import _FakeAttachment, _FakeMessage, _run
from tle.cogs.starboard import Starboard


PLOT_URL = (
    'https://cdn.discordapp.com/attachments/1273763001949753344/'
    '1554522845122535535/plot.png?ex=6abd31b7&is=6abbe037&hm=signature&'
)


@pytest.fixture(params=['⭐', '💊'], ids=['starboard', 'pillboard'])
def emoji(request):
    return request.param


def _card(kind='rich', media='image', url=PLOT_URL):
    card = SimpleNamespace(
        type=kind, title='Rating graph on Codeforces',
        footer=SimpleNamespace(text='Requested by nifeshe'),
        image=None, thumbnail=None, url=None,
    )
    setattr(card, media, SimpleNamespace(url=url))
    return card


def _render(message, emoji, **kwargs):
    return _run(Starboard.build_starboard_message(
        message, emoji, 5, 0xffaa10, **kwargs))


@pytest.mark.parametrize('attached', [False, True])
@pytest.mark.parametrize('kind', ['rich', 'link', 'article'])
@pytest.mark.parametrize('media', ['image', 'thumbnail'])
def test_card_media_is_not_duplicated_in_header(emoji, attached, kind, media):
    card = _card(kind, media)
    attachments = [_FakeAttachment('plot.png', PLOT_URL)] if attached else []
    message = _FakeMessage(content='', embeds=[card], attachments=attachments)

    content, embeds, files = _render(message, emoji)

    assert content == f'{emoji} **5** | {message.jump_url}'
    assert len(embeds) == 2
    assert embeds[0].author_data['name'] == message.author.display_name
    assert embeds[0].image_url is None
    assert embeds[1] is card
    assert embeds[1].title == 'Rating graph on Codeforces'
    assert embeds[1].footer.text == 'Requested by nifeshe'
    assert getattr(embeds[1], media).url == PLOT_URL
    assert files == []


def test_distinct_attachment_images_stay_in_gallery(emoji):
    card = _card()
    photos = ['https://example.com/first.png', 'https://example.com/second.png']
    attachments = [
        _FakeAttachment('plot.png', PLOT_URL),
        *[_FakeAttachment(f'photo{i}.png', url)
          for i, url in enumerate(photos)],
    ]
    message = _FakeMessage(content='My plots', embeds=[card],
                           attachments=attachments)

    _content, embeds, _files = _render(message, emoji)

    assert [embed.image_url for embed in embeds[:-1]] == photos
    assert embeds[0].description == 'My plots'
    assert embeds[-1] is card


def test_card_does_not_hide_later_plain_image_embed(emoji):
    card = _card()
    image = SimpleNamespace(type='image', url='https://example.com/photo.png',
                            image=None, thumbnail=None)
    message = _FakeMessage(content='', embeds=[card, image])

    _content, embeds, _files = _render(message, emoji)

    assert len(embeds) == 2
    assert embeds[0].image_url == image.url
    assert embeds[1] is card


@pytest.mark.parametrize('use_fallback', [False, True])
def test_forwarded_card_media_is_not_duplicated(emoji, use_fallback):
    card = _card()
    snapshot = _FakeMessage(content='', embeds=[card],
                            attachments=[_FakeAttachment('plot.png', PLOT_URL)])
    message = _FakeMessage(content='')
    kwargs = {}
    if use_fallback:
        kwargs['forward_snapshot'] = snapshot
    else:
        message.message_snapshots = [snapshot]

    _content, embeds, _files = _render(message, emoji, **kwargs)

    assert len(embeds) == 2
    assert embeds[0].author_data['name'] == 'Forwarded by TestUser'
    assert embeds[0].image_url is None
    assert embeds[1] is card

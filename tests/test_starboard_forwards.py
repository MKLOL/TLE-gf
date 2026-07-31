"""Forwarded-message support for starboard and pillboard reactions."""

import asyncio
from datetime import datetime
from types import SimpleNamespace

import discord
import pytest

from tests.starboard_test_utils import (
    FakeUserDb,
    GUILD_A,
    _FakeAttachment,
    _FakeAuthor,
    _FakeMessage,
    _FakeReference,
    _run,
)
from tle.cogs.starboard import Starboard


PILL = '\N{PILL}'


class _Forwarder(_FakeAuthor):
    id = 42


class _Snapshot:
    def __init__(self, *, content='', embeds=None, attachments=None,
                 created_at=None):
        self.content = content
        self.embeds = embeds or []
        self.attachments = attachments or []
        self.created_at = created_at or datetime(2024, 6, 15, 12, 30)
        self.type = discord.MessageType.default


class _ForwardedMessage(_FakeMessage):
    def __init__(self, snapshot, *, message_id=5001, reference=None,
                 reactions=None):
        super().__init__(
            content='', embeds=[], attachments=[], reference=reference)
        self.id = message_id
        self.author = _Forwarder()
        self.message_snapshots = [snapshot]
        self.reactions = reactions or []


class _Reaction:
    def __init__(self, emoji, users):
        self.emoji = emoji
        self._users = [SimpleNamespace(id=user_id) for user_id in users]
        self.count = len(self._users)

    def __str__(self):
        return self.emoji

    async def users(self):
        for user in self._users:
            yield user


class _SourceChannel:
    def __init__(self, channel_id, message):
        self.id = channel_id
        self.nsfw = False
        self._message = message

    async def fetch_message(self, _message_id):
        return self._message


class _StarboardChannel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.sent = []

    async def send(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(id=7001)


class _Guild:
    def __init__(self, guild_id, starboard_channel):
        self.id = guild_id
        self._starboard_channel = starboard_channel

    def get_channel(self, channel_id):
        if channel_id == self._starboard_channel.id:
            return self._starboard_channel
        return None


class _Bot:
    def __init__(self, guild, source_channel):
        self._guild = guild
        self._source_channel = source_channel

    def get_guild(self, _guild_id):
        return self._guild

    def get_channel(self, channel_id):
        if channel_id == self._source_channel.id:
            return self._source_channel
        return None

    async def fetch_channel(self, channel_id):
        if channel_id == self._source_channel.id:
            return self._source_channel
        raise discord.NotFound()


class _Payload:
    guild_id = GUILD_A
    channel_id = 222
    message_id = 5001
    user_id = 99


@pytest.fixture
def db():
    value = FakeUserDb()
    yield value
    value.close()


def test_forward_snapshot_renders_content_embed_attachment_and_timestamp():
    class _RichEmbed:
        type = 'rich'
        title = 'Forwarded card'
        image = None
        thumbnail = None
        url = None

    snapshot_time = datetime(2024, 6, 15, 12, 30)
    snapshot = _Snapshot(
        content='The forwarded body',
        embeds=[_RichEmbed()],
        attachments=[
            _FakeAttachment(
                'photo.png', url='https://cdn.example.com/forwarded.png')
        ],
        created_at=snapshot_time,
    )
    replied_to = _FakeMessage(content='Must not become reply context')
    reference = _FakeReference(message_id=123, resolved=replied_to)
    reference.type = discord.MessageReferenceType.forward
    message = _ForwardedMessage(snapshot, reference=reference)

    content, embeds, files = _run(
        Starboard.build_starboard_message(message, PILL, 5, 0x55AA77))

    assert message.jump_url in content
    assert embeds[0].description == 'The forwarded body'
    assert embeds[0].timestamp == snapshot_time
    assert embeds[0].image_url == 'https://cdn.example.com/forwarded.png'
    assert embeds[0].author_data['name'] == 'Forwarded by TestUser'
    assert embeds[0].author_data['url'] == message.jump_url
    assert embeds[1].title == 'Forwarded card'
    assert all(
        getattr(embed, 'description', None) != 'Must not become reply context'
        for embed in embeds)
    assert files == []


def test_forwarded_video_attributes_outer_forwarder_and_uploads_snapshot_file():
    snapshot = _Snapshot(
        attachments=[
            _FakeAttachment(
                'clip.mp4', url='https://cdn.example.com/forwarded.mp4')
        ])
    message = _ForwardedMessage(snapshot)

    content, embeds, files = _run(
        Starboard.build_starboard_message(message, PILL, 3, 0x55AA77))

    assert 'Forwarded by TestUser' in content
    assert message.jump_url in content
    assert embeds == []
    assert len(files) == 1
    assert files[0] == 'File:clip.mp4'


def test_forward_reference_never_renders_as_reply_context():
    replied_to = _FakeMessage(content='Not reply context')
    reference = _FakeReference(message_id=123, resolved=replied_to)
    reference.type = discord.MessageReferenceType.forward
    message = _FakeMessage(content='Forward fallback', reference=reference)

    _content, embeds, _files = _run(
        Starboard.build_starboard_message(message, PILL, 2, 0x55AA77))

    assert len(embeds) == 1
    assert embeds[0].description == 'Forward fallback'
    assert embeds[0].author_data['name'] == 'Forwarded by TestUser'


def test_reply_renders_when_discord_lacks_message_reference_type(monkeypatch):
    """discord.py 2.4 has reply references but not the forward-type enum."""
    monkeypatch.delattr(discord, 'MessageReferenceType')
    replied_to = _FakeMessage(content='The replied-to message')
    reference = _FakeReference(message_id=123, resolved=replied_to)
    reference.type = 0
    message = _FakeMessage(content='A normal reply', reference=reference)

    _content, embeds, _files = _run(
        Starboard.build_starboard_message(message, PILL, 2, 0x55AA77))

    assert embeds[0].description == 'The replied-to message'
    assert embeds[1].description == 'A normal reply'


def test_forward_value_is_recognized_without_named_enum(monkeypatch):
    monkeypatch.delattr(discord, 'MessageReferenceType')
    replied_to = _FakeMessage(content='Must not become reply context')
    reference = _FakeReference(message_id=123, resolved=replied_to)
    reference.type = 1
    message = _FakeMessage(content='Forward fallback', reference=reference)

    _content, embeds, _files = _run(
        Starboard.build_starboard_message(message, PILL, 2, 0x55AA77))

    assert len(embeds) == 1
    assert embeds[0].description == 'Forward fallback'
    assert embeds[0].author_data['name'] == 'Forwarded by TestUser'


def test_snapshot_only_forward_can_be_added_by_pill_reaction(
        db, monkeypatch):
    from tle.util import codeforces_common as cf_common

    monkeypatch.setattr(cf_common, 'user_db', db)
    db.add_starboard_emoji(GUILD_A, PILL, 1, 0x55AA77)
    db.set_starboard_channel(GUILD_A, PILL, '888')

    snapshot = _Snapshot(content='Snapshot-only message')
    reaction = _Reaction(PILL, [_Payload.user_id])
    message = _ForwardedMessage(snapshot, reactions=[reaction])
    source_channel = _SourceChannel(_Payload.channel_id, message)
    starboard_channel = _StarboardChannel(888)
    guild = _Guild(GUILD_A, starboard_channel)
    cog = Starboard.__new__(Starboard)
    cog.bot = _Bot(guild, source_channel)
    cog.locks = {}

    asyncio.run(cog.check_and_add_to_starboard(
        starboard_channel_id=888,
        threshold=1,
        color=0x55AA77,
        emoji_str=PILL,
        payload=_Payload(),
    ))

    assert len(starboard_channel.sent) == 1
    sent = starboard_channel.sent[0]
    assert sent['embeds'][0].description == 'Snapshot-only message'
    assert sent['embeds'][0].author_data['name'] == 'Forwarded by TestUser'
    stored = db.get_starboard_message_v1(message.id, PILL)
    assert stored is not None
    assert stored.author_id == str(message.author.id)
    assert stored.star_count == 1

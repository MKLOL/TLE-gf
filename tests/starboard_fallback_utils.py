"""Discord surfaces for exercising real fallback posting/rendering end to end."""
from datetime import datetime
from types import SimpleNamespace

import discord
import pytest

from tests.starboard_test_utils import FakeUserDb, GUILD_A, _FakeAuthor
from tle.cogs.starboard import Starboard
from tle.util import codeforces_common as cf_common
from tle.util.starboard_routing import STAR, PILL

SOURCE, STAR_CHANNEL, PILL_CHANNEL, MESSAGE = 101, 102, 103, 201
GASP, ELEPHANT = '<:chocoGasp:123>', '🐘'


@pytest.fixture
def board(monkeypatch):
    db = FakeUserDb()
    yield Harness(monkeypatch, db), db
    db.close()


class Reaction:
    def __init__(self, emoji, users):
        self.emoji = emoji
        self.ids = list(users)

    @property
    def count(self):
        return len(self.ids)

    def __str__(self):
        return self.emoji

    async def users(self):
        for user_id in self.ids:
            yield SimpleNamespace(id=user_id)


class Post:
    def __init__(self, post_id, content, embeds):
        self.id, self.content, self.embeds = post_id, content, embeds

    async def edit(self, *, content, embeds=None, attachments=None):
        self.content = content
        if embeds is not None:
            self.embeds = embeds


class Channel:
    nsfw = False

    def __init__(self, channel_id):
        self.id = channel_id
        self.messages, self.sent = {}, []

    async def fetch_message(self, message_id):
        return self.messages[int(message_id)]

    async def send(self, *, content, embeds, files):
        post = Post(self.id * 100 + len(self.sent), content, embeds)
        self.messages[post.id] = post
        self.sent.append(post)
        return post


class Harness:
    def __init__(self, monkeypatch, db):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self.channels = {key: Channel(key) for key in (SOURCE, STAR_CHANNEL, PILL_CHANNEL)}
        author = _FakeAuthor()
        author.id = 42
        self.message = SimpleNamespace(
            id=MESSAGE, guild=SimpleNamespace(id=GUILD_A), channel=self.channels[SOURCE],
            author=author, content='a message', embeds=[], attachments=[],
            reactions=[], created_at=datetime(2025, 1, 1), reference=None,
            type=discord.MessageType.default,
            jump_url=f'https://discord.com/channels/{GUILD_A}/{SOURCE}/{MESSAGE}')
        self.channels[SOURCE].messages[MESSAGE] = self.message
        guild = SimpleNamespace(id=GUILD_A, get_channel=self.channels.get)
        self.bot = SimpleNamespace(get_guild=lambda _: guild, get_channel=self.channels.get)
        self.restart()
        for emoji, threshold, channel in ((STAR, 3, STAR_CHANNEL), (PILL, 4, PILL_CHANNEL)):
            db.add_starboard_emoji(GUILD_A, emoji, threshold, 0xffaa10)
            db.set_starboard_channel(GUILD_A, emoji, channel)

    def restart(self):
        self.cog = Starboard(self.bot)

    def react(self, emoji, users):
        reaction = Reaction(emoji, users)
        self.message.reactions = [r for r in self.message.reactions if str(r) != emoji]
        self.message.reactions.append(reaction)
        return reaction

    def payload(self, emoji, user, *, channel=SOURCE, message=MESSAGE):
        return SimpleNamespace(guild_id=GUILD_A, channel_id=channel,
                               message_id=message, user_id=user, emoji=emoji)

    async def add(self, emoji, user, **kwargs):
        await self.cog._handle_reaction_add(self.payload(emoji, user, **kwargs))

    async def remove(self, emoji, user, **kwargs):
        await self.cog._handle_reaction_remove(self.payload(emoji, user, **kwargs))

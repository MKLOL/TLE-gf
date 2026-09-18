"""Discord-shaped fakes shared by counting cog integration tests."""

import asyncio
from datetime import datetime, timedelta, timezone


_BASE_TIME = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


class FakeAuthor:
    def __init__(self, user_id, name=None, *, bot=False):
        self.id = user_id
        self.display_name = name or f'User {user_id}'
        self.bot = bot


class FakeGuild:
    def __init__(self, guild_id=100):
        self.id = guild_id


class FakeMessage:
    def __init__(self, message_id, content, channel, *, guild=None,
                 author=None, offset=0, reaction_error=None):
        self.id = message_id
        self.content = content
        self.channel = channel
        self.guild = guild
        self.author = author or FakeAuthor(message_id + 1000)
        self.created_at = _BASE_TIME + timedelta(seconds=offset)
        self.reactions = []
        self.reaction_error = reaction_error

    async def add_reaction(self, emoji):
        # Yield so concurrency tests exercise the cog's per-channel lock.
        await asyncio.sleep(0)
        if self.reaction_error is not None:
            raise self.reaction_error
        self.reactions.append(emoji)


class FakeThreadChannel:
    """A thread-like channel with a recorded async history call."""

    def __init__(self, channel_id=200, messages=None, parent_id=150):
        self.id = channel_id
        self.parent_id = parent_id
        self.mention = f'<#{channel_id}>'
        self.messages = list(messages or ())
        self.history_calls = []

    def history(self, *, limit=None, before=None, oldest_first=None):
        self.history_calls.append({
            'limit': limit,
            'before': before,
            'oldest_first': oldest_first,
        })
        messages = iter(self.messages)

        class _History:
            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(messages)
                except StopIteration:
                    raise StopAsyncIteration

        return _History()


class FakeContext:
    def __init__(self, guild, channel, author=None, message_id=9000):
        self.guild = guild
        self.channel = channel
        self.author = author or FakeAuthor(999, 'Admin')
        self.message = FakeMessage(
            message_id, ';counting here', channel,
            guild=guild, author=self.author, offset=1000)
        self.command = None
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))

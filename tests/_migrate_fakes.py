"""Shared infrastructure for migration cog tests.

Fake Discord objects, DB helper, constants, fixtures, and the _run helper.
This file is NOT a test file (starts with _), so pytest won't collect it.
"""
import asyncio
import json
import sqlite3
import time

import pytest

import discord
from tle.cogs._migrate_helpers import (
    parse_old_bot_message,
    serialize_embed_fallback,
    build_fallback_message,
)
from tle.cogs.starboard import Starboard, _starboard_content
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin
from tle.util.db.migration_db import MigrationDbMixin


# =====================================================================
# Fake Discord objects
# =====================================================================

_next_send_id = 900000


class _FakeUser:
    def __init__(self, user_id=777, name='TestUser'):
        self.id = user_id
        self.display_name = name
        self.display_avatar = type('A', (), {'url': 'https://cdn.example.com/avatar.png'})()

    def __str__(self):
        return f'{self.display_name}#0001'


class _FakeReaction:
    def __init__(self, emoji_str, count=1, user_ids=None):
        self.emoji = emoji_str
        self.count = count
        self._user_ids = user_ids or []

    async def users(self):
        for uid in self._user_ids:
            yield _FakeUser(uid)


class _FakeMessage:
    def __init__(self, msg_id=333, content='', embeds=None, reactions=None, author=None,
                 channel=None):
        self.id = msg_id
        self.content = content
        self.embeds = embeds or []
        self.reactions = reactions or []
        self.author = author or _FakeUser()
        self.channel = channel
        self.created_at = None
        self.jump_url = f'https://discord.com/channels/111/222/{msg_id}'
        self.reference = None
        self.type = discord.MessageType.default
        self.attachments = []
        self.deleted = False

    async def delete(self):
        self.deleted = True


class _FakeChannel:
    def __init__(self, channel_id=100, messages=None):
        self.id = channel_id
        self.mention = f'<#{channel_id}>'
        self._messages = {m.id: m for m in (messages or [])}
        self.sent = []

    async def fetch_message(self, msg_id):
        if msg_id in self._messages:
            return self._messages[msg_id]
        raise discord.NotFound(None, 'Not found')

    async def history(self, after=None, oldest_first=True, limit=None):
        msgs = sorted(self._messages.values(), key=lambda m: m.id,
                       reverse=not oldest_first)
        after_id = after.id if after else 0
        for m in msgs:
            if m.id > after_id:
                yield m

    async def send(self, content=None, embeds=None, files=None):
        global _next_send_id
        _next_send_id += 1
        sent = _FakeMessage(msg_id=_next_send_id, content=content or '', embeds=embeds or [])
        self.sent.append(sent)
        return sent


class _FakeBot:
    """Minimal bot fake for migration cog tests."""
    def __init__(self, channels=None, guilds=None):
        self._channels = {ch.id: ch for ch in (channels or [])}
        self.guilds = guilds or []
        self._ready = True

    def get_channel(self, channel_id):
        return self._channels.get(channel_id)

    async def fetch_channel(self, channel_id):
        ch = self._channels.get(channel_id)
        if ch is None:
            raise discord.NotFound(None, 'Not found')
        return ch

    async def wait_until_ready(self):
        pass


class _FakeMigrateDb(StarboardDbMixin, MigrationDbMixin):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        for sql in [
            '''CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id TEXT, emoji TEXT, threshold INTEGER NOT NULL DEFAULT 3,
                color INTEGER NOT NULL DEFAULT 16755216, channel_id TEXT,
                PRIMARY KEY (guild_id, emoji))''',
            '''CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id TEXT, starboard_msg_id TEXT, guild_id TEXT,
                emoji TEXT, author_id TEXT, star_count INTEGER DEFAULT 0,
                channel_id TEXT, PRIMARY KEY (original_msg_id, emoji))''',
            '''CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT, emoji TEXT, user_id TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id))''',
            '''CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id TEXT, alias_emoji TEXT, main_emoji TEXT,
                PRIMARY KEY (guild_id, alias_emoji))''',
            '''CREATE TABLE IF NOT EXISTS guild_config (
                guild_id TEXT, key TEXT, value TEXT,
                PRIMARY KEY (guild_id, key))''',
            '''CREATE TABLE IF NOT EXISTS starboard_migration (
                guild_id TEXT PRIMARY KEY, old_channel_id TEXT NOT NULL,
                new_channel_id TEXT NOT NULL, emojis TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'crawling',
                last_crawled_msg_id TEXT, crawl_total INTEGER DEFAULT 0,
                crawl_done INTEGER DEFAULT 0, crawl_failed INTEGER DEFAULT 0,
                post_total INTEGER DEFAULT 0, post_done INTEGER DEFAULT 0,
                started_at REAL NOT NULL,
                alias_map TEXT)''',
            '''CREATE TABLE IF NOT EXISTS starboard_migration_entry (
                guild_id TEXT NOT NULL, original_msg_id TEXT NOT NULL,
                emoji TEXT NOT NULL, old_bot_msg_id TEXT NOT NULL,
                old_channel_id TEXT NOT NULL, source_channel_id TEXT,
                author_id TEXT, star_count INTEGER DEFAULT 0,
                new_starboard_msg_id TEXT,
                crawl_status TEXT NOT NULL DEFAULT 'pending',
                embed_fallback TEXT,
                last_error TEXT,
                PRIMARY KEY (original_msg_id, emoji))''',
        ]:
            self.conn.execute(sql)
        self.conn.commit()

    def close(self):
        self.conn.close()


def _run(coro):
    return asyncio.run(coro)


GUILD = 111
PILL = '\N{PILL}'
CHOC = '\N{CHOCOLATE BAR}'


@pytest.fixture(autouse=True)
def _zero_rate_delay():
    """Patch _RATE_DELAY to 0 so async tests don't sleep."""
    import tle.cogs.migrate as _mod
    orig = _mod._RATE_DELAY
    _mod._RATE_DELAY = 0
    yield
    _mod._RATE_DELAY = orig


@pytest.fixture
def db():
    d = _FakeMigrateDb()
    yield d
    d.close()

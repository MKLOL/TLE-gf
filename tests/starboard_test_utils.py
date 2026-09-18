"""Shared fakes and constants for the starboard test suite.

Houses the ``FakeUserDb`` test double (real SQL from ``StarboardDbMixin`` over
an in-memory schema) plus the lightweight discord object fakes used by the
``build_starboard_message`` and snowflake-filtering tests.  Import these from
the various ``tests/test_starboard_*`` modules per the repo convention
(``from tests.starboard_test_utils import ...``).
"""
import asyncio
import sqlite3
from datetime import datetime, timezone

from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin


class FakeUserDb(StarboardDbMixin):
    """Test double for starboard DB methods. Inherits real SQL from StarboardDbMixin,
    only needs to set up the schema and provide self.conn."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        """Create the starboard tables (matches create_tables in UserDbConn)."""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id    TEXT,
                emoji       TEXT,
                threshold   INTEGER NOT NULL DEFAULT 3,
                color       INTEGER NOT NULL DEFAULT 16755216,
                channel_id  TEXT,
                PRIMARY KEY (guild_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id     TEXT,
                starboard_msg_id    TEXT,
                guild_id            TEXT,
                emoji               TEXT,
                author_id           TEXT,
                star_count          INTEGER DEFAULT 0,
                channel_id          TEXT,
                PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_proxy_reactors (
                original_msg_id      TEXT,
                emoji                TEXT,
                user_id              TEXT,
                via_starboard_msg_id TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id,
                             via_starboard_msg_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_narcissus (
                guild_id        TEXT,
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id    TEXT,
                alias_emoji TEXT,
                main_emoji  TEXT,
                PRIMARY KEY (guild_id, alias_emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id    TEXT,
                key         TEXT,
                value       TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS user_starboard_default (
                guild_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                emoji    TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.commit()

    def close(self):
        self.conn.close()


# Shared guild/emoji constants used across the starboard test modules.
GUILD = 111111111111111111
GUILD_A = 111111111111111111
GUILD_B = 222222222222222222
GUILD_OTHER = 222222222222222222
STAR = '⭐'
FIRE = '🔥'
HEART = '❤️'
THUMBS_UP = '\N{THUMBS UP SIGN}'


def _run(coro):
    """Run an async coroutine synchronously for testing."""
    return asyncio.run(coro)


def _make_snowflake(year, month, day):
    """Create a Discord snowflake ID from a date. Used in tests to create
    messages at known timestamps for time-range filtering."""
    dt_obj = datetime(year, month, day, tzinfo=timezone.utc)
    ts_ms = int(dt_obj.timestamp() * 1000)
    discord_epoch_ms = 1420070400000
    snowflake = (ts_ms - discord_epoch_ms) << 22
    return str(snowflake)


# ---------------------------------------------------------------------------
# Lightweight discord object fakes for build_starboard_message tests.
# discord is stubbed by conftest; importing it here is safe at collection time.
# ---------------------------------------------------------------------------
import discord  # noqa: E402  (must follow conftest stubbing)


class _FakeDisplayAvatar:
    url = 'https://cdn.example.com/avatar.png'


class _FakeAuthor:
    display_name = 'TestUser'
    display_avatar = _FakeDisplayAvatar()

    def __str__(self):
        return 'TestUser#1234'


class _FakeChannel:
    id = 222
    mention = '#general'

    async def fetch_message(self, msg_id):
        raise discord.NotFound()


class _FakeReference:
    def __init__(self, message_id=None, resolved=None):
        self.message_id = message_id
        self.resolved = resolved


class _FakeFile:
    """Stand-in for the discord.File produced by Attachment.to_file()."""
    def __init__(self, filename, spoiler):
        self.filename = filename
        self.spoiler = spoiler

    def __eq__(self, other):
        # Backwards-compat with assertions that compare against the old
        # 'File:<name>' string form.
        if isinstance(other, str):
            return f'File:{self.filename}' == other
        return NotImplemented

    def __repr__(self):
        return f'File:{self.filename}(spoiler={self.spoiler})'


class _FakeAttachment:
    def __init__(self, filename, url='https://cdn.example.com/file'):
        self.filename = filename
        self.url = url

    def is_spoiler(self):
        return self.filename.startswith('SPOILER_')

    async def to_file(self, *, spoiler=False):
        return _FakeFile(self.filename, spoiler)


class _FakeMessage:
    """Minimal message mock for build_starboard_message tests."""
    def __init__(self, content='Hello world', embeds=None, attachments=None, reference=None):
        self.content = content
        self.embeds = embeds or []
        self.attachments = attachments or []
        self.created_at = datetime(2025, 1, 1)
        self.channel = _FakeChannel()
        self.jump_url = 'https://discord.com/channels/111/222/333'
        self.author = _FakeAuthor()
        self.reference = reference
        self.type = discord.MessageType.default

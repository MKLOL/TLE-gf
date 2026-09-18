"""Shared test helpers for the great day feature tests."""
import sqlite3
from types import SimpleNamespace

import pytest

from tle.util.db.greatday_db import create_greatday_signup_event_table
from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory


class FakeGreatDayDb:
    """Minimal in-memory DB with greatday_signup, greatday_ban, and kvs tables."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE greatday_signup (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE greatday_ban (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE guild_config (
                guild_id TEXT,
                key      TEXT,
                value    TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE kvs (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE greatday_pick (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                message_id  TEXT NOT NULL,
                picked_at   REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id, message_id)
            )
        ''')
        create_greatday_signup_event_table(self.conn)
        self.conn.commit()

    greatday_signup = UserDbConn.greatday_signup
    greatday_signup_with_event = UserDbConn.greatday_signup_with_event
    greatday_remove = UserDbConn.greatday_remove
    greatday_remove_with_event = UserDbConn.greatday_remove_with_event
    greatday_get_signups = UserDbConn.greatday_get_signups
    greatday_ban = UserDbConn.greatday_ban
    greatday_ban_with_event = UserDbConn.greatday_ban_with_event
    greatday_unban = UserDbConn.greatday_unban
    greatday_is_banned = UserDbConn.greatday_is_banned
    greatday_get_banned = UserDbConn.greatday_get_banned
    greatday_record_picks = UserDbConn.greatday_record_picks
    greatday_get_stats = UserDbConn.greatday_get_stats
    greatday_get_count = UserDbConn.greatday_get_count
    greatday_get_latest_pick = UserDbConn.greatday_get_latest_pick
    greatday_get_pick_history = UserDbConn.greatday_get_pick_history
    greatday_get_post_times = UserDbConn.greatday_get_post_times
    greatday_is_signed_up = UserDbConn.greatday_is_signed_up
    greatday_record_signup_events = UserDbConn.greatday_record_signup_events
    greatday_record_signup_backfill = UserDbConn.greatday_record_signup_backfill
    greatday_record_signup_event = UserDbConn.greatday_record_signup_event
    _greatday_insert_signup_event = UserDbConn._greatday_insert_signup_event
    greatday_get_signup_events = UserDbConn.greatday_get_signup_events
    greatday_get_last_signup = UserDbConn.greatday_get_last_signup
    kvs_set = UserDbConn.kvs_set
    kvs_get = UserDbConn.kvs_get
    kvs_delete = UserDbConn.kvs_delete

    def get_guild_config(self, guild_id, key):
        row = self.conn.execute(
            'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)).fetchone()
        return row.value if row else None

    def set_guild_config(self, guild_id, key, value):
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (str(guild_id), key, value))
        self.conn.commit()


@pytest.fixture
def db():
    return FakeGreatDayDb()


GUILD = '111'
USER_A = '100'
USER_B = '200'
USER_C = '300'


class DiscordAuthor:
    def __init__(self, user_id, display_name='someone', *, username=None,
                 nick=None):
        self.id = int(user_id)
        self.name = username or display_name
        self.global_name = None
        self.nick = nick
        self.display_name = nick or display_name
        self.mention = f'<@{user_id}>'

    def __str__(self):
        return self.name


class DiscordEmbed:
    def __init__(self, description='', title='', target_id=None):
        self.description = description
        self.title = title
        self.footer = ({'text': f'Great Day user ID: {target_id}'}
                       if target_id is not None else None)

    def set_footer(self, *, text=None, **kwargs):
        self.footer = {'text': text}


class DiscordMessage:
    def __init__(self, content, author=None, msg_id=1, at=0.0, embeds=(),
                 reference_id=None):
        self.content = content
        self.author = author
        self.id = msg_id
        self.embeds = list(embeds)
        self.reference = (SimpleNamespace(message_id=reference_id)
                          if reference_id is not None else None)
        self.edits = []

        class _Created:
            def timestamp(_self):
                return at
        self.created_at = _Created()

    async def edit(self, **kwargs):
        self.edits.append(kwargs)


class DiscordContext:
    def __init__(self, guild, author, message):
        self.guild = guild
        self.author = author
        self.message = message
        self.sent = []
        self.send_kwargs = []

    async def send(self, content=None, embed=None, **kwargs):
        self.sent.append(embed if embed is not None else content)
        self.send_kwargs.append(kwargs)
        return DiscordMessage('', msg_id=999)


class DiscordGuild:
    def __init__(self, guild_id, members=(), channels=(), threads=()):
        self.id = int(guild_id)
        self.members = list(members)
        self.channels = list(channels)
        self.threads = list(threads)

    def get_member(self, user_id):
        return next((member for member in self.members
                     if member.id == int(user_id)), None)

    def get_member_named(self, name):
        for member in self.members:
            if name in {member.name, member.nick, member.display_name}:
                return member
        return None


class HistoryChannel:
    def __init__(self, messages, *, channel_id=None, mention=None,
                 history_error=None):
        self._messages = list(messages)
        self.id = channel_id
        self.mention = mention or f'<#{channel_id}>'
        self._history_error = history_error

    def history(self, limit=None, oldest_first=False):
        ordered = (self._messages if oldest_first
                   else list(reversed(self._messages)))

        async def _gen():
            if self._history_error is not None:
                error = self._history_error
                raise error() if isinstance(error, type) else error
            for message in ordered:
                yield message
        return _gen()


def bot_result(description, message_id, at, *, bot_id=7, target_id=None,
               reference_id=None):
    embed = DiscordEmbed(description, target_id=target_id)
    return DiscordMessage(
        '', DiscordAuthor(bot_id), message_id, at, [embed], reference_id)


class _FakeMessage:
    _next_id = 1000

    def __init__(self, content):
        self.content = content
        type(self)._next_id += 1
        self.id = type(self)._next_id

        class _Created:
            def timestamp(self_inner):
                return 0.0
        self.created_at = _Created()


class _FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, content):
        self.sent.append(content)
        return _FakeMessage(content)


class _FakeGuild:
    def __init__(self, guild_id, channel=None, absent_user_ids=()):
        self.id = guild_id
        self._channel = channel
        self._absent = {int(uid) for uid in absent_user_ids}

    def get_channel(self, cid):
        return self._channel

    def get_member(self, uid):
        return None if int(uid) in self._absent else object()

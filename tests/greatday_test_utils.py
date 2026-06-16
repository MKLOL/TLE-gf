"""Shared test helpers for the great day feature tests."""
import sqlite3

import pytest

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
        self.conn.commit()

    greatday_signup = UserDbConn.greatday_signup
    greatday_remove = UserDbConn.greatday_remove
    greatday_get_signups = UserDbConn.greatday_get_signups
    greatday_ban = UserDbConn.greatday_ban
    greatday_unban = UserDbConn.greatday_unban
    greatday_is_banned = UserDbConn.greatday_is_banned
    greatday_record_picks = UserDbConn.greatday_record_picks
    greatday_get_stats = UserDbConn.greatday_get_stats
    greatday_get_count = UserDbConn.greatday_get_count
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

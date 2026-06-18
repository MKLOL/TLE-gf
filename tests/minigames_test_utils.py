"""Shared fakes, fixtures, and helpers for the split minigames test suite."""
import asyncio
import datetime as dt
import json
import sqlite3
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_14_0, upgrade_1_15_0
from tle.util.db.minigame_db import (
    MinigameDbMixin, merged_minigame_winners, diff_merged_winners,
)
from tle.cogs._minigame_common import (
    compute_vs,
    compute_streak,
    compute_longest_streak,
    compute_top,
    parse_date_args,
    resolve_scoring,
    strip_codeblock,
)
from tle.cogs._minigame_akari import AKARI_GAME, parse_akari_message, puzzle_date_for
from tle.cogs._minigame_guessgame import (
    GUESSGAME_GAME,
    parse_guessgame_message,
    guessgame_score_matchup,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
    parse_queens_leaderboard,
    parse_queens_message,
    rank_queens_participants,
)
from tle.cogs.minigames import Minigames
from tle.cogs.minigames import (
    MinigameCogError,
    _SlashCtx,
    _akari_puzzle_table_rows,
    _akari_rating_table_rows,
    _format_akari_puzzle_table,
    _get_akari_puzzle_table_image_file,
    _get_akari_puzzle_table_image,
    _maybe_parse_puzzle_selector,
)
from tle.util.minigame_rating import RatingState


_GAME = 'akari'


def _queens_number(value):
    if isinstance(value, str):
        value = dt.date.fromisoformat(value)
    return minigames_module._queens_puzzle_number_for_date(value)


class FakeMinigameDb(MinigameDbMixin):
    """In-memory SQLite with the minigame schema, reusing the real DB mixin."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_config (
                guild_id   TEXT NOT NULL,
                game       TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                PRIMARY KEY (guild_id, game)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_result (
                message_id     TEXT NOT NULL,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0,
                raw_content    TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (message_id, game, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_import_result (
                message_id     TEXT NOT NULL,
                guild_id       TEXT NOT NULL,
                game           TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                user_id        TEXT NOT NULL,
                puzzle_number  INTEGER NOT NULL,
                puzzle_date    TEXT NOT NULL,
                accuracy       INTEGER NOT NULL,
                time_seconds   INTEGER NOT NULL,
                is_perfect     INTEGER NOT NULL DEFAULT 0,
                raw_content    TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (message_id, game, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_raw_message (
                message_id  TEXT NOT NULL PRIMARY KEY,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                raw_content TEXT NOT NULL
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
            CREATE TABLE IF NOT EXISTS kvs (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_player_link (
                guild_id        TEXT NOT NULL,
                game            TEXT NOT NULL,
                user_id         TEXT NOT NULL,
                external_name   TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                external_url    TEXT,
                linked_at       REAL NOT NULL,
                linked_by       TEXT NOT NULL,
                PRIMARY KEY (guild_id, game, user_id),
                UNIQUE (guild_id, game, normalized_name)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_unresolved_result (
                guild_id        TEXT NOT NULL,
                game            TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                external_name   TEXT NOT NULL,
                channel_id      TEXT NOT NULL,
                puzzle_number   INTEGER NOT NULL,
                puzzle_date     TEXT NOT NULL,
                accuracy        INTEGER NOT NULL,
                time_seconds    INTEGER NOT NULL,
                is_perfect      INTEGER NOT NULL DEFAULT 0,
                raw_content     TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, game, normalized_name, puzzle_number)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_rating (
                guild_id    TEXT NOT NULL,
                game        TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, game, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_ban (
                guild_id   TEXT NOT NULL,
                game       TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                banned_at  REAL NOT NULL,
                banned_by  TEXT NOT NULL,
                reason     TEXT,
                PRIMARY KEY (guild_id, game, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_registrant (
                guild_id      TEXT NOT NULL,
                user_id       TEXT NOT NULL,
                registered_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_optout (
                guild_id     TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                opted_out_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_ban (
                guild_id   TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                banned_at  REAL NOT NULL,
                banned_by  TEXT NOT NULL,
                reason     TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_rating (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_puzzle_difficulty (
                puzzle_number INTEGER NOT NULL PRIMARY KEY,
                difficulty    INTEGER NOT NULL,
                fetched_at    REAL NOT NULL,
                CHECK (difficulty BETWEEN 1 AND 5)
            )
        ''')
        self.conn.commit()

    def get_guild_config(self, guild_id, key):
        row = self.conn.execute(
            'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)
        ).fetchone()
        return row.value if row else None

    def set_guild_config(self, guild_id, key, value):
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (str(guild_id), key, value)
        )
        self.conn.commit()

    def delete_guild_config(self, guild_id, key):
        rc = self.conn.execute(
            'DELETE FROM guild_config WHERE guild_id = ? AND key = ?',
            (str(guild_id), key)
        ).rowcount
        self.conn.commit()
        return rc

    def kvs_set(self, key, value):
        self.conn.execute(
            'INSERT OR REPLACE INTO kvs (key, value) VALUES (?, ?)',
            (key, value)
        )
        self.conn.commit()

    def kvs_get(self, key):
        row = self.conn.execute(
            'SELECT value FROM kvs WHERE key = ?', (key,)
        ).fetchone()
        return row.value if row else None

    def kvs_delete(self, key):
        self.conn.execute('DELETE FROM kvs WHERE key = ?', (key,))
        self.conn.commit()

    def close(self):
        self.conn.close()


@pytest.fixture
def db():
    d = FakeMinigameDb()
    yield d
    d.close()


def _row(message_id, user_id, puzzle_date, is_perfect, time_seconds, accuracy=100, number=1):
    Row = namedtuple(
        'Row',
        'message_id user_id puzzle_date puzzle_number is_perfect time_seconds accuracy'
    )
    return Row(str(message_id), str(user_id), puzzle_date, number, is_perfect, time_seconds, accuracy)


class _FakeGuild:
    def __init__(self, guild_id, members=None, channels=None):
        self.id = guild_id
        self.members = members or []
        self.channels = {
            int(channel.id): channel
            for channel in (channels or [])
        }

    def get_member(self, user_id):
        for member in self.members:
            if getattr(member, 'id', None) == user_id:
                return member
        return None

    def get_channel(self, channel_id):
        return self.channels.get(int(channel_id))


class _FakeChannel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.mention = f'<#{channel_id}>'
        self.sent = []

    async def send(self, content=None, *, embed=None, **kwargs):
        self.sent.append({'content': content, 'embed': embed, 'kwargs': kwargs})
        return SimpleNamespace(
            id=len(self.sent),
            created_at=dt.datetime(2026, 6, 13, tzinfo=dt.timezone.utc),
        )


class _FakeAttachment:
    def __init__(self, filename, payload):
        self.filename = filename
        self._payload = (
            payload if isinstance(payload, bytes) else payload.encode('utf-8'))
        self.size = len(self._payload)

    async def read(self):
        return self._payload


class _FakeAuthor:
    def __init__(self, user_id, bot=False):
        self.id = user_id
        self.bot = bot


class _FakeDiscordMember(_FakeAuthor):
    def __init__(self, user_id, name, display_name=None, bot=False, roles=None):
        super().__init__(user_id, bot=bot)
        self.name = name
        self.display_name = display_name or name
        self.roles = roles or []


class _FakeMessage:
    def __init__(self, msg_id, guild_id, channel_id, user_id, content):
        self.id = msg_id
        self.guild = _FakeGuild(guild_id)
        self.channel = _FakeChannel(channel_id)
        self.author = _FakeAuthor(user_id)
        self.content = content
        self.created_at = dt.datetime(2026, 3, 26, tzinfo=dt.timezone.utc)
        self.replies = []  # captures notice / reply embeds for assertions

    async def reply(self, *args, **kwargs):
        self.replies.append({'args': args, 'kwargs': kwargs})


class _FakeMember:
    def __init__(self, name, display_name=None):
        self.name = name
        self.display_name = display_name or name


class _FakeFollowup:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, *, embed=None, view=None, wait=False, **kw):
        msg = type('Msg', (), {'id': len(self.sent) + 1})()
        self.sent.append({'content': content, 'embed': embed, 'view': view})
        return msg


class _FakeResponse:
    def __init__(self):
        self.deferred = False

    async def defer(self, **kw):
        self.deferred = True


class _FakeInteraction:
    def __init__(self, guild_id=1, user_id=10, channel_id=100):
        self.guild = _FakeGuild(guild_id)
        self.user = _FakeAuthor(user_id)
        self.channel_id = channel_id
        self.client = None
        self.id = 999
        self.response = _FakeResponse()
        self.followup = _FakeFollowup()


class _FakeGroup:
    """Minimal Group stand-in for testing cog_load's backcompat aliasing.

    Real discord.py Groups expose all_commands + get_command; the conftest
    stub doesn't, so we build a real one here.
    """
    def __init__(self, name='stub', aliases=()):
        self.name = name
        self.aliases = list(aliases)
        self.all_commands = {}

    def add(self, cmd):
        self.all_commands[cmd.name] = cmd
        for alias in cmd.aliases:
            self.all_commands[alias] = cmd

    def get_command(self, name):
        return self.all_commands.get(name)


class _QueensCommandsBase:
    """Shared helpers for the split TestQueensCommands classes."""

    @staticmethod
    def _make_ctx(guild, author):
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        return SimpleNamespace(
            guild=guild,
            author=author,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )

    @staticmethod
    def _save_queens_result(db, message_id, user_id, puzzle_date, time_seconds,
                            is_perfect=True, accuracy=100):
        day = dt.date.fromisoformat(puzzle_date)
        db.save_minigame_result(
            message_id, 100, 'queens', 200, user_id, _queens_number(day),
            puzzle_date, accuracy, time_seconds, is_perfect, puzzle_date)


class _AkariRatingHelpers:
    """Shared static helpers for Akari rating/ban/non-pro test classes."""

    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    @staticmethod
    def _akari_msg(msg_id, user_id, body, guild=1, channel=10):
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari 445\n✅2026-03-26✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _akari_msg_n(msg_id, user_id, puzzle, body, guild=1, channel=10):
        puzzle_date = puzzle_date_for(puzzle).isoformat()
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari {puzzle}\n✅{puzzle_date}✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _no_puzzle_filter(monkeypatch):
        # Make recompute clock-independent: don't drop the test's puzzle numbers
        # as "far ahead of today" regardless of the machine's date.
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: 10 ** 9)

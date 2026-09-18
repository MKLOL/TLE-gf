"""In-memory poll and Discord fakes for recovery command tests."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from tests.rpoll_test_utils import CHANNEL, GUILD, FakeRpollDb
from tle.cogs.rpoll import Rpoll
from tle.util import codeforces_common as cf_common

ORIGINAL = 333333333333333333
RESULT = ORIGINAL + 1
BOT = 444444444444444444
VOTER = 555555555555555555


def run(coroutine):
    return asyncio.run(coroutine)


def reference(message_id, *, guild_id=GUILD, channel_id=CHANNEL):
    return SimpleNamespace(message_id=message_id, guild_id=guild_id, channel_id=channel_id)


def message(message_id, *, content='', ref=None, author=BOT):
    return SimpleNamespace(
        id=message_id, content=content, reference=ref,
        author=SimpleNamespace(id=author), embeds=[],
        flags=SimpleNamespace(suppress_embeds=True), edit=AsyncMock(),
    )


def snapshot(db):
    return [db.conn.execute(f'SELECT * FROM {table} ORDER BY 1, 2').fetchall()
            for table in ('rpoll', 'rpoll_option', 'rpoll_vote')]


@pytest.fixture
def recovery(monkeypatch):
    db = FakeRpollDb()
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(discord, 'MessageReference', lambda **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(discord, 'AllowedMentions', SimpleNamespace(
        none=lambda: SimpleNamespace(everyone=False, users=False, roles=False, replied_user=False),
    ), raising=False)
    # The global button stub omits disabled; use a local fake for ended views.
    monkeypatch.setattr(discord.ui, 'Button', lambda **kw: SimpleNamespace(**kw))
    pid = db.create_rpoll(GUILD, CHANNEL, 'Best approach?', ['BFS', 'DFS'], VOTER,
                         time.time(), expires_at=time.time() + 3600, formula='exp')
    db.set_rpoll_message_id(pid, ORIGINAL)
    db.toggle_rpoll_vote(pid, VOTER, 0, 1600)
    db.toggle_rpoll_vote(pid, VOTER + 1, 1, 1200)
    original = message(ORIGINAL)
    result = message(RESULT, content='Poll done!', ref=reference(ORIGINAL))
    messages = {ORIGINAL: original, RESULT: result}

    async def fetch(message_id):
        if message_id not in messages:
            raise discord.NotFound(None, 'Unknown message')
        return messages[message_id]

    channel = SimpleNamespace(id=CHANNEL, fetch_message=AsyncMock(side_effect=fetch), send=AsyncMock())
    bot = SimpleNamespace(user=SimpleNamespace(id=BOT), get_channel=lambda _: channel)
    ctx = SimpleNamespace(
        bot=bot, guild=SimpleNamespace(id=GUILD), channel=channel,
        message=SimpleNamespace(reference=reference(ORIGINAL)), send=AsyncMock(),
    )
    yield SimpleNamespace(db=db, pid=pid, ctx=ctx, cog=Rpoll(bot), channel=channel,
                          original=original, result=result, messages=messages)
    db.close()

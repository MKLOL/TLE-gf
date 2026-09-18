"""Startup backfill integration over the real user database."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import pytest

from tests.starboard_test_utils import GUILD_A, STAR
from tle.cogs import _starboard_backfill as backfill
from tle.cogs.starboard import Starboard
from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import UserDbConn


class Guild:
    id = GUILD_A
    name = 'Test guild'


class Reaction:
    count = 3

    def __str__(self):
        return STAR

    async def users(self):
        for user_id in (51, 52, 53):
            yield SimpleNamespace(id=user_id)


@pytest.fixture
def db(monkeypatch):
    database = UserDbConn(':memory:')
    monkeypatch.setattr(cf_common, 'user_db', database)
    monkeypatch.setattr(database, 'get_all_starboard_messages_for_guild', Mock(
        side_effect=AssertionError('Startup must not read complete history')))
    yield database
    database.close()


def test_completed_history_does_not_trigger_discord_work(db):
    db.add_starboard_message_v1(
        101, 201, GUILD_A, STAR, author_id='42', channel_id=500)
    db.add_starboard_message_v1(
        102, 202, GUILD_A, STAR, author_id='__UNKNOWN__')
    bot = SimpleNamespace(
        guilds=[Guild()], wait_until_ready=AsyncMock(), get_channel=Mock())
    cog = Starboard(bot)

    asyncio.run(cog._backfill_star_counts())

    assert cog.backfill_complete
    assert not cog.backfill_running
    assert (cog.backfill_total, cog.backfill_done, cog.backfill_failed) == (0, 0, 0)
    bot.get_channel.assert_not_called()
    db.get_all_starboard_messages_for_guild.assert_not_called()


def test_backfill_recovers_pending_metadata_and_preserves_checkpoints(db, monkeypatch):
    db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
    db.set_starboard_channel(GUILD_A, STAR, 900)
    db.add_starboard_message_v1(101, 201, GUILD_A, STAR, channel_id=500)
    db.add_starboard_message_v1(102, 202, GUILD_A, STAR, author_id='42')
    db.add_starboard_message_v1(103, 203, GUILD_A, STAR)
    db.add_starboard_message_v1(
        104, 204, GUILD_A, STAR, author_id='43', channel_id=500)
    db.add_starboard_message_v1(
        105, 205, GUILD_A, STAR, author_id='__UNKNOWN__')
    db.add_starboard_message_v1(106, 206, GUILD_A, STAR, author_id='44')
    db.update_starboard_star_count(104, STAR, 8)
    db.update_starboard_star_count(106, STAR, 9)

    original_channel = SimpleNamespace(id=500)

    async def fetch_original(message_id):
        assert message_id in (101, 102)
        return SimpleNamespace(
            author=SimpleNamespace(id=42), channel=original_channel,
            reactions=[Reaction()])

    async def fetch_post(message_id):
        if message_id in (203, 206):
            raise discord.NotFound()
        assert message_id == 202
        field = SimpleNamespace(
            name='Jump to',
            value=f'https://discord.com/channels/{GUILD_A}/500/102')
        return SimpleNamespace(embeds=[SimpleNamespace(fields=[field])])

    original_channel.fetch_message = AsyncMock(side_effect=fetch_original)
    board_channel = SimpleNamespace(id=900, fetch_message=AsyncMock(side_effect=fetch_post))
    bot = SimpleNamespace(
        guilds=[Guild()], wait_until_ready=AsyncMock(),
        get_channel=Mock(side_effect={500: original_channel, 900: board_channel}.get))
    monkeypatch.setattr(backfill.asyncio, 'sleep', AsyncMock())
    cog = Starboard(bot)

    asyncio.run(cog._backfill_star_counts())

    assert cog.backfill_complete
    assert not cog.backfill_running
    assert (cog.backfill_total, cog.backfill_done, cog.backfill_failed) == (4, 4, 1)
    rows = {row.original_msg_id: row for row in db.conn.execute(
        'SELECT * FROM starboard_message_v1')}
    for message_id in ('101', '102'):
        assert (rows[message_id].author_id, rows[message_id].channel_id,
                rows[message_id].star_count) == ('42', '500', 3)
    assert rows['103'].author_id == '__UNKNOWN__'
    assert rows['104'].star_count == 8
    assert rows['105'].author_id == '__UNKNOWN__'
    assert (rows['106'].author_id, rows['106'].star_count) == ('44', 9)
    db.get_all_starboard_messages_for_guild.assert_not_called()

    # A later startup retries only the still-missing channel, preserving its
    # known author/count; completed and newly-unfetchable rows stay skipped.
    original_channel.fetch_message.reset_mock()
    board_channel.fetch_message.reset_mock()
    restarted = Starboard(bot)
    asyncio.run(restarted._backfill_star_counts())
    assert restarted.backfill_total == restarted.backfill_done == 1
    original_channel.fetch_message.assert_not_called()
    board_channel.fetch_message.assert_awaited_once_with(206)

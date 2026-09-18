"""Cog-level counting tests over the real in-memory user database."""

import asyncio
from types import SimpleNamespace

import discord
import pytest

from tests.counting_test_utils import (
    FakeAuthor,
    FakeContext,
    FakeGuild,
    FakeMessage,
    FakeThreadChannel,
)
from tle.cogs import counting as counting_module
from tle.cogs.counting import Counting
from tle.util import codeforces_common as cf_common
from tle.util.db.counting_db import CountingStateConflict
from tle.util.db.user_db_conn import UserDbConn


GOOD = '✅'
BAD = '❌'


@pytest.fixture
def db(monkeypatch):
    database = UserDbConn(':memory:')
    monkeypatch.setattr(cf_common, 'user_db', database)
    try:
        yield database
    finally:
        database.conn.close()


def _run(coro):
    return asyncio.run(coro)


def _configured(db, *, guild_id=100, channel_id=200, current_count=0):
    return db.counting_configure(
        guild_id, channel_id, current_count=current_count, now=1.0)


def _live_message(message_id, content, channel, *, guild=None, author=None,
                  offset=0, reaction_error=None):
    guild = guild or FakeGuild()
    author = author or FakeAuthor(10, 'Alice')
    return FakeMessage(
        message_id, content, channel, guild=guild, author=author,
        offset=offset, reaction_error=reaction_error)


class TestCountingHere:
    def test_scans_full_thread_history_and_installs_current_state(
            self, db, monkeypatch):
        guild = FakeGuild()
        thread = FakeThreadChannel()
        human = FakeAuthor(10, 'Alice')
        bot = FakeAuthor(999, 'CounterBot', bot=True)
        entries = [
            ('1', human),          # decimal 1
            ('10', human),         # binary 2
            ('ordinary prose', human),
            ('3', human),          # decimal 3
            ('0x4', human),        # hexadecimal 4
            ('101', human),        # binary 5
            ('6', bot),            # must not advance history
            ('6', human),
            ('11', human),         # wrong while 7 is expected
            ('73.7', human),       # invalid while 7 is expected
            ('version2', human),   # prose-with-digit is ignored
            ('7', human),
        ]
        thread.messages = [
            FakeMessage(
                index, content, thread, guild=guild, author=author,
                offset=index)
            for index, (content, author) in enumerate(entries, start=1)
        ]
        ctx = FakeContext(guild, thread)
        monkeypatch.setattr(
            counting_module.discord_common, 'embed_success',
            lambda description: SimpleNamespace(description=description))

        _run(Counting.here.__wrapped__(Counting(bot=None), ctx))

        assert thread.history_calls == [{
            'limit': None,
            'before': ctx.message,
            'oldest_first': True,
        }]
        state = db.counting_get_channel(guild.id, thread.id)
        assert (state.current_count, state.last_message_id,
                state.configured_by) == (7, '12', '999')

        attempts = db.counting_get_attempts(guild.id, thread.id)
        assert [row.content for row in attempts] == [
            '1', '10', '3', '0x4', '101', '6', '11', '73.7', '7']
        assert [row.accepted for row in attempts] == [
            1, 1, 1, 1, 1, 1, 0, 0, 1,
        ]
        assert [row.radix for row in attempts] == [
            10, 2, 10, 16, 2, 10, None, None, 10]
        assert [(row.expected_value, row.reason) for row in attempts[-3:]] == [
            (7, 'wrong_number'),
            (7, 'invalid_format'),
            (7, 'correct'),
        ]
        assert all(row.user_id != '999' for row in attempts)

        assert len(ctx.sent) == 1
        description = ctx.sent[0][1]['embed'].description
        assert 'Scanned all **12** earlier messages' in description
        assert 'saved **9** numeric attempts' in description
        assert 'Current count: **7**' in description
        assert 'decimal `8`, binary `1000`, hex `8`' in description

    def test_rescan_deletes_attempt_missing_from_channel_history(
            self, db, monkeypatch):
        guild = FakeGuild()
        thread = FakeThreadChannel()
        author = FakeAuthor(10, 'Alice')
        thread.messages = [
            FakeMessage(1, '1', thread, guild=guild, author=author, offset=1),
            FakeMessage(2, '10', thread, guild=guild, author=author, offset=2),
        ]
        monkeypatch.setattr(
            counting_module.discord_common, 'embed_success',
            lambda description: SimpleNamespace(description=description))
        cog = Counting(bot=None)

        _run(Counting.here.__wrapped__(cog, FakeContext(guild, thread)))
        thread.messages.pop()  # the accepted binary 2 was deleted
        _run(Counting.here.__wrapped__(cog, FakeContext(
            guild, thread, message_id=9001)))

        assert db.counting_get_channel(guild.id, thread.id).current_count == 1
        assert [row.message_id for row in db.counting_get_attempts(
            guild.id, thread.id)] == ['1']
        assert [row.message_id for row in db.counting_get_attempts(
            guild.id, thread.id, include_inactive=True)] == ['1']


class TestCountingLiveMessages:
    def test_correct_count_reacts_advances_and_logs_fields(self, db):
        channel = FakeThreadChannel()
        _configured(db)
        message = _live_message(101, '0x1', channel, offset=5)

        _run(Counting(bot=None).on_message(message))

        assert message.reactions == [GOOD]
        state = db.counting_get_channel(100, 200)
        assert (state.current_count, state.last_message_id) == (1, '101')
        row = db.counting_get_attempt(100, 200, 101)
        assert (row.guild_id, row.channel_id, row.message_id) == \
            ('100', '200', '101')
        assert (row.user_id, row.author_name, row.content) == \
            ('10', 'Alice', '0x1')
        assert (row.expected_value, row.submitted_value, row.accepted,
                row.radix, row.reason) == (1, 1, 1, 16, 'correct')
        assert row.created_at == message.created_at.timestamp()
        assert isinstance(row.recorded_at, float)

    def test_wrong_number_and_decimal_shape_get_cross_without_advancing(self, db):
        channel = FakeThreadChannel()
        _configured(db, current_count=8)
        wrong = _live_message(102, '11', channel, offset=1)
        decimal = _live_message(103, '73.7', channel, offset=2)
        cog = Counting(bot=None)

        _run(cog.on_message(wrong))
        _run(cog.on_message(decimal))

        assert wrong.reactions == [BAD]
        assert decimal.reactions == [BAD]
        assert db.counting_get_channel(100, 200).current_count == 8
        rows = db.counting_get_attempts(100, 200)
        assert [(row.expected_value, row.accepted, row.reason) for row in rows] == [
            (9, 0, 'wrong_number'),
            (9, 0, 'invalid_format'),
        ]

    def test_longer_number_is_rejected_before_bounded_match_advances(self, db):
        channel = FakeThreadChannel()
        _configured(db, current_count=107)
        longer = _live_message(114, '1087', channel, offset=1)
        bounded = _live_message(115, '108 (test)', channel, offset=2)
        cog = Counting(bot=None)

        _run(cog.on_message(longer))
        _run(cog.on_message(bounded))

        assert longer.reactions == [BAD]
        assert bounded.reactions == [GOOD]
        assert db.counting_get_channel(100, 200).current_count == 108
        rows = db.counting_get_attempts(100, 200)
        assert [(row.content, row.expected_value, row.accepted) for row in rows] \
            == [('1087', 108, 0), ('108 (test)', 108, 1)]

    def test_random_prose_including_version2_is_ignored(self, db):
        channel = FakeThreadChannel()
        _configured(db)
        cog = Counting(bot=None)
        messages = [
            _live_message(104, 'hello there', channel),
            _live_message(105, 'version2', channel),
        ]

        for message in messages:
            _run(cog.on_message(message))

        assert [message.reactions for message in messages] == [[], []]
        assert db.counting_get_channel(100, 200).current_count == 0
        assert db.counting_get_attempts(100, 200) == []

    def test_unconfigured_dm_and_bot_messages_are_ignored(self, db):
        channel = FakeThreadChannel()
        guild = FakeGuild()
        cog = Counting(bot=None)
        unconfigured = _live_message(106, '1', channel, guild=guild)
        dm = _live_message(107, '1', channel, guild=None)
        dm.guild = None

        _run(cog.on_message(unconfigured))

        _configured(db)
        bot_message = _live_message(
            108, '1', channel, guild=guild,
            author=FakeAuthor(999, 'CounterBot', bot=True))

        _run(cog.on_message(dm))
        _run(cog.on_message(bot_message))

        assert unconfigured.reactions == []
        assert dm.reactions == []
        assert bot_message.reactions == []
        assert db.counting_get_attempts(100, 200) == []

    def test_duplicate_gateway_event_is_idempotent(self, db):
        channel = FakeThreadChannel()
        _configured(db)
        message = _live_message(109, '1', channel)
        cog = Counting(bot=None)

        _run(cog.on_message(message))
        _run(cog.on_message(message))

        assert message.reactions == [GOOD]
        assert db.counting_get_channel(100, 200).current_count == 1
        assert len(db.counting_get_attempts(100, 200)) == 1

    def test_reaction_failure_keeps_accepted_state_and_ledger(self, db):
        channel = FakeThreadChannel()
        _configured(db)
        message = _live_message(
            110, '1', channel, reaction_error=discord.Forbidden())

        _run(Counting(bot=None).on_message(message))

        assert message.reactions == []
        assert db.counting_get_channel(100, 200).current_count == 1
        assert db.counting_get_attempt(100, 200, 110).accepted == 1

    def test_concurrent_duplicate_number_accepts_once_and_logs_both(self, db):
        channel = FakeThreadChannel()
        _configured(db)
        first = _live_message(111, '1', channel, offset=1)
        second = _live_message(112, '1', channel, offset=2)
        cog = Counting(bot=None)

        async def run_both():
            await asyncio.gather(
                cog.on_message(first),
                cog.on_message(second),
            )

        _run(run_both())

        assert sorted(first.reactions + second.reactions) == sorted([GOOD, BAD])
        assert db.counting_get_channel(100, 200).current_count == 1
        rows = db.counting_get_attempts(100, 200)
        assert len(rows) == 2
        assert [(row.expected_value, row.accepted) for row in rows] == [
            (1, 1),
            (2, 0),
        ]

    def test_cross_process_state_conflict_is_reclassified(
            self, db, monkeypatch):
        channel = FakeThreadChannel()
        _configured(db)
        message = _live_message(113, '1', channel, offset=2)
        cog = Counting(bot=None)
        original_record = db.counting_record_attempt
        raced = False

        def record_with_one_race(*args, **kwargs):
            nonlocal raced
            if not raced:
                raced = True
                original_record(
                    100, 200, 999, 20, 'Other', '1', 1.0,
                    expected_value=1, submitted_value=1, accepted=True,
                    radix=10, reason='correct', recorded_at=1.0)
                raise CountingStateConflict(1, 2)
            return original_record(*args, **kwargs)

        monkeypatch.setattr(db, 'counting_record_attempt', record_with_one_race)

        _run(cog.on_message(message))

        assert message.reactions == [BAD]
        assert db.counting_get_channel(100, 200).current_count == 1
        row = db.counting_get_attempt(100, 200, 113)
        assert (row.expected_value, row.accepted, row.reason) == \
            (2, 0, 'wrong_number')


class TestCountingStatsCommand:
    def test_stats_embed_includes_key_metrics_and_timing_gaps(self, db):
        channel = FakeThreadChannel()
        guild = FakeGuild()
        _configured(db)
        attempts = [
            (201, 1, 'Alice', '1', 100.0, 1, 1, True, 10, 'correct'),
            (202, 2, 'Bob', '3', 105.0, 2, None, False, None, 'wrong_number'),
            (203, 1, 'Alice', '10', 110.0, 2, 2, True, 2, 'correct'),
            (204, 2, 'Bob', '0x3', 130.0, 3, 3, True, 16, 'correct'),
        ]
        for (message_id, user_id, name, content, created_at, expected,
             submitted, accepted, radix, reason) in attempts:
            db.counting_record_attempt(
                guild.id, channel.id, message_id, user_id, name, content,
                created_at, expected_value=expected,
                submitted_value=submitted, accepted=accepted, radix=radix,
                reason=reason, recorded_at=created_at)
        ctx = FakeContext(guild, channel)

        _run(Counting.stats.__wrapped__(Counting(bot=None), ctx))

        embed = ctx.sent[0][1]['embed']
        assert embed.title == 'Counting stats'
        assert '**Current count:** 3' in embed.description
        assert '3/4 correct' in embed.description
        assert '75% accuracy' in embed.description
        assert '2 unique counters' in embed.description
        fields = {field['name']: field['value'] for field in embed.fields}
        assert 'DEC 1' in fields['🔢 Base usage']
        assert 'BIN 1' in fields['🔢 Base usage']
        assert 'HEX 1' in fields['🔢 Base usage']
        pace = fields['⏱️ Pace']
        assert 'fastest 10s (#1→#2)' in pace
        assert 'average 15s' in pace
        assert 'longest 20s (#2→#3)' in pace
        assert fields['🔥 Current same-user streak'] == 'Bob — 1 (#3–#3)'
        assert fields['🔥 Longest same-user streak'] == 'Alice — 2 (#1–#2)'
        assert embed.footer == {'text': 'Next number: 4', 'icon_url': None}

"""Guild-wide Great Day signup-history backfill tests."""
import asyncio
import inspect
from types import SimpleNamespace

import discord
import pytest

from tle.cogs import _greatday_event_backfill as backfill
from tle.cogs._greatday_event_channels import discover_guild_history_channels
from tle.cogs._greatday_history_commands import GreatDayHistoryCommandsMixin
from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import (
    GUILD,
    USER_A,
    DiscordAuthor,
    DiscordGuild,
    DiscordMessage,
    HistoryChannel,
    bot_result,
    db,  # noqa: F401
)


BOT_ID = 7
SIGNUP_OK = 'You have been signed up for great day pings!'
REMOVE_OK = 'You have been removed from great day pings.'
BAN_OK = '`someone` has been banned from great day.'


def command(content, author_id, message_id, at):
    return DiscordMessage(
        content, DiscordAuthor(author_id), message_id, at)


def scan(channels, **kwargs):
    return asyncio.run(backfill.scan_signup_events_channels_audited(
        channels, GUILD, BOT_ID, **kwargs))


class TestGuildWideScanner:
    def test_merges_channels_before_replaying_membership_state(self):
        later_channel = HistoryChannel([
            command(';greatday ban <@100>', USER_A, 3, 20),
            bot_result(BAN_OK, 4, 21),
        ], channel_id=20)
        earlier_channel = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], channel_id=10)

        result = scan(
            [later_channel, earlier_channel],
            current_signup_ids=set(), current_ban_ids={USER_A})

        assert [event[2] for event in result.events] == [
            'signup', 'signout']
        assert result.audit.unknown_ban_states == 0
        assert result.audit.trustworthy

    def test_never_matches_a_result_from_another_channel(self):
        commands = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
        ], channel_id=10)
        results = HistoryChannel([
            bot_result(SIGNUP_OK, 2, 11),
        ], channel_id=20)

        result = scan([commands, results])

        assert result.events == []
        assert result.audit.commands_without_result == 1
        assert result.audit.unmatched_results == 1

    def test_skips_unreadable_channel_and_marks_audit_incomplete(self):
        readable = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], channel_id=10)
        unreadable = HistoryChannel(
            [], channel_id=20, history_error=discord.Forbidden)

        result = scan(
            [unreadable, readable], current_signup_ids={USER_A},
            current_ban_ids=set())

        assert len(result.events) == 1
        assert result.audit.unreadable_channels == 1
        assert not result.audit.trustworthy

    def test_thread_discovery_failure_prevents_clean_audit(self):
        result = scan([], discovery_failures=1)
        assert result.audit.discovery_failures == 1
        assert not result.audit.trustworthy

    def test_transient_history_failure_aborts_instead_of_claiming_complete(self):
        failed = HistoryChannel(
            [], channel_id=20, history_error=discord.HTTPException)
        with pytest.raises(discord.HTTPException):
            scan([failed])


class ArchiveParent(discord.TextChannel):
    def __init__(self, active, public_archived, private_archived):
        self.id = 10
        self._active = active
        self._public = public_archived
        self._private = private_archived

    def history(self, limit=None, oldest_first=False):
        return HistoryChannel([]).history(limit, oldest_first)

    async def archived_threads(self, *, limit=None, private=False,
                               joined=False):
        threads = self._private if private and joined else self._public
        for thread in threads:
            yield thread


def test_discovers_cached_and_archived_threads_without_duplicates():
    active = HistoryChannel([], channel_id=20)
    public = HistoryChannel([], channel_id=30)
    private = HistoryChannel([], channel_id=40)
    parent = ArchiveParent(active, [public], [private])
    guild = DiscordGuild(GUILD, channels=[parent], threads=[active])

    scope = asyncio.run(discover_guild_history_channels(guild, (active,)))

    assert [channel.id for channel in scope.channels] == [10, 20, 30, 40]
    assert scope.discovery_failures == 0


class UnlistableArchiveParent(discord.TextChannel):
    id = 50

    def history(self, limit=None, oldest_first=False):
        return HistoryChannel([]).history(limit, oldest_first)

    async def archived_threads(self, **kwargs):
        if False:
            yield None
        raise discord.Forbidden()


def test_archived_thread_listing_failures_are_reported():
    parent = UnlistableArchiveParent()
    guild = DiscordGuild(GUILD, channels=[parent])
    scope = asyncio.run(discover_guild_history_channels(guild))
    assert scope.discovery_failures == 2


class TestBackfillCommand:
    @staticmethod
    def make_context(guild, invocation_channel):
        ctx = SimpleNamespace(
            guild=guild,
            channel=invocation_channel,
            author=DiscordAuthor('999'),
            sent=[],
        )

        async def send(content=None, embed=None, **kwargs):
            message = DiscordMessage('', msg_id=999)
            message.sent_content = content
            message.sent_embed = embed
            ctx.sent.append(message)
            return message

        ctx.send = send
        return ctx

    def test_no_argument_scans_all_channels_and_saves_guild_audit(
            self, db, monkeypatch):
        from tle.cogs.greatday import GreatDay

        monkeypatch.setattr(cf_common, 'user_db', db)
        signup_channel = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], channel_id=10)
        remove_channel = HistoryChannel([
            command(';greatday remove', USER_A, 3, 20),
            bot_result(REMOVE_OK, 4, 21),
        ], channel_id=20)
        guild = DiscordGuild(
            GUILD, [DiscordAuthor(USER_A)],
            channels=[remove_channel, signup_channel])
        ctx = self.make_context(guild, signup_channel)
        cog = GreatDay(SimpleNamespace(user=DiscordAuthor(BOT_ID)))

        asyncio.run(cog.backfill_signups.callback(cog, ctx))
        asyncio.run(cog.backfill_signups.callback(cog, ctx))

        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [row.action for row in reversed(rows)] == [
            'signup', 'signout']
        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'clean:guild'

    def test_targeted_clean_scan_is_recorded_as_partial(
            self, db, monkeypatch):
        from tle.cogs.greatday import GreatDay

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.greatday_signup(GUILD, USER_A)
        channel = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], channel_id=10)
        guild = DiscordGuild(GUILD, [DiscordAuthor(USER_A)])
        ctx = self.make_context(guild, channel)
        cog = GreatDay(SimpleNamespace(user=DiscordAuthor(BOT_ID)))

        asyncio.run(cog.backfill_signups.callback(cog, ctx, channel))

        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'partial:10'

    def test_unreadable_channel_saves_incomplete_guild_audit(
            self, db, monkeypatch):
        from tle.cogs.greatday import GreatDay

        monkeypatch.setattr(cf_common, 'user_db', db)
        readable = HistoryChannel([], channel_id=10)
        unreadable = HistoryChannel(
            [], channel_id=20, history_error=discord.Forbidden)
        guild = DiscordGuild(
            GUILD, channels=[readable, unreadable])
        ctx = self.make_context(guild, readable)
        cog = GreatDay(SimpleNamespace(user=DiscordAuthor(BOT_ID)))

        asyncio.run(cog.backfill_signups.callback(cog, ctx))

        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'incomplete:guild'


def test_both_backfills_allow_admin_and_moderator_roles():
    decorator = (
        '@commands.has_any_role(constants.TLE_ADMIN, '
        'constants.TLE_MODERATOR)')
    pick_source = inspect.getsource(
        GreatDayHistoryCommandsMixin.backfill.callback)
    signup_source = inspect.getsource(
        GreatDayHistoryCommandsMixin.backfill_signups.callback)

    assert decorator in pick_source
    assert decorator in signup_source


def test_full_scan_can_replace_a_partial_audit(db):
    event = [(GUILD, USER_A, 'signup', 10.0, '1')]
    db.greatday_record_signup_backfill(event, GUILD, 'partial:10')
    db.greatday_record_signup_backfill(event, GUILD, 'clean:guild')
    assert db.get_guild_config(
        GUILD, 'greatday_signup_history_audit') == 'clean:guild'

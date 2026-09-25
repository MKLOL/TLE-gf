import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from tests.complaint_helpers import COMMIT, ComplaintDb, fake_bot
from tle.util.complaints import ComplaintError, ComplaintService, validate_resolution


@pytest.fixture
def workflow(monkeypatch):
    class Mentions(SimpleNamespace):
        @classmethod
        def none(cls):
            return cls(everyone=False, roles=False, users=False, replied_user=False)
    monkeypatch.setattr(discord, 'AllowedMentions', Mentions, raising=False)
    monkeypatch.setattr(discord, 'MessageReference', SimpleNamespace)
    db = ComplaintDb()
    bot = fake_bot()
    service = ComplaintService(bot, lambda: db)
    cid = db.add_complaint(1, 20, 'broken', 'https://discord.com/channels/1/2/3')
    return service, db, bot, cid


def test_resolve_replies_with_fix_and_only_pings_author(workflow):
    service, db, bot, cid = workflow
    row = asyncio.run(service.resolve(1, cid, 10, 'Fixed @everyone', COMMIT))
    assert row.notification_status == 'sent'
    kwargs = bot.channel.send.call_args.kwargs
    assert kwargs['reference'].message_id == 3
    assert kwargs['reference'].fail_if_not_exists
    assert kwargs['content'] == '<@20>'
    assert kwargs['embed'].description == 'Fixed @everyone'
    assert COMMIT in kwargs['embed'].fields[0]['value']
    assert [u.id for u in kwargs['allowed_mentions'].users] == [20]
    assert not kwargs['allowed_mentions'].everyone
    bot.user.send.assert_not_called()


def test_concurrent_and_repeated_resolution_notifies_once(workflow):
    service, db, bot, cid = workflow
    async def run():
        await asyncio.gather(*(service.resolve(1, cid, 10, 'fixed', COMMIT) for _ in range(3)))
    asyncio.run(run())
    assert bot.channel.send.await_count == 1
    assert len(db.get_complaint_events(cid, 1)) == 1
    with pytest.raises(ComplaintError) as exc:
        asyncio.run(service.resolve(1, cid, 10, 'different', COMMIT))
    assert exc.value.status == 409


def test_deleted_message_falls_back_to_dm(workflow):
    service, db, bot, cid = workflow
    bot.channel.send.side_effect = discord.HTTPException('deleted')
    row = asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
    assert row.notification_status == 'sent'
    bot.user.send.assert_awaited_once()


def test_notification_failure_is_visible_and_retryable(workflow):
    service, db, bot, cid = workflow
    bot.channel.send.side_effect = discord.HTTPException('forbidden')
    bot.user.send.side_effect = discord.HTTPException('DMs disabled')
    row = asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
    assert row.notification_status == 'failed' and row.resolved_at
    bot.channel.send.side_effect = None
    row = asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
    assert row.notification_status == 'sent'
    assert row.notification_attempts == 2
    assert len(db.get_complaint_events(cid, 1)) == 1


def test_restart_retries_pending_resolution(workflow):
    service, db, bot, cid = workflow
    db.resolve_complaint(cid, 1, 10, 'fixed', COMMIT)
    fresh = ComplaintService(bot, lambda: db)
    asyncio.run(fresh.retry_notifications())
    bot.channel.send.assert_awaited_once()
    assert db.get_complaint(cid).notification_status == 'sent'


def test_cancellation_leaves_recoverable_notification(workflow):
    service, db, bot, cid = workflow
    bot.channel.send.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
    assert db.get_complaint(cid).notification_status == 'pending'
    bot.channel.send.side_effect = None
    asyncio.run(service.retry_notifications())
    assert db.get_complaint(cid).notification_status == 'sent'


def test_reopen_retains_audit_and_cancels_queued_reply(workflow):
    service, db, bot, cid = workflow
    db.resolve_complaint(cid, 1, 10, 'fixed', COMMIT)
    asyncio.run(service.reopen(1, cid, 10))
    asyncio.run(service.retry_notifications())
    assert db.get_complaint(cid).resolved_at is None
    assert len(db.get_complaint_events(cid, 1)) == 2
    bot.channel.send.assert_not_called()


def test_cross_guild_resolution_fails_without_reply(workflow):
    service, db, bot, cid = workflow
    with pytest.raises(ComplaintError) as exc:
        asyncio.run(service.resolve(2, cid, 10, 'fixed', COMMIT))
    assert exc.value.status == 404
    assert db.get_complaint(cid).resolved_at is None
    bot.channel.send.assert_not_called()


@pytest.mark.parametrize('link', [None, 'http://github.com/a/b/commit/abcdef1',
    'https://github.com.evil/a/b/commit/abcdef1', 'https://github.com/a/b/pull/1',
    'https://github.com/a/b/commit/abcdef1) @everyone', COMMIT + '?token=secret',
    'https://github.com/a/' + 'b' * 1024 + '/commit/abcdef1'])
def test_invalid_commit_rejected(link):
    with pytest.raises(ComplaintError):
        validate_resolution('fixed', link)

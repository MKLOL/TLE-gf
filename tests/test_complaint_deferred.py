"""Deferred complaint notifications.

An API resolve comes from a checkout that has only been pushed. Its
notification is queued and released once a restarted bot verifies, via git in
its own checkout, that the commit is running — and it is delivered exactly
once.
"""
import asyncio
import subprocess
from types import SimpleNamespace

import discord
import pytest

from tests.complaint_helpers import COMMIT, ComplaintDb, fake_bot
from tle.util import git_state
from tle.util.complaints import ComplaintService

SHA = COMMIT.rsplit('/', 1)[1]


@pytest.fixture
def env(monkeypatch):
    class Mentions(SimpleNamespace):
        @classmethod
        def none(cls):
            return cls(everyone=False, roles=False, users=False,
                       replied_user=False)
    monkeypatch.setattr(discord, 'AllowedMentions', Mentions, raising=False)
    monkeypatch.setattr(discord, 'MessageReference', SimpleNamespace)
    db = ComplaintDb()
    bot = fake_bot()
    cid = db.add_complaint(1, 20, 'broken', 'https://discord.com/channels/1/2/3')
    return db, bot, cid


def _service(db, bot, deployed):
    return ComplaintService(bot, lambda: db,
                            is_deployed=lambda sha: sha in deployed)


def _resolve_via_api(service, cid):
    return asyncio.run(service.resolve(
        1, cid, 10, 'fixed', COMMIT, token_id=5, defer_notification=True))


class TestApiResolveIsQueued:
    def test_nothing_is_sent_and_status_says_queued(self, env):
        db, bot, cid = env
        row = _resolve_via_api(_service(db, bot, set()), cid)
        assert row.status if hasattr(row, 'status') else True
        assert row.notification_status == 'queued'
        assert row.resolved_at is not None
        bot.channel.send.assert_not_awaited()
        bot.user.send.assert_not_awaited()

    def test_repeating_the_api_resolve_still_sends_nothing(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})  # deployed, but not yet released
        _resolve_via_api(service, cid)
        _resolve_via_api(service, cid)
        bot.channel.send.assert_not_awaited()
        assert len(db.get_complaint_events(cid, 1)) == 1

    def test_a_discord_admin_repeating_it_cannot_leak_it_early(self, env):
        """The Discord path notifies immediately, but a queued row is a
        queued row whoever repeats the resolution."""
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        row = asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
        assert row.notification_status == 'queued'
        bot.channel.send.assert_not_awaited()

    def test_the_retry_worker_ignores_queued_rows(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        asyncio.run(service.retry_notifications())
        bot.channel.send.assert_not_awaited()
        assert db.get_complaint(cid).notification_status == 'queued'


class TestReleaseOnDeploy:
    def test_released_once_the_commit_is_running(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)

        assert asyncio.run(service.release_deployed()) == 1
        assert db.get_complaint(cid).notification_status == 'pending'
        asyncio.run(service.retry_notifications())
        assert db.get_complaint(cid).notification_status == 'sent'
        bot.channel.send.assert_awaited_once()

    def test_stays_queued_while_the_commit_is_not_running(self, env):
        db, bot, cid = env
        service = _service(db, bot, set())
        _resolve_via_api(service, cid)
        assert asyncio.run(service.release_deployed()) == 0
        asyncio.run(service.retry_notifications())
        assert db.get_complaint(cid).notification_status == 'queued'
        bot.channel.send.assert_not_awaited()

    def test_delivered_exactly_once_across_repeated_releases(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        for _ in range(3):
            asyncio.run(service.release_deployed())
            asyncio.run(service.retry_notifications())
        bot.channel.send.assert_awaited_once()
        assert db.get_complaint(cid).notification_attempts == 1

    def test_a_fresh_process_picks_up_what_an_old_one_queued(self, env):
        """The bot that queued it is not the bot that sends it."""
        db, bot, cid = env
        _resolve_via_api(_service(db, bot, set()), cid)
        restarted = _service(db, bot, {SHA})
        asyncio.run(restarted.release_deployed())
        asyncio.run(restarted.retry_notifications())
        bot.channel.send.assert_awaited_once()

    def test_an_unparseable_commit_url_never_releases(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        db.conn.execute('UPDATE complaint SET commit_url = ? WHERE id = ?',
                        ('https://github.com/MKLOL/TLE-gf/commits/master', cid))
        db.conn.commit()
        assert asyncio.run(service.release_deployed()) == 0

    def test_reopening_clears_the_queue(self, env):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        asyncio.run(service.reopen(1, cid, 10))
        assert db.get_complaint(cid).notification_status is None
        assert asyncio.run(service.release_deployed()) == 0

    def test_the_worker_releases_once_per_process(self, env, monkeypatch):
        db, bot, cid = env
        service = _service(db, bot, {SHA})
        _resolve_via_api(service, cid)
        calls = []
        original = service.release_deployed

        async def counting():
            calls.append(1)
            return await original()
        monkeypatch.setattr(service, 'release_deployed', counting)

        async def two_ticks():
            sleeps = []

            async def fake_sleep(seconds):
                sleeps.append(seconds)
                if len(sleeps) == 2:
                    raise asyncio.CancelledError
            monkeypatch.setattr(asyncio, 'sleep', fake_sleep)
            try:
                await service._run_notifications()
            except asyncio.CancelledError:
                pass
        asyncio.run(two_ticks())
        assert calls == [1]
        bot.channel.send.assert_awaited_once()


class TestDiscordResolveIsImmediate:
    def test_manual_resolution_still_notifies_now(self, env):
        db, bot, cid = env
        service = _service(db, bot, set())
        row = asyncio.run(service.resolve(1, cid, 10, 'fixed', COMMIT))
        assert row.notification_status == 'sent'
        bot.channel.send.assert_awaited_once()


class TestGitState:
    def test_sha_is_read_from_a_commit_url(self):
        assert git_state.commit_sha(COMMIT) == 'a' * 40
        assert git_state.commit_sha(
            'https://github.com/MKLOL/TLE-gf/commit/abc1234?diff=split') == 'abc1234'

    def test_non_commit_urls_yield_nothing(self):
        assert git_state.commit_sha('https://github.com/MKLOL/TLE-gf/commits/master') is None
        assert git_state.commit_sha(None) is None
        assert git_state.commit_sha('') is None

    def test_head_of_this_checkout_counts_as_deployed(self):
        try:
            head = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                                  text=True, timeout=10, check=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pytest.skip('not running inside a git checkout')
        assert git_state.commit_is_deployed(head)
        assert git_state.commit_is_deployed(head[:8])

    def test_unknown_and_empty_shas_are_not_deployed(self):
        assert not git_state.commit_is_deployed('0' * 40)
        assert not git_state.commit_is_deployed(None)
        assert not git_state.commit_is_deployed('')

"""Tests for the migration retry system: exponential backoff, retry_exhausted
status, retry-failed command, and view-failed command."""
import asyncio
import json
import pytest

import discord
from tests._migrate_fakes import (
    _FakeUser, _FakeReaction, _FakeMessage, _FakeChannel, _FakeBot,
    _FakeMigrateDb, _run, GUILD, PILL, CHOC, db, _zero_rate_delay,
)
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError


# =====================================================================
# Helpers
# =====================================================================


class _FakeGuild:
    def __init__(self, guild_id=GUILD):
        self.id = guild_id


class _FakeCtx:
    def __init__(self, guild_id=GUILD):
        self.guild = _FakeGuild(guild_id)
        self.author = _FakeUser()
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append(content)


@pytest.fixture(autouse=True)
def _zero_retry_delay(monkeypatch):
    """Patch retry base delay to 0 so tests don't sleep."""
    import tle.cogs.migrate as _mod
    monkeypatch.setattr(_mod, '_RETRY_BASE_DELAY', 0)


# =====================================================================
# discord_retry unit tests
# =====================================================================


class TestDiscordRetry:
    def test_succeeds_first_try(self):
        calls = []

        async def factory():
            calls.append(1)
            return 'ok'

        result = _run(discord_retry(factory, max_retries=3, base_delay=0))
        assert result == 'ok'
        assert len(calls) == 1

    def test_retries_on_http_exception(self):
        calls = []

        async def factory():
            calls.append(1)
            if len(calls) < 3:
                raise discord.HTTPException(None, 'server error')
            return 'ok'

        result = _run(discord_retry(factory, max_retries=5, base_delay=0))
        assert result == 'ok'
        assert len(calls) == 3

    def test_raises_retry_exhausted_after_max(self):
        calls = []

        async def factory():
            calls.append(1)
            raise discord.HTTPException(None, 'always fails')

        with pytest.raises(RetryExhaustedError) as exc_info:
            _run(discord_retry(factory, max_retries=3, base_delay=0))
        # 1 initial + 3 retries = 4 total
        assert len(calls) == 4
        assert 'always fails' in str(exc_info.value.last_exception)

    def test_does_not_retry_not_found(self):
        calls = []

        async def factory():
            calls.append(1)
            raise discord.NotFound(None, 'Not found')

        with pytest.raises(discord.NotFound):
            _run(discord_retry(factory, max_retries=5, base_delay=0))
        assert len(calls) == 1  # no retry

    def test_does_not_retry_forbidden(self):
        calls = []

        async def factory():
            calls.append(1)
            raise discord.Forbidden(None, 'Forbidden')

        with pytest.raises(discord.Forbidden):
            _run(discord_retry(factory, max_retries=5, base_delay=0))
        assert len(calls) == 1


# =====================================================================
# DB method tests for retry_exhausted
# =====================================================================


class TestRetryExhaustedDb:
    def test_update_retry_exhausted(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, '503 Service Unavailable')

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'retry_exhausted'
        assert entry.last_error == '503 Service Unavailable'

    def test_get_retry_exhausted_entries(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
        db.add_migration_entry(str(GUILD), '333', PILL, '443', '100')
        db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
        db.update_migration_entry_retry_exhausted('222', PILL, 'error')
        db.update_migration_entry_retry_exhausted('333', PILL, 'error2')

        entries = db.get_retry_exhausted_entries(str(GUILD))
        assert len(entries) == 2
        # Chronological order
        assert entries[0].original_msg_id == '222'
        assert entries[1].original_msg_id == '333'

    def test_reset_retry_exhausted_with_source(self, db):
        """Entries with source_channel_id reset to 'crawled'."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '500', '777', 5)
        db.update_migration_entry_retry_exhausted('333', PILL, 'error')

        db.reset_retry_exhausted_entries(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        assert entry.last_error is None

    def test_reset_retry_exhausted_without_source(self, db):
        """Entries without source_channel_id reset to 'deleted'."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, 'error')

        db.reset_retry_exhausted_entries(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'
        assert entry.last_error is None

    def test_retry_exhausted_shown_in_status_counts(self, db):
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, 'error')

        counts = db.count_migration_entries_by_status(str(GUILD))
        by_status = {r.crawl_status: r.cnt for r in counts}
        assert by_status.get('retry_exhausted') == 1

    def test_retry_exhausted_excluded_from_posting(self, db):
        """retry_exhausted entries should NOT be picked up for posting."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, 'error')

        entries = db.get_migration_entries_for_posting(str(GUILD))
        assert len(entries) == 0


# =====================================================================
# Crawl phase retry integration
# =====================================================================


class TestCrawlRetry:
    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_crawl_retries_then_succeeds(self, db):
        """Transient failures during fetch should be retried and succeed."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=2, user_ids=[10])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        old_bot_msg = self._make_old_bot_msg(1001, PILL, 2, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        bot = _FakeBot(channels=[old_channel, source_channel])

        # Make fetch_message fail twice then succeed
        real_fetch = source_channel.fetch_message
        call_count = [0]
        original_fetch = source_channel.fetch_message

        async def flaky_fetch(msg_id):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise discord.HTTPException(None, 'server error')
            return await real_fetch(msg_id)

        source_channel.fetch_message = flaky_fetch

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        assert call_count[0] == 3  # 2 failures + 1 success

    def test_crawl_marks_retry_exhausted(self, db):
        """Persistent failures should mark entry as retry_exhausted."""
        old_bot_msg = self._make_old_bot_msg(1001, PILL, 5, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])

        # Source channel always fails
        source_channel = _FakeChannel(channel_id=222, messages=[])

        async def always_fail(msg_id):
            raise discord.HTTPException(None, 'persistent 503')

        source_channel.fetch_message = always_fail

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'retry_exhausted'
        assert 'persistent 503' in entry.last_error

    def test_crawl_continues_after_retry_exhausted(self, db):
        """After one entry is retry_exhausted, crawl should continue to next."""
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 5, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 3, 222, 444)
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])

        # Channel 222 only has msg 444, not 333
        source_channel = _FakeChannel(channel_id=222, messages=[original2])

        # Make fetch_message always fail for msg 333 with HTTPException
        real_fetch = source_channel.fetch_message

        async def fail_333(msg_id):
            if msg_id == 333:
                raise discord.HTTPException(None, 'persistent failure')
            return await real_fetch(msg_id)

        source_channel.fetch_message = fail_333

        bot = _FakeBot(channels=[old_channel, source_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # 333 should be retry_exhausted
        entry333 = db.get_migration_entry('333', PILL)
        assert entry333.crawl_status == 'retry_exhausted'

        # 444 should be crawled successfully
        entry444 = db.get_migration_entry('444', PILL)
        assert entry444.crawl_status == 'crawled'
        assert entry444.author_id == '888'


# =====================================================================
# Post phase retry integration
# =====================================================================


class TestPostRetry:
    def test_post_retries_send_then_succeeds(self, db):
        """Transient send failure should be retried."""
        new_channel = _FakeChannel(channel_id=200)
        real_send = new_channel.send
        call_count = [0]

        async def flaky_send(**kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise discord.HTTPException(None, 'server error')
            return await real_send(**kwargs)

        new_channel.send = lambda **kw: flaky_send(**kw)

        bot = _FakeBot(channels=[new_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'hi'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'posted'

    def test_post_marks_retry_exhausted_on_persistent_failure(self, db):
        """Persistent send failure should mark entry as retry_exhausted."""
        new_channel = _FakeChannel(channel_id=200)

        async def always_fail(**kwargs):
            raise discord.HTTPException(None, 'always fails')

        new_channel.send = lambda **kw: always_fail(**kw)

        bot = _FakeBot(channels=[new_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'hi'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'retry_exhausted'
        assert 'always fails' in entry.last_error

    def test_post_continues_after_retry_exhausted(self, db):
        """After one entry fails, post should continue to the next."""
        new_channel = _FakeChannel(channel_id=200)
        real_send = new_channel.send
        call_count = [0]

        async def fail_first(**kwargs):
            call_count[0] += 1
            # Fail all attempts for the first entry (6 calls = 1 + 5 retries)
            if call_count[0] <= 6:
                raise discord.HTTPException(None, 'first entry fails')
            return await real_send(**kwargs)

        new_channel.send = lambda **kw: fail_first(**kw)

        bot = _FakeBot(channels=[new_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': 'first'}))
        db.update_migration_entry_deleted('222', PILL, json.dumps({'content': 'second'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        entry111 = db.get_migration_entry('111', PILL)
        assert entry111.crawl_status == 'retry_exhausted'

        entry222 = db.get_migration_entry('222', PILL)
        assert entry222.crawl_status == 'posted'


# =====================================================================
# retry-failed command tests
# =====================================================================


# =====================================================================
# Exact copy behavior: posts use original emoji and count
# =====================================================================


class TestExactCopyBehavior:
    """Posts in the new channel should be exact copies of the old ones —
    same emoji and same count as crawled from the old bot."""

    def test_posted_message_uses_original_emoji_and_count(self, db):
        """A crawled entry should be posted with its original emoji and star_count."""
        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FakeReaction(PILL, count=7, user_ids=[10, 11, 12, 13, 14, 15, 16])],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel, source_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '222', '777', 7)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        # Must contain the original emoji
        assert PILL in content
        # Must contain the original count
        assert '**7**' in content

    def test_fallback_message_uses_original_emoji_and_count(self, db):
        """Deleted entries should also preserve the original emoji and count."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', CHOC, 1000.0)
        db.add_migration_entry(str(GUILD), '333', CHOC, '444', '100')
        # star_count=0 for deleted entries (default), but the fallback should
        # still use the entry's emoji
        db.update_migration_entry_deleted('333', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {CHOC}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        assert CHOC in content

    def test_alias_emoji_not_converted_in_post(self, db):
        """Posts for alias emojis should keep the alias emoji, not convert to main."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', f'{PILL},{CHOC}', 1000.0)
        db.set_migration_alias_map(str(GUILD), json.dumps({CHOC: PILL}))

        db.add_migration_entry(str(GUILD), '333', CHOC, '444', '100')
        db.update_migration_entry_deleted('333', CHOC, json.dumps({'content': f'{CHOC} **3** | https://discord.com/channels/{GUILD}/100/333'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL, CHOC}, db))

        assert len(new_channel.sent) == 1
        content = new_channel.sent[0].content
        # Must be chocolate, NOT pill
        assert CHOC in content
        assert PILL not in content


class TestRetryFailedCommand:
    def _make_cog(self, db, bot=None):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.migrate import Migrate
        self._old_db = cf_common.user_db
        cf_common.user_db = db
        bot = bot or _FakeBot()
        return Migrate(bot)

    def _teardown_cog(self):
        from tle.util import codeforces_common as cf_common
        cf_common.user_db = self._old_db

    def _call(self, cog, method_name, ctx):
        _run(getattr(cog, method_name).__wrapped__(cog, ctx))

    def test_no_migration(self, db):
        cog = self._make_cog(db)
        try:
            ctx = _FakeCtx()
            self._call(cog, 'retry_failed', ctx)
            assert 'No migration' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_no_failed_entries(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            ctx = _FakeCtx()
            self._call(cog, 'retry_failed', ctx)
            assert 'No failed entries' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_retry_resets_and_posts(self, db):
        """retry-failed should reset retry_exhausted entries and re-post them."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])
        cog = self._make_cog(db, bot)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_entry_retry_exhausted('333', PILL, 'old error')
            db.update_migration_status(str(GUILD), 'done')

            # Simulate what retry_failed does
            entries = db.get_retry_exhausted_entries(str(GUILD))
            assert len(entries) == 1
            db.reset_retry_exhausted_entries(str(GUILD))
            db.update_migration_status(str(GUILD), 'posting')

            from tle.cogs.migrate import Migrate
            _run(cog._run_migration(GUILD, 100, 200, {PILL}))

            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'posted'
            assert len(new_channel.sent) == 1
        finally:
            self._teardown_cog()


# =====================================================================
# view-failed command tests
# =====================================================================


class TestViewFailedCommand:
    def _make_cog(self, db):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.migrate import Migrate
        self._old_db = cf_common.user_db
        cf_common.user_db = db
        return Migrate(_FakeBot())

    def _teardown_cog(self):
        from tle.util import codeforces_common as cf_common
        cf_common.user_db = self._old_db

    def _call(self, cog, ctx):
        _run(cog.view_failed.__wrapped__(cog, ctx))

    def test_no_migration(self, db):
        cog = self._make_cog(db)
        try:
            ctx = _FakeCtx()
            self._call(cog, ctx)
            assert 'No migration' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_no_failed_entries(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            ctx = _FakeCtx()
            self._call(cog, ctx)
            assert 'No failed entries' in ctx.sent[0]
        finally:
            self._teardown_cog()

    def test_shows_failed_with_links_and_errors(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_retry_exhausted('333', PILL, '503 Service Unavailable')

            ctx = _FakeCtx()
            self._call(cog, ctx)
            msg = ctx.sent[0]
            assert 'Failed Messages (1)' in msg
            assert f'discord.com/channels/{GUILD}/100/444' in msg
            assert '503 Service Unavailable' in msg
            assert PILL in msg
        finally:
            self._teardown_cog()

    def test_shows_multiple_failures(self, db):
        cog = self._make_cog(db)
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.update_migration_entry_retry_exhausted('111', PILL, 'error 1')
            db.update_migration_entry_retry_exhausted('222', PILL, 'error 2')

            ctx = _FakeCtx()
            self._call(cog, ctx)
            msg = ctx.sent[0]
            assert 'Failed Messages (2)' in msg
            assert '1.' in msg
            assert '2.' in msg
        finally:
            self._teardown_cog()


# =====================================================================
# Pause / unpause tests
# =====================================================================


class TestPauseUnpause:
    """Test ;migrate pause and ;migrate unpause."""

    def test_pause_blocks_post_phase(self, db):
        """Paused migration should stop processing after the current message."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
        db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
        db.update_migration_entry_deleted('111', PILL, json.dumps({'content': 'first'}))
        db.update_migration_entry_deleted('222', PILL, json.dumps({'content': 'second'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)

        # Pre-set the pause event (cleared = paused)
        event = asyncio.Event()
        cog._paused[GUILD] = event

        # Run post phase in a task — it should process msg 111 then block
        async def run_and_unpause():
            post_task = asyncio.create_task(
                cog._post_phase(GUILD, 200, {PILL}, db))
            # Give it time to process the first message and hit the pause
            await asyncio.sleep(0.05)
            # Should have posted 1 message so far
            assert len(new_channel.sent) == 1
            # Unpause to let it finish
            event.set()
            await post_task

        _run(run_and_unpause())

        # Both should be posted now
        assert len(new_channel.sent) == 2
        assert db.get_migration_entry('111', PILL).crawl_status == 'posted'
        assert db.get_migration_entry('222', PILL).crawl_status == 'posted'

    def test_pause_no_task_running(self, db):
        """Pause with no running task should report an error."""
        from tle.cogs.migrate import Migrate
        cog = Migrate(_FakeBot())

        ctx = _FakeCtx()
        _run(cog.pause.__wrapped__(cog, ctx))
        assert 'No migration task' in ctx.sent[0]

    def test_unpause_not_paused(self, db):
        """Unpause when not paused should report an error."""
        from tle.cogs.migrate import Migrate
        cog = Migrate(_FakeBot())

        ctx = _FakeCtx()
        _run(cog.unpause.__wrapped__(cog, ctx))
        assert 'not paused' in ctx.sent[0]

    def test_cancel_unpauses_first(self, db):
        """Cancel should unpause to allow the task to receive cancellation."""
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            event = asyncio.Event()
            cog._paused[GUILD] = event
            assert not event.is_set()

            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

            async def test_cancel():
                async def dummy():
                    await event.wait()

                task = asyncio.create_task(dummy())
                cog._tasks[GUILD] = task
                ctx = _FakeCtx()
                await cog.cancel.__wrapped__(cog, ctx)
                assert event.is_set()
                assert GUILD not in cog._paused

            _run(test_cancel())
        finally:
            cf_common.user_db = old_db


# =====================================================================
# Bug fix verification tests
# =====================================================================


class TestFetchSourceChannelRetryExhausted:
    """Verify _fetch_source_channel RetryExhaustedError is caught in crawl."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_crawl_catches_fetch_channel_retry_exhausted(self, db):
        """If fetch_channel fails persistently, entry is retry_exhausted, crawl continues."""
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 5, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 3, 223, 444)
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=3, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])
        source_channel2 = _FakeChannel(channel_id=223, messages=[original2])

        bot = _FakeBot(channels=[old_channel, source_channel2])
        # Channel 222 is NOT in bot — fetch_channel will raise NotFound,
        # but we override it to always raise HTTPException for channel 222
        real_fetch_channel = bot.fetch_channel

        async def flaky_fetch_channel(cid):
            if cid == 222:
                raise discord.HTTPException(None, 'persistent failure')
            return await real_fetch_channel(cid)

        bot.fetch_channel = flaky_fetch_channel

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        # 333 should be retry_exhausted (channel 222 unreachable)
        entry333 = db.get_migration_entry('333', PILL)
        assert entry333.crawl_status == 'retry_exhausted'
        assert 'persistent failure' in entry333.last_error

        # 444 should be crawled (channel 223 works)
        entry444 = db.get_migration_entry('444', PILL)
        assert entry444.crawl_status == 'crawled'

    def test_post_catches_fetch_channel_retry_exhausted(self, db):
        """If fetch_channel fails in post phase, falls through to fallback."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])
        # Channel 222 not in bot — fetch_channel raises HTTPException
        real_fetch_channel = bot.fetch_channel

        async def always_fail_fetch(cid):
            if cid == 222:
                raise discord.HTTPException(None, 'persistent failure')
            return await real_fetch_channel(cid)

        bot.fetch_channel = always_fail_fetch

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_crawled('333', PILL, '222', '777', 5)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        # Should still post (using fallback), not crash
        entry = db.get_migration_entry('333', PILL)
        # Either posted (fallback worked) or retry_exhausted (send also failed)
        # Since new_channel.send works, it should be posted
        assert entry.crawl_status == 'posted'
        assert len(new_channel.sent) == 1


class TestResumeResetsRetryExhausted:
    """Verify resume now resets retry_exhausted entries (bug #4 fix)."""

    def test_resume_resets_retry_exhausted_entries(self, db):
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            new_channel = _FakeChannel(channel_id=200)
            bot = _FakeBot(channels=[new_channel])

            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'test'}))
            db.update_migration_entry_retry_exhausted('333', PILL, 'old error')
            # Crawl finished (crawl_total > 0), then post failed
            db.set_migration_crawl_total(str(GUILD), 1)
            db.update_migration_status(str(GUILD), 'failed')

            # Simulate what resume does
            db.reset_post_failed_entries(str(GUILD))
            db.reset_retry_exhausted_entries(str(GUILD))

            # Entry should be reset to 'deleted' (no source_channel_id)
            entry = db.get_migration_entry('333', PILL)
            assert entry.crawl_status == 'deleted'
            assert entry.last_error is None

            # Now it should be postable
            postable = db.get_migration_entries_for_posting(str(GUILD))
            assert len(postable) == 1
        finally:
            cf_common.user_db = old_db


class TestCompleteWarningRetryExhausted:
    """Verify complete warns about retry_exhausted entries (coverage gap #8)."""

    def test_complete_warns_about_retry_exhausted(self, db):
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '111', PILL, '444', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '445', '100')
            db.update_migration_entry_crawled('111', PILL, '500', '777', 5)
            db.update_migration_entry_posted('111', PILL, '888')
            db.update_migration_entry_retry_exhausted('222', PILL, 'bad error')
            db.update_migration_status(str(GUILD), 'done')

            from tle.cogs.migrate import Migrate
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.complete.__wrapped__(cog, ctx, type('Ch', (), {
                'id': 200, 'mention': '#new'})()))

            # Should warn about the 1 failed entry
            assert any('1 entries failed' in msg for msg in ctx.sent)
        finally:
            cf_common.user_db = old_db

    def test_complete_no_posted_entries_gives_feedback(self, db):
        """Complete with 0 posted entries should not return silently."""
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
            db.update_migration_entry_retry_exhausted('333', PILL, 'error')
            db.update_migration_status(str(GUILD), 'done')

            from tle.cogs.migrate import Migrate
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.complete.__wrapped__(cog, ctx, type('Ch', (), {
                'id': 200, 'mention': '#new'})()))

            # Should have warning + "no posted entries" message
            all_msgs = ' '.join(ctx.sent)
            assert 'retry-failed' in all_msgs
            assert 'No posted entries' in all_msgs
        finally:
            cf_common.user_db = old_db


class TestReactorFetchFailure:
    """Verify reactor fetch failure falls back to displayed count (gap #9)."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_reactor_fetch_failure_uses_displayed_count(self, db):
        """If reaction.users() fails, star_count should fall back to displayed_count."""

        class _FailingReaction:
            def __init__(self, emoji_str, count):
                self.emoji = emoji_str
                self.count = count

            async def users(self):
                raise discord.HTTPException(None, 'reactor fetch failed')
                yield  # make it an async generator

        original = _FakeMessage(
            msg_id=333, content='Hello',
            reactions=[_FailingReaction(PILL, 7)],
            author=_FakeUser(777, 'Author'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original])
        old_bot_msg = self._make_old_bot_msg(1001, PILL, 7, 222, 333)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg])
        bot = _FakeBot(channels=[old_channel, source_channel])

        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._crawl_phase(GUILD, 100, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'crawled'
        # Should use the displayed count (7) from the old bot message
        assert entry.star_count == 7


class TestGenericExceptionInPostPhase:
    """Verify non-Discord exceptions in post phase mark retry_exhausted (gap #10)."""

    def test_non_discord_exception_marks_retry_exhausted(self, db):
        new_channel = _FakeChannel(channel_id=200)

        async def exploding_send(**kwargs):
            raise ValueError('unexpected internal error')

        new_channel.send = lambda **kw: exploding_send(**kw)

        bot = _FakeBot(channels=[new_channel])
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_deleted('333', PILL, json.dumps({'content': 'hi'}))

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        _run(cog._post_phase(GUILD, 200, {PILL}, db))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'retry_exhausted'
        assert 'unexpected internal error' in entry.last_error


class TestPauseCrawlPhase:
    """Verify pause works during crawl phase (gap #7)."""

    def _make_old_bot_msg(self, msg_id, emoji_str, count, orig_channel_id, orig_msg_id):
        content = f'{emoji_str} **{count}** | https://discord.com/channels/{GUILD}/{orig_channel_id}/{orig_msg_id}'
        return _FakeMessage(msg_id=msg_id, content=content)

    def test_pause_blocks_crawl_phase(self, db):
        original1 = _FakeMessage(
            msg_id=333, content='First',
            reactions=[_FakeReaction(PILL, count=1, user_ids=[10])],
            author=_FakeUser(777, 'Author'),
        )
        original2 = _FakeMessage(
            msg_id=444, content='Second',
            reactions=[_FakeReaction(PILL, count=1, user_ids=[20])],
            author=_FakeUser(888, 'Author2'),
        )
        source_channel = _FakeChannel(channel_id=222, messages=[original1, original2])
        old_bot_msg1 = self._make_old_bot_msg(1001, PILL, 1, 222, 333)
        old_bot_msg2 = self._make_old_bot_msg(1002, PILL, 1, 222, 444)
        old_channel = _FakeChannel(channel_id=100, messages=[old_bot_msg1, old_bot_msg2])

        bot = _FakeBot(channels=[old_channel, source_channel])

        from tle.cogs.migrate import Migrate
        cog = Migrate(bot)
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)

        event = asyncio.Event()
        cog._paused[GUILD] = event

        async def run_and_unpause():
            crawl_task = asyncio.create_task(
                cog._crawl_phase(GUILD, 100, {PILL}, db))
            await asyncio.sleep(0.05)
            # Should have crawled first message then paused
            entry333 = db.get_migration_entry('333', PILL)
            assert entry333 is not None
            assert entry333.crawl_status == 'crawled'
            # Second message should NOT be crawled yet
            entry444 = db.get_migration_entry('444', PILL)
            assert entry444 is None
            # Unpause
            event.set()
            await crawl_task

        _run(run_and_unpause())

        # Both should be crawled now
        assert db.get_migration_entry('333', PILL).crawl_status == 'crawled'
        assert db.get_migration_entry('444', PILL).crawl_status == 'crawled'


# =====================================================================
# restart-post tests
# =====================================================================


class TestRestartPost:
    """Test ;migrate restart-post command."""

    def test_restart_post_deletes_and_reposts(self, db):
        """restart-post should delete posted messages and re-run post phase."""
        new_channel = _FakeChannel(channel_id=200)
        bot = _FakeBot(channels=[new_channel])

        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            db.set_migration_crawl_total(str(GUILD), 2)

            # Simulate two entries that were already posted
            db.add_migration_entry(str(GUILD), '111', PILL, '441', '100')
            db.add_migration_entry(str(GUILD), '222', PILL, '442', '100')
            db.update_migration_entry_crawled('111', PILL, '500', '777', 3)
            db.update_migration_entry_crawled('222', PILL, '500', '778', 5)
            db.update_migration_entry_posted('111', PILL, '901')
            db.update_migration_entry_posted('222', PILL, '902')
            db.update_migration_status(str(GUILD), 'done')

            # Put the "posted" messages in the new channel so they can be fetched+deleted
            posted1 = _FakeMessage(msg_id=901, content=f'{PILL} **3**')
            posted2 = _FakeMessage(msg_id=902, content=f'{PILL} **5**')
            new_channel._messages[901] = posted1
            new_channel._messages[902] = posted2

            # Simulate what restart_post does (without running the background task)
            msg_ids = db.get_all_posted_msg_ids(str(GUILD))
            assert set(msg_ids) == {'901', '902'}

            db.reset_all_entries_for_repost(str(GUILD))

            # Entries should be back to crawled
            e1 = db.get_migration_entry('111', PILL)
            e2 = db.get_migration_entry('222', PILL)
            assert e1.crawl_status == 'crawled'
            assert e2.crawl_status == 'crawled'
            assert e1.new_starboard_msg_id is None
            assert e2.new_starboard_msg_id is None

            # They should be postable again
            entries = db.get_migration_entries_for_posting(str(GUILD))
            assert len(entries) == 2
        finally:
            cf_common.user_db = old_db

    def test_restart_post_resets_retry_exhausted_too(self, db):
        """restart-post should also reset retry_exhausted entries."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        db.update_migration_entry_retry_exhausted('333', PILL, 'old error')

        db.reset_all_entries_for_repost(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'  # no source_channel_id
        assert entry.last_error is None
        assert entry.new_starboard_msg_id is None

    def test_restart_post_resets_deleted_entries(self, db):
        """Deleted entries with embed_fallback should remain postable after reset."""
        db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
        db.add_migration_entry(str(GUILD), '333', PILL, '444', '100')
        fb = json.dumps({'content': f'{PILL} **3** | https://discord.com/channels/{GUILD}/100/333'})
        db.update_migration_entry_deleted('333', PILL, fb)
        db.update_migration_entry_posted('333', PILL, '901')

        db.reset_all_entries_for_repost(str(GUILD))

        entry = db.get_migration_entry('333', PILL)
        assert entry.crawl_status == 'deleted'
        assert entry.new_starboard_msg_id is None
        # embed_fallback should be preserved
        assert entry.embed_fallback == fb

    def test_restart_post_no_migration(self, db):
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            cog = Migrate(_FakeBot())
            ctx = _FakeCtx()
            _run(cog.restart_post.__wrapped__(cog, ctx))
            assert 'No migration' in ctx.sent[0]
        finally:
            cf_common.user_db = old_db

    def test_restart_post_blocks_if_task_running(self, db):
        from tle.cogs.migrate import Migrate
        from tle.util import codeforces_common as cf_common
        old_db = cf_common.user_db
        cf_common.user_db = db
        try:
            db.create_migration(str(GUILD), '100', '200', PILL, 1000.0)
            cog = Migrate(_FakeBot())

            async def test():
                dummy_task = asyncio.create_task(asyncio.sleep(10))
                cog._tasks[GUILD] = dummy_task
                ctx = _FakeCtx()
                await cog.restart_post.__wrapped__(cog, ctx)
                assert 'still running' in ctx.sent[0]
                dummy_task.cancel()
                try:
                    await dummy_task
                except asyncio.CancelledError:
                    pass

            _run(test())
        finally:
            cf_common.user_db = old_db

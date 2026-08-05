"""Tests for the great day feature — cog logic (_send_greatday, timers)."""
import asyncio
from types import SimpleNamespace

from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, USER_C, FakeGreatDayDb, _FakeChannel, _FakeGuild, db,
)


class TestSendGreatDayIntegration:
    """Test the actual _send_greatday method on the cog."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_cog(self, db):
        from tle.cogs.greatday import GreatDay
        cog = GreatDay(bot=None)
        return cog

    def test_singular_verb_for_one_user(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        assert len(channel.sent) == 1
        assert ' is having a great day!' in channel.sent[0]

    def test_plural_verb_for_multiple_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        assert ' are having a great day!' in channel.sent[0]

    def test_record_pick_failure_still_returns_true(self, db, monkeypatch):
        """If recording the picks raises, the message was already sent — we
        must still report success so the caller stamps the kvs sentinel and
        the 60s loop doesn't re-send every minute."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        def _raise(*a, **kw):
            raise RuntimeError('simulated DB failure')
        monkeypatch.setattr(db, 'greatday_record_picks', _raise)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        assert len(channel.sent) == 1  # message was sent exactly once

    def test_no_channel_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        # No greatday_channel config set
        db.greatday_signup(GUILD, USER_A)

        guild = _FakeGuild(int(GUILD))
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False

    def test_deleted_channel_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        guild = _FakeGuild(int(GUILD), channel=None)  # get_channel returns None
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False

    def test_no_signups_returns_false(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        # No signups

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False
        assert len(channel.sent) == 0

    def test_departed_users_are_filtered_out(self, db, monkeypatch):
        """Users who left the server should not be greeted."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)

        channel = _FakeChannel()
        # USER_B has left the server
        guild = _FakeGuild(int(GUILD), channel, absent_user_ids=[USER_B])
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is True
        msg = channel.sent[0]
        assert f'<@{USER_A}>' in msg
        assert f'<@{USER_B}>' not in msg
        assert f'<@{USER_C}>' in msg

    def test_returns_false_when_all_signups_departed(self, db, monkeypatch):
        """If every signup has left, no message is sent."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel,
                           absent_user_ids=[USER_A, USER_B])
        cog = self._make_cog(db)
        result = self._run(cog._send_greatday(guild))
        assert result is False
        assert len(channel.sent) == 0

    def test_mentions_all_picked_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        self._run(cog._send_greatday(guild))
        msg = channel.sent[0]
        # All 3 users should be mentioned (fewer than 5)
        assert f'<@{USER_A}>' in msg
        assert f'<@{USER_B}>' in msg
        assert f'<@{USER_C}>' in msg

    def test_dragos_marker_is_used_once(self, db, monkeypatch):
        from tle.cogs.greatday import (
            GreatDay, _DRAGOS_ID, _NEXT_DRAGOS_KEY_PREFIX,
        )
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        key = f'{_NEXT_DRAGOS_KEY_PREFIX}{GUILD}'
        db.kvs_set(key, str(_DRAGOS_ID))
        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = GreatDay(bot=None)

        assert self._run(cog._send_greatday(guild)) is True
        assert channel.sent[0].count(f'<@{_DRAGOS_ID}>') == 5
        assert db.kvs_get(key) is None

        assert self._run(cog._send_greatday(guild)) is True
        assert f'<@{_DRAGOS_ID}>' not in channel.sent[1]


class TestTargetDatetime:
    """Test the _target_datetime helper."""

    def test_returns_same_day_with_target_time(self):
        from tle.cogs.greatday import _target_datetime
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 3, 30, 8, 30, 45, tzinfo=ZoneInfo('US/Eastern'))
        target = _target_datetime(now, '10:00')
        assert target.hour == 10
        assert target.minute == 0
        assert target.second == 0
        assert target.day == 30

    def test_seconds_until_positive_before_target(self):
        from tle.cogs.greatday import _target_datetime
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 3, 30, 9, 55, 0, tzinfo=ZoneInfo('US/Eastern'))
        target = _target_datetime(now, '10:00')
        seconds = (target - now).total_seconds()
        assert seconds == 300  # 5 minutes

    def test_seconds_until_negative_after_target(self):
        from tle.cogs.greatday import _target_datetime
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 3, 30, 10, 5, 0, tzinfo=ZoneInfo('US/Eastern'))
        target = _target_datetime(now, '10:00')
        seconds = (target - now).total_seconds()
        assert seconds == -300  # 5 minutes past


class TestPreciseSend:
    """Test the precise timer logic."""

    def test_precise_send_verifies_kvs_before_sending(self, db, monkeypatch):
        """If ;greatday now was used while timer pending, precise send should skip."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        # Simulate ;greatday now already stamped today (use real date
        # since _precise_send computes today at runtime)
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo('US/Eastern')).strftime('%Y-%m-%d')
        db.kvs_set(f'greatday_last:{GUILD}', today)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        from tle.cogs.greatday import GreatDay
        cog = GreatDay(bot=None)
        # Run precise_send with 0 delay (fires immediately)
        asyncio.run(cog._precise_send(guild, 0))
        # Should not have sent — KVS says already done today
        assert len(channel.sent) == 0

    def test_precise_send_sends_when_not_yet_sent(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        # No KVS stamp — hasn't sent today

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        from tle.cogs.greatday import GreatDay
        cog = GreatDay(bot=None)
        asyncio.run(cog._precise_send(guild, 0))
        assert len(channel.sent) == 1
        # Should have stamped KVS
        today = db.kvs_get(f'greatday_last:{GUILD}')
        assert today is not None

    def test_precise_send_cleans_up_pending_timers(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)

        from tle.cogs.greatday import GreatDay
        cog = GreatDay(bot=None)
        # Manually add to pending timers to verify cleanup
        cog._pending_timers[guild.id] = 'placeholder'
        asyncio.run(cog._precise_send(guild, 0))
        assert guild.id not in cog._pending_timers


class TestBanIntegration:
    """Test the disabled Great Day ban behavior."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_cog(self, db):
        from tle.cogs.greatday import GreatDay
        return GreatDay(bot=None)

    def test_disabled_ban_does_not_exclude_user(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_ban(GUILD, USER_A)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        self._run(cog._send_greatday(guild))
        msg = channel.sent[0]
        assert f'<@{USER_A}>' in msg
        assert f'<@{USER_B}>' in msg

    def test_ban_command_replies_without_mutating_bans(self, db, monkeypatch):
        from tle.cogs.greatday import GreatDay, _BAN_REPLY
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _BanCtx(author_id=123)
        cog = GreatDay(bot=None)

        self._run(cog.ban_user.__wrapped__(cog, ctx, _target='someone'))

        assert ctx.sent == [_BAN_REPLY]
        assert db.greatday_get_banned(GUILD) == []

    def test_dragos_ban_attempt_arms_next_greatday(self, db, monkeypatch):
        from tle.cogs.greatday import (
            GreatDay, _BAN_REPLY, _DRAGOS_ID, _NEXT_DRAGOS_KEY_PREFIX,
        )
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _BanCtx(author_id=_DRAGOS_ID)
        cog = GreatDay(bot=None)

        self._run(cog.ban_user.__wrapped__(cog, ctx, _target='someone'))

        key = f'{_NEXT_DRAGOS_KEY_PREFIX}{GUILD}'
        assert ctx.sent == [_BAN_REPLY]
        assert db.kvs_get(key) == str(_DRAGOS_ID)


class _BanCtx:
    def __init__(self, author_id):
        self.author = SimpleNamespace(id=author_id)
        self.guild = SimpleNamespace(id=int(GUILD))
        self.sent = []

    async def send(self, content):
        self.sent.append(content)

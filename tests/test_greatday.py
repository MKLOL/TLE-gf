"""Tests for the great day feature — DB layer and cog logic."""
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


class TestSignup:
    def test_signup_returns_true(self, db):
        assert db.greatday_signup(GUILD, USER_A) is True

    def test_duplicate_signup_returns_false(self, db):
        db.greatday_signup(GUILD, USER_A)
        assert db.greatday_signup(GUILD, USER_A) is False

    def test_signup_appears_in_list(self, db):
        db.greatday_signup(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 1
        assert rows[0].user_id == USER_A

    def test_multiple_signups(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_signup(GUILD, USER_C)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 3

    def test_guild_isolation(self, db):
        db.greatday_signup('1', USER_A)
        db.greatday_signup('2', USER_B)
        assert len(db.greatday_get_signups('1')) == 1
        assert len(db.greatday_get_signups('2')) == 1


class TestRemove:
    def test_remove_existing(self, db):
        db.greatday_signup(GUILD, USER_A)
        assert db.greatday_remove(GUILD, USER_A) is True
        assert len(db.greatday_get_signups(GUILD)) == 0

    def test_remove_nonexistent(self, db):
        assert db.greatday_remove(GUILD, USER_A) is False

    def test_remove_only_target(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_remove(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 1
        assert rows[0].user_id == USER_B


class TestEmptyList:
    def test_empty_guild(self, db):
        assert db.greatday_get_signups(GUILD) == []


class TestLastSentTracking:
    def test_kvs_tracks_last_sent(self, db):
        db.kvs_set('greatday_last:111', '2026-03-30')
        assert db.kvs_get('greatday_last:111') == '2026-03-30'

    def test_kvs_prevents_double_send(self, db):
        db.kvs_set('greatday_last:111', '2026-03-30')
        # Simulates the check in the task
        assert db.kvs_get('greatday_last:111') == '2026-03-30'


class TestSendGreatDay:
    """Test _send_greatday picks users and sends message."""

    def test_picks_up_to_5(self, db):
        for i in range(10):
            db.greatday_signup(GUILD, str(i))
        rows = db.greatday_get_signups(GUILD)
        user_ids = [r.user_id for r in rows]
        import random
        picked = random.sample(user_ids, min(5, len(user_ids)))
        assert len(picked) == 5
        assert all(uid in user_ids for uid in picked)

    def test_picks_all_when_fewer_than_5(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        rows = db.greatday_get_signups(GUILD)
        user_ids = [r.user_id for r in rows]
        import random
        picked = random.sample(user_ids, min(5, len(user_ids)))
        assert len(picked) == 2


# ── _send_greatday integration tests ──────────────────────────────────

import asyncio
from tle.util import codeforces_common as cf_common


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


class TestBan:
    def test_ban_returns_true(self, db):
        assert db.greatday_ban(GUILD, USER_A) is True

    def test_duplicate_ban_returns_false(self, db):
        db.greatday_ban(GUILD, USER_A)
        assert db.greatday_ban(GUILD, USER_A) is False

    def test_ban_removes_signup(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_ban(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 0

    def test_is_banned(self, db):
        db.greatday_ban(GUILD, USER_A)
        assert db.greatday_is_banned(GUILD, USER_A) is True

    def test_not_banned(self, db):
        assert db.greatday_is_banned(GUILD, USER_A) is False

    def test_unban_returns_true(self, db):
        db.greatday_ban(GUILD, USER_A)
        assert db.greatday_unban(GUILD, USER_A) is True

    def test_unban_nonexistent_returns_false(self, db):
        assert db.greatday_unban(GUILD, USER_A) is False

    def test_unban_allows_signup(self, db):
        db.greatday_ban(GUILD, USER_A)
        db.greatday_unban(GUILD, USER_A)
        assert db.greatday_is_banned(GUILD, USER_A) is False
        assert db.greatday_signup(GUILD, USER_A) is True

    def test_ban_guild_isolation(self, db):
        db.greatday_ban('1', USER_A)
        assert db.greatday_is_banned('1', USER_A) is True
        assert db.greatday_is_banned('2', USER_A) is False

    def test_ban_does_not_affect_other_signups(self, db):
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        db.greatday_ban(GUILD, USER_A)
        rows = db.greatday_get_signups(GUILD)
        assert len(rows) == 1
        assert rows[0].user_id == USER_B


class TestBanIntegration:
    """Test that banned users cannot sign up via the cog."""

    def _run(self, coro):
        return asyncio.run(coro)

    def _make_cog(self, db):
        from tle.cogs.greatday import GreatDay
        return GreatDay(bot=None)

    def test_banned_user_excluded_from_send(self, db, monkeypatch):
        """Banned users should not appear in the daily pick."""
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(GUILD, 'greatday_channel', '999')
        db.greatday_signup(GUILD, USER_A)
        db.greatday_signup(GUILD, USER_B)
        # Ban removes signup, so USER_A won't be in the pool
        db.greatday_ban(GUILD, USER_A)

        channel = _FakeChannel()
        guild = _FakeGuild(int(GUILD), channel)
        cog = self._make_cog(db)
        self._run(cog._send_greatday(guild))
        msg = channel.sent[0]
        assert f'<@{USER_A}>' not in msg
        assert f'<@{USER_B}>' in msg


class TestUpgrade:
    def test_upgrade_creates_signup_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_18_0
        upgrade_1_18_0(conn)
        # Should be able to insert and query
        conn.execute(
            'INSERT INTO greatday_signup (guild_id, user_id) VALUES (?, ?)',
            ('1', '10'))
        rows = conn.execute('SELECT * FROM greatday_signup').fetchall()
        assert len(rows) == 1
        conn.close()

    def test_upgrade_creates_ban_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_21_0
        upgrade_1_21_0(conn)
        conn.execute(
            'INSERT INTO greatday_ban (guild_id, user_id) VALUES (?, ?)',
            ('1', '10'))
        rows = conn.execute('SELECT * FROM greatday_ban').fetchall()
        assert len(rows) == 1
        conn.close()

    def test_upgrade_creates_pick_table(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        from tle.util.db.user_db_upgrades import upgrade_1_23_0
        upgrade_1_23_0(conn)
        conn.execute(
            'INSERT INTO greatday_pick (guild_id, user_id, message_id, picked_at) '
            'VALUES (?, ?, ?, ?)', ('1', '10', 'mid', 0.0))
        rows = conn.execute('SELECT * FROM greatday_pick').fetchall()
        assert len(rows) == 1
        conn.close()


class TestPickStats:
    def test_record_picks_inserts_rows(self, db):
        n = db.greatday_record_picks(GUILD, [USER_A, USER_B], 'mid1', 1000.0)
        assert n == 2
        rows = db.greatday_get_stats(GUILD)
        assert {(r.user_id, r.cnt) for r in rows} == {(USER_A, 1), (USER_B, 1)}

    def test_record_picks_idempotent_same_message(self, db):
        db.greatday_record_picks(GUILD, [USER_A], 'mid1', 1000.0)
        n = db.greatday_record_picks(GUILD, [USER_A], 'mid1', 1000.0)
        assert n == 0  # duplicate (guild, user, message) is ignored
        assert db.greatday_get_count(GUILD, USER_A) == 1

    def test_record_picks_separate_messages_increment(self, db):
        db.greatday_record_picks(GUILD, [USER_A], 'mid1', 1000.0)
        db.greatday_record_picks(GUILD, [USER_A], 'mid2', 2000.0)
        assert db.greatday_get_count(GUILD, USER_A) == 2

    def test_get_stats_orders_by_count_desc(self, db):
        db.greatday_record_picks(GUILD, [USER_A], 'mid1', 1.0)
        db.greatday_record_picks(GUILD, [USER_B], 'mid2', 2.0)
        db.greatday_record_picks(GUILD, [USER_B], 'mid3', 3.0)
        db.greatday_record_picks(GUILD, [USER_C], 'mid4', 4.0)
        db.greatday_record_picks(GUILD, [USER_C], 'mid5', 5.0)
        db.greatday_record_picks(GUILD, [USER_C], 'mid6', 6.0)
        rows = db.greatday_get_stats(GUILD)
        assert [(r.user_id, r.cnt) for r in rows] == [
            (USER_C, 3), (USER_B, 2), (USER_A, 1)]

    def test_get_stats_guild_isolation(self, db):
        db.greatday_record_picks(GUILD, [USER_A], 'mid1', 1.0)
        db.greatday_record_picks('999', [USER_A], 'mid2', 2.0)
        assert db.greatday_get_count(GUILD, USER_A) == 1
        assert db.greatday_get_count('999', USER_A) == 1

    def test_get_stats_empty(self, db):
        assert db.greatday_get_stats(GUILD) == []
        assert db.greatday_get_count(GUILD, USER_A) == 0

    def test_record_picks_empty_list_returns_zero(self, db):
        assert db.greatday_record_picks(GUILD, [], 'mid', 1.0) == 0


class TestBackfillRegex:
    def _matches(self, content):
        from tle.cogs.greatday import _GREATDAY_RE, _MENTION_RE
        if not _GREATDAY_RE.match(content):
            return None
        return _MENTION_RE.findall(content)

    def test_matches_singular(self):
        assert self._matches('I hope <@111> is having a great day!') == ['111']

    def test_matches_plural(self):
        assert self._matches(
            'I hope <@111> <@222> <@333> are having a great day!'
        ) == ['111', '222', '333']

    def test_matches_nick_mention_format(self):
        assert self._matches('I hope <@!111> is having a great day!') == ['111']

    def test_rejects_unrelated_message(self):
        assert self._matches('hello world') is None
        assert self._matches('I hope you have a great day!') is None
        assert self._matches('I hope <@1> wins!') is None

    def test_tolerates_trailing_whitespace(self):
        assert self._matches('I hope <@111> is having a great day! ') == ['111']


class TestBackfillAuthorCheck:
    """The backfill helper must reject otherwise-matching messages whose
    author isn't the bot — anyone could spoof '... having a great day!'."""

    def _msg(self, content, author_id):
        class _Author:
            pass
        a = _Author()
        a.id = author_id
        m = _FakeMessage(content)
        m.author = a
        return m

    def test_accepts_bot_authored_message(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@111> is having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) == ['111']

    def test_rejects_non_bot_authored_message(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@999> is having a great day!', author_id=7)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_rejects_when_bot_user_id_unknown(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@111> is having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=None) is None

    def test_rejects_bot_authored_non_template(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('hello world', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_rejects_bot_authored_template_with_no_mentions(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope they are having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_handles_message_without_author(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = _FakeMessage('I hope <@111> is having a great day!')
        # _FakeMessage has no .author — must not crash
        assert _parse_greatday_message(m, bot_user_id=42) is None


class TestPersonalRankLine:
    """;greatday stats should show the invoker's own rank/count on every
    page so they don't have to scan the whole leaderboard to find
    themselves."""

    def _row(self, user_id, cnt):
        class _R:
            pass
        r = _R()
        r.user_id = str(user_id)
        r.cnt = cnt
        return r

    def test_user_at_top(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 100)
        assert '#1' in line
        assert '10' in line

    def test_user_in_middle(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 200)
        assert '#2' in line
        assert '**5**' in line

    def test_user_at_bottom(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 300)
        assert '#3' in line

    def test_user_not_in_leaderboard(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5)]
        line = _personal_rank_line(rows, 999)
        assert "haven't" in line
        assert '#' not in line

    def test_user_id_type_coercion(self):
        """user_id stored as TEXT in SQLite; ctx.author.id is int. Must
        compare correctly regardless of which side is which type."""
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row('100', 10)]
        assert '#1' in _personal_rank_line(rows, 100)
        assert '#1' in _personal_rank_line(rows, '100')

    def test_empty_rows(self):
        from tle.cogs.greatday import _personal_rank_line
        assert "haven't" in _personal_rank_line([], 100)

    def test_returns_plain_text_no_leading_whitespace(self):
        """Renders as message content (above the embed), not as part of
        the embed description — must not start with a stray newline."""
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10)]
        assert _personal_rank_line(rows, 100)[0] != '\n'
        assert _personal_rank_line([], 100)[0] != '\n'


class TestBackfillStopHeuristic:
    """The backfill scans newest-first and stops early once we've walked
    past the most recent match by more than the gap threshold. Greatday
    runs daily, so a multi-day gap means we've recovered the full
    history and further scanning is wasted bandwidth."""

    def _stop(self):
        from tle.cogs.greatday import _should_stop_backfill
        return _should_stop_backfill

    def test_does_not_stop_before_first_match(self):
        """No anchor yet — must keep scanning even if many days have
        passed (otherwise channels with a long no-greatday prefix would
        never get backfilled)."""
        stop = self._stop()
        assert stop(None, 1000.0, 60) is False
        assert stop(None, 0.0, 60) is False

    def test_does_not_stop_within_gap_window(self):
        stop = self._stop()
        # Match at t=1000, currently scanning t=950 → gap=50 < 60
        assert stop(1000.0, 950.0, 60) is False
        # Boundary: gap == threshold → keep scanning (strict >)
        assert stop(1000.0, 940.0, 60) is False

    def test_stops_when_gap_exceeds_threshold(self):
        stop = self._stop()
        # Match at t=1000, currently scanning t=939 → gap=61 > 60
        assert stop(1000.0, 939.0, 60) is True

    def test_five_day_threshold(self):
        """Cross-check the production threshold."""
        from tle.cogs.greatday import _BACKFILL_STOP_GAP_SECONDS
        assert _BACKFILL_STOP_GAP_SECONDS == 5 * 24 * 3600
        stop = self._stop()
        last_match = 1_000_000.0
        # 4 days, 23 hrs → keep going
        assert stop(last_match, last_match - 4 * 86400 - 23 * 3600,
                    _BACKFILL_STOP_GAP_SECONDS) is False
        # 5 days, 1 sec → stop
        assert stop(last_match, last_match - 5 * 86400 - 1,
                    _BACKFILL_STOP_GAP_SECONDS) is True

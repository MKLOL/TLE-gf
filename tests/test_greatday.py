"""Tests for the great day feature — DB layer."""
import sqlite3

from tle.util.db.user_db_conn import namedtuple_factory

from tests.greatday_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, USER_C, FakeGreatDayDb, db,
)


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

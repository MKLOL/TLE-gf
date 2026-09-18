"""Tests for the LLM key/quota DB layer (``tle/util/db/llm_db.py``)."""
import sqlite3

import pytest

from tle.util.db.llm_db import LlmDbMixin, key_fingerprint
from tle.util.db.user_db_conn import namedtuple_factory
from tests.llm_test_utils import FakeLlmDb


@pytest.fixture
def db():
    return FakeLlmDb()


class TestKeyStorage:
    def test_add_key_returns_added_and_is_retrievable(self, db):
        assert db.llm_add_key('AIzaSyExampleKeyValue123', label='proj-a') == 'added'
        rows = db.llm_get_keys()
        assert len(rows) == 1
        assert rows[0].api_key == 'AIzaSyExampleKeyValue123'
        assert rows[0].label == 'proj-a'
        assert rows[0].active == 1

    def test_same_key_twice_is_a_duplicate_not_a_second_row(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123')
        assert db.llm_add_key('AIzaSyExampleKeyValue123') == 'duplicate'
        assert len(db.llm_get_keys()) == 1

    def test_surrounding_whitespace_does_not_create_a_duplicate(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123')
        assert db.llm_add_key('  AIzaSyExampleKeyValue123  ') == 'duplicate'
        assert len(db.llm_get_keys()) == 1

    def test_readding_a_forgotten_key_reactivates_the_same_row(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123', label='old')
        key_id = db.llm_get_keys()[0].id
        db.llm_forget_key(key_id)
        assert db.llm_get_keys() == []

        assert db.llm_add_key('AIzaSyExampleKeyValue123', label='new') == 'reactivated'
        rows = db.llm_get_keys()
        assert len(rows) == 1
        assert rows[0].id == key_id  # same row, not a second one
        assert rows[0].label == 'new'

    def test_forget_returns_false_for_unknown_or_already_inactive(self, db):
        assert db.llm_forget_key(999) is False
        db.llm_add_key('AIzaSyExampleKeyValue123')
        key_id = db.llm_get_keys()[0].id
        assert db.llm_forget_key(key_id) is True
        assert db.llm_forget_key(key_id) is False

    def test_inactive_keys_are_visible_only_when_asked_for(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123')
        db.llm_forget_key(db.llm_get_keys()[0].id)
        assert db.llm_get_keys(active_only=True) == []
        assert len(db.llm_get_keys(active_only=False)) == 1

    def test_discord_ids_are_stored_as_text(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123', guild_id=12345, added_by=678)
        row = db.llm_get_keys()[0]
        assert row.guild_id == '12345'
        assert row.added_by == '678'

    def test_fingerprint_is_stable_and_not_the_key(self, db):
        fp = key_fingerprint('AIzaSyExampleKeyValue123')
        assert fp == key_fingerprint('  AIzaSyExampleKeyValue123 ')
        assert 'AIzaSy' not in fp

    def test_provider_defaults_to_gemini_and_filters_xai(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue123')
        db.llm_add_key('xai-ExampleKeyValue123456', provider='xai')

        gemini = db.llm_get_keys()
        xai = db.llm_get_keys(provider='xai')
        assert [(row.provider, row.api_key) for row in gemini] == [
            ('gemini', 'AIzaSyExampleKeyValue123')]
        assert [(row.provider, row.api_key) for row in xai] == [
            ('xai', 'xai-ExampleKeyValue123456')]

    def test_same_key_cannot_cross_provider_boundaries(self, db):
        key = 'xai-ExampleKeyValue123456'
        assert db.llm_add_key(key, provider='xai') == 'added'
        assert db.llm_add_key(key, provider='gemini') == 'provider_conflict'
        assert db.llm_get_keys(provider='gemini') == []
        assert len(db.llm_get_keys(provider='xai')) == 1

    def test_forget_is_scoped_to_the_provider(self, db):
        db.llm_add_key('xai-ExampleKeyValue123456', provider='xai')
        key_id = db.llm_get_keys(provider='xai')[0].id

        assert db.llm_forget_key(key_id, provider='gemini') is False
        assert len(db.llm_get_keys(provider='xai')) == 1
        assert db.llm_forget_key(key_id, provider='xai') is True
        assert db.llm_get_keys(provider='xai') == []

    def test_provider_is_normalized_and_validated(self, db):
        db.llm_add_key('xai-ExampleKeyValue123456', provider=' XAI ')
        assert db.llm_get_keys(provider='xai')[0].provider == 'xai'
        with pytest.raises(ValueError, match='Unsupported LLM key provider'):
            db.llm_get_keys(provider='openai')


class TestBuckets:
    def test_exhausted_bucket_is_returned_until_it_expires(self, db):
        db.llm_set_bucket_exhausted(1, 'model-a', until=500.0)
        assert len(db.llm_get_buckets(now=100.0)) == 1
        assert db.llm_get_buckets(now=600.0) == []

    def test_buckets_are_independent_per_model(self, db):
        db.llm_set_bucket_exhausted(1, 'model-a', until=500.0)
        buckets = db.llm_get_buckets(now=100.0)
        assert [(b.key_id, b.model) for b in buckets] == [(1, 'model-a')]

    def test_setting_the_same_bucket_twice_updates_in_place(self, db):
        db.llm_set_bucket_exhausted(1, 'model-a', until=500.0)
        db.llm_set_bucket_exhausted(1, 'model-a', until=900.0, last_error='again')
        buckets = db.llm_get_buckets(now=100.0)
        assert len(buckets) == 1
        assert buckets[0].exhausted_until == 900.0
        assert buckets[0].last_error == 'again'

    def test_clear_bucket_removes_it(self, db):
        db.llm_set_bucket_exhausted(1, 'model-a', until=500.0)
        db.llm_clear_bucket(1, 'model-a')
        assert db.llm_get_buckets(now=100.0) == []

    def test_purge_expired_keeps_live_buckets(self, db):
        db.llm_set_bucket_exhausted(1, 'model-a', until=100.0)
        db.llm_set_bucket_exhausted(2, 'model-a', until=900.0)
        assert db.llm_purge_expired_buckets(now=500.0) == 1
        remaining = db.llm_get_buckets(now=500.0)
        assert [b.key_id for b in remaining] == [2]


class TestUsage:
    def test_usage_starts_at_zero(self, db):
        assert db.llm_get_usage(1, 2, '2026-07-30') == 0

    def test_bump_increments_and_returns_the_new_count(self, db):
        assert db.llm_bump_usage(1, 2, '2026-07-30') == 1
        assert db.llm_bump_usage(1, 2, '2026-07-30') == 2
        assert db.llm_get_usage(1, 2, '2026-07-30') == 2

    def test_usage_is_scoped_per_user_guild_and_day(self, db):
        db.llm_bump_usage(1, 2, '2026-07-30')
        assert db.llm_get_usage(1, 3, '2026-07-30') == 0   # other user
        assert db.llm_get_usage(9, 2, '2026-07-30') == 0   # other guild
        assert db.llm_get_usage(1, 2, '2026-07-31') == 0   # next day

    def test_purge_old_usage_keeps_the_current_day(self, db):
        db.llm_bump_usage(1, 2, '2026-07-28')
        db.llm_bump_usage(1, 2, '2026-07-30')
        assert db.llm_purge_old_usage('2026-07-30') == 1
        assert db.llm_get_usage(1, 2, '2026-07-30') == 1


class TestXaiRequestLimits:
    USER_LIMIT = 15
    WINDOW = 60 * 60
    DAILY_LIMIT = 200

    def reserve(self, db, user_id, now):
        return db.llm_reserve_xai_request(
            user_id, self.USER_LIMIT, self.WINDOW, self.DAILY_LIMIT, now=now)

    @staticmethod
    def event_count(db):
        return db.conn.execute(
            'SELECT COUNT(*) AS count FROM llm_xai_request'
        ).fetchone().count

    def test_fifteenth_request_is_accepted_and_next_is_rejected(self, db):
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=1_000) is None

        denial = self.reserve(db, 7, now=1_000)
        assert isinstance(denial, str) and denial == 'user'
        assert denial.retry_at == 1_000 + self.WINDOW
        assert self.event_count(db) == self.USER_LIMIT

    def test_retry_handles_more_active_rows_than_the_new_limit(self, db):
        for offset in range(25):
            assert db.llm_reserve_xai_request(
                7, 100, self.WINDOW, self.DAILY_LIMIT,
                now=1_000 + offset) is None

        denial = self.reserve(db, 7, now=1_025)
        assert denial == 'user'
        assert denial.retry_at == 1_010 + self.WINDOW

    def test_user_limit_bypass_still_enforces_shared_daily_limit(self, db):
        kwargs = dict(
            user_limit=1, window_seconds=self.WINDOW, daily_limit=2,
            now=1_000, enforce_user_limit=False)

        assert db.llm_reserve_xai_request(7, **kwargs) is None
        assert db.llm_reserve_xai_request(7, **kwargs) is None
        denial = db.llm_reserve_xai_request(7, **kwargs)
        assert denial == 'daily'
        assert self.event_count(db) == 2

    def test_user_limit_is_global_across_discord_id_representations(self, db):
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=1_000) is None

        assert self.reserve(db, '7', now=1_001) == 'user'
        assert self.reserve(db, 8, now=1_001) is None

    def test_supplied_guild_scopes_only_the_personal_limit(self, db):
        kwargs = dict(
            user_limit=2, window_seconds=self.WINDOW,
            daily_limit=self.DAILY_LIMIT, now=1_000)
        assert db.llm_reserve_xai_request(7, guild_id=100, **kwargs) is None
        assert db.llm_reserve_xai_request(7, guild_id=100, **kwargs) is None
        assert db.llm_reserve_xai_request(7, guild_id=100, **kwargs) == 'user'
        assert db.llm_reserve_xai_request(7, guild_id=200, **kwargs) is None

    def test_scoped_retry_ignores_another_guilds_older_request(self, db):
        kwargs = dict(
            user_limit=1, window_seconds=self.WINDOW,
            daily_limit=self.DAILY_LIMIT)
        assert db.llm_reserve_xai_request(
            7, guild_id=200, now=900, **kwargs) is None
        assert db.llm_reserve_xai_request(
            7, guild_id=100, now=1_000, **kwargs) is None
        denial = db.llm_reserve_xai_request(
            7, guild_id=100, now=1_001, **kwargs)
        assert denial == 'user'
        assert denial.retry_at == 1_000 + self.WINDOW

    def test_shared_daily_limit_still_spans_supplied_guilds(self, db):
        kwargs = dict(
            user_limit=10, window_seconds=self.WINDOW,
            daily_limit=2, now=100)
        assert db.llm_reserve_xai_request(1, guild_id=100, **kwargs) is None
        assert db.llm_reserve_xai_request(2, guild_id=200, **kwargs) is None
        assert db.llm_reserve_xai_request(
            3, guild_id=300, **kwargs) == 'daily'

    def test_shared_spend_guard_still_spans_supplied_guilds(self, db):
        kwargs = dict(
            user_limit=10, window_seconds=self.WINDOW,
            daily_limit=10, daily_budget_microusd=1_000, now=100)
        assert db.llm_reserve_xai_request(
            1, guild_id=100, reserved_microusd=400, **kwargs) is None
        assert db.llm_reserve_xai_request(
            2, guild_id=200, reserved_microusd=600, **kwargs) is None
        assert db.llm_reserve_xai_request(
            3, guild_id=300, reserved_microusd=1, **kwargs) == 'budget'

    def test_pruning_preserves_the_active_call_window(self, db):
        window = 40 * 86400
        kwargs = dict(user_limit=1, window_seconds=window,
                      daily_limit=self.DAILY_LIMIT)
        assert db.llm_reserve_xai_request(
            7, now=18 * 86400, **kwargs) is None
        assert db.llm_reserve_xai_request(
            7, now=50 * 86400, **kwargs) == 'user'

    def test_exact_rolling_window_boundary_reopens_a_slot(self, db):
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=1_000) is None

        assert self.reserve(db, 7, now=1_000 + self.WINDOW) is None

    def test_global_daily_limit_spans_users(self, db):
        now = 2 * 86_400 + 100
        for user_id in range(self.DAILY_LIMIT):
            assert self.reserve(db, user_id, now=now) is None

        denial = self.reserve(db, 999, now=now)
        assert denial == 'daily'
        assert denial.retry_at == 3 * 86_400
        assert self.event_count(db) == self.DAILY_LIMIT

    def test_retry_waits_for_a_later_simultaneous_daily_guard(self, db):
        now = 1_000
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=now) is None
        for user_id in range(self.DAILY_LIMIT - self.USER_LIMIT):
            assert self.reserve(db, 100 + user_id, now=now) is None

        denial = self.reserve(db, 7, now=now)
        assert denial == 'daily'
        assert denial.retry_at == 86_400

    def test_utc_day_resets_but_rolling_user_window_spans_midnight(self, db):
        before_midnight = 86_400 - 10
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=before_midnight) is None

        after_midnight = 86_400 + 10
        assert self.reserve(db, 7, now=after_midnight) == 'user'
        assert self.reserve(db, 8, now=after_midnight) is None

    def test_full_daily_pool_resets_at_utc_midnight(self, db):
        before_midnight = 86_400 - 1
        for user_id in range(self.DAILY_LIMIT):
            assert self.reserve(db, user_id, now=before_midnight) is None

        assert self.reserve(db, 999, now=before_midnight) == 'daily'
        assert self.reserve(db, 999, now=86_400) is None

    def test_rejected_request_does_not_create_an_event(self, db):
        for _ in range(self.USER_LIMIT):
            assert self.reserve(db, 7, now=1_000) is None

        before = self.event_count(db)
        assert self.reserve(db, 7, now=1_001) == 'user'
        assert self.event_count(db) == before

    def test_reservations_survive_database_reopen(self, tmp_path):
        class FileLlmDb(LlmDbMixin):
            def __init__(self, path):
                self.conn = sqlite3.connect(path)
                self.conn.row_factory = namedtuple_factory
                self._create_llm_tables()
                self.conn.commit()

        path = tmp_path / 'user.db'
        first = FileLlmDb(path)
        for _ in range(self.USER_LIMIT):
            assert self.reserve(first, 7, now=1_000) is None
        first.conn.close()

        reopened = FileLlmDb(path)
        try:
            assert self.reserve(reopened, 7, now=1_001) == 'user'
        finally:
            reopened.conn.close()

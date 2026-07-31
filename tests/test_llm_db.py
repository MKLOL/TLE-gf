"""Tests for the LLM key/quota DB layer (``tle/util/db/llm_db.py``)."""
import pytest

from tle.util.db.llm_db import key_fingerprint
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

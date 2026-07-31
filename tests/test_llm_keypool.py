"""Tests for the rotating Gemini key pool (``tle/util/llm_keypool.py``).

The behaviour that matters here is the distinction between a per-minute 429
(cool the bucket briefly, in memory) and a per-day one (persist it, because
rediscovering it after a restart costs a real request).
"""
import pytest

from tle.util import llm_keypool
from tle.util.llm_keypool import (QUOTA_DAY, QUOTA_MINUTE, QUOTA_UNKNOWN,
                                  KeyPool, classify_quota_error,
                                  next_daily_reset, parse_retry_delay)
from tests.llm_test_utils import FakeClock, FakeLlmDb, quota_error, run


@pytest.fixture
def db():
    database = FakeLlmDb()
    database.llm_add_key('AIzaSyKeyNumberOne000000', label='proj-a')
    database.llm_add_key('AIzaSyKeyNumberTwo000000', label='proj-b')
    return database


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def pool(db, clock):
    return KeyPool(db, ['model-a', 'model-b'], now_fn=clock)


class TestClassifyQuotaError:
    def test_per_day_quota_id_is_classified_as_daily(self):
        body = quota_error('GenerateRequestsPerDayPerProjectPerModel-FreeTier')
        assert classify_quota_error(body)[0] == QUOTA_DAY

    def test_per_minute_quota_id_is_classified_as_minute(self):
        body = quota_error('GenerateRequestsPerMinutePerProjectPerModel-FreeTier')
        assert classify_quota_error(body)[0] == QUOTA_MINUTE

    def test_retry_delay_is_extracted(self):
        body = quota_error('GenerateRequestsPerMinutePerProject', retry_delay='51s')
        scope, retry_after = classify_quota_error(body)
        assert scope == QUOTA_MINUTE
        assert retry_after == 51.0

    def test_falls_back_to_the_message_when_details_are_missing(self):
        # The OpenAI-compatibility layer flattens details away; the human
        # message is all that survives.
        body = {'error': {'code': 429,
                          'message': 'You exceeded your current quota: '
                                     'limit 500 per day'}}
        assert classify_quota_error(body)[0] == QUOTA_DAY

    def test_per_minute_wins_when_the_message_also_mentions_daily(self):
        # Google's prose routinely names both windows: "limit 15 per minute ...
        # learn more about daily limits". Reading that as daily parks a healthy
        # bucket until midnight Pacific with quota still on it.
        body = {'error': {'code': 429, 'message':
                          'You exceeded your quota: limit 15 per minute. '
                          'Learn more about daily limits and upgrading.'}}
        assert classify_quota_error(body)[0] == QUOTA_MINUTE

    def test_structured_details_outrank_the_message(self):
        body = quota_error('GenerateRequestsPerMinutePerProjectPerModel',
                           message='...see your daily quota in AI Studio')
        assert classify_quota_error(body)[0] == QUOTA_MINUTE

    def test_a_daily_quota_id_is_still_daily_despite_the_bias(self):
        body = quota_error('GenerateRequestsPerDayPerProjectPerModel-FreeTier',
                           message='quota exceeded')
        assert classify_quota_error(body)[0] == QUOTA_DAY

    def test_unrecognizable_429_is_unknown(self):
        assert classify_quota_error(
            {'error': {'message': 'Too many requests'}})[0] == QUOTA_UNKNOWN

    def test_empty_and_none_payloads_do_not_raise(self):
        assert classify_quota_error(None)[0] == QUOTA_UNKNOWN
        assert classify_quota_error({})[0] == QUOTA_UNKNOWN


class TestParseRetryDelay:
    @pytest.mark.parametrize('value,expected', [
        ('51s', 51.0), ('1.5s', 1.5), ('7', 7.0), (12, 12.0), (3.5, 3.5),
    ])
    def test_parses_durations(self, value, expected):
        assert parse_retry_delay(value) == expected

    @pytest.mark.parametrize('value', [None, '', 'soon', '5m'])
    def test_unparseable_is_none(self, value):
        assert parse_retry_delay(value) is None


class TestNextDailyReset:
    def test_reset_is_in_the_future_and_within_a_day(self, clock):
        reset = next_daily_reset(clock.now)
        assert clock.now < reset <= clock.now + 86400

    def test_reset_is_stable_across_a_day(self, clock):
        first = next_daily_reset(clock.now)
        assert next_daily_reset(clock.now + 60) == first


class TestRotation:
    def test_acquires_the_first_model_before_the_fallback(self, pool):
        lease = run(pool.acquire())
        assert lease.model == 'model-a'

    def test_consecutive_calls_alternate_between_keys(self, pool, clock):
        first = run(pool.acquire())
        clock.advance(1)
        second = run(pool.acquire())
        assert first.key_id != second.key_id

    def test_key_material_is_carried_on_the_lease(self, pool):
        lease = run(pool.acquire())
        assert lease.api_key.startswith('AIzaSyKeyNumber')

    def test_no_keys_means_no_lease(self, clock):
        pool = KeyPool(FakeLlmDb(), ['model-a'], now_fn=clock)
        assert pool.key_count() == 0
        assert run(pool.acquire()) is None


class TestMinuteQuota:
    def test_minute_limit_moves_to_the_other_key_not_the_other_model(self, pool):
        first = run(pool.acquire())
        pool.report_quota(first, QUOTA_MINUTE, retry_after=60)
        second = run(pool.acquire())
        assert second.key_id != first.key_id
        assert second.model == 'model-a'

    def test_bucket_recovers_after_the_cooldown(self, pool, clock):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_MINUTE, retry_after=60)
        clock.advance(61)
        assert pool._blocked_until(lease.key_id, lease.model) is None

    def test_minute_limit_is_not_persisted(self, pool, db):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_MINUTE, retry_after=60)
        assert db.llm_get_buckets(now=pool._now()) == []


class TestDailyQuota:
    def test_daily_limit_blocks_until_the_reset(self, pool, clock):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_DAY)
        blocked_until = pool._blocked_until(lease.key_id, lease.model)
        assert blocked_until == next_daily_reset(clock.now)

    def test_daily_limit_is_persisted_and_survives_a_reload(self, pool, db):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_DAY, message='out of quota')
        assert len(db.llm_get_buckets(now=pool._now())) == 1

        pool.reload()
        assert pool._blocked_until(lease.key_id, lease.model) is not None

    def test_a_short_retry_delay_on_a_daily_is_ignored(self, pool, clock):
        # A daily quota does not come back in 30 seconds; trusting that would
        # make the pool re-probe a dead bucket all day.
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_DAY, retry_after=30)
        assert pool._blocked_until(lease.key_id, lease.model) == \
            next_daily_reset(clock.now)

    def test_a_long_retry_delay_on_a_daily_is_honored(self, pool, clock):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_DAY, retry_after=7200)
        assert pool._blocked_until(lease.key_id, lease.model) == clock.now + 7200

    def test_exhausting_a_model_everywhere_falls_back_to_the_next(self, pool, clock):
        for _ in range(2):
            lease = run(pool.acquire())
            assert lease.model == 'model-a'
            pool.report_quota(lease, QUOTA_DAY)
            clock.advance(1)
        fallback = run(pool.acquire())
        assert fallback.model == 'model-b'

    def test_all_buckets_spent_yields_no_lease_and_a_wait_hint(self, pool, clock):
        for _ in range(4):
            lease = run(pool.acquire())
            pool.report_quota(lease, QUOTA_DAY)
            clock.advance(1)
        assert run(pool.acquire()) is None
        assert pool.retry_after_hint() > 0


class TestUnknownQuota:
    def test_first_unknown_429_only_cools_the_bucket(self, pool, db):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_UNKNOWN)
        assert pool._blocked_until(lease.key_id, lease.model) is not None
        assert db.llm_get_buckets(now=pool._now()) == []

    def test_repeated_unknown_429s_escalate_to_a_daily_block(self, pool, db, clock):
        lease = run(pool.acquire())
        for _ in range(llm_keypool._UNKNOWN_STRIKES_TO_DAILY):
            pool.report_quota(lease, QUOTA_UNKNOWN)
            clock.advance(200)
        assert len(db.llm_get_buckets(now=pool._now())) == 1

    def test_a_success_resets_the_strike_count(self, pool, db, clock):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_UNKNOWN)
        pool.report_success(lease)
        for _ in range(llm_keypool._UNKNOWN_STRIKES_TO_DAILY - 1):
            pool.report_quota(lease, QUOTA_UNKNOWN)
            clock.advance(200)
        assert db.llm_get_buckets(now=pool._now()) == []


class TestOtherOutcomes:
    def test_success_clears_a_persisted_block(self, pool, db):
        lease = run(pool.acquire())
        pool.report_quota(lease, QUOTA_DAY)
        pool.report_success(lease)
        assert db.llm_get_buckets(now=pool._now()) == []
        assert pool._blocked_until(lease.key_id, lease.model) is None

    def test_transient_error_cools_briefly_without_persisting(self, pool, db, clock):
        lease = run(pool.acquire())
        pool.report_transient(lease)
        assert pool._blocked_until(lease.key_id, lease.model) is not None
        assert db.llm_get_buckets(now=pool._now()) == []
        clock.advance(llm_keypool._TRANSIENT_COOLDOWN + 1)
        assert pool._blocked_until(lease.key_id, lease.model) is None

    def test_first_rejection_only_benches_the_key(self, pool, db):
        # PERMISSION_DENIED also covers transient billing/enablement blips;
        # retiring on the first one would make those permanent.
        lease = run(pool.acquire())
        assert pool.report_invalid(lease, message='API key not valid') is False
        assert pool.key_count() == 2
        assert len(db.llm_get_keys()) == 2

    def test_benching_blocks_every_model_for_that_key(self, pool):
        lease = run(pool.acquire())
        pool.report_invalid(lease)
        assert all(pool._blocked_until(lease.key_id, model) is not None
                   for model in pool.models)

    def test_second_rejection_retires_the_key(self, pool, db):
        lease = run(pool.acquire())
        pool.report_invalid(lease)
        assert pool.report_invalid(lease, message='API key not valid') is True
        assert pool.key_count() == 1
        assert [row.id for row in db.llm_get_keys()] != [lease.key_id]

    def test_a_success_between_rejections_resets_the_strike(self, pool, db):
        lease = run(pool.acquire())
        pool.report_invalid(lease)
        pool.report_success(lease)
        assert pool.report_invalid(lease) is False  # back to strike 1, not 2
        assert len(db.llm_get_keys()) == 2

    def test_retiring_the_last_key_leaves_nothing_to_acquire(self, clock):
        db = FakeLlmDb()
        db.llm_add_key('AIzaSyOnlyKeyInThePool000')
        pool = KeyPool(db, ['model-a'], now_fn=clock)
        lease = run(pool.acquire())
        pool.report_invalid(lease)
        pool.report_invalid(lease)
        assert run(pool.acquire()) is None


class TestStatus:
    def test_status_covers_every_key_model_pair(self, pool):
        rows = pool.status()
        assert len(rows) == 4  # 2 keys x 2 models
        assert all(row['state'] == 'ready' for row in rows)

    def test_status_reports_daily_and_cooling_states(self, pool, clock):
        spent = run(pool.acquire())
        pool.report_quota(spent, QUOTA_DAY)
        clock.advance(1)
        cooling = run(pool.acquire())
        pool.report_quota(cooling, QUOTA_MINUTE, retry_after=30)

        states = {(row['key_id'], row['model']): row['state'] for row in pool.status()}
        assert states[(spent.key_id, spent.model)] == 'daily quota spent'
        assert states[(cooling.key_id, cooling.model)] == 'cooling down'

    def test_status_never_contains_key_material(self, pool):
        assert 'AIzaSy' not in str(pool.status())
        assert all('label' not in row for row in pool.status())


class TestExcludingAttemptedBuckets:
    def test_an_excluded_bucket_is_skipped(self, pool):
        first = run(pool.acquire())
        second = run(pool.acquire(exclude={(first.key_id, first.model)}))
        assert (second.key_id, second.model) != (first.key_id, first.model)

    def test_excluding_everything_drains_the_pool(self, pool):
        every = {(row['key_id'], row['model']) for row in pool.status()}
        assert run(pool.acquire(exclude=every)) is None

    def test_no_exclusion_behaves_as_before(self, pool):
        assert run(pool.acquire(exclude=None)) is not None


class TestReloadKeepsStrikes:
    def test_reload_frees_cooling_buckets(self, pool, clock):
        # A moderator re-adding a key should not have to wait out a cooldown
        # recorded against the old one.
        cooling = run(pool.acquire())
        pool.report_quota(cooling, QUOTA_MINUTE, retry_after=60)
        pool.reload()
        states = {(row['key_id'], row['model']): row['state']
                  for row in pool.status()}
        assert states[(cooling.key_id, cooling.model)] == 'ready'

    def test_reload_does_not_forget_a_rejected_key(self, pool):
        # Two rejections retire a key. If reload cleared the strike counter,
        # any `;llm keys` edit would reset the count and a revoked key would
        # bench-and-return forever instead of being retired.
        lease = run(pool.acquire())
        pool.report_invalid(lease, message='API key not valid')
        pool.reload()
        pool.report_invalid(lease, message='API key not valid')
        assert all(row.id != lease.key_id
                   for row in pool._db.llm_get_keys(active_only=True))

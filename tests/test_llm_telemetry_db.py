"""Security, telemetry, and budget DB behavior for LLM requests."""
import sqlite3

from tle import constants
from tle.util.db.llm_db import key_fingerprint
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import (
    upgrade_1_45_0, upgrade_1_46_0, upgrade_1_47_0, upgrade_1_48_0,
)
from tests.llm_test_utils import FakeLlmDb
from tle.cogs import _llm_accounting as accounting


def test_forget_erases_secret_and_bucket_but_keeps_fingerprint():
    db = FakeLlmDb()
    key = 'xai-SecretValueThatMustDisappear123'
    db.llm_add_key(key, provider='xai')
    row = db.llm_get_keys(provider='xai')[0]
    db.llm_set_bucket_exhausted(row.id, 'model-a', 999)

    assert db.llm_forget_key(row.id, provider='xai') is True
    erased = db.llm_get_keys(active_only=False, provider='xai')[0]
    assert erased.api_key == ''
    assert erased.fingerprint == key_fingerprint(key)
    assert db.llm_get_buckets(now=0) == []


def test_erased_key_can_be_readded_from_fingerprint_tombstone():
    db = FakeLlmDb()
    key = 'AIzaSySecretValueThatMustDisappear123'
    db.llm_add_key(key)
    key_id = db.llm_get_keys()[0].id
    db.llm_forget_key(key_id)

    assert db.llm_add_key(key, label='restored') == 'reactivated'
    restored = db.llm_get_keys()[0]
    assert restored.id == key_id
    assert restored.api_key == key


def test_provider_telemetry_is_separate_and_prompt_free():
    db = FakeLlmDb()
    db.llm_record_request(
        1, 2, 'xai', '2026-07-31', 'success', model='grok-test',
        router_attempts=1, answer_attempts=2, input_tokens=30,
        output_tokens=10, total_tokens=40, latency_ms=125,
        cost_microusd=99, context_mode='recent', context_messages=5,
        now=100)
    db.llm_record_request(
        1, 3, 'gemini', '2026-07-31', 'failed', model='gemini-test',
        answer_attempts=1, now=101)

    xai = db.llm_provider_summary('xai', '2026-07-31')
    gemini = db.llm_provider_summary('gemini', '2026-07-31')
    assert (xai.calls, xai.successes, xai.total_tokens,
            xai.cost_microusd) == (1, 1, 40, 99)
    assert (gemini.calls, gemini.successes) == (1, 0)
    columns = {row[1] for row in db.conn.execute(
        'PRAGMA table_info(llm_request_usage)').fetchall()}
    assert not {'prompt', 'answer', 'api_key'} & columns


def test_xai_budget_reservation_is_atomic_and_reconcilable():
    db = FakeLlmDb()
    kwargs = dict(user_limit=10, window_seconds=1800, daily_limit=100,
                  now=100, reserved_microusd=600,
                  daily_budget_microusd=1000, return_id=True)
    reservation_id = db.llm_reserve_xai_request(7, **kwargs)
    assert isinstance(reservation_id, int)
    denial = db.llm_reserve_xai_request(8, **kwargs)
    assert denial == 'budget' and denial.retry_at == 86_400

    assert db.llm_finalize_xai_request(
        reservation_id, actual_microusd=100, outcome='success',
        model='grok-test')
    second = db.llm_reserve_xai_request(8, **kwargs)
    assert isinstance(second, int)
    summary = db.llm_xai_daily_summary(now=100)
    assert (summary.calls, summary.actual_microusd,
            summary.guarded_microusd) == (2, 100, 700)


def test_xai_daily_limit_reset_is_utc_bounded_and_keeps_telemetry():
    db = FakeLlmDb()
    day_start = 2 * 86_400
    rows = [
        ('before', day_start - 1),
        ('start', day_start),
        ('today', day_start + 123),
        ('next', day_start + 86_400),
    ]
    with db.conn:
        db.conn.executemany(
            'INSERT INTO llm_xai_request (user_id, requested_at) VALUES (?, ?)',
            rows)
    db.llm_record_request(
        1, 7, 'xai', '1970-01-03', 'success', cost_microusd=99,
        now=day_start + 10)

    assert db.llm_reset_xai_daily_limits(now=day_start + 500) == 2
    remaining = db.conn.execute(
        'SELECT requested_at FROM llm_xai_request ORDER BY requested_at'
    ).fetchall()
    assert [row.requested_at for row in remaining] == [
        day_start - 1, day_start + 86_400]
    summary = db.llm_provider_summary('xai', '1970-01-03')
    assert (summary.calls, summary.cost_microusd) == (1, 99)
    assert db.llm_reserve_xai_request(
        'before', user_limit=1, window_seconds=3600, daily_limit=200,
        now=day_start + 500) == 'user'
    assert db.llm_reserve_xai_request(
        'new', user_limit=1, window_seconds=3600, daily_limit=200,
        now=day_start + 500) is None


def test_telemetry_retention_is_bounded():
    db = FakeLlmDb()
    for timestamp in (10, 20):
        db.llm_record_request(
            1, 2, 'xai', '2026-07-31', 'success', now=timestamp)
    assert db.llm_purge_request_usage(15) == 1
    assert db.llm_provider_summary('xai', '2026-07-31').calls == 1


def test_xai_cost_prefers_exact_stage_cost_and_estimates_missing_stage():
    exact = {'cost_microusd': 7, 'input_tokens': 9999,
             'output_tokens': 9999}
    estimated = {'input_tokens': 4, 'output_tokens': 2}
    expected_estimate = (
        4 * constants.XAI_INPUT_USD_PER_MILLION
        + 2 * constants.XAI_OUTPUT_USD_PER_MILLION)
    assert accounting.xai_cost_microusd(exact, estimated) == \
        7 + int(expected_estimate)
    assert accounting.has_xai_cost_observation({'cost_microusd': 0})
    assert not accounting.has_xai_cost_observation({'attempts': 1})


def test_xai_default_reservation_stays_inside_private_daily_budget():
    expected = (
        constants.XAI_REQUEST_RESERVE_INPUT_TOKENS
        * constants.XAI_INPUT_USD_PER_MILLION
        + (constants.XAI_MAX_OUTPUT_TOKENS
           + constants.XAI_ROUTER_MAX_OUTPUT_TOKENS)
        * constants.XAI_OUTPUT_USD_PER_MILLION)
    assert accounting.xai_reservation_microusd() == int(expected)
    assert accounting.daily_budget_microusd() == 500_000
    assert accounting.xai_reservation_microusd() < \
        accounting.daily_budget_microusd()


def test_1_48_migration_is_idempotent_and_preserves_old_ledger_rows():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    upgrade_1_45_0(conn)
    upgrade_1_46_0(conn)
    upgrade_1_47_0(conn)
    conn.execute(
        'INSERT INTO llm_xai_request (user_id, requested_at) VALUES (?, ?)',
        ('7', 123.0))
    upgrade_1_48_0(conn)
    upgrade_1_48_0(conn)

    columns = {row[1] for row in conn.execute(
        'PRAGMA table_info(llm_xai_request)').fetchall()}
    assert {'guild_id', 'model', 'reserved_microusd',
            'actual_microusd', 'outcome'} <= columns
    row = conn.execute('SELECT * FROM llm_xai_request').fetchone()
    assert row.user_id == '7'
    assert row.reserved_microusd == 0
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='llm_request_usage'").fetchone() is not None
    conn.close()

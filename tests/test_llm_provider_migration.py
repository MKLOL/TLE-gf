"""Migration coverage for provider-isolated LLM state and Grok limits."""
import sqlite3

import pytest

from tle.util.db.llm_db import LlmDbMixin, key_fingerprint
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import (
    upgrade_1_45_0, upgrade_1_46_0, upgrade_1_47_0,
)


@pytest.fixture
def legacy_db():
    db = sqlite3.connect(':memory:')
    db.row_factory = namedtuple_factory
    upgrade_1_45_0(db)
    db.execute(
        'INSERT INTO llm_api_key '
        '(id, api_key, fingerprint, label, guild_id, added_by, added_at, active) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (7, 'AIzaSyLegacyKeyValue123',
         key_fingerprint('AIzaSyLegacyKeyValue123'), 'legacy', '100', '200',
         123.0, 1),
    )
    db.execute(
        'INSERT INTO llm_bucket '
        '(key_id, model, exhausted_until, last_error, updated_at) '
        'VALUES (?, ?, ?, ?, ?)',
        (7, 'gemini-test', 999.0, 'quota', 456.0),
    )
    db.commit()
    yield db
    db.close()


def test_upgrade_defaults_legacy_keys_to_gemini_and_preserves_state(legacy_db):
    upgrade_1_46_0(legacy_db)

    key = legacy_db.execute(
        'SELECT id, api_key, provider, label, active FROM llm_api_key'
    ).fetchone()
    bucket = legacy_db.execute(
        'SELECT key_id, model, exhausted_until, last_error FROM llm_bucket'
    ).fetchone()
    assert (key.id, key.provider, key.label, key.active) == (
        7, 'gemini', 'legacy', 1)
    assert key.api_key == 'AIzaSyLegacyKeyValue123'
    assert (bucket.key_id, bucket.model, bucket.exhausted_until,
            bucket.last_error) == (7, 'gemini-test', 999.0, 'quota')


def test_upgrade_is_idempotent(legacy_db):
    upgrade_1_46_0(legacy_db)
    upgrade_1_46_0(legacy_db)

    columns = [
        row[1] for row in legacy_db.execute(
            'PRAGMA table_info(llm_api_key)').fetchall()
    ]
    assert columns.count('provider') == 1
    assert legacy_db.execute(
        'SELECT provider FROM llm_api_key WHERE id = 7'
    ).fetchone().provider == 'gemini'
    indexes = {
        row[1] for row in legacy_db.execute(
            'PRAGMA index_list(llm_api_key)').fetchall()
    }
    assert 'llm_api_key_provider_active' in indexes


def test_pre_migration_table_setup_skips_the_future_provider_index(legacy_db):
    class Db(LlmDbMixin):
        pass

    database = Db()
    database.conn = legacy_db
    database._create_llm_tables()

    columns = {row[1] for row in legacy_db.execute(
        'PRAGMA table_info(llm_api_key)').fetchall()}
    assert 'provider' not in columns


def test_grok_request_ledger_migration_is_idempotent(legacy_db):
    upgrade_1_47_0(legacy_db)
    upgrade_1_47_0(legacy_db)

    columns = {
        row[1] for row in legacy_db.execute(
            'PRAGMA table_info(llm_xai_request)').fetchall()
    }
    indexes = {
        row[1] for row in legacy_db.execute(
            'PRAGMA index_list(llm_xai_request)').fetchall()
    }
    assert columns == {'id', 'user_id', 'requested_at'}
    assert indexes == {
        'llm_xai_request_time', 'llm_xai_request_user_time'}

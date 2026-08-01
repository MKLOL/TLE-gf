"""User database upgrades after 1.41.0."""

import logging

from tle.util.db._user_db_upgrade_registry import registry


logger = logging.getLogger(__name__)


@registry.register('1.42.0', 'Separate Queens rating opt-out from unregister')
def upgrade_1_42_0(db):
    """Discard opt-outs created by the former unregister workflow.

    Before 1.42, every Queens row in ``minigame_optout`` was created
    implicitly by ``;queens unregister``. There was no explicit opt-out
    command, so those rows cannot represent the new independent rating choice.
    """
    logger.info('1.42.0: Clearing legacy Queens unregister opt-outs')
    db.execute(
        'DELETE FROM minigame_optout WHERE game = ?',
        ('queens',),
    )
    db.commit()
    logger.info('1.42.0: Upgrade complete')


@registry.register('1.43.0', 'Persistent Queens result status and provenance')
def upgrade_1_43_0(db):
    """Add durable rating state, save time, and Discord provenance.

    Legacy sources have no reliable creation timestamp or rating-state
    evidence, so they intentionally enter the new system as rated with time 0.
    Per-result permanence begins when this upgrade is installed.
    """
    logger.info('1.43.0: Adding Queens result status and provenance')
    columns = {
        row[1] for row in db.execute(
            'PRAGMA table_info(minigame_unresolved_result)').fetchall()
    }
    if 'is_rated' not in columns:
        db.execute(
            'ALTER TABLE minigame_unresolved_result '
            'ADD COLUMN is_rated INTEGER NOT NULL DEFAULT 1 '
            'CHECK (is_rated IN (0, 1))'
        )
    if 'stored_at' not in columns:
        db.execute(
            'ALTER TABLE minigame_unresolved_result '
            'ADD COLUMN stored_at REAL NOT NULL DEFAULT 0'
        )
    if 'source_message_id' not in columns:
        db.execute(
            'ALTER TABLE minigame_unresolved_result '
            'ADD COLUMN source_message_id TEXT'
        )
    optout_columns = {
        row[1] for row in db.execute(
            'PRAGMA table_info(minigame_optout)').fetchall()
    }
    if 'normalized_name' not in optout_columns:
        db.execute(
            'ALTER TABLE minigame_optout '
            'ADD COLUMN normalized_name TEXT'
        )
    has_links = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'minigame_player_link'"
    ).fetchone()
    if has_links:
        db.execute(
            '''
            UPDATE minigame_optout
            SET normalized_name = (
                SELECT link.normalized_name
                FROM minigame_player_link link
                WHERE link.guild_id = minigame_optout.guild_id
                  AND link.game = minigame_optout.game
                  AND link.user_id = minigame_optout.user_id
            )
            WHERE game = 'queens' AND normalized_name IS NULL
            '''
        )
    db.execute(
        'CREATE INDEX IF NOT EXISTS '
        'idx_minigame_unresolved_result_message '
        'ON minigame_unresolved_result '
        '(guild_id, game, source_message_id)'
    )
    db.execute(
        'CREATE INDEX IF NOT EXISTS idx_minigame_optout_name '
        'ON minigame_optout (guild_id, game, normalized_name)'
    )
    db.commit()
    logger.info('1.43.0: Upgrade complete')


@registry.register('1.44.0', 'Preserve explicit Queens result rating overrides')
def upgrade_1_44_0(db):
    """Distinguish moderator choices from automatic opt-out defaults."""
    logger.info('1.44.0: Adding Queens result rating overrides')
    columns = {
        row[1] for row in db.execute(
            'PRAGMA table_info(minigame_unresolved_result)').fetchall()
    }
    if 'rating_override' not in columns:
        db.execute(
            'ALTER TABLE minigame_unresolved_result '
            'ADD COLUMN rating_override INTEGER '
            'CHECK (rating_override IN (0, 1))'
        )
    db.commit()
    logger.info('1.44.0: Upgrade complete')


@registry.register('1.45.0', 'LLM API key pool, per-bucket quota, and usage')
def upgrade_1_45_0(db):
    """Tables backing ``;llm``.

    Quota on Gemini's free tier is metered per project per model, so
    ``llm_bucket`` is keyed on ``(key_id, model)`` rather than on the key
    alone — one key can be spent for one model and healthy for another.
    """
    logger.info('1.45.0: Adding LLM key pool tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_api_key (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key     TEXT NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            label       TEXT,
            guild_id    TEXT,
            added_by    TEXT,
            added_at    REAL NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_bucket (
            key_id          INTEGER NOT NULL,
            model           TEXT NOT NULL,
            exhausted_until REAL,
            last_error      TEXT,
            updated_at      REAL,
            PRIMARY KEY (key_id, model)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_usage (
            guild_id TEXT NOT NULL,
            user_id  TEXT NOT NULL,
            day      TEXT NOT NULL,
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, day)
        )
    ''')
    db.commit()
    logger.info('1.45.0: Upgrade complete')


@registry.register('1.46.0', 'Provider-isolated LLM API keys')
def upgrade_1_46_0(db):
    """Tag existing keys as Gemini so xAI credentials stay separate."""
    logger.info('1.46.0: Adding LLM API key provider')
    columns = {
        row[1] for row in db.execute('PRAGMA table_info(llm_api_key)').fetchall()
    }
    if 'provider' not in columns:
        db.execute(
            "ALTER TABLE llm_api_key ADD COLUMN provider TEXT NOT NULL "
            "DEFAULT 'gemini'"
        )
    db.execute(
        'CREATE INDEX IF NOT EXISTS llm_api_key_provider_active '
        'ON llm_api_key (provider, active)'
    )
    db.commit()
    logger.info('1.46.0: Upgrade complete')


@registry.register('1.47.0', 'Persistent Grok request limits')
def upgrade_1_47_0(db):
    """Add the timestamp ledger used by Grok's credit guard."""
    logger.info('1.47.0: Adding Grok request ledger')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_xai_request (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      TEXT NOT NULL,
            requested_at REAL NOT NULL
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS llm_xai_request_time
        ON llm_xai_request (requested_at)
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS llm_xai_request_user_time
        ON llm_xai_request (user_id, requested_at)
    ''')
    db.commit()
    logger.info('1.47.0: Upgrade complete')


@registry.register('1.48.0', 'Provider telemetry and Grok spend reservations')
def upgrade_1_48_0(db):
    """Add prompt-free request metrics and credit reservation metadata."""
    logger.info('1.48.0: Adding LLM telemetry and spend guards')
    columns = {
        row[1] for row in db.execute(
            'PRAGMA table_info(llm_xai_request)').fetchall()
    }
    additions = {
        'guild_id': 'TEXT',
        'model': 'TEXT',
        'reserved_microusd': 'INTEGER NOT NULL DEFAULT 0',
        'actual_microusd': 'INTEGER',
        'outcome': 'TEXT',
    }
    for name, declaration in additions.items():
        if name not in columns:
            db.execute(
                f'ALTER TABLE llm_xai_request ADD COLUMN {name} {declaration}')

    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_request_usage (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            requested_at      REAL NOT NULL,
            day               TEXT NOT NULL,
            guild_id          TEXT NOT NULL,
            user_id           TEXT NOT NULL,
            provider          TEXT NOT NULL,
            model             TEXT,
            outcome           TEXT NOT NULL,
            router_attempts    INTEGER NOT NULL DEFAULT 0,
            answer_attempts    INTEGER NOT NULL DEFAULT 0,
            input_tokens       INTEGER NOT NULL DEFAULT 0,
            output_tokens      INTEGER NOT NULL DEFAULT 0,
            total_tokens       INTEGER NOT NULL DEFAULT 0,
            latency_ms         INTEGER NOT NULL DEFAULT 0,
            cost_microusd      INTEGER NOT NULL DEFAULT 0,
            context_mode       TEXT,
            context_messages   INTEGER NOT NULL DEFAULT 0
        )
    ''')
    db.execute(
        'CREATE INDEX IF NOT EXISTS llm_request_usage_provider_day '
        'ON llm_request_usage (provider, day)')
    db.execute(
        'CREATE INDEX IF NOT EXISTS llm_request_usage_guild_day '
        'ON llm_request_usage (guild_id, day)')
    db.commit()
    logger.info('1.48.0: Upgrade complete')


@registry.register('1.49.0', 'Guild-scoped LLM request bans')
def upgrade_1_49_0(db):
    """Add persistent user bans shared by Gemini, Grok, and ``@grok``."""
    logger.info('1.49.0: Adding guild-scoped LLM request bans')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_user_ban (
            guild_id  TEXT NOT NULL,
            user_id   TEXT NOT NULL,
            banned_by TEXT,
            banned_at REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.execute(
        'CREATE INDEX IF NOT EXISTS llm_user_ban_guild_time '
        'ON llm_user_ban (guild_id, banned_at)')
    db.commit()
    logger.info('1.49.0: Upgrade complete')


@registry.register('1.50.0', 'Persistent shared LLM cooldowns')
def upgrade_1_50_0(db):
    """Add server-wide and parent-channel prompt admission cooldowns."""
    logger.info('1.50.0: Adding shared LLM cooldowns')
    db.execute('''
        CREATE TABLE IF NOT EXISTS llm_cooldown (
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            seconds        INTEGER NOT NULL
                           CHECK (seconds BETWEEN 1 AND 86400),
            last_attempt_at REAL,
            PRIMARY KEY (guild_id, channel_id)
        )
    ''')
    db.commit()
    logger.info('1.50.0: Upgrade complete')


@registry.register('1.51.0', 'Exact channel and thread LLM cooldown scopes')
def upgrade_1_51_0(db):
    """Keep 1.50 parent-channel cooldowns covering their child threads."""
    logger.info('1.51.0: Migrating shared LLM cooldown scopes')
    # A prefixed row can only pre-exist after a partial rollout. In that case,
    # the raw row may already represent an exact scope, so preserve both.
    db.execute('''
        UPDATE OR IGNORE llm_cooldown
        SET channel_id = 'family:' || channel_id
        WHERE channel_id != '*' AND channel_id NOT LIKE 'family:%'
    ''')
    ambiguous = db.execute('''
        SELECT COUNT(*) FROM llm_cooldown
        WHERE channel_id != '*' AND channel_id NOT LIKE 'family:%'
    ''').fetchone()[0]
    if ambiguous:
        logger.warning(
            '1.51.0: Preserved %d ambiguous exact cooldown scope(s)',
            ambiguous)
    db.commit()
    logger.info('1.51.0: Upgrade complete')

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

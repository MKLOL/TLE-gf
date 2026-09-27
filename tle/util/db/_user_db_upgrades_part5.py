"""User database upgrades after 1.53.0."""

import logging
import time

from tle.util.db._user_db_upgrade_registry import registry
from tle.util.db.counting_db import create_counting_schema
from tle.util.db.greatday_db import create_greatday_signup_event_table
from tle.util.db._starboard_db_backfill import create_pending_backfill_index


logger = logging.getLogger(__name__)


@registry.register('1.54.0', 'Persistent counting channels and attempt ledger')
def upgrade_1_54_0(db):
    """Add counting checkpoints and numeric-attempt audit rows."""
    logger.info('1.54.0: Adding counting channel state and attempt ledger')
    create_counting_schema(db)
    db.commit()
    logger.info('1.54.0: Upgrade complete')


@registry.register('1.55.0', 'Rebuild counting ledgers from Discord history')
def upgrade_1_55_0(db):
    """Clear parsed attempts while preserving configured channel IDs."""
    logger.info('1.55.0: Clearing counting ledgers for full history reparse')
    create_counting_schema(db)
    db.execute('DELETE FROM counting_attempt')
    db.commit()
    logger.info('1.55.0: Upgrade complete')


@registry.register('1.56.0', 'Great Day signup/signout event log')
def upgrade_1_56_0(db):
    """Add the signup/signout ledger behind ;greatday history and stats."""
    logger.info('1.56.0: Creating greatday_signup_event table')
    create_greatday_signup_event_table(db)
    db.commit()
    logger.info('1.56.0: Upgrade complete')


@registry.register('1.57.0', 'Share LinkedIn player links between Queens and Tango')
def upgrade_1_57_0(db):
    """Move Queens' LinkedIn links into the ``linkedin`` namespace.

    LinkedIn Tango resolves players through the same LinkedIn profile as
    Queens, so ``minigame_player_link`` rows now live under the shared game
    key ``linkedin`` (``GameDef.link_key``) and one registration serves both
    games.  Results, opt-outs, and bans stay per game.  No ``linkedin`` rows
    can exist before this upgrade, so the PK/UNIQUE constraints cannot clash.
    """
    logger.info('1.57.0: Moving queens player links to the shared linkedin namespace')
    # A DB that predates the minigame tables has nothing to move; the table
    # is created (empty) by ``create_tables`` after the upgrade chain runs.
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'minigame_player_link'").fetchone()
    if exists:
        db.execute(
            "UPDATE minigame_player_link SET game = 'linkedin' "
            "WHERE game = 'queens'")
    db.commit()
    logger.info('1.57.0: Upgrade complete')


@registry.register('1.58.0', 'Index pending starboard backfill work')
def upgrade_1_58_0(db):
    """Keep restart queries independent of completed starboard history."""
    create_pending_backfill_index(db)
    db.commit()
    logger.info('1.58.0: Pending starboard backfill index created')


@registry.register('1.59.0', 'Complaint resolutions and scoped API tokens')
def upgrade_1_59_0(db):
    from tle.util.db.complaint_workflow_db import upgrade_complaint_schema
    from tle.util.db.api_token_db import create_api_token_schema
    upgrade_complaint_schema(db)
    create_api_token_schema(db)
    db.commit()


@registry.register('1.60.0', 'Store the messages preceding a complaint')
def upgrade_1_60_0(db):
    """Add ``complaint.context``.

    ``upgrade_complaint_schema`` only runs for fresh databases and at 1.59.0,
    so an existing database needs this to pick up the new column.
    """
    from tle.util.db.complaint_workflow_db import upgrade_complaint_schema
    upgrade_complaint_schema(db)
    db.commit()
    logger.info('1.60.0: Complaint context column ready')


@registry.register('1.61.0', 'Complaint tags')
def upgrade_1_61_0(db):
    """Add ``complaint_tag``; ``upgrade_complaint_schema`` creates it."""
    from tle.util.db.complaint_workflow_db import upgrade_complaint_schema
    upgrade_complaint_schema(db)
    db.commit()
    logger.info('1.61.0: Complaint tag table ready')



@registry.register('1.62.0', ';virtual sessions')
def upgrade_1_62_0(db):
    """Add ``virtual_session`` for blind random virtual contests."""
    from tle.util.db.virtual_db import create_virtual_schema
    create_virtual_schema(db)
    db.commit()
    logger.info('1.62.0: virtual_session table ready')


@registry.register('1.63.0', 'Repair virtual_session from its first cut')
def upgrade_1_63_0(db):
    """Add base_rating and the per-user unique index to an early table."""
    from tle.util.db.virtual_db import create_virtual_schema, repair_virtual_schema
    repair_virtual_schema(db)
    create_virtual_schema(db)
    db.commit()
    logger.info('1.63.0: virtual_session schema repaired')


@registry.register('1.64.0', 'Personal LinkedIn games API tokens')
def upgrade_1_64_0(db):
    from tle.util.db.games_token_db import create_games_token_schema
    create_games_token_schema(db)
    db.commit()


@registry.register('1.65.0', 'Personal games submission receipts')
def upgrade_1_65_0(db):
    from tle.util.db.games_submission_db import create_games_submission_schema
    create_games_submission_schema(db)
    db.commit()


@registry.register('1.66.0', 'Keep active games tokens valid until revoked')
def upgrade_1_66_0(db):
    from tle.util.db.games_token_db import PERMANENT_EXPIRY
    db.execute('''UPDATE games_api_token SET expires_at = ?
        WHERE revoked_at IS NULL AND expires_at > ?''',
        (PERMANENT_EXPIRY, time.time()))
    db.commit()

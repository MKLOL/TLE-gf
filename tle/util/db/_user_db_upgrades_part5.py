"""User database upgrades after 1.53.0."""

import logging

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

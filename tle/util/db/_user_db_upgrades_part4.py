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

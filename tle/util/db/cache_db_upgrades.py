"""
Database upgrade functions for cache.db.
Register upgrades in version order; they run automatically on startup.
"""
import logging

from tle.util.db.upgrades import UpgradeRegistry

logger = logging.getLogger(__name__)

registry = UpgradeRegistry(version_table='cache_db_version')


@registry.register('1.0.0', 'Handle alias table for CF rename tracking')
def upgrade_1_0_0(db):
    logger.info('1.0.0: Creating handle_alias table')
    db.execute(
        'CREATE TABLE IF NOT EXISTS handle_alias ('
        'handle          TEXT PRIMARY KEY,'
        'current_handle  TEXT NOT NULL,'
        'resolved_at     INTEGER NOT NULL'
        ')'
    )
    db.commit()


@registry.register('1.1.0', 'Clear stale handle aliases from buggy deployment')
def upgrade_1_1_0(db):
    logger.info('1.1.0: Clearing handle_alias table')
    db.execute('DELETE FROM handle_alias')
    db.commit()

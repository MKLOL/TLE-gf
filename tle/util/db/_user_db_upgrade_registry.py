"""The shared UpgradeRegistry instance for user.db migrations.

Kept in its own module so the versioned upgrade functions can be split across
several ``_user_db_upgrades_part*`` modules that all register on the same
registry without a circular import. ``user_db_upgrades`` re-exports ``registry``.
"""
from tle.util.db.upgrades import UpgradeRegistry

registry = UpgradeRegistry(version_table='db_version')

"""
Database upgrade functions for user.db.
Register upgrades in version order; they run automatically on startup.

The upgrade functions are defined across ``_user_db_upgrades_part1/2/3`` (split
to keep each module under the 500-line limit). Importing those modules below
registers every upgrade on the shared ``registry``; the registry runs them in
version order regardless of import order. ``registry`` is re-exported here so
``from tle.util.db.user_db_upgrades import registry`` keeps working.
"""
from tle.util.db._user_db_upgrade_registry import registry

# Importing the part modules executes their @registry.register decorators and
# re-exports the individual upgrade_X_Y_Z functions, which the migration tests
# import by name from this module.
from tle.util.db._user_db_upgrades_part1 import *  # noqa: F401,F403
from tle.util.db._user_db_upgrades_part2 import *  # noqa: F401,F403
from tle.util.db._user_db_upgrades_part3 import *  # noqa: F401,F403

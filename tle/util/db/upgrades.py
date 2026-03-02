"""
Generic upgrade registry for SQLite database migrations.
Each app creates its own UpgradeRegistry instance with a version table name.
"""
import logging

logger = logging.getLogger(__name__)


class UpgradeRegistry:
    """Manages a list of ordered database upgrades."""

    def __init__(self, version_table='db_version'):
        self.upgrades = []
        self.version_table = version_table

    def register(self, version, description):
        """Decorator to register an upgrade function."""
        def decorator(func):
            self.upgrades.append((version, description, func))
            return func
        return decorator

    def get_current_version(self, db):
        """Get the current database version."""
        try:
            result = db.execute(
                f'SELECT version FROM {self.version_table} LIMIT 1'
            ).fetchone()
            version = result.version if result else None
            logger.debug(f"[{self.version_table}] Current DB version: {version}")
            return version
        except Exception as e:
            logger.debug(f"[{self.version_table}] Could not read version table: {e}")
            return None

    def set_version(self, db, version):
        """Set the database version."""
        db.execute(f'DELETE FROM {self.version_table}')
        db.execute(
            f'INSERT INTO {self.version_table} (version) VALUES (?)',
            (version,)
        )
        db.commit()
        logger.info(f"[{self.version_table}] Version set to {version}")

    def ensure_version_table(self, db):
        """Create the version table if it doesn't exist."""
        db.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.version_table} (
                version TEXT NOT NULL
            )
        ''')
        db.commit()
        logger.debug(f"[{self.version_table}] Version table ensured")

    def run(self, db):
        """Run all pending upgrades."""
        self.ensure_version_table(db)
        current_version = self.get_current_version(db)

        logger.info(f"[{self.version_table}] Looking for upgrades from {current_version}")

        start_index = 0
        if current_version:
            found = False
            for i, (version, _, _) in enumerate(self.upgrades):
                if version == current_version:
                    start_index = i + 1
                    found = True
                    break
            if not found:
                logger.error(
                    f"[{self.version_table}] Current version {current_version!r} not found "
                    f"in upgrade registry! Refusing to re-run all upgrades. "
                    f"Known versions: {[v for v, _, _ in self.upgrades]}"
                )
                raise RuntimeError(
                    f"Database version {current_version!r} is not recognized by the upgrade "
                    f"registry. This may indicate a downgrade or corruption. Aborting."
                )

        pending = self.upgrades[start_index:]
        if pending:
            logger.info(f"[{self.version_table}] {len(pending)} upgrade(s) to apply")

        for version, description, upgrade_func in pending:
            logger.info(f"[{self.version_table}] Running upgrade {version}: {description}")
            try:
                upgrade_func(db)
                self.set_version(db, version)
                logger.info(f"[{self.version_table}] Upgrade {version} complete")
            except Exception:
                logger.exception(f"[{self.version_table}] Upgrade {version} FAILED")
                raise

        if start_index >= len(self.upgrades):
            logger.info(f"[{self.version_table}] Database is up to date")

    @property
    def latest_version(self):
        """Get the latest registered version."""
        return self.upgrades[-1][0] if self.upgrades else None

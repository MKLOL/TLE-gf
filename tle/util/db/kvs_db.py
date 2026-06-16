"""General key-value store DB methods — extracted from user_db_conn.py.

Owns the ``kvs`` table.
"""
import logging

logger = logging.getLogger(__name__)


class KvsDbMixin:
    """Mixin providing the general key-value store DB methods."""

    def _create_kvs_tables(self):
        # General key-value store
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS kvs (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            )
        ''')

    def kvs_set(self, key, value):
        """Set a key-value pair. Overwrites if key exists."""
        with self.conn:
            self.conn.execute(
                'INSERT OR REPLACE INTO kvs (key, value) VALUES (?, ?)',
                (key, value)
            )

    def kvs_get(self, key):
        """Get a value by key, or None if not found."""
        row = self.conn.execute(
            'SELECT value FROM kvs WHERE key = ?', (key,)
        ).fetchone()
        return row[0] if row else None

    def kvs_delete(self, key):
        """Delete a key-value pair."""
        with self.conn:
            self.conn.execute('DELETE FROM kvs WHERE key = ?', (key,))

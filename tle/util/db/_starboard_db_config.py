"""Guild key-value config DB methods.

Split out of ``starboard_db`` as a mixin to keep that module under the
500-line limit. ``StarboardDbMixin`` inherits this, so every method
resolves on the combined connection object exactly as before.
"""


class GuildConfigDbMixin:
    """Per-guild key-value config (feature gates etc.). Expects ``self.conn``."""

    def get_guild_config(self, guild_id, key):
        """Get a guild config value. Returns the value string or None."""
        guild_id = str(guild_id)
        query = 'SELECT value FROM guild_config WHERE guild_id = ? AND key = ?'
        res = self.conn.execute(query, (guild_id, key)).fetchone()
        return res.value if res else None

    def set_guild_config(self, guild_id, key, value):
        """Set a guild config value."""
        guild_id = str(guild_id)
        self.conn.execute(
            'INSERT OR REPLACE INTO guild_config (guild_id, key, value) VALUES (?, ?, ?)',
            (guild_id, key, value)
        )
        self.conn.commit()

    def delete_guild_config(self, guild_id, key):
        """Delete a guild config value."""
        guild_id = str(guild_id)
        self.conn.execute(
            'DELETE FROM guild_config WHERE guild_id = ? AND key = ?',
            (guild_id, key)
        )
        self.conn.commit()

    def delete_guild_configs_by_prefix(self, guild_id, key_prefix):
        """Delete and count this guild's config keys under ``key_prefix``."""
        if not key_prefix:
            raise ValueError('Guild config prefix must not be empty')
        guild_id = str(guild_id)
        with self.conn:
            cursor = self.conn.execute(
                'DELETE FROM guild_config '
                'WHERE guild_id = ? AND substr(key, 1, ?) = ?',
                (guild_id, len(key_prefix), key_prefix))
        return cursor.rowcount

    def get_all_guild_configs(self, guild_id):
        """Get all config entries for a guild."""
        guild_id = str(guild_id)
        query = 'SELECT key, value FROM guild_config WHERE guild_id = ?'
        return self.conn.execute(query, (guild_id,)).fetchall()

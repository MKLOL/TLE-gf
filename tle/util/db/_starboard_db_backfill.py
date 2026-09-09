"""Indexed selection of unfinished starboard backfill work."""


# Keep the query and partial-index predicates identical so SQLite can use the
# index. Unknown authors are terminal; known authors still need a channel.
_PENDING_BACKFILL = (
    "(author_id IS NULL OR "
    "(channel_id IS NULL AND author_id != '__UNKNOWN__'))")


def create_pending_backfill_index(conn):
    """Create the index for fresh schemas and migration 1.58.0."""
    columns = {row[1] for row in conn.execute(
        'PRAGMA table_info(starboard_message_v1)')}
    if not {'guild_id', 'author_id', 'channel_id'} <= columns:
        # create_tables() runs before upgrades. Older schemas gain these
        # columns in 1.3.0, then build the index in 1.58.0.
        return
    conn.execute(
        'CREATE INDEX IF NOT EXISTS ix_starboard_message_v1_pending_backfill '
        f'ON starboard_message_v1 (guild_id) WHERE {_PENDING_BACKFILL}')


class StarboardBackfillDbMixin:
    def get_pending_starboard_messages_for_guild(self, guild_id):
        """Read only unfinished rows, without scanning completed history."""
        return self.conn.execute(
            'SELECT * FROM starboard_message_v1 '
            f'WHERE guild_id = ? AND {_PENDING_BACKFILL}',
            (str(guild_id),)).fetchall()

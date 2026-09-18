"""Proxy reactor tracking for the starboard.

Reactions placed on a bot *starboard post* are stored here keyed by the
original message they stand in for.  They are kept separate from
``starboard_reactors`` (reactions physically on the original message) so
that resyncing the original from Discord never wipes them, and so drift
checks can compare Discord's visible reaction counts against the physical
pool only.  Counts shown to users are the distinct-user union of the two
pools — one user reacting on both surfaces counts once.
"""


class StarboardProxyDbMixin:
    """Proxy reactors and pooled reactor counts. Expects ``self.conn``."""

    def add_proxy_reactor(self, original_msg_id, emoji, user_id,
                          via_starboard_msg_id):
        """Record a reaction made on a starboard post. INSERT OR IGNORE (idempotent).

        ``via_starboard_msg_id`` is the post the reaction physically sits on,
        so deleting that post can cascade exactly its own proxy rows."""
        self.conn.execute(
            'INSERT OR IGNORE INTO starboard_proxy_reactors '
            '(original_msg_id, emoji, user_id, via_starboard_msg_id) '
            'VALUES (?, ?, ?, ?)',
            (str(original_msg_id), emoji, str(user_id),
             str(via_starboard_msg_id))
        )
        self.conn.commit()

    def remove_proxy_reactor(self, original_msg_id, emoji, user_id,
                             via_starboard_msg_id):
        """Remove a starboard-post reaction from that surface only.
        Returns rowcount (0 or 1)."""
        rc = self.conn.execute(
            'DELETE FROM starboard_proxy_reactors '
            'WHERE original_msg_id = ? AND emoji = ? AND user_id = ? '
            'AND via_starboard_msg_id = ?',
            (str(original_msg_id), emoji, str(user_id),
             str(via_starboard_msg_id))
        ).rowcount
        self.conn.commit()
        return rc

    def get_proxy_reactors(self, original_msg_id, emoji):
        """Get all user IDs whose reaction with this emoji came via a starboard
        post (each user once, however many posts they reacted on)."""
        query = ('SELECT DISTINCT user_id FROM starboard_proxy_reactors '
                 'WHERE original_msg_id = ? AND emoji = ?')
        return [r.user_id for r in self.conn.execute(
            query, (str(original_msg_id), emoji)).fetchall()]

    def get_merged_reactor_count(self, original_msg_id, emojis):
        """Count distinct users who reacted with ANY of the given emojis on a message,
        whether on the original message or on one of its starboard posts.
        A user who reacted on both surfaces counts once."""
        if not emojis:
            return 0
        placeholders = ','.join('?' * len(emojis))
        query = (
            f'SELECT COUNT(*) AS cnt FROM ('
            f'SELECT user_id FROM starboard_reactors '
            f'WHERE original_msg_id = ? AND emoji IN ({placeholders}) '
            f'UNION '
            f'SELECT user_id FROM starboard_proxy_reactors '
            f'WHERE original_msg_id = ? AND emoji IN ({placeholders})'
            f')'
        )
        args = (str(original_msg_id), *emojis, str(original_msg_id), *emojis)
        return self.conn.execute(query, args).fetchone().cnt

    def get_merged_physical_reactor_count(self, original_msg_id, emojis):
        """Like get_merged_reactor_count, but only reactions physically on the
        original message — the number Discord's own reaction counts can be
        compared against for drift detection."""
        if not emojis:
            return 0
        placeholders = ','.join('?' * len(emojis))
        query = (f'SELECT COUNT(DISTINCT user_id) AS cnt FROM starboard_reactors '
                 f'WHERE original_msg_id = ? AND emoji IN ({placeholders})')
        return self.conn.execute(
            query, (str(original_msg_id), *emojis)).fetchone().cnt

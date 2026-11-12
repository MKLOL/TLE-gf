"""Resolve fallback reactions without turning them into configured aliases.

Board membership follows current routing; qualification history retains
previously displayed groups even after their last reaction is removed.
"""
from tle.util.starboard_routing import STAR, PILL, fallback_board


class StarboardRoutingDbMixin:
    def get_starboard_reaction_groups(self, guild_id, main_emoji,
                                      original_msg_id=None, candidates=()):
        """Return (display emoji, reactor emojis) groups for a board.

        Only the configured family pools users. Each fallback is a singleton
        and must independently meet the board's configured threshold.
        With no message id, discover reactions on this guild's tracked posts
        for leaderboard queries; otherwise include even not-yet-posted rows.
        """
        family = self.get_emoji_family(guild_id, main_emoji)
        groups = [(main_emoji, family)]
        if main_emoji not in (STAR, PILL):
            return groups
        if original_msg_id is None:
            predicate = ('original_msg_id IN (SELECT original_msg_id '
                         'FROM starboard_message_v1 WHERE guild_id = ?)')
            parameter = str(guild_id)
        else:
            predicate = 'original_msg_id = ?'
            parameter = str(original_msg_id)
        rows = self.conn.execute(
            f'SELECT emoji FROM starboard_reactors WHERE {predicate} '
            f'UNION SELECT emoji FROM starboard_proxy_reactors WHERE {predicate}',
            (parameter, parameter)).fetchall()
        candidates = set(candidates) | {row.emoji for row in rows}
        configured = {row.emoji for row in self.get_starboard_emojis_for_guild(guild_id)}
        aliases = {row.alias_emoji for row in self.get_all_aliases_for_guild(guild_id)}
        for emoji in sorted(candidates - configured - aliases - set(family)):
            if fallback_board(emoji) == main_emoji:
                groups.append((emoji, [emoji]))
        return groups

    def get_starboard_reactor_emojis(self, guild_id, main_emoji,
                                    original_msg_id=None, candidates=()):
        """All board reactors, for statistics/resync only, NOT admission."""
        return [emoji for _, family in self.get_starboard_reaction_groups(
            guild_id, main_emoji, original_msg_id, candidates) for emoji in family]

    def get_starboard_reaction_counts(self, guild_id, original_msg_id, main_emoji):
        """Independent raw counts plus the distinct-user board total.

        Rendering uses qualification history to hide groups that never met
        threshold while keeping historical qualifiers visible, including zero.
        The leaderboard total counts a person once across all board reactions.
        """
        qualified = self.get_starboard_qualified_emojis(original_msg_id, main_emoji)
        groups = self.get_starboard_reaction_groups(
            guild_id, main_emoji, original_msg_id, candidates=qualified)
        counts = [(emoji, self.get_merged_reactor_count(original_msg_id, family))
                  for emoji, family in groups]
        total = self.get_merged_reactor_count(
            original_msg_id, [emoji for _, family in groups for emoji in family])
        return [(emoji, count) for emoji, count in counts
                if count or emoji in qualified] or [(main_emoji, 0)], total

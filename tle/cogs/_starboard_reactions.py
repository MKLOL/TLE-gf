"""Fallback admission and recounting shared by live events and repairs."""
from tle.util import codeforces_common as cf_common
from tle.util.starboard_routing import STAR, PILL, fallback_board
from tle.cogs._starboard_helpers import _emoji_str


class ReactionGroupsMixin:
    def _resolve_reaction(self, guild_id, emoji):
        """Explicit config wins, including disabled boards and broken aliases.

        Command argument resolution stays strict in _resolve_emoji: heuristic
        routing is only for reactions, never for management commands.
        """
        main, entry = self._resolve_emoji(guild_id, emoji)
        if entry is not None or main != emoji:
            return main, entry
        target = fallback_board(emoji)
        if target is None:
            return emoji, None
        return target, cf_common.user_db.get_starboard_entry(guild_id, target)

    async def _sync_board_reactions(self, message, guild_id, main, *, force=False):
        """Sync each independent pool, preserving proxy reactions.

        Known DB emojis are included even if absent from Discord, so repairs
        can remove stale physical rows. Unknown names are never aliased.
        """
        groups = cf_common.user_db.get_starboard_reaction_groups(
            guild_id, main, message.id, (_emoji_str(r) for r in message.reactions))
        for _, family in groups:
            visible = sum(r.count for r in message.reactions if _emoji_str(r) in family)
            stored = cf_common.user_db.get_merged_physical_reactor_count(message.id, family)
            if force or visible != stored:
                await self._resync_reactors(message, family)
        return cf_common.user_db.get_starboard_reaction_counts(guild_id, message.id, main)

    def _board_header_counts(self, guild_id, message_id, main):
        """Keep past qualifiers visible; never reveal an unqualified group."""
        counts, _ = cf_common.user_db.get_starboard_reaction_counts(guild_id, message_id, main)
        if main in (STAR, PILL):
            entry = cf_common.user_db.get_starboard_entry(guild_id, main)
            if entry is not None:
                qualified = cf_common.user_db.get_starboard_qualified_emojis(message_id, main)
                counts = [(emoji, count) for emoji, count in counts
                          if count >= entry.threshold or emoji in qualified]
        return counts

    async def _build_board_message(self, message, main, count, color, guild_id,
                                   forward_snapshot=None):
        counts = self._board_header_counts(guild_id, message.id, main)
        args = (message, main, count, color)
        if forward_snapshot is not None:
            args += (forward_snapshot,)
        # Preserve the public renderer's old call shape for single-family posts.
        kwargs = {'reaction_counts': counts} if counts != [(main, count)] else {}
        return await self.build_starboard_message(*args, **kwargs)

"""Proxy reactions — reacting on a bot starboard post counts toward the
original message.

Some members can't see the channel a starboarded message was written in;
letting them react on the starboard post keeps their star meaningful.  The
reaction is stored as a *proxy reactor* against the original message and
the normal engine then recounts/posts/updates, so a user who reacts on
both the original and the starboard post still counts once (counts are a
distinct-user union of both pools — see ``StarboardProxyDbMixin``).

``ProxyReactionMixin`` expects the host class to also provide ``CoreMixin``
(``check_and_add_to_starboard``, ``_refresh_starboard_display``) plus
``self.bot`` and ``self.locks``.  Errors from the add path propagate to the
reaction listener's existing handler in ``CoreMixin``.
"""
import logging
from types import SimpleNamespace

from tle.util import codeforces_common as cf_common

logger = logging.getLogger(__name__)


class ProxyReactionMixin:
    """Forwards reactions on starboard posts to the original message."""

    @staticmethod
    def _proxy_target_is_board_surface(payload, sb_row):
        """True when the entry's "original" is itself a board surface.

        Rows created by the pre-exclusion abuse (star/pill spam put a bot
        post onto another board) have a starboard post as their original —
        redirecting the engine at one would resurrect exactly that abuse.
        Migration 1.53.0 purges such rows; this guards any that reappear
        from stale backups."""
        if cf_common.user_db.get_starboard_message_by_starboard_id(
                sb_row.original_msg_id) is not None:
            return True
        return bool(sb_row.channel_id) and cf_common.user_db.is_starboard_channel(
            payload.guild_id, sb_row.channel_id)

    async def _handle_proxy_reaction_add(self, payload, sb_row, main_emoji,
                                         entry, raw_emoji):
        """A reaction on a starboard post == a reaction on its original message."""
        if str(sb_row.guild_id) != str(payload.guild_id):
            return
        if self._proxy_target_is_board_surface(payload, sb_row):
            return
        if not sb_row.channel_id:
            logger.info(f'Proxy reaction: no source channel stored for '
                        f'original={sb_row.original_msg_id}, skipping')
            return
        cf_common.user_db.add_proxy_reactor(
            sb_row.original_msg_id, raw_emoji, payload.user_id,
            payload.message_id)
        logger.debug(f'Proxy reaction add: sb_post={payload.message_id} -> '
                     f'original={sb_row.original_msg_id} emoji={raw_emoji} '
                     f'user={payload.user_id}')
        redirected = SimpleNamespace(
            guild_id=payload.guild_id,
            channel_id=int(sb_row.channel_id),
            message_id=int(sb_row.original_msg_id),
            user_id=payload.user_id,
        )
        # The proxy reactor row is already stored, so the engine must not also
        # record it as a physical reactor on the original message.
        await self.check_and_add_to_starboard(
            int(entry.channel_id), entry.threshold, entry.color,
            main_emoji, redirected, raw_emoji=raw_emoji, record_reactor=False,
        )

    async def _handle_proxy_reaction_remove(self, payload, sb_row, main_emoji,
                                            raw_emoji):
        """Un-reacting on a starboard post removes only the proxy row — a
        reaction the same user still has on the original message keeps
        counting."""
        if str(sb_row.guild_id) != str(payload.guild_id):
            return
        # Deleting a proxy row is always safe, so do it before the surface
        # guard — a row recorded before its source channel became a board
        # (or before an abuse entry was purged) must not be stuck in the
        # count forever just because adds are now refused.
        cf_common.user_db.remove_proxy_reactor(
            sb_row.original_msg_id, raw_emoji, payload.user_id,
            payload.message_id)
        logger.debug(f'Proxy reaction remove: sb_post={payload.message_id} -> '
                     f'original={sb_row.original_msg_id} emoji={raw_emoji} '
                     f'user={payload.user_id}')
        if self._proxy_target_is_board_surface(payload, sb_row):
            return  # never run the display engine against a board surface
        if not cf_common.user_db.check_exists_starboard_message_v1(
                sb_row.original_msg_id, main_emoji):
            return
        if not sb_row.channel_id:
            return
        await self._refresh_starboard_display(
            payload.guild_id, int(sb_row.original_msg_id),
            int(sb_row.channel_id), main_emoji)

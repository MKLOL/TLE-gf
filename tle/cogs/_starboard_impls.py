"""Thin command implementations for the starboard cog.

``ImplMixin`` holds the bodies of the admin/config commands plus ``fix`` and
``backfill_status``.  The cog's command callbacks delegate here so each
callback stays a few lines long.  These methods rely on the engine helpers
provided by ``CoreMixin`` (``_resync_reactors``, ``_update_starboard_message``)
and on the backfill counters from ``BackfillMixin``.
"""
import logging

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.cogs._starboard_helpers import _parse_jump_url
from tle.cogs._starboard_core import StarboardCogError

logger = logging.getLogger(__name__)


class ImplMixin:
    """Command-body implementations delegated to from the cog callbacks."""

    # --- Admin config command impls ---

    async def _impl_add(self, ctx, emoji, threshold, color):
        if threshold < 1:
            raise StarboardCogError('Threshold must be at least 1')
        color_val = constants._DEFAULT_STAR_COLOR
        if color is None:
            logger.debug(f'No color specified for starboard add, using default: #{color_val:06x}')
        if color is not None:
            try:
                color_val = int(color.lstrip('#'), 16)
            except ValueError:
                raise StarboardCogError(f'Invalid color `{color}`. Use hex format like `#ffaa10`.')
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is not None:
            raise StarboardCogError(f'Emoji {emoji} is already configured. '
                                    f'Use `edit_threshold` or `edit_color` to modify.')
        cf_common.user_db.add_starboard_emoji(ctx.guild.id, emoji, threshold, color_val)
        logger.info(f'CMD starboard add: guild={ctx.guild.id} emoji={emoji} '
                    f'threshold={threshold} color=#{color_val:06x} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Added {emoji} to starboard (threshold={threshold}, color=#{color_val:06x})'
        ))

    async def _impl_delete(self, ctx, emoji):
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        cf_common.user_db.remove_starboard_emoji(ctx.guild.id, emoji)
        logger.info(f'CMD starboard delete: guild={ctx.guild.id} emoji={emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Removed {emoji} from starboard'))

    async def _impl_edit_threshold(self, ctx, threshold, emoji):
        if threshold < 1:
            raise StarboardCogError('Threshold must be at least 1')
        rc = cf_common.user_db.update_starboard_threshold(ctx.guild.id, emoji, threshold)
        if not rc:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        logger.info(f'CMD starboard edit_threshold: guild={ctx.guild.id} emoji={emoji} '
                    f'threshold={threshold} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Updated {emoji} threshold to {threshold}'
        ))

    async def _impl_edit_color(self, ctx, color, emoji):
        try:
            color_val = int(color.lstrip('#'), 16)
        except ValueError:
            raise StarboardCogError(f'Invalid color `{color}`. Use hex format like `#ffaa10`.')
        rc = cf_common.user_db.update_starboard_color(ctx.guild.id, emoji, color_val)
        if not rc:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        logger.info(f'CMD starboard edit_color: guild={ctx.guild.id} emoji={emoji} '
                    f'color=#{color_val:06x} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Updated {emoji} color to #{color_val:06x}'
        ))

    async def _impl_here(self, ctx, emoji):
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured. Add it first with `;starboard add {emoji}`.')
        rc = cf_common.user_db.set_starboard_channel(ctx.guild.id, emoji, ctx.channel.id)
        if not rc:
            raise StarboardCogError(f'Failed to set channel for {emoji}.')
        logger.info(f'CMD starboard here: guild={ctx.guild.id} emoji={emoji} '
                    f'channel={ctx.channel.id} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Starboard channel for {emoji} set to {ctx.channel.mention}'
        ))

    async def _impl_clear(self, ctx, emoji):
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured.')
        cf_common.user_db.clear_starboard_channel(ctx.guild.id, emoji)
        logger.info(f'CMD starboard clear: guild={ctx.guild.id} emoji={emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Starboard channel for {emoji} cleared'))

    async def _impl_remove(self, ctx, original_message_id, emoji):
        rc = cf_common.user_db.remove_starboard_message(
            original_msg_id=original_message_id, emoji=emoji
        )
        if rc:
            logger.info(f'CMD starboard remove: guild={ctx.guild.id} emoji={emoji} '
                        f'original_msg={original_message_id} by user={ctx.author.id}')
            await ctx.send(embed=discord_common.embed_success('Successfully removed'))
        else:
            logger.info(f'CMD starboard remove: NOT FOUND guild={ctx.guild.id} emoji={emoji} '
                        f'original_msg={original_message_id} by user={ctx.author.id}')
            await ctx.send(embed=discord_common.embed_alert('Not found in database'))

    async def _impl_show(self, ctx):
        entries = cf_common.user_db.get_starboard_emojis_for_guild(ctx.guild.id)
        if not entries:
            raise StarboardCogError('No starboard emojis configured.')

        aliases = cf_common.user_db.get_all_aliases_for_guild(ctx.guild.id)
        alias_map = {}
        for a in aliases:
            alias_map.setdefault(a.main_emoji, []).append(a.alias_emoji)

        lines = []
        for e in entries:
            channel = f'<#{e.channel_id}>' if e.channel_id else 'not set'
            color = f'#{e.color:06x}' if e.color is not None else 'default'
            line = f'{e.emoji}  threshold={e.threshold}  color={color}  channel={channel}'
            emoji_aliases = alias_map.get(e.emoji)
            if emoji_aliases:
                line += f'  aliases={", ".join(emoji_aliases)}'
            lines.append(line)

        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    # --- fix / backfill_status impls ---

    async def _impl_fix(self, ctx, message_ref, emoji):
        # Parse message reference — link or bare ID
        parsed = _parse_jump_url(message_ref)
        if parsed:
            _, channel_id, message_id = parsed
        else:
            try:
                message_id = int(message_ref)
            except ValueError:
                raise StarboardCogError('Provide a message link or a numeric message ID.')
            channel_id = None

        # Find starboard entries for this message
        entries = cf_common.user_db.get_starboard_entries_for_message(message_id)
        if not entries:
            raise StarboardCogError(f'Message `{message_id}` is not on the starboard.')

        if emoji is not None:
            entries = [e for e in entries if e.emoji == emoji]
            if not entries:
                raise StarboardCogError(f'Message `{message_id}` has no starboard entry for {emoji}.')

        # Resolve channel — from link, from DB, or from the current channel
        if channel_id is None:
            stored_ch = next((e.channel_id for e in entries if e.channel_id), None)
            if stored_ch:
                channel_id = int(stored_ch)
            else:
                raise StarboardCogError(
                    'Cannot determine the source channel. Use a message link instead.')

        channel = self.bot.get_channel(channel_id)
        if channel is None:
            raise StarboardCogError(f'Channel `{channel_id}` not found in bot cache.')

        try:
            message = await channel.fetch_message(message_id)
        except discord.NotFound:
            raise StarboardCogError(f'Message `{message_id}` not found in <#{channel_id}>.')

        fixed = []
        for entry in entries:
            _, old_count = cf_common.user_db.get_starboard_reaction_counts(
                ctx.guild.id, message_id, entry.emoji)
            _, new_count = await self._sync_board_reactions(
                message, ctx.guild.id, entry.emoji, force=True)
            cf_common.user_db.update_starboard_author_and_count(
                message_id, entry.emoji, str(message.author.id), new_count,
                channel_id=channel_id,
            )
            await self._update_starboard_message(
                ctx.guild.id, message_id, entry.emoji, new_count,
                original_message=message,
            )
            fixed.append(f'{entry.emoji}: {old_count} → {new_count}')
            logger.info(f'CMD starboard fix: msg={message_id} emoji={entry.emoji} '
                        f'old={old_count} new={new_count} by user={ctx.author.id}')

        await ctx.send(embed=discord_common.embed_success(
            'Resynced reactors:\n' + '\n'.join(fixed)
        ))

    async def _impl_backfill_status(self, ctx):
        if self.backfill_complete:
            await ctx.send(embed=discord_common.embed_success(
                f'Backfill complete: {self.backfill_done}/{self.backfill_total} messages '
                f'({self.backfill_failed} failed)'
            ))
        elif self.backfill_running:
            await ctx.send(embed=discord_common.embed_neutral(
                f'Backfill in progress: {self.backfill_done}/{self.backfill_total} messages '
                f'({self.backfill_failed} failed)',
                color=discord_common._ALERT_AMBER
            ))
        else:
            await ctx.send(embed=discord_common.embed_neutral('No backfill running.'))

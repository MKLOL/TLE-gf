"""Core starboard logic split out of the cog.

`CoreMixin` carries the reaction/threshold engine, the message-update
machinery, the leaderboard page builders, and thin ``_impl_*`` helpers
that back the command callbacks defined on the cog.  These are plain
methods (not commands.Cog stuff) so the cog can inherit them.
"""
import asyncio
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import ranking
from tle.cogs._starboard_helpers import _emoji_str
from tle.cogs._starboard_render import (
    _starboard_content,
    build_starboard_message as _build_sb_msg,
)

logger = logging.getLogger(__name__)

# When True, every starboard update fully re-renders the embed from the
# original message instead of just patching the count in the content line.
FULL_RE_RENDER = True


class StarboardCogError(commands.CommandError):
    pass


class CoreMixin:
    """Reaction engine, starboard message updates, and command impl helpers.

    Expects the host class to provide: ``self.bot``, ``self.locks`` (dict),
    and the ``build_starboard_message`` staticmethod.  ``StarboardCogError``
    is resolved lazily through the module attribute so the cog can override
    it with its own subclass.
    """

    build_starboard_message = staticmethod(_build_sb_msg)

    async def _resolve_channel(self, channel_id):
        """Get a channel from cache, falling back to fetch_channel for threads."""
        ch = self.bot.get_channel(channel_id)
        if ch is not None:
            return ch
        return await self.bot.fetch_channel(channel_id)

    @staticmethod
    def _user_default_emoji(guild_id, user_id):
        """Per-user default emoji for ``;starboard`` leaderboard commands.

        Falls back to ``constants._DEFAULT_STAR`` when the user has no saved
        preference for this guild.  Resolution order in callers is:
        explicit arg > this helper > the function's own default.
        """
        saved = cf_common.user_db.get_user_starboard_default(guild_id, user_id)
        return saved or constants._DEFAULT_STAR

    @staticmethod
    def _resolve_emoji(guild_id, emoji_str):
        """Resolve an emoji to its main emoji (if alias) and return (main_emoji, entry).

        Returns (main_emoji, entry) where entry is the starboard config for the main emoji,
        or (emoji_str, None) if the emoji is not configured and not an alias.
        """
        entry = cf_common.user_db.get_starboard_entry(guild_id, emoji_str)
        if entry is not None:
            return emoji_str, entry
        # Check if it's an alias
        main_emoji = cf_common.user_db.resolve_alias(guild_id, emoji_str)
        if main_emoji is not None:
            entry = cf_common.user_db.get_starboard_entry(guild_id, main_emoji)
            return main_emoji, entry
        return emoji_str, None

    # --- Reaction listener helpers ---

    async def _handle_reaction_add(self, payload):
        """Body of on_raw_reaction_add (see cog listener)."""
        if payload.guild_id is None:
            return
        if cf_common.user_db is None:
            return
        channel = self.bot.get_channel(payload.channel_id)
        if channel is not None and getattr(channel, 'nsfw', False):
            return
        raw_emoji = _emoji_str(payload.emoji)
        main_emoji, entry = self._resolve_emoji(payload.guild_id, raw_emoji)
        if entry is None:
            return
        if entry.channel_id is None:
            return  # Emoji configured but no starboard channel set yet
        # Starboard posts are display surfaces, not starrable content — a
        # reaction on one is forwarded to the original message it points at
        # (see ProxyReactionMixin).  Anything else living in a board channel
        # stays excluded entirely, so star/pill spam on a board can't put a
        # board message onto another board.
        sb_row = cf_common.user_db.get_starboard_message_by_starboard_id(payload.message_id)
        if sb_row is None and cf_common.user_db.is_starboard_channel(
                payload.guild_id, payload.channel_id):
            return
        channel_id, threshold, color = int(entry.channel_id), entry.threshold, entry.color
        logger.debug(f'Reaction add: raw_emoji={raw_emoji} main_emoji={main_emoji} '
                     f'guild={payload.guild_id} msg={payload.message_id} user={payload.user_id} '
                     f'threshold={threshold} starboard_channel={channel_id}')
        try:
            if sb_row is not None:
                await self._handle_proxy_reaction_add(
                    payload, sb_row, main_emoji, entry, raw_emoji)
            else:
                await self.check_and_add_to_starboard(
                    channel_id, threshold, color, main_emoji, payload, raw_emoji=raw_emoji,
                )
        except StarboardCogError as e:
            logger.info(f'Failed to starboard msg={payload.message_id} emoji={main_emoji}: {e!r}')
        except Exception as e:
            logger.error(f'Unexpected error in starboard processing msg={payload.message_id} '
                         f'emoji={main_emoji} guild={payload.guild_id}: {e}', exc_info=True)

    async def _handle_reaction_remove(self, payload):
        """Body of on_raw_reaction_remove (see cog listener)."""
        if payload.guild_id is None:
            return
        if cf_common.user_db is None:
            return
        channel = self.bot.get_channel(payload.channel_id)
        if channel is not None and getattr(channel, 'nsfw', False):
            return
        raw_emoji = _emoji_str(payload.emoji)
        main_emoji, entry = self._resolve_emoji(payload.guild_id, raw_emoji)
        if entry is None:
            return
        logger.debug(f'Reaction remove: raw_emoji={raw_emoji} main_emoji={main_emoji} '
                     f'guild={payload.guild_id} msg={payload.message_id} user={payload.user_id}')
        # Un-reacting on a starboard post removes the proxy reactor for the
        # original message instead (see ProxyReactionMixin).
        sb_row = cf_common.user_db.get_starboard_message_by_starboard_id(payload.message_id)
        if sb_row is not None:
            await self._handle_proxy_reaction_remove(payload, sb_row, main_emoji, raw_emoji)
            return
        # Always remove the reactor from DB — even if the message isn't on the
        # starboard yet.  This prevents ghost reactors from inflating counts
        # when a user reacts then un-reacts before the threshold is reached.
        cf_common.user_db.remove_reactor(payload.message_id, raw_emoji, payload.user_id)

        # Update starboard display if the message is already tracked
        if cf_common.user_db.check_exists_starboard_message_v1(payload.message_id, main_emoji):
            await self._refresh_starboard_display(
                payload.guild_id, payload.message_id, payload.channel_id, main_emoji)

    async def _refresh_starboard_display(self, guild_id, original_msg_id,
                                         source_channel_id, main_emoji):
        """Recount a tracked message and re-render its starboard post.

        Shared by the physical and proxy reaction-remove paths."""
        lock = self.locks.get(guild_id)
        if lock is None:
            self.locks[guild_id] = lock = asyncio.Lock()
        async with lock:
            try:
                try:
                    channel = await self._resolve_channel(source_channel_id)
                except discord.NotFound:
                    logger.warning(f'Reaction remove: channel {source_channel_id} not found')
                    return
                emoji_family = cf_common.user_db.get_emoji_family(guild_id, main_emoji)
                count = cf_common.user_db.get_merged_reactor_count(original_msg_id, emoji_family)
                message = await channel.fetch_message(original_msg_id)
                cf_common.user_db.update_starboard_author_and_count(
                    original_msg_id, main_emoji, str(message.author.id), count
                )
                logger.info(f'Updated star count for msg={original_msg_id} emoji={main_emoji} '
                            f'author={message.author.id} new_count={count}')
                await self._update_starboard_message(
                    guild_id, original_msg_id, main_emoji, count,
                    original_message=message,
                )
            except discord.NotFound:
                logger.warning(f'Reaction remove: message {original_msg_id} not found '
                               f'(may have been deleted)')
            except Exception as e:
                logger.warning(f'Failed to update star count on reaction remove for '
                               f'msg={original_msg_id}: {e}', exc_info=True)

    @staticmethod
    def _handle_message_delete(payload):
        """Body of on_raw_message_delete (see cog listener)."""
        if payload.guild_id is None:
            return
        if cf_common.user_db is None:
            return
        rc = cf_common.user_db.remove_starboard_message(starboard_msg_id=payload.message_id)
        if rc:
            logger.info(f'Cleaned up deleted starboard message: starboard_msg={payload.message_id} '
                        f'guild={payload.guild_id}')

    # --- Core logic ---

    @staticmethod
    def _is_old_format(sb_msg):
        """Check if a starboard message uses the old embed format.

        Old format has embed fields like 'Jump to' and 'Channel'.
        """
        for embed in sb_msg.embeds:
            for f in getattr(embed, 'fields', []):
                name = f.name if hasattr(f, 'name') else f.get('name')
                if name in ('Jump to', 'Channel'):
                    return True
        return False

    async def _resync_reactors(self, message, emoji_family):
        """Resync reactors for a message from Discord to the DB.

        Fetches actual reactors via the Discord API and replaces the DB rows.
        Returns the new merged reactor count.
        """
        emoji_family_set = set(emoji_family)
        new_reactors = []
        for r in message.reactions:
            r_emoji = _emoji_str(r)
            if r_emoji in emoji_family_set:
                async for user in r.users():
                    new_reactors.append((r_emoji, str(user.id)))
        cf_common.user_db.replace_reactors(message.id, emoji_family, new_reactors)
        return cf_common.user_db.get_merged_reactor_count(message.id, emoji_family)

    async def _update_starboard_message(self, guild_id, original_msg_id, emoji_str, count,
                                        original_message=None):
        """Edit the starboard message to reflect an updated reaction count.

        If FULL_RE_RENDER is True or the message uses the old embed format,
        the entire starboard post is rebuilt from the original message.
        Otherwise only the count in the content line is updated.
        """
        sb_entry = cf_common.user_db.get_starboard_message_v1(original_msg_id, emoji_str)
        if sb_entry is None or sb_entry.starboard_msg_id is None:
            return
        entry = cf_common.user_db.get_starboard_entry(guild_id, emoji_str)
        if entry is None or entry.channel_id is None:
            return
        sb_channel = self.bot.get_channel(int(entry.channel_id))
        if sb_channel is None:
            return
        try:
            sb_msg = await sb_channel.fetch_message(int(sb_entry.starboard_msg_id))

            if FULL_RE_RENDER or self._is_old_format(sb_msg):
                if original_message is None:
                    if not sb_entry.channel_id:
                        return
                    try:
                        source_ch = await self._resolve_channel(int(sb_entry.channel_id))
                    except discord.NotFound:
                        return
                    original_message = await source_ch.fetch_message(int(original_msg_id))
                content, embeds, files = await self.build_starboard_message(
                    original_message, emoji_str, count, entry.color
                )
                await sb_msg.edit(content=content, embeds=embeds, attachments=files)
                logger.info(f'Full re-render starboard message: msg={original_msg_id} '
                            f'emoji={emoji_str} count={count}')
            else:
                source_channel_id = sb_entry.channel_id or '0'
                jump_url = f'https://discord.com/channels/{guild_id}/{source_channel_id}/{original_msg_id}'
                new_content = _starboard_content(emoji_str, count, jump_url)
                await sb_msg.edit(content=new_content)
                logger.debug(f'Live-updated starboard content: msg={original_msg_id} '
                             f'emoji={emoji_str} count={count}')
        except Exception as e:
            logger.warning(f'Failed to live-update starboard message for '
                           f'original={original_msg_id}: {e}')

    async def check_and_add_to_starboard(self, starboard_channel_id, threshold, color,
                                          emoji_str, payload, raw_emoji=None,
                                          record_reactor=True):
        """Check if a message meets the starboard threshold and post/update it.

        emoji_str is the main emoji. raw_emoji is the actual emoji the user reacted with
        (may be an alias). If raw_emoji is None, it defaults to emoji_str.
        record_reactor=False skips storing the payload's reactor row — used by
        the proxy path, which has already stored it as a proxy reactor.
        """
        if raw_emoji is None:
            raw_emoji = emoji_str
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            raise StarboardCogError(f'Guild {payload.guild_id} not found in bot cache')
        starboard_channel = guild.get_channel(starboard_channel_id)
        if starboard_channel is None:
            raise StarboardCogError(f'Starboard channel {starboard_channel_id} not found in guild {guild.id}')

        try:
            channel = await self._resolve_channel(payload.channel_id)
        except discord.NotFound:
            raise StarboardCogError(f'Source channel {payload.channel_id} not found')
        message = await channel.fetch_message(payload.message_id)

        snapshots = getattr(message, 'message_snapshots', None) or ()
        if ((message.type != discord.MessageType.default and message.type != discord.MessageType.reply)
                or (len(message.content) == 0 and len(message.attachments) == 0
                    and len(message.embeds) == 0 and not snapshots)):
            raise StarboardCogError(f'Cannot starboard message {message.id}: invalid type or empty content')

        if record_reactor:
            # Track the reactor under the raw emoji they actually used
            cf_common.user_db.add_reactor(message.id, raw_emoji, payload.user_id)

        # Count = union of unique reactors (original message + starboard posts)
        # across main + all aliases
        emoji_family = cf_common.user_db.get_emoji_family(payload.guild_id, emoji_str)
        reaction_count = cf_common.user_db.get_merged_reactor_count(message.id, emoji_family)

        # Self-healing: if Discord shows more reactions than the DB knows about
        # (e.g. reactions added while the bot had a bug), sync from Discord.
        # Discord's counts only cover reactions physically on this message, so
        # compare against the physical pool — proxy reactors live elsewhere.
        emoji_family_set = set(emoji_family)
        discord_count = sum(r.count for r in message.reactions
                            if _emoji_str(r) in emoji_family_set)
        physical_count = cf_common.user_db.get_merged_physical_reactor_count(
            message.id, emoji_family)
        if discord_count > physical_count:
            logger.info(f'Reactor drift on new msg={message.id} emoji={emoji_str}: '
                        f'db_count={physical_count} discord_count={discord_count}, resyncing')
            reaction_count = await self._resync_reactors(message, emoji_family)

        logger.debug(f'Message {message.id}: {emoji_str} (family={emoji_family}) '
                     f'union_count={reaction_count} threshold={threshold}')
        # Below threshold blocks the *initial* post but must not block updates
        # to a message that's already on the starboard — otherwise re-adding a
        # reaction after the count fell below threshold leaves the displayed
        # count frozen at its last value.
        if reaction_count < threshold:
            if not cf_common.user_db.check_exists_starboard_message_v1(message.id, emoji_str):
                return

        lock = self.locks.get(payload.guild_id)
        if lock is None:
            self.locks[payload.guild_id] = lock = asyncio.Lock()

        async with lock:
            # Recount inside the lock to avoid stale values from concurrent removes
            reaction_count = cf_common.user_db.get_merged_reactor_count(message.id, emoji_family)
            already_exists = cf_common.user_db.check_exists_starboard_message_v1(message.id, emoji_str)
            if already_exists:
                # Self-healing: if the physical DB count exceeds visible Discord
                # reactions, resync from Discord to purge ghost entries.  Proxy
                # reactors are excluded — they legitimately exceed what is
                # visible on the original message.
                emoji_family_set = set(emoji_family)
                discord_count = sum(r.count for r in message.reactions
                                    if _emoji_str(r) in emoji_family_set)
                physical_count = cf_common.user_db.get_merged_physical_reactor_count(
                    message.id, emoji_family)
                if physical_count > discord_count:
                    logger.info(f'Reactor drift detected for msg={message.id} emoji={emoji_str}: '
                                f'db_count={physical_count} discord_count={discord_count}, resyncing')
                    reaction_count = await self._resync_reactors(message, emoji_family)
                cf_common.user_db.update_starboard_author_and_count(
                    message.id, emoji_str, str(message.author.id), reaction_count
                )
                logger.debug(f'Updated existing starboard entry: msg={message.id} emoji={emoji_str} '
                             f'author={message.author.id} count={reaction_count}')
                await self._update_starboard_message(
                    payload.guild_id, message.id, emoji_str, reaction_count,
                    original_message=message,
                )
                return

            if reaction_count < threshold:
                return  # Concurrent remove dropped count below threshold

            content, embeds, files = await self.build_starboard_message(
                message, emoji_str, reaction_count, color
            )
            starboard_message = await starboard_channel.send(
                content=content, embeds=embeds, files=files,
            )
            cf_common.user_db.add_starboard_message_v1(
                message.id, starboard_message.id, guild.id, emoji_str,
                author_id=str(message.author.id),
                channel_id=str(channel.id)
            )
            # Collect all current reactors for each emoji in the family
            for r in message.reactions:
                r_emoji = _emoji_str(r)
                if r_emoji in emoji_family:
                    user_ids = [str(user.id) async for user in r.users()]
                    cf_common.user_db.bulk_add_reactors(message.id, r_emoji, user_ids)
            # Count update runs after reactor collection so the sticky
            # narcissus hook sees a self-react that predates the post.
            cf_common.user_db.update_starboard_star_count(message.id, emoji_str, reaction_count)
            logger.info(f'NEW starboard entry: original_msg={message.id} starboard_msg={starboard_message.id} '
                        f'guild={guild.id} emoji={emoji_str} author={message.author} ({message.author.id}) '
                        f'channel={channel.id} count={reaction_count} '
                        f'(triggered by user {payload.user_id})')

    # --- Leaderboard page builders ---

    @staticmethod
    def _get_user_id(row):
        """Extract user ID from a leaderboard row (author_id or user_id)."""
        return getattr(row, 'author_id', None) or row.user_id

    @staticmethod
    def _get_count(row):
        """Extract count from a leaderboard row."""
        for attr in ('message_count', 'total_stars', 'stars_given', 'self_stars'):
            val = getattr(row, attr, None)
            if val is not None:
                return val
        return 0

    @staticmethod
    def _row_suffix(row):
        """Suffix for rows carrying an `unreacted` column (narcissus): how
        many sticky self-stars had their reaction sneakily removed."""
        unreacted = getattr(row, 'unreacted', None)
        return f' ({unreacted} unreacted)' if unreacted else ''

    def _get_personal_rank_line(self, ctx, ranked, unit):
        """Get the invoking user's rank as a string for embedding. `ranked`
        is the output of `ranking.rank_items` — `(rank, row)` pairs in display
        order, so tied users share a rank."""
        user_id_str = str(ctx.author.id)
        for rank, row in ranked:
            if self._get_user_id(row) == user_id_str:
                count = self._get_count(row)
                return (f'\nYour rank: **#{rank}** with **{count}** {unit}'
                        f'{self._row_suffix(row)}')
        return '\nYou are not on this leaderboard yet.'

    def _make_leaderboard_pages(self, ctx, rows, emoji, title, unit):
        """Build paginated embed pages from leaderboard rows."""
        # Rank the whole list once with standard competition ranking so tied
        # counts share a rank rather than being split by the query's secondary
        # sort; ties also number correctly across page boundaries.
        ranked = ranking.rank_items(rows, self._get_count)
        personal = self._get_personal_rank_line(ctx, ranked, unit)
        per_page = 10
        chunks = paginator.chunkify(ranked, per_page)
        pages = []
        for chunk in chunks:
            lines = []
            for rank, row in chunk:
                user_id = self._get_user_id(row)
                count = self._get_count(row)
                member = ctx.guild.get_member(int(user_id))
                name = member.mention if member else f'<@{user_id}>'
                lines.append(f'**#{rank}** {name} — {count} {unit}{self._row_suffix(row)}')
            lines.append(personal)
            embed = discord.Embed(
                title=f'{emoji} {title}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color()
            )
            pages.append((None, embed))
        return pages

    def _paginate_leaderboard(self, ctx, rows, emoji, title, unit):
        """Build leaderboard pages and start the paginator."""
        pages = self._make_leaderboard_pages(ctx, rows, emoji, title, unit)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    # --- Leaderboard guard / lookup ---

    def _leaderboard_entry(self, ctx, emoji):
        """Validate the feature gate + emoji config for a leaderboard command.

        Returns the configured main emoji. Raises StarboardCogError on failure.
        """
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        main_emoji, entry = self._resolve_emoji(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        return main_emoji

import asyncio
import datetime
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.cogs._starboard_helpers import _emoji_str
from tle.cogs._starboard_backfill import BackfillMixin, _BACKFILL_UNKNOWN

logger = logging.getLogger(__name__)

_TIMELINE_KEYWORDS = {'week', 'month', 'year'}

# No time bound sentinel — matches the DB layer constant
_NO_TIME_BOUND = 10 ** 10

# Muted embed color for reply context
_REPLY_EMBED_COLOR = discord.Color.from_rgb(47, 49, 54)

# Image extensions that Discord can render inline in embeds
_IMAGE_EXTENSIONS = ('png', 'jpeg', 'jpg', 'gif', 'webp')

# Video extensions that need to be re-uploaded as files
_VIDEO_EXTENSIONS = ('mp4', 'mov', 'webm')

# When True, every starboard update fully re-renders the embed from the
# original message instead of just patching the count in the content line.
FULL_RE_RENDER = True


def _starboard_content(emoji_str, count, jump_url):
    """Build the header line for a starboard message.

    Format: ⭐ **5** | jump_url
    The jump URL is auto-linked by Discord in message content.
    """
    return f'{emoji_str} **{count}** | {jump_url}'


def _parse_starboard_args(args, default_emoji=constants._DEFAULT_STAR):
    """Parse args for starboard leaderboard/top commands.

    Returns (emoji, dlo, dhi) where dlo/dhi are unix timestamps (seconds).
    Supports:
      - timeline keywords: week, month, year
      - date ranges: d>=[[dd]mm]yyyy  d<[[dd]mm]yyyy
      - emoji (anything else that isn't a keyword or date arg)
    If no emoji is provided, defaults to default_emoji.
    If no time filter is provided, dlo=0 and dhi=_NO_TIME_BOUND.
    """
    emoji = None
    dlo = 0
    dhi = _NO_TIME_BOUND

    for arg in args:
        lower = arg.lower()
        if lower in _TIMELINE_KEYWORDS:
            now = datetime.datetime.now()
            if lower == 'week':
                # Monday of this week at 00:00
                monday = now - datetime.timedelta(days=now.weekday())
                dlo = time.mktime(monday.replace(hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'month':
                dlo = time.mktime(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'year':
                dlo = time.mktime(now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
        elif lower.startswith('d>='):
            dlo = max(dlo, cf_common.parse_date(arg[3:]))
        elif lower.startswith('d<'):
            dhi = min(dhi, cf_common.parse_date(arg[2:]))
        else:
            emoji = arg

    if emoji is None:
        emoji = default_emoji
        logger.debug(f'No emoji specified, falling back to default: {default_emoji!r}')
    return emoji, dlo, dhi


class StarboardCogError(commands.CommandError):
    pass


class Starboard(BackfillMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.locks = {}
        self._init_backfill_state()
        logger.info('Starboard cog initialized')

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        logger.info('Starboard cog on_ready fired, launching backfill task')
        asyncio.create_task(self._backfill_star_counts())

    # --- Building starboard messages ---

    @staticmethod
    async def build_starboard_message(message, emoji_str, count, color):
        """Build content, embeds, and files for a starboard message.

        Returns (content, embeds, files) where:
          - content: the header line with emoji count and jump URL
          - embeds: list of Embed objects (reply context + main + carried-over)
          - files: list of discord.File for video attachments

        For videos: author goes in the content header so the rendering
        order is: content (author) → file attachment (video) → embeds.
        For non-video: author goes in the main embed via set_author.
        """
        content = _starboard_content(emoji_str, count, message.jump_url)
        embeds = []
        files = []

        # Scan attachments to categorise them
        image_url = None
        video_attachments = []
        other_attachments = []
        for att in message.attachments:
            ext = att.filename.lower().rsplit('.', 1)[-1] if '.' in att.filename else ''
            if ext in _IMAGE_EXTENSIONS:
                if image_url is None:
                    image_url = att.url
            elif ext in _VIDEO_EXTENSIONS:
                video_attachments.append(att)
            else:
                other_attachments.append(att)

        has_video = bool(video_attachments)

        # For video messages, put author in the content header so it
        # appears above the video player (file attachments render after
        # content but before embeds).
        if has_video:
            safe_name = discord.utils.escape_mentions(message.author.display_name)
            content = (
                f'{emoji_str} **{count}** \u00b7 **{safe_name}** '
                f'| {message.jump_url}'
            )
            for att in video_attachments:
                try:
                    files.append(await att.to_file())
                except Exception:
                    logger.debug(f'Failed to download video attachment {att.filename}')

        # --- Reply context embed (goes first / above main embed) ---
        if message.reference and message.reference.message_id:
            try:
                ref_msg = message.reference.resolved
                if ref_msg is None or isinstance(ref_msg, discord.DeletedReferencedMessage):
                    ref_msg = await message.channel.fetch_message(message.reference.message_id)
                reply_embed = discord.Embed(
                    color=_REPLY_EMBED_COLOR,
                    timestamp=ref_msg.created_at,
                )
                reply_embed.set_author(
                    name=f'Replying to {ref_msg.author.display_name}',
                    icon_url=ref_msg.author.display_avatar.url,
                )
                if ref_msg.content:
                    text = ref_msg.content
                    if len(text) > 4096:
                        text = text[:4093] + '...'
                    reply_embed.description = text
                embeds.append(reply_embed)
            except Exception:
                logger.debug(f'Could not fetch referenced message {message.reference.message_id}')

        # --- Main embed ---
        # For video-only messages (no text), skip the main embed entirely.
        has_text = bool(message.content)
        has_other = bool(other_attachments)
        need_embed = not has_video or has_text or has_other or image_url

        if need_embed:
            embed = discord.Embed(color=color, timestamp=message.created_at)
            if not has_video:
                embed.set_author(
                    name=message.author.display_name,
                    icon_url=message.author.display_avatar.url,
                    url=message.jump_url,
                )

            if has_text:
                text = message.content
                if len(text) > 4096:
                    text = text[:4093] + '...'
                embed.description = text

            if image_url:
                embed.set_image(url=image_url)

            for att in other_attachments:
                embed.add_field(
                    name='Attachment', value=f'[{att.filename}]({att.url})', inline=False
                )

            # Pull an image from the original message's embeds if we don't
            # already have one from attachments.
            if not image_url and message.embeds:
                for e in message.embeds:
                    if e.type == 'image' and e.url:
                        embed.set_image(url=e.url)
                        break
                    if e.type == 'rich' and e.image and e.image.url:
                        embed.set_image(url=e.image.url)
                        break
                    if e.thumbnail and e.thumbnail.url:
                        embed.set_image(url=e.thumbnail.url)
                        break

            embeds.append(embed)

        # Carry over rich and link embeds from the original message (bot
        # embeds like Codeforces problem cards, and URL previews like blog
        # post link previews). Skip image/video/gifv auto-embeds.
        for e in message.embeds:
            if e.type in ('rich', 'link', 'article'):
                embeds.append(e)

        # Discord allows a maximum of 10 embeds per message
        embeds = embeds[:10]

        return content, embeds, files

    # --- Event listeners ---

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

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
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
        channel_id, threshold, color = int(entry.channel_id), entry.threshold, entry.color
        logger.debug(f'Reaction add: raw_emoji={raw_emoji} main_emoji={main_emoji} '
                     f'guild={payload.guild_id} msg={payload.message_id} user={payload.user_id} '
                     f'threshold={threshold} starboard_channel={channel_id}')
        try:
            await self.check_and_add_to_starboard(
                channel_id, threshold, color, main_emoji, payload, raw_emoji=raw_emoji,
            )
        except StarboardCogError as e:
            logger.info(f'Failed to starboard msg={payload.message_id} emoji={main_emoji}: {e!r}')
        except Exception as e:
            logger.error(f'Unexpected error in starboard processing msg={payload.message_id} '
                         f'emoji={main_emoji} guild={payload.guild_id}: {e}', exc_info=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
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
        # Always remove the reactor from DB — even if the message isn't on the
        # starboard yet.  This prevents ghost reactors from inflating counts
        # when a user reacts then un-reacts before the threshold is reached.
        cf_common.user_db.remove_reactor(payload.message_id, raw_emoji, payload.user_id)

        # Update starboard display if the message is already tracked
        if cf_common.user_db.check_exists_starboard_message_v1(payload.message_id, main_emoji):
            lock = self.locks.get(payload.guild_id)
            if lock is None:
                self.locks[payload.guild_id] = lock = asyncio.Lock()
            async with lock:
                try:
                    channel = self.bot.get_channel(payload.channel_id)
                    if channel is None:
                        logger.warning(f'Reaction remove: channel {payload.channel_id} not found in cache')
                        return
                    emoji_family = cf_common.user_db.get_emoji_family(payload.guild_id, main_emoji)
                    count = cf_common.user_db.get_merged_reactor_count(payload.message_id, emoji_family)
                    message = await channel.fetch_message(payload.message_id)
                    cf_common.user_db.update_starboard_author_and_count(
                        payload.message_id, main_emoji, str(message.author.id), count
                    )
                    logger.info(f'Updated star count for msg={payload.message_id} emoji={main_emoji} '
                                f'author={message.author.id} new_count={count}')
                    await self._update_starboard_message(
                        payload.guild_id, payload.message_id, main_emoji, count,
                        original_message=message,
                    )
                except discord.NotFound:
                    logger.warning(f'Reaction remove: message {payload.message_id} not found '
                                   f'(may have been deleted)')
                except Exception as e:
                    logger.warning(f'Failed to update star count on reaction remove for '
                                   f'msg={payload.message_id}: {e}', exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
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
                    source_ch = self.bot.get_channel(int(sb_entry.channel_id)) if sb_entry.channel_id else None
                    if source_ch is None:
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
                                          emoji_str, payload, raw_emoji=None):
        """Check if a message meets the starboard threshold and post/update it.

        emoji_str is the main emoji. raw_emoji is the actual emoji the user reacted with
        (may be an alias). If raw_emoji is None, it defaults to emoji_str.
        """
        if raw_emoji is None:
            raw_emoji = emoji_str
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            raise StarboardCogError(f'Guild {payload.guild_id} not found in bot cache')
        starboard_channel = guild.get_channel(starboard_channel_id)
        if starboard_channel is None:
            raise StarboardCogError(f'Starboard channel {starboard_channel_id} not found in guild {guild.id}')

        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            raise StarboardCogError(f'Source channel {payload.channel_id} not found in bot cache')
        message = await channel.fetch_message(payload.message_id)
        if ((message.type != discord.MessageType.default and message.type != discord.MessageType.reply)
                or (len(message.content) == 0 and len(message.attachments) == 0
                    and len(message.embeds) == 0)):
            raise StarboardCogError(f'Cannot starboard message {message.id}: invalid type or empty content')

        # Track the reactor under the raw emoji they actually used
        cf_common.user_db.add_reactor(message.id, raw_emoji, payload.user_id)

        # Count = union of unique reactors across main + all aliases
        emoji_family = cf_common.user_db.get_emoji_family(payload.guild_id, emoji_str)
        reaction_count = cf_common.user_db.get_merged_reactor_count(message.id, emoji_family)

        logger.debug(f'Message {message.id}: {emoji_str} (family={emoji_family}) '
                     f'union_count={reaction_count} threshold={threshold}')
        if reaction_count < threshold:
            return

        lock = self.locks.get(payload.guild_id)
        if lock is None:
            self.locks[payload.guild_id] = lock = asyncio.Lock()

        async with lock:
            # Recount inside the lock to avoid stale values from concurrent removes
            reaction_count = cf_common.user_db.get_merged_reactor_count(message.id, emoji_family)
            already_exists = cf_common.user_db.check_exists_starboard_message_v1(message.id, emoji_str)
            if already_exists:
                # Self-healing: if DB count exceeds visible Discord reactions,
                # resync reactors from Discord to purge ghost entries.
                emoji_family_set = set(emoji_family)
                discord_count = sum(r.count for r in message.reactions
                                    if _emoji_str(r) in emoji_family_set)
                if reaction_count > discord_count:
                    logger.info(f'Reactor drift detected for msg={message.id} emoji={emoji_str}: '
                                f'db_count={reaction_count} discord_count={discord_count}, resyncing')
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
            cf_common.user_db.update_starboard_star_count(message.id, emoji_str, reaction_count)
            # Collect all current reactors for each emoji in the family
            for r in message.reactions:
                r_emoji = _emoji_str(r)
                if r_emoji in emoji_family:
                    user_ids = [str(user.id) async for user in r.users()]
                    cf_common.user_db.bulk_add_reactors(message.id, r_emoji, user_ids)
            logger.info(f'NEW starboard entry: original_msg={message.id} starboard_msg={starboard_message.id} '
                        f'guild={guild.id} emoji={emoji_str} author={message.author} ({message.author.id}) '
                        f'channel={channel.id} count={reaction_count} '
                        f'(triggered by user {payload.user_id})')

    # --- Commands ---

    @commands.group(brief='Starboard commands', invoke_without_command=True)
    async def starboard(self, ctx):
        """Group for commands involving the starboard."""
        await ctx.send_help(ctx.command)

    @starboard.command(brief='Add an emoji to the starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def add(self, ctx, emoji: str = constants._DEFAULT_STAR, threshold: int = 3, color: str = None):
        """Add an emoji to the starboard with optional threshold and color.
        Example: ;starboard add ⭐ 3 #ffaa10"""
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

    @starboard.command(brief='Delete an emoji from starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def delete(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Remove an emoji and all its tracked messages from the starboard."""
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        cf_common.user_db.remove_starboard_emoji(ctx.guild.id, emoji)
        logger.info(f'CMD starboard delete: guild={ctx.guild.id} emoji={emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Removed {emoji} from starboard'))

    @starboard.command(brief='Edit threshold for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def edit_threshold(self, ctx, threshold: int, emoji: str = constants._DEFAULT_STAR):
        """Update the reaction threshold for an emoji."""
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

    @starboard.command(brief='Edit color for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def edit_color(self, ctx, color: str, emoji: str = constants._DEFAULT_STAR):
        """Update the embed color for an emoji. Use hex format like #ffaa10."""
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

    @starboard.command(brief='Set starboard channel for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Set the current channel as the starboard channel for a specific emoji.
        Example: ;starboard here ⭐"""
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

    @starboard.command(brief='Clear starboard channel for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def clear(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Clear the starboard channel for a specific emoji.
        Example: ;starboard clear ⭐"""
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured.')
        cf_common.user_db.clear_starboard_channel(ctx.guild.id, emoji)
        logger.info(f'CMD starboard clear: guild={ctx.guild.id} emoji={emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Starboard channel for {emoji} cleared'))

    @starboard.command(brief='Remove a message from starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def remove(self, ctx, original_message_id: int, emoji: str = constants._DEFAULT_STAR):
        """Remove a particular message from the starboard database for the given emoji."""
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

    # --- Alias commands ---

    @starboard.group(brief='Manage emoji aliases', invoke_without_command=True)
    async def alias(self, ctx):
        """Manage emoji aliases. Aliases count toward the main emoji's starboard."""
        await ctx.send_help(ctx.command)

    @alias.command(name='add', brief='Add an alias for a main emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def alias_add(self, ctx, alias_emoji: str, main_emoji: str = constants._DEFAULT_STAR):
        """Add an alias emoji that counts toward a main emoji's starboard.
        Example: ;starboard alias add 👍 ⭐"""
        # Validate main emoji is configured
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, main_emoji)
        if entry is None:
            raise StarboardCogError(f'Main emoji {main_emoji} is not configured for this starboard.')
        # Can't alias a main emoji
        if cf_common.user_db.get_starboard_entry(ctx.guild.id, alias_emoji) is not None:
            raise StarboardCogError(f'{alias_emoji} is already a main starboard emoji. '
                                    f'Remove it first before using it as an alias.')
        # Can't alias an alias
        existing = cf_common.user_db.resolve_alias(ctx.guild.id, alias_emoji)
        if existing is not None:
            raise StarboardCogError(f'{alias_emoji} is already an alias for {existing}. '
                                    f'Remove it first with `;starboard alias remove {alias_emoji}`.')
        cf_common.user_db.add_starboard_alias(ctx.guild.id, alias_emoji, main_emoji)
        logger.info(f'CMD starboard alias add: guild={ctx.guild.id} alias={alias_emoji} '
                    f'main={main_emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Added {alias_emoji} as alias for {main_emoji}'
        ))

    @alias.command(name='remove', brief='Remove an emoji alias')
    @commands.has_role(constants.TLE_ADMIN)
    async def alias_remove(self, ctx, alias_emoji: str):
        """Remove an alias emoji.
        Example: ;starboard alias remove 👍"""
        rc = cf_common.user_db.remove_starboard_alias(ctx.guild.id, alias_emoji)
        if not rc:
            raise StarboardCogError(f'{alias_emoji} is not an alias.')
        logger.info(f'CMD starboard alias remove: guild={ctx.guild.id} alias={alias_emoji} '
                    f'by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Removed alias {alias_emoji}'))

    @alias.command(name='list', brief='List all emoji aliases')
    async def alias_list(self, ctx):
        """Show all emoji aliases configured for this server."""
        rows = cf_common.user_db.get_all_aliases_for_guild(ctx.guild.id)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral('No aliases configured.'))
            return
        lines = [f'{r.alias_emoji} \u2192 {r.main_emoji}' for r in rows]
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    # --- Leaderboard commands ---

    @starboard.command(brief='Show starboard leaderboard by message count',
                       usage='[emoji] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def leaderboard(self, ctx, *args):
        """Show top users by number of starboarded messages for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports timeline filters: week, month, year, d>=date, d<date."""
        emoji, dlo, dhi = _parse_starboard_args(args)
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        rows = cf_common.user_db.get_starboard_leaderboard(ctx.guild.id, emoji, dlo, dhi)
        if not rows:
            raise StarboardCogError(f'No starboarded messages found for {emoji}.')

        logger.info(f'CMD starboard leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} {len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Starboard Leaderboard', 'messages')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True, author_id=ctx.author.id)

    @starboard.command(name='star-leaderboard', brief='Show starboard leaderboard by star count',
                       usage='[emoji] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def star_leaderboard(self, ctx, *args):
        """Show top users by total star count for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports timeline filters: week, month, year, d>=date, d<date."""
        emoji, dlo, dhi = _parse_starboard_args(args)
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        rows = cf_common.user_db.get_starboard_star_leaderboard(ctx.guild.id, emoji, dlo, dhi)
        if not rows:
            raise StarboardCogError(f'No star data found for {emoji}. '
                                    'Star counts are populated via backfill and live tracking.')

        logger.info(f'CMD starboard star-leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} {len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Star Leaderboard', 'stars')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True, author_id=ctx.author.id)

    @starboard.command(name='star-givers', brief='Show top star givers',
                       usage='[emoji] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def star_givers(self, ctx, *args):
        """Show top users by number of stars given (reactions) for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports timeline filters: week, month, year, d>=date, d<date."""
        emoji, dlo, dhi = _parse_starboard_args(args)
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        emoji_family = cf_common.user_db.get_emoji_family(ctx.guild.id, emoji)
        rows = cf_common.user_db.get_star_givers_leaderboard(ctx.guild.id, emoji, dlo, dhi,
                                                              emoji_family=emoji_family)
        if not rows:
            raise StarboardCogError(f'No reactor data found for {emoji}.')

        logger.info(f'CMD starboard star-givers: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} {len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Star Givers', 'stars given')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True, author_id=ctx.author.id)

    @starboard.command(brief='Show who stars their own messages the most',
                       usage='[emoji] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def narcissus(self, ctx, *args):
        """Show users who star their own messages the most.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports timeline filters: week, month, year, d>=date, d<date."""
        emoji, dlo, dhi = _parse_starboard_args(args)
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        emoji_family = cf_common.user_db.get_emoji_family(ctx.guild.id, emoji)
        rows = cf_common.user_db.get_narcissus_leaderboard(ctx.guild.id, emoji, dlo, dhi,
                                                            emoji_family=emoji_family)
        if not rows:
            raise StarboardCogError(f'No self-stars found for {emoji}. How humble!')

        logger.info(f'CMD starboard narcissus: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} {len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Narcissus Leaderboard', 'self-stars')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True, author_id=ctx.author.id)

    @starboard.command(brief='Show top starred messages',
                       usage='[emoji] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def top(self, ctx, *args):
        """Show top starboarded messages sorted by star count for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports timeline filters: week, month, year, d>=date, d<date."""
        emoji, dlo, dhi = _parse_starboard_args(args)
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config enable starboard_leaderboard`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        rows = cf_common.user_db.get_top_starboard_messages(ctx.guild.id, emoji, dlo, dhi)
        if not rows:
            raise StarboardCogError(f'No starred messages found for {emoji}.')

        logger.info(f'CMD starboard top: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} {len(rows)} messages by user={ctx.author.id}')

        per_page = 10
        chunks = paginator.chunkify(rows, per_page)
        pages = []
        for page_idx, chunk in enumerate(chunks):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                jump_url = f'https://discord.com/channels/{ctx.guild.id}/{row.channel_id}/{row.original_msg_id}'
                member = ctx.guild.get_member(int(row.author_id))
                name = member.mention if member else f'<@{row.author_id}>'
                lines.append(f'**#{rank}** {name} — **{row.star_count}** {emoji} — [Jump]({jump_url})')
            embed = discord.Embed(
                title=f'{emoji} Top Starred Messages',
                description='\n'.join(lines),
                color=discord_common.random_cf_color()
            )
            pages.append((None, embed))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True, author_id=ctx.author.id)

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

    def _get_personal_rank_line(self, ctx, rows, unit):
        """Get the invoking user's rank as a string for embedding."""
        user_id_str = str(ctx.author.id)
        for i, row in enumerate(rows):
            if self._get_user_id(row) == user_id_str:
                rank = i + 1
                count = self._get_count(row)
                return f'\nYour rank: **#{rank}** with **{count}** {unit}'
        return '\nYou are not on this leaderboard yet.'

    def _make_leaderboard_pages(self, ctx, rows, emoji, title, unit):
        """Build paginated embed pages from leaderboard rows."""
        personal = self._get_personal_rank_line(ctx, rows, unit)
        per_page = 10
        chunks = paginator.chunkify(rows, per_page)
        pages = []
        for page_idx, chunk in enumerate(chunks):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                user_id = self._get_user_id(row)
                count = self._get_count(row)
                member = ctx.guild.get_member(int(user_id))
                name = member.mention if member else f'<@{user_id}>'
                lines.append(f'**#{rank}** {name} — {count} {unit}')
            lines.append(personal)
            embed = discord.Embed(
                title=f'{emoji} {title}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color()
            )
            pages.append((None, embed))
        return pages

    # --- Fix / resync commands ---

    @starboard.command(brief='Resync star count for a message from Discord')
    @commands.has_role(constants.TLE_ADMIN)
    async def fix(self, ctx, message_ref: str, emoji: str = None):
        """Resync reactors for a starboarded message from Discord.

        Accepts a message link or a bare message ID.  If no emoji is given,
        all emoji entries for that message are resynced.

        Examples:
            ;starboard fix https://discord.com/channels/123/456/789
            ;starboard fix 789 ⭐
        """
        from tle.cogs._starboard_helpers import _parse_jump_url

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
            emoji_family = cf_common.user_db.get_emoji_family(ctx.guild.id, entry.emoji)
            old_count = cf_common.user_db.get_merged_reactor_count(message_id, emoji_family)
            new_count = await self._resync_reactors(message, emoji_family)
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

    # --- Backfill status ---

    @starboard.command(brief='Show backfill progress')
    async def backfill_status(self, ctx):
        """Show the progress of the background star count backfill."""
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

    @discord_common.send_error_if(StarboardCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Starboard(bot))

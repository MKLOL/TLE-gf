import asyncio
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

logger = logging.getLogger(__name__)

# Sentinel value for author_id when a message could not be fetched during backfill.
# This prevents infinite retry loops on restart. Excluded from leaderboard queries.
_BACKFILL_UNKNOWN = '__UNKNOWN__'


class StarboardCogError(commands.CommandError):
    pass


def _emoji_str(emoji):
    """Normalize a discord emoji to its string representation."""
    return str(emoji)


class Starboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.locks = {}
        # Backfill state
        self.backfill_total = 0
        self.backfill_done = 0
        self.backfill_failed = 0
        self.backfill_running = False
        self.backfill_complete = False
        logger.info('Starboard cog initialized')

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        logger.info('Starboard cog on_ready fired, launching backfill task')
        asyncio.create_task(self._backfill_star_counts())

    # --- Event listeners ---

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        if payload.guild_id is None:
            return
        emoji_str = _emoji_str(payload.emoji)
        entry = cf_common.user_db.get_starboard_entry(payload.guild_id, emoji_str)
        if entry is None:
            return
        if entry.channel_id is None:
            return  # Emoji configured but no starboard channel set yet
        channel_id, threshold, color = int(entry.channel_id), entry.threshold, entry.color
        logger.debug(f'Reaction add: emoji={emoji_str} guild={payload.guild_id} '
                     f'msg={payload.message_id} user={payload.user_id} '
                     f'threshold={threshold} starboard_channel={channel_id}')
        try:
            await self.check_and_add_to_starboard(channel_id, threshold, color, emoji_str, payload)
        except StarboardCogError as e:
            logger.info(f'Failed to starboard msg={payload.message_id} emoji={emoji_str}: {e!r}')
        except Exception as e:
            logger.error(f'Unexpected error in starboard processing msg={payload.message_id} '
                         f'emoji={emoji_str} guild={payload.guild_id}: {e}', exc_info=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
        if payload.guild_id is None:
            return
        emoji_str = _emoji_str(payload.emoji)
        entry = cf_common.user_db.get_starboard_entry(payload.guild_id, emoji_str)
        if entry is None:
            return
        logger.debug(f'Reaction remove: emoji={emoji_str} guild={payload.guild_id} '
                     f'msg={payload.message_id} user={payload.user_id}')
        # Update star count and author if the message is tracked
        if cf_common.user_db.check_exists_starboard_message_v1(payload.message_id, emoji_str):
            try:
                channel = self.bot.get_channel(payload.channel_id)
                if channel is None:
                    logger.warning(f'Reaction remove: channel {payload.channel_id} not found in cache')
                    return
                message = await channel.fetch_message(payload.message_id)
                count = sum(r.count for r in message.reactions if _emoji_str(r) == emoji_str)
                cf_common.user_db.update_starboard_author_and_count(
                    payload.message_id, emoji_str, str(message.author.id), count
                )
                logger.info(f'Updated star count for msg={payload.message_id} emoji={emoji_str} '
                            f'author={message.author.id} new_count={count}')
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
        rc = cf_common.user_db.remove_starboard_message(starboard_msg_id=payload.message_id)
        if rc:
            logger.info(f'Cleaned up deleted starboard message: starboard_msg={payload.message_id} '
                        f'guild={payload.guild_id}')

    # --- Core logic ---

    @staticmethod
    def prepare_embed(message, color, emoji_str, star_count):
        embed = discord.Embed(color=color, timestamp=message.created_at)
        embed.add_field(name='Channel', value=message.channel.mention)
        embed.add_field(name='Jump to', value=f'[Original]({message.jump_url})')

        header = f'{emoji_str} {star_count}' if star_count else emoji_str
        embed.title = header

        if message.content:
            content = message.content
            if len(content) > 1024:
                content = content[:1021] + '...'
            embed.add_field(name='Content', value=content, inline=False)

        if message.embeds:
            data = message.embeds[0]
            if data.type == 'image':
                embed.set_image(url=data.url)

        if message.attachments:
            file = message.attachments[0]
            if file.filename.lower().endswith(('png', 'jpeg', 'jpg', 'gif', 'webp')):
                embed.set_image(url=file.url)
            else:
                embed.add_field(name='Attachment', value=f'[{file.filename}]({file.url})', inline=False)

        embed.set_footer(text=str(message.author), icon_url=message.author.display_avatar.url)
        return embed

    async def check_and_add_to_starboard(self, starboard_channel_id, threshold, color, emoji_str, payload):
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
                or (len(message.content) == 0 and len(message.attachments) == 0)):
            raise StarboardCogError(f'Cannot starboard message {message.id}: invalid type or empty content')

        reaction_count = sum(r.count for r in message.reactions if _emoji_str(r) == emoji_str)
        logger.debug(f'Message {message.id}: {emoji_str} reaction_count={reaction_count} threshold={threshold}')
        if reaction_count < threshold:
            return

        lock = self.locks.get(payload.guild_id)
        if lock is None:
            self.locks[payload.guild_id] = lock = asyncio.Lock()

        async with lock:
            already_exists = cf_common.user_db.check_exists_starboard_message_v1(message.id, emoji_str)
            if already_exists:
                # Update star count AND author_id for existing entry.
                # author_id may be NULL if this message was created before backfill ran.
                cf_common.user_db.update_starboard_author_and_count(
                    message.id, emoji_str, str(message.author.id), reaction_count
                )
                logger.debug(f'Updated existing starboard entry: msg={message.id} emoji={emoji_str} '
                             f'author={message.author.id} count={reaction_count}')
                return
            embed = self.prepare_embed(message, color, emoji_str, reaction_count)
            starboard_message = await starboard_channel.send(embed=embed)
            cf_common.user_db.add_starboard_message_v1(
                message.id, starboard_message.id, guild.id, emoji_str,
                author_id=str(message.author.id),
                channel_id=str(channel.id)
            )
            cf_common.user_db.update_starboard_star_count(message.id, emoji_str, reaction_count)
            logger.info(f'NEW starboard entry: original_msg={message.id} starboard_msg={starboard_message.id} '
                        f'guild={guild.id} emoji={emoji_str} author={message.author} ({message.author.id}) '
                        f'channel={channel.id} count={reaction_count} '
                        f'(triggered by user {payload.user_id})')

    # --- Backfill background task ---

    async def _backfill_star_counts(self):
        """One-time background task to backfill star_count and author_id for existing starboard messages."""
        await self.bot.wait_until_ready()
        self.backfill_running = True
        logger.info('=== BACKFILL START ===')

        try:
            guilds = list(self.bot.guilds)  # Snapshot to avoid mutation during iteration

            # Phase 1: collect all work, skipping already-backfilled entries
            guild_work = {}
            for guild in guilds:
                all_messages = cf_common.user_db.get_all_starboard_messages_for_guild(str(guild.id))
                # Skip entries that already have author_id set (already backfilled)
                pending = [m for m in all_messages if m.author_id is None]
                if pending:
                    guild_work[guild] = pending
                self.backfill_total += len(pending)
                logger.info(f'Backfill: guild={guild.name} ({guild.id}) has {len(pending)} '
                            f'pending messages (of {len(all_messages)} total)')

            if self.backfill_total == 0:
                logger.info('Backfill: no starboard messages to backfill')
                return

            logger.info(f'Backfill: {self.backfill_total} total messages across {len(guilds)} guild(s)')

            # Phase 2: process each message
            for guild, messages in guild_work.items():
                emojis = cf_common.user_db.get_starboard_emojis_for_guild(str(guild.id))
                emoji_set = {e.emoji for e in emojis}
                logger.info(f'Backfill: processing guild={guild.name} ({guild.id}), '
                            f'{len(messages)} messages, tracked emojis={emoji_set}')

                for msg in messages:
                    try:
                        if msg.emoji not in emoji_set:
                            logger.debug(f'Backfill: skipping msg={msg.original_msg_id}, '
                                         f'emoji {msg.emoji} no longer tracked')
                            # Mark as done so we don't retry on next restart
                            cf_common.user_db.update_starboard_author_and_count(
                                msg.original_msg_id, msg.emoji, _BACKFILL_UNKNOWN, 0
                            )
                            self.backfill_done += 1
                            continue

                        # Try to fetch the message using stored channel_id first
                        original_msg = None
                        stored_channel_id = getattr(msg, 'channel_id', None)

                        if stored_channel_id:
                            channel = self.bot.get_channel(int(stored_channel_id))
                            if channel:
                                try:
                                    original_msg = await channel.fetch_message(int(msg.original_msg_id))
                                    logger.debug(f'Backfill: fetched msg={msg.original_msg_id} '
                                                 f'from stored channel={stored_channel_id}')
                                except (discord.NotFound, discord.Forbidden) as e:
                                    logger.debug(f'Backfill: msg={msg.original_msg_id} not found in '
                                                 f'stored channel={stored_channel_id}: {e}')
                                except discord.HTTPException as e:
                                    logger.warning(f'Backfill: HTTP error fetching msg={msg.original_msg_id} '
                                                   f'from channel={stored_channel_id}: {e}')
                            else:
                                logger.debug(f'Backfill: stored channel={stored_channel_id} not in bot cache')

                        # Fallback: scan all text channels
                        if original_msg is None:
                            logger.debug(f'Backfill: scanning channels for msg={msg.original_msg_id} '
                                         f'in guild={guild.id}')
                            channels_tried = 0
                            for ch in guild.text_channels:
                                try:
                                    original_msg = await ch.fetch_message(int(msg.original_msg_id))
                                    logger.info(f'Backfill: found msg={msg.original_msg_id} '
                                                f'in channel={ch.name} ({ch.id}) after scanning '
                                                f'{channels_tried + 1} channels')
                                    break
                                except (discord.NotFound, discord.Forbidden):
                                    channels_tried += 1
                                    continue
                                except discord.HTTPException as e:
                                    channels_tried += 1
                                    logger.debug(f'Backfill: HTTP error on channel={ch.id}: {e}')
                                    continue

                        if original_msg is None:
                            logger.warning(f'Backfill: FAILED to find msg={msg.original_msg_id} '
                                           f'in any channel of guild={guild.id} '
                                           f'(message may have been deleted)')
                            # Mark with sentinel so we don't retry on next restart
                            cf_common.user_db.update_starboard_author_and_count(
                                msg.original_msg_id, msg.emoji, _BACKFILL_UNKNOWN, 0
                            )
                            self.backfill_failed += 1
                            self.backfill_done += 1
                            await asyncio.sleep(0.5)
                            continue

                        count = sum(r.count for r in original_msg.reactions
                                    if _emoji_str(r) == msg.emoji)
                        cf_common.user_db.update_starboard_author_and_count(
                            msg.original_msg_id, msg.emoji,
                            str(original_msg.author.id), count
                        )
                        logger.info(f'Backfill: updated msg={msg.original_msg_id} '
                                    f'author={original_msg.author} ({original_msg.author.id}) '
                                    f'emoji={msg.emoji} star_count={count} '
                                    f'[{self.backfill_done + 1}/{self.backfill_total}]')
                        self.backfill_done += 1
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.error(f'Backfill: EXCEPTION for msg={msg.original_msg_id} '
                                     f'emoji={msg.emoji}: {e}', exc_info=True)
                        # Mark with sentinel so a persistent crash doesn't retry forever
                        try:
                            cf_common.user_db.update_starboard_author_and_count(
                                msg.original_msg_id, msg.emoji, _BACKFILL_UNKNOWN, 0
                            )
                        except Exception:
                            logger.debug(f'Backfill: could not set sentinel for msg={msg.original_msg_id}')
                        self.backfill_failed += 1
                        self.backfill_done += 1
                        await asyncio.sleep(1)

                logger.info(f'Backfill: finished guild={guild.name} ({guild.id}), '
                            f'progress={self.backfill_done}/{self.backfill_total} '
                            f'({self.backfill_failed} failed so far)')

        except Exception as e:
            logger.error(f'Backfill: FATAL ERROR: {e}', exc_info=True)
        finally:
            self.backfill_running = False
            self.backfill_complete = True
            logger.info(f'=== BACKFILL COMPLETE === '
                        f'{self.backfill_done}/{self.backfill_total} processed, '
                        f'{self.backfill_failed} failed')

    # --- Commands ---

    @commands.group(brief='Starboard commands', invoke_without_command=True)
    async def starboard(self, ctx):
        """Group for commands involving the starboard."""
        await ctx.send_help(ctx.command)

    @starboard.command(brief='Add an emoji to the starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def add(self, ctx, emoji: str, threshold: int = 3, color: str = None):
        """Add an emoji to the starboard with optional threshold and color.
        Example: ;starboard add ⭐ 3 #ffaa10"""
        if threshold < 1:
            raise StarboardCogError('Threshold must be at least 1')
        color_val = constants._DEFAULT_STAR_COLOR
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
    async def delete(self, ctx, emoji: str):
        """Remove an emoji and all its tracked messages from the starboard."""
        existing = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if existing is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')
        cf_common.user_db.remove_starboard_emoji(ctx.guild.id, emoji)
        logger.info(f'CMD starboard delete: guild={ctx.guild.id} emoji={emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Removed {emoji} from starboard'))

    @starboard.command(brief='Edit threshold for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def edit_threshold(self, ctx, emoji: str, threshold: int):
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
    async def edit_color(self, ctx, emoji: str, color: str):
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
    async def here(self, ctx, emoji: str):
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
    async def clear(self, ctx, emoji: str):
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
    async def remove(self, ctx, emoji: str, original_message_id: int):
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

    # --- Leaderboard commands ---

    @starboard.command(brief='Show starboard leaderboard by message count')
    async def leaderboard(self, ctx, emoji: str):
        """Show top users by number of starboarded messages for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled."""
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config starboard_leaderboard enable`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        rows = cf_common.user_db.get_starboard_leaderboard(ctx.guild.id, emoji)
        if not rows:
            raise StarboardCogError(f'No starboarded messages found for {emoji}.')

        logger.info(f'CMD starboard leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'{len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Starboard Leaderboard', 'messages')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True)

        # Send personal rank
        await self._send_personal_rank(ctx, rows, 'messages')

    @starboard.command(name='star-leaderboard', brief='Show starboard leaderboard by star count')
    async def star_leaderboard(self, ctx, emoji: str):
        """Show top users by total star count for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled."""
        if cf_common.user_db.get_guild_config(ctx.guild.id, 'starboard_leaderboard') != '1':
            raise StarboardCogError('Starboard leaderboard is not enabled. '
                                    'An admin can enable it with `;meta config starboard_leaderboard enable`.')
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(f'Emoji {emoji} is not configured for this starboard.')

        rows = cf_common.user_db.get_starboard_star_leaderboard(ctx.guild.id, emoji)
        if not rows:
            raise StarboardCogError(f'No star data found for {emoji}. '
                                    'Star counts are populated via backfill and live tracking.')

        logger.info(f'CMD starboard star-leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'{len(rows)} users by user={ctx.author.id}')
        pages = self._make_leaderboard_pages(ctx, rows, emoji, 'Star Leaderboard', 'stars')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300, set_pagenum_footers=True)

        # Send personal rank
        await self._send_personal_rank(ctx, rows, 'stars')

    def _make_leaderboard_pages(self, ctx, rows, emoji, title, unit):
        """Build paginated embed pages from leaderboard rows."""
        per_page = 10
        chunks = paginator.chunkify(rows, per_page)
        pages = []
        for page_idx, chunk in enumerate(chunks):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                user_id = row.author_id
                count = row.message_count if unit == 'messages' else row.total_stars
                member = ctx.guild.get_member(int(user_id))
                name = member.mention if member else f'<@{user_id}>'
                lines.append(f'**#{rank}** {name} — {count} {unit}')
            embed = discord.Embed(
                title=f'{emoji} {title}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color()
            )
            pages.append((None, embed))
        return pages

    async def _send_personal_rank(self, ctx, rows, unit):
        """Send a separate message with the invoking user's rank."""
        user_id_str = str(ctx.author.id)
        for i, row in enumerate(rows):
            if row.author_id == user_id_str:
                rank = i + 1
                count = row.message_count if unit == 'messages' else row.total_stars
                await ctx.send(f'You are ranked **#{rank}** with **{count}** {unit}.')
                return
        await ctx.send('You are not on this leaderboard yet.')

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

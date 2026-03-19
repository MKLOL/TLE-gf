"""Starboard migration cog — crawls an old bot's pillboard channel and
re-posts everything into TLE-gf's starboard system.

Flow:
  1. ;migrate start #old_channel #new_channel :emoji1: :emoji2:
  2. ;migrate status
  3. ;migrate complete #new_channel
  4. ;migrate resume  (retry after failure)
  5. ;migrate cancel
"""
import asyncio
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.cogs._starboard_helpers import _emoji_str
from tle.cogs._migrate_helpers import (
    parse_old_bot_message,
    serialize_embed_fallback,
    build_fallback_message,
)
from tle.cogs.starboard import Starboard, _starboard_content

logger = logging.getLogger(__name__)

# Rate limit delay between Discord API calls during crawl/post
_RATE_DELAY = 1.5


class Migrate(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._tasks = {}  # guild_id -> asyncio.Task

    # ------------------------------------------------------------------
    # Background crawl + post task
    # ------------------------------------------------------------------

    async def _run_migration(self, guild_id, old_channel_id, new_channel_id, emoji_set):
        """Background task: crawl old channel, then post to new channel."""
        await self.bot.wait_until_ready()
        db = cf_common.user_db

        logger.info(f'=== MIGRATION START === guild={guild_id}')

        try:
            migration = db.get_migration(guild_id)
            if migration is None:
                logger.warning(f'Migration: guild={guild_id} migration record not found, aborting')
                return

            # Skip crawl if already in posting phase (resume from posting)
            if migration.status != 'posting':
                await self._crawl_phase(guild_id, old_channel_id, emoji_set, db)

                migration = db.get_migration(guild_id)
                if migration is None or migration.status == 'failed':
                    logger.warning(f'Migration: guild={guild_id} crawl phase ended with '
                                   f'status={migration.status if migration else "deleted"}')
                    return

                db.update_migration_status(guild_id, 'posting')

            await self._post_phase(guild_id, new_channel_id, emoji_set, db)

            db.update_migration_status(guild_id, 'done')
            logger.info(f'=== MIGRATION COMPLETE === guild={guild_id}')

        except asyncio.CancelledError:
            logger.info(f'Migration: guild={guild_id} cancelled')
            # Don't update status — the cancel command already cleaned up
            raise
        except Exception as e:
            logger.error(f'Migration: guild={guild_id} FAILED: {e}', exc_info=True)
            db.update_migration_status(guild_id, 'failed')
        finally:
            self._tasks.pop(guild_id, None)

    async def _crawl_phase(self, guild_id, old_channel_id, emoji_set, db):
        """Crawl the old bot's channel, collecting entries and reactors."""
        old_channel = self.bot.get_channel(old_channel_id)
        if old_channel is None:
            logger.error(f'Migration: guild={guild_id} old channel {old_channel_id} not found')
            db.update_migration_status(guild_id, 'failed')
            return

        migration = db.get_migration(guild_id)
        after = None
        if migration.last_crawled_msg_id:
            after = discord.Object(id=int(migration.last_crawled_msg_id))

        crawl_done = migration.crawl_done
        crawl_failed = migration.crawl_failed

        logger.info(f'Migration crawl: guild={guild_id} channel={old_channel_id} '
                     f'checkpoint={migration.last_crawled_msg_id} '
                     f'done={crawl_done} failed={crawl_failed}')

        async for old_bot_msg in old_channel.history(after=after, oldest_first=True, limit=None):
            if not old_bot_msg.content:
                continue

            parsed = parse_old_bot_message(old_bot_msg.content)
            if parsed is None:
                continue

            emoji_str, displayed_count, msg_guild_id, source_channel_id, original_msg_id = parsed

            if emoji_str not in emoji_set:
                continue

            # Add entry (idempotent for resume)
            db.add_migration_entry(
                guild_id, str(original_msg_id), emoji_str,
                str(old_bot_msg.id), str(old_channel_id)
            )

            # Try to fetch the original message
            try:
                source_channel = self.bot.get_channel(source_channel_id)
                if source_channel is None:
                    raise discord.NotFound(None, 'channel not found')

                original_msg = await source_channel.fetch_message(original_msg_id)

                # Count reactions and collect reactors for this emoji
                star_count = 0
                reactor_ids = []
                for reaction in original_msg.reactions:
                    if _emoji_str(reaction.emoji) == emoji_str:
                        star_count = reaction.count
                        async for user in reaction.users():
                            reactor_ids.append(str(user.id))
                        break

                if reactor_ids:
                    db.bulk_add_reactors(str(original_msg_id), emoji_str, reactor_ids)

                db.update_migration_entry_crawled(
                    str(original_msg_id), emoji_str,
                    str(source_channel_id), str(original_msg.author.id),
                    star_count
                )
                crawl_done += 1
                logger.info(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                            f'emoji={emoji_str} msg={original_msg_id} '
                            f'author={original_msg.author} count={star_count}')

            except (discord.NotFound, discord.Forbidden):
                # Original message deleted or inaccessible — serialize old bot embed as fallback
                fallback = serialize_embed_fallback(old_bot_msg)
                db.update_migration_entry_deleted(
                    str(original_msg_id), emoji_str, fallback
                )
                crawl_done += 1
                crawl_failed += 1
                logger.info(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                            f'emoji={emoji_str} msg={original_msg_id} DELETED/FORBIDDEN')

            except discord.HTTPException as e:
                # Mark entry as deleted so it's not orphaned as 'pending'
                db.update_migration_entry_deleted(str(original_msg_id), emoji_str, None)
                crawl_failed += 1
                crawl_done += 1
                logger.warning(f'Migration crawl: guild={guild_id} [{crawl_done}] '
                               f'HTTP error for msg={original_msg_id}: {e}')

            # Checkpoint after each message
            db.update_migration_checkpoint(
                guild_id, str(old_bot_msg.id), crawl_done, crawl_failed
            )

            await asyncio.sleep(_RATE_DELAY)

        db.set_migration_crawl_total(guild_id, crawl_done)
        logger.info(f'Migration crawl: guild={guild_id} finished — '
                     f'{crawl_done} processed, {crawl_failed} failed')

    async def _post_phase(self, guild_id, new_channel_id, emoji_set, db):
        """Post crawled entries to the new starboard channel in chronological order."""
        new_channel = self.bot.get_channel(new_channel_id)
        if new_channel is None:
            logger.error(f'Migration post: guild={guild_id} new channel {new_channel_id} not found')
            db.update_migration_status(guild_id, 'failed')
            return

        entries = db.get_migration_entries_for_posting(guild_id)
        db.set_migration_post_totals(guild_id, len(entries))

        logger.info(f'Migration post: guild={guild_id} starting — {len(entries)} entries to post')

        post_done = 0
        post_failed = 0
        color = constants._DEFAULT_STAR_COLOR

        for entry in entries:
            try:
                if entry.crawl_status == 'crawled' and entry.source_channel_id:
                    # Try to fetch original and build proper starboard message
                    try:
                        source_channel = self.bot.get_channel(int(entry.source_channel_id))
                        if source_channel is None:
                            raise discord.NotFound(None, 'channel gone')
                        original_msg = await source_channel.fetch_message(int(entry.original_msg_id))
                        content, embeds, files = await Starboard.build_starboard_message(
                            original_msg, entry.emoji, entry.star_count, color
                        )
                        sent = await new_channel.send(content=content, embeds=embeds, files=files)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        # Fall back to simple content line
                        content, embeds = build_fallback_message(entry, entry.embed_fallback, entry.emoji)
                        sent = await new_channel.send(content=content, embeds=embeds)
                else:
                    # Deleted original — use fallback
                    content, embeds = build_fallback_message(entry, entry.embed_fallback, entry.emoji)
                    sent = await new_channel.send(content=content, embeds=embeds)

                db.update_migration_entry_posted(entry.original_msg_id, entry.emoji, str(sent.id))
                post_done += 1
                db.update_migration_post_done(guild_id, post_done)

                logger.info(f'Migration post: guild={guild_id} [{post_done}/{len(entries)}] '
                            f'msg={entry.original_msg_id} emoji={entry.emoji}')

            except Exception as e:
                # Mark as post_failed — won't be retried unless user runs ;migrate resume
                logger.error(f'Migration post: guild={guild_id} FAILED '
                             f'msg={entry.original_msg_id} emoji={entry.emoji}: {e}',
                             exc_info=True)
                db.update_migration_entry_post_failed(entry.original_msg_id, entry.emoji)
                post_done += 1
                post_failed += 1
                db.update_migration_post_done(guild_id, post_done)

            await asyncio.sleep(_RATE_DELAY)

        logger.info(f'Migration post: guild={guild_id} finished — '
                     f'{post_done}/{len(entries)} ({post_failed} failed)')

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    @commands.group(name='migrate', invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def migrate(self, ctx):
        """Starboard migration commands."""
        await ctx.send_help(ctx.command)

    @migrate.command(name='start')
    @commands.has_role(constants.TLE_ADMIN)
    async def start(self, ctx, old_channel: discord.TextChannel,
                    new_channel: discord.TextChannel, *emojis: str):
        """Start migrating from an old bot's starboard channel.

        Usage: ;migrate start #old-pillboard #new-pillboard :pill: :chocolate_bar:
        """
        guild_id = ctx.guild.id

        if not emojis:
            await ctx.send('Please specify at least one emoji to migrate.')
            return

        existing = cf_common.user_db.get_migration(guild_id)
        if existing is not None:
            await ctx.send(f'A migration is already in progress (status: {existing.status}). '
                           f'Use `;migrate cancel` first.')
            return

        emoji_csv = ','.join(emojis)
        cf_common.user_db.create_migration(
            guild_id, old_channel.id, new_channel.id, emoji_csv, time.time()
        )

        emoji_set = set(emojis)
        task = asyncio.create_task(
            self._run_migration(guild_id, old_channel.id, new_channel.id, emoji_set)
        )
        self._tasks[guild_id] = task

        logger.info(f'Migration: guild={guild_id} started by {ctx.author} '
                     f'old={old_channel.id} new={new_channel.id} emojis={emoji_csv}')
        await ctx.send(f'Migration started! Crawling {old_channel.mention} for '
                        f'{", ".join(emojis)}.\n'
                        f'Use `;migrate status` to check progress.')

    @migrate.command(name='status')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def status(self, ctx):
        """Check the progress of the current migration."""
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration in progress.')
            return

        status_counts = cf_common.user_db.count_migration_entries_by_status(guild_id)
        counts = {r.crawl_status: r.cnt for r in status_counts}

        lines = [
            f'**Migration Status:** {migration.status}',
            f'**Emojis:** {migration.emojis}',
            f'**Crawl:** {migration.crawl_done} done, {migration.crawl_failed} failed'
            f' (total: {migration.crawl_total})',
        ]

        if migration.status in ('posting', 'done', 'failed'):
            lines.append(f'**Post:** {migration.post_done}/{migration.post_total}')

        if counts:
            parts = [f'{k}: {v}' for k, v in sorted(counts.items())]
            lines.append(f'**Entries by status:** {", ".join(parts)}')

        await ctx.send('\n'.join(lines))

    @migrate.command(name='complete')
    @commands.has_role(constants.TLE_ADMIN)
    async def complete(self, ctx, new_channel: discord.TextChannel):
        """Finalize migration: create emoji configs and activate live tracking.

        Usage: ;migrate complete #new-pillboard
        """
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to complete.')
            return

        if migration.status != 'done':
            await ctx.send(f'Migration is not done yet (status: {migration.status}). '
                           f'Wait for it to finish first.')
            return

        db = cf_common.user_db
        emojis = migration.emojis.split(',')

        posted_entries = db.get_posted_migration_entries(guild_id)

        # Warn about post_failed entries before completing
        failed_counts = db.count_migration_entries_by_status(guild_id)
        failed_map = {r.crawl_status: r.cnt for r in failed_counts}
        pf_count = failed_map.get('post_failed', 0)

        if pf_count > 0:
            await ctx.send(f'**Warning:** {pf_count} entries failed to post and will not be '
                           f'imported. Use `;migrate resume` to retry them, or proceed with '
                           f'`;migrate complete {new_channel.mention}` again to accept the loss.')
            # Only warn once — if they call complete again, we skip this check
            # by checking if there are actually posted entries to import
            if not posted_entries:
                return

        logger.info(f'Migration complete: guild={guild_id} importing {len(posted_entries)} entries '
                     f'({pf_count} post_failed entries discarded)')

        # Copy posted entries into starboard tables
        for entry in posted_entries:
            db.add_starboard_message_v1(
                entry.original_msg_id, entry.new_starboard_msg_id,
                str(guild_id), entry.emoji,
                author_id=entry.author_id,
                channel_id=entry.source_channel_id
            )
            if entry.star_count is not None and entry.star_count > 0:
                db.update_starboard_star_count(
                    entry.original_msg_id, entry.emoji, entry.star_count
                )

        # Create emoji configs pointing at the new channel
        for emoji in emojis:
            db.add_starboard_emoji(str(guild_id), emoji, 1, constants._DEFAULT_STAR_COLOR)
            db.set_starboard_channel(str(guild_id), emoji, new_channel.id)

        # Clean up migration data
        db.delete_migration_entries(guild_id)
        db.delete_migration(guild_id)

        emoji_list = ', '.join(emojis)
        logger.info(f'Migration complete: guild={guild_id} done — '
                     f'{len(posted_entries)} imported, emojis={emoji_list}')
        await ctx.send(f'Migration complete! {len(posted_entries)} messages imported.\n'
                        f'Emoji configs created for {emoji_list} in {new_channel.mention}.\n'
                        f'Live reaction tracking is now active.')

    @migrate.command(name='resume')
    @commands.has_role(constants.TLE_ADMIN)
    async def resume(self, ctx):
        """Resume a failed migration. Retries any post_failed entries.

        Usage: ;migrate resume
        """
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to resume.')
            return

        if migration.status not in ('failed', 'crawling', 'posting'):
            await ctx.send(f'Migration cannot be resumed (status: {migration.status}). '
                           f'Use `;migrate complete` if status is done.')
            return

        if guild_id in self._tasks:
            task = self._tasks[guild_id]
            if not task.done():
                await ctx.send('Migration task is already running.')
                return

        db = cf_common.user_db

        # Reset post_failed entries so they can be retried
        db.reset_post_failed_entries(guild_id)

        # Determine which phase to resume
        postable = db.get_migration_entries_for_posting(guild_id)
        if postable:
            db.update_migration_status(guild_id, 'posting')
        else:
            db.update_migration_status(guild_id, 'crawling')

        emoji_set = set(migration.emojis.split(','))
        task = asyncio.create_task(
            self._run_migration(
                guild_id,
                int(migration.old_channel_id),
                int(migration.new_channel_id),
                emoji_set
            )
        )
        self._tasks[guild_id] = task

        logger.info(f'Migration resume: guild={guild_id} by {ctx.author} '
                     f'(was status={migration.status})')
        await ctx.send(f'Migration resumed! Use `;migrate status` to check progress.')

    @migrate.command(name='cancel')
    @commands.has_role(constants.TLE_ADMIN)
    async def cancel(self, ctx):
        """Cancel the current migration and clean up."""
        guild_id = ctx.guild.id
        migration = cf_common.user_db.get_migration(guild_id)

        if migration is None:
            await ctx.send('No migration to cancel.')
            return

        logger.info(f'Migration cancel: guild={guild_id} by {ctx.author} '
                     f'(was status={migration.status})')

        # Cancel background task if running
        task = self._tasks.pop(guild_id, None)
        if task and not task.done():
            task.cancel()

        # Clean up DB
        cf_common.user_db.delete_migration_entries(guild_id)
        cf_common.user_db.delete_migration(guild_id)

        await ctx.send('Migration cancelled and data cleaned up.')

    # ------------------------------------------------------------------
    # Resume on restart
    # ------------------------------------------------------------------

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        """Resume any in-progress migrations after bot restart."""
        # Wait for user_db to be initialized
        if cf_common.user_db is None:
            logger.debug('Migration: user_db not ready in on_ready, skipping resume')
            return

        resumed = 0
        for guild in self.bot.guilds:
            migration = cf_common.user_db.get_migration(guild.id)
            if migration is None:
                continue

            if migration.status in ('crawling', 'posting'):
                emoji_set = set(migration.emojis.split(','))
                logger.info(f'Migration resume: guild={guild.id} status={migration.status} '
                            f'checkpoint={migration.last_crawled_msg_id} '
                            f'crawl_done={migration.crawl_done} emojis={migration.emojis}')
                task = asyncio.create_task(
                    self._run_migration(
                        guild.id,
                        int(migration.old_channel_id),
                        int(migration.new_channel_id),
                        emoji_set
                    )
                )
                self._tasks[guild.id] = task
                resumed += 1

        if resumed:
            logger.info(f'Migration resume: {resumed} migration(s) resumed across all guilds')


async def setup(bot):
    await bot.add_cog(Migrate(bot))

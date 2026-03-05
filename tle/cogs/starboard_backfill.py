"""One-time background backfill task for starboard messages.

Populates author_id, star_count, channel_id, and reactors for existing
starboard entries by fetching them from Discord.  Uses author_id IS NULL
as a checkpoint — already-processed messages are skipped on restart.
Unfetchable messages get an __UNKNOWN__ sentinel to prevent infinite retries.
"""
import asyncio
import logging

import discord

from tle.util import codeforces_common as cf_common
from tle.cogs.starboard_helpers import _emoji_str, _parse_jump_url

logger = logging.getLogger(__name__)

# Sentinel value for author_id when a message could not be fetched during backfill.
_BACKFILL_UNKNOWN = '__UNKNOWN__'


class BackfillMixin:
    """Mixin providing the backfill background task and status command helpers.

    Expects the host class to set:
      - self.bot          (discord.py Bot instance)
      - self.backfill_*   state attrs (total, done, failed, running, complete)
    """

    def _init_backfill_state(self):
        self.backfill_total = 0
        self.backfill_done = 0
        self.backfill_failed = 0
        self.backfill_running = False
        self.backfill_complete = False

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
                # Skip entries that are fully backfilled (have both author_id and channel_id)
                pending = [m for m in all_messages
                           if m.author_id is None or
                           (m.channel_id is None and m.author_id != _BACKFILL_UNKNOWN)]
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
                # Build map of emoji -> starboard channel for embed lookup
                emoji_sb_channels = {}
                for e in emojis:
                    if e.channel_id:
                        ch = self.bot.get_channel(int(e.channel_id))
                        if ch:
                            emoji_sb_channels[e.emoji] = ch
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

                        # Try to find original channel via the starboard embed's "Jump to" link.
                        # We intentionally do not fall back to scanning every guild text channel.
                        if original_msg is None and msg.starboard_msg_id:
                            sb_channel = emoji_sb_channels.get(msg.emoji)
                            if sb_channel:
                                try:
                                    sb_msg = await sb_channel.fetch_message(int(msg.starboard_msg_id))
                                    for embed in sb_msg.embeds:
                                        for field in embed.fields:
                                            if field.name == 'Jump to':
                                                parsed = _parse_jump_url(field.value)
                                                if parsed:
                                                    _, orig_ch_id, _ = parsed
                                                    orig_ch = self.bot.get_channel(orig_ch_id)
                                                    if orig_ch:
                                                        try:
                                                            original_msg = await orig_ch.fetch_message(
                                                                int(msg.original_msg_id)
                                                            )
                                                            logger.debug(
                                                                f'Backfill: fetched msg={msg.original_msg_id} '
                                                                f'via starboard embed (channel={orig_ch_id})'
                                                            )
                                                        except (discord.NotFound, discord.Forbidden):
                                                            logger.debug(
                                                                f'Backfill: original msg={msg.original_msg_id} '
                                                                f'not found via embed link (channel={orig_ch_id})'
                                                            )
                                                break
                                        if original_msg is not None:
                                            break
                                        if any(field.name == 'Jump to' for field in embed.fields):
                                            break
                                except (discord.NotFound, discord.Forbidden):
                                    logger.debug(
                                        f'Backfill: starboard embed {msg.starboard_msg_id} not found '
                                        f'in channel={sb_channel.id}'
                                    )
                                except discord.HTTPException as e:
                                    logger.debug(
                                        f'Backfill: HTTP error fetching starboard embed '
                                        f'{msg.starboard_msg_id}: {e}'
                                    )

                        if original_msg is None:
                            if msg.author_id is not None:
                                # Already has author_id from a previous backfill, just
                                # can't fetch the message to get channel_id. Don't
                                # overwrite good data with __UNKNOWN__.
                                logger.debug(
                                    f'Backfill: msg={msg.original_msg_id} already has '
                                    f'author_id={msg.author_id} but channel_id is NULL '
                                    f'and message is unfetchable; skipping'
                                )
                            else:
                                logger.warning(
                                    f'Backfill: unresolved msg={msg.original_msg_id} in guild={guild.id} '
                                    f'after stored-channel and jump-url lookup; marking as unknown'
                                )
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
                            str(original_msg.author.id), count,
                            channel_id=original_msg.channel.id
                        )
                        # Collect all reactors for this emoji
                        for r in original_msg.reactions:
                            if _emoji_str(r) == msg.emoji:
                                user_ids = [str(user.id) async for user in r.users()]
                                cf_common.user_db.bulk_add_reactors(
                                    msg.original_msg_id, msg.emoji, user_ids
                                )
                                break
                        logger.info(f'Backfill: updated msg={msg.original_msg_id} '
                                    f'author={original_msg.author} ({original_msg.author.id}) '
                                    f'emoji={msg.emoji} star_count={count} '
                                    f'[{self.backfill_done + 1}/{self.backfill_total}]')
                        self.backfill_done += 1
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.error(f'Backfill: EXCEPTION for msg={msg.original_msg_id} '
                                     f'emoji={msg.emoji}: {e}', exc_info=True)
                        # Mark with sentinel so a persistent crash doesn't retry forever,
                        # but only if we don't already have good data
                        if msg.author_id is None:
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

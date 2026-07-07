"""Background channel-history import machinery. (Minigames cog impl mixin; see minigames.py)."""

import asyncio
import logging

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._migrate_retry import RetryExhaustedError
from tle.cogs._minigame_common import (
    strip_codeblock,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError,
)
from tle.cogs._minigame_queens_cog import (
    _IMPORT_BATCH_SIZE, _IMPORT_RATE_DELAY,
)

logger = logging.getLogger(__name__)

_UNSET = object()  # cache sentinel: None is a meaningful cutoff value


class ImplImportMixin:
    # ── Import ──────────────────────────────────────────────────────────

    _KVS_IMPORT_PREFIX = 'mg_import_reply:'

    async def _resolve_channel(self, channel_id):
        """Get a channel from cache, falling back to fetch_channel for threads."""
        ch = self.bot.get_channel(channel_id)
        if ch is not None:
            return ch
        return await self.bot.fetch_channel(channel_id)

    async def _notify_import_complete(self, guild_id, game, status):
        """Reply to the original import command message with the final result."""
        kvs_key = f'{self._KVS_IMPORT_PREFIX}{guild_id}:{game.name}'
        try:
            reply_info = cf_common.user_db.kvs_get(kvs_key)
            if reply_info is None:
                return
            cf_common.user_db.kvs_delete(kvs_key)
            reply_channel_id, reply_message_id = reply_info.split(':')
            reply_channel = await self._resolve_channel(int(reply_channel_id))
            reply_message = await reply_channel.fetch_message(int(reply_message_id))

            state = status['state']
            skipped = status.get('skipped', [])
            lines = [
                f'**{game.display_name} import {state}.**',
                f'Messages scanned: **{status["scanned"]}**',
                f'Results imported: **{status["done"]}**',
            ]
            if skipped:
                lines.append(f'Detected but unparseable: **{len(skipped)}**')
            if status.get('error'):
                lines.append(f'Error: `{status["error"]}`')

            embed_fn = discord_common.embed_success if state == 'done' else discord_common.embed_alert
            await reply_message.reply(embed=embed_fn('\n'.join(lines)))
        except BaseException:
            logger.warning('Failed to send import completion reply for guild=%s game=%s',
                           guild_id, game.name, exc_info=True)
            # Clean up KVS key even on CancelledError
            try:
                cf_common.user_db.kvs_delete(kvs_key)
            except Exception:
                pass

    async def _run_import(self, guild_id, channel_id, game):
        key = (guild_id, game.name)
        status = self._import_status[key]
        try:
            try:
                channel = await self._resolve_channel(channel_id)
            except discord.NotFound:
                raise MinigameCogError(f'Channel `{channel_id}` is not available.')

            uncommitted = 0
            ban_cutoffs = {}
            async for message in channel.history(oldest_first=True, limit=None):
                status['scanned'] += 1
                if message.author.bot or not message.content:
                    continue

                ban_cutoff = ban_cutoffs.get(message.author.id, _UNSET)
                if ban_cutoff is _UNSET:
                    ban_cutoff = self._ingest_ban_cutoff(
                        guild_id, message.author.id, game)
                    ban_cutoffs[message.author.id] = ban_cutoff
                if (ban_cutoff is not None
                        and message.created_at.timestamp() >= ban_cutoff):
                    # Forward-only ban: pre-ban messages still import (the
                    # rebuild must not erase history that predates the ban);
                    # only post-ban ones are dropped (no raw, no result).
                    continue

                cleaned = strip_codeblock(message.content)
                if await self._notify_invalid_minigame_submission(
                        message, game, cleaned):
                    status.setdefault('skipped', []).append(str(message.id))
                    continue

                # Save every non-bot message for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, guild_id, channel_id, message.author.id,
                    message.created_at.isoformat(), message.content,
                    commit=False,
                )
                uncommitted += 1

                results = game.parse(cleaned)
                if not results:
                    if game.detect and game.detect.search(cleaned):
                        status.setdefault('skipped', []).append(str(message.id))
                        logger.warning(
                            '%s import: detected but unparseable msg=%s user=%s content=%r',
                            game.display_name, message.id, message.author.id,
                            message.content[:200],
                        )
                else:
                    puzzle_date_fallback = message.created_at.date()
                    for parsed in results:
                        puzzle_date = parsed.puzzle_date or puzzle_date_fallback
                        cf_common.user_db.save_imported_minigame_result(
                            message.id, guild_id, game.name, channel_id,
                            message.author.id, parsed.puzzle_number,
                            puzzle_date.isoformat(), parsed.accuracy,
                            parsed.time_seconds, parsed.is_perfect,
                            message.content, commit=False,
                        )
                        status['done'] += 1
                    status['latest_message_id'] = str(message.id)

                if uncommitted >= _IMPORT_BATCH_SIZE:
                    cf_common.user_db.conn.commit()
                    logger.info(
                        '%s import progress: guild=%s channel=%s scanned=%d imported=%d latest_msg=%s',
                        game.display_name, guild_id, channel_id,
                        status['scanned'], status['done'], status['latest_message_id'],
                    )
                    uncommitted = 0
                    await asyncio.sleep(_IMPORT_RATE_DELAY)

            if uncommitted > 0:
                cf_common.user_db.conn.commit()

            status['state'] = 'done'
            logger.info(
                '%s import complete: guild=%s channel=%s scanned=%d imported=%d',
                game.display_name, guild_id, channel_id,
                status['scanned'], status['done'],
            )
        except asyncio.CancelledError:
            status['state'] = 'cancelled'
            cf_common.user_db.conn.rollback()
            logger.info('%s import cancelled: guild=%s scanned=%d imported=%d',
                        game.display_name, guild_id, status['scanned'], status['done'])
            raise
        except RetryExhaustedError as exc:
            status['state'] = 'failed'
            status['error'] = f'Discord API retries exhausted: {exc.last_exception}'
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed (retries exhausted): guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        except Exception as exc:
            status['state'] = 'failed'
            status['error'] = str(exc)
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed: guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        finally:
            self._import_tasks.pop(key, None)
            # Recompute once after the whole import (committed batches persist even
            # on cancel/fail), rather than per imported row.
            self._recompute_game_ratings(guild_id, game)
            await self._notify_import_complete(guild_id, game, status)


"""Background channel-history import machinery. (Minigames cog impl mixin; see minigames.py)."""

import asyncio
import datetime as dt
import io
import json
import logging
import os
import pathlib
import sqlite3
import sys
import time
import zipfile
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import tasks
from tle.util.akari_rating import rank_for_rating
from tle.util.minigame_rating import compute_ratings
from tle.util.db.minigame_db import (
    merged_minigame_winners, diff_merged_winners,
)

from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError
from tle.cogs._minigame_common import (
    compute_vs, compute_vs_matchups, compute_streak, compute_longest_streak,
    compute_top, pick_best_results, format_duration, normalize_puzzle_date,
    parse_date_args, resolve_scoring, strip_codeblock, _NO_TIME_BOUND,
)
from tle.cogs._minigame_akari import (
    AKARI_GAME, akari_date_number_mismatch, expected_puzzle_number,
    looks_like_non_pro_akari, puzzle_date_for,
)
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens import (
    QUEENS_GAME, normalize_queens_name, parse_queens_leaderboard,
    parse_queens_time, queens_status_flags,
)
from tle.cogs._minigame_stats import (
    plot_akari_performance, plot_akari_rating,
    plot_akari_stats, plot_guessgame_stats, plot_queens_stats,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, CaseInsensitiveMember, _mg, _safe_member_name,
    _safe_user_name, _safe_cf_handle,
    _legend_name_for, _format_score, _format_akari_history_line,
    _format_minigame_history_line, _format_akari_ban_line, _ScheduledCtx,
)
from tle.cogs._minigame_tables import (
    _PuzzlePlayerInfo, _maybe_parse_puzzle_selector,
    _get_akari_puzzle_table_image_file, _get_akari_rating_table_image_file,
    _get_queens_results_table_image_file,
)
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _filter_queens_weekday_rows,
    _split_queens_rating_date_filter, _split_queens_recalculate_filter,
    _filter_queens_rating_date_rows, _filter_queens_rating_date_history,
    _format_queens_weekday_filter, _queens_weekday_filter_suffix,
    _format_queens_date_filter, _queens_filter_suffix,
    _filter_queens_contested_rating_history,
)
from tle.cogs._minigame_queens_cog import (
    _QueensResolvedEntry, _QueensImportPreview, _QueensImportSaveResult,
    _QueensBackfillResult, _QueensPendingRegistration,
    _QUEENS_CONNECTION_ACCOUNT_KEY, _QUEENS_DEFAULT_CONNECTION_ACCOUNT,
    _QUEENS_ANONYMOUS_LINK_MARKER, _QUEENS_ANONYMOUS_LABEL,
    _QUEENS_ANONYMOUS_FLAGS, _QUEENS_PENDING_REGISTRATION_DELAY,
    _QUEENS_CONNECT_TIMEOUT, _QUEENS_IMPORTER_KEY, _QUEENS_LINKEDIN_NAME_KEY,
    _QUEENS_ADMINS_KEY, _QUEENS_STATE_PATH_KEY, _QUEENS_UPDATE_THROTTLE_PREFIX,
    _QUEENS_UPDATE_THROTTLE_SECONDS, _QUEENS_DAILY_UPDATE_LAST_PREFIX,
    _QUEENS_DAILY_UPDATE_CHECK_INTERVAL, _QUEENS_DAILY_UPDATE_PRECISE_WINDOW,
    _QUEENS_DAILY_UPDATE_TIME, _QUEENS_DAILY_UPDATE_TZ,
    _QUEENS_AUTO_PLAY_MIN_SECONDS, _QUEENS_SCRAPER_TIMEOUT,
    _QUEENS_WHOAMI_TIMEOUT, _QUEENS_PLAYWRIGHT_PLATFORM,
    _QUEENS_STATE_MAX_BYTES, _QUEENS_BACKFILL_MAX_BYTES, _QUEENS_HISTORY_PER_PAGE,
    _parse_queens_date, _queens_puzzle_number_for_date,
    _queens_date_for_puzzle_number, _parse_queens_date_or_number,
    _queens_update_target_date, _queens_daily_update_target_datetime,
    _parse_queens_update_args, _queens_puzzle_numbers_for_date,
    _queens_puzzle_date_text, _queens_result_message_id, _format_queens_date,
    _is_queens_link_anonymous, _queens_public_link_name,
    _split_queens_anonymous_flag, _is_queens_anonymous_modal_request,
    _clean_queens_linkedin_name, _split_queens_connection_account_text,
    _format_queens_result, _queens_best_results_by_date, _queens_streak_info,
    _QueensAnonymousRegisterModal, _QueensAnonymousRegisterView,
    _QUEENS_SCRAPER_SCRIPT, _QUEENS_DEFAULT_STATE_PATH,
    _AKARI_DIFF_MAX_BYTES, _IMPORT_BATCH_SIZE, _IMPORT_RATE_DELAY,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = logging.getLogger(__name__)


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
            async for message in channel.history(oldest_first=True, limit=None):
                status['scanned'] += 1
                if message.author.bot or not message.content:
                    continue

                if self._is_akari_banned(guild_id, message.author.id, game):
                    continue  # skip banned users entirely (no raw, no result)

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


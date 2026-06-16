"""Message ingestion and Discord listeners. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplIngestMixin:
    # ── Listeners ───────────────────────────────────────────────────────

    async def _ingest_message(self, message, game):
        results = game.parse(strip_codeblock(message.content))
        if not results:
            return 0

        puzzle_date_fallback = message.created_at.date()

        saved = 0
        for parsed in results:
            existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
                message.guild.id, game.name, message.author.id, parsed.puzzle_number
            )
            if existing is not None and str(existing.message_id) != str(message.id):
                logger.info(
                    '%s result ignored (duplicate): guild=%s msg=%s user=%s puzzle=%s first_msg=%s',
                    game.display_name, message.guild.id, message.id,
                    message.author.id, parsed.puzzle_number, existing.message_id,
                )
                continue

            puzzle_date = parsed.puzzle_date or puzzle_date_fallback

            cf_common.user_db.save_minigame_result(
                message.id, message.guild.id, game.name, message.channel.id,
                message.author.id, parsed.puzzle_number,
                puzzle_date.isoformat(), parsed.accuracy,
                parsed.time_seconds, parsed.is_perfect, message.content,
            )
            saved += 1
        return saved

    @staticmethod
    def _is_akari_banned(guild_id, user_id, game):
        """True iff this is an Akari message from a banned user.

        Banning is akari-only — other games (e.g. GuessGame) don't have a
        banlist and pass through.  Used to short-circuit ingest at every entry
        point: live messages, edits, history import, and reparse.
        """
        return (game.name == AKARI_GAME.name
                and cf_common.user_db.is_akari_banned(guild_id, user_id))

    async def _notify_non_pro_mode(self, message):
        """Reply to a non-pro Daily Akari submission asking the user to enable Pro Mode.

        Same best-effort pattern as :meth:`_notify_banned_submission` —
        a failed reply is logged and swallowed so the ingestion path can't be
        broken by a notice failure.
        """
        embed = discord_common.embed_alert(
            "Your result doesn't include accuracy. Please turn on "
            "Pro Mode \U0001f3af\U0001f31f in the settings and submit "
            "again for it to count.")
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify non-pro mode for message %s',
                           message.id, exc_info=True)

    @staticmethod
    def _invalid_minigame_submission_message(game, content):
        if game.name != AKARI_GAME.name:
            return None
        mismatch = akari_date_number_mismatch(content)
        if mismatch is None:
            return None
        if getattr(mismatch, 'out_of_range', False):
            return (
                f'Invalid submission: puzzle number/date mismatch. '
                f'Daily Akari #{mismatch.puzzle_number} is outside the supported '
                f'Daily Akari date range, but this message says '
                f'{mismatch.puzzle_date.isoformat()} (that date is '
                f'#{mismatch.expected_number}). Result not counted. '
                f'You should never play this game again.')
        return (
            f'Invalid submission: puzzle number/date mismatch. '
            f'Daily Akari #{mismatch.puzzle_number} '
            f'is for {mismatch.expected_date.isoformat()}, but this message says '
            f'{mismatch.puzzle_date.isoformat()} (that date is '
            f'#{mismatch.expected_number}). Result not counted. '
            f'You should never play this game again.')

    async def _notify_invalid_minigame_submission(self, message, game, content):
        body = self._invalid_minigame_submission_message(game, content)
        if body is None:
            return False
        embed = discord_common.embed_alert(body)
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify invalid minigame message %s',
                           message.id, exc_info=True)
        return True

    async def _notify_invalid_minigame_submission_from_raw(self, row, game, content):
        if self.bot is None:
            return False
        try:
            channel = await self._resolve_channel(int(row.channel_id))
            message = await channel.fetch_message(int(row.message_id))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException,
                AttributeError, TypeError, ValueError):
            logger.warning(
                'Failed to fetch invalid minigame message %s for retroactive reply',
                getattr(row, 'message_id', None), exc_info=True)
            return False
        return await self._notify_invalid_minigame_submission(
            message, game, content)

    async def _notify_banned_submission(self, message, game):
        """Reply to a banned user's parsable Akari post explaining the ban.

        Only called after we've confirmed the message *would have* produced a
        result — chat messages from banned users in the Akari channel stay
        silent so we don't spam unrelated conversation.  Best-effort: a failed
        reply (deleted message, missing perms) is logged and swallowed so the
        ingestion path can't be broken by a notice failure.
        """
        ban = cf_common.user_db.get_akari_ban(message.guild.id, message.author.id)
        reason = ban.reason if ban is not None else None
        body = f'You are banned from posting {game.display_name} results.'
        if reason:
            body += f'\nReason: {reason}'
        body += '\nAsk a moderator to lift the ban.'
        embed = discord_common.embed_alert(body)
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify banned user for message %s',
                           message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(message)
        if game is not None:
            try:
                cleaned = strip_codeblock(message.content)
                invalid_message = self._invalid_minigame_submission_message(
                    game, cleaned)
                is_submission = bool(game.parse(cleaned)) or (
                    invalid_message is not None) or (
                    game.name == AKARI_GAME.name
                    and looks_like_non_pro_akari(message.content))
                if self._is_akari_banned(message.guild.id, message.author.id, game):
                    # Reply only if this post is a submission attempt — banned
                    # users chatting in the channel stay silent.
                    if is_submission:
                        await self._notify_banned_submission(message, game)
                    return  # never save/ingest for banned users
                if invalid_message is not None:
                    await self._notify_invalid_minigame_submission(
                        message, game, cleaned)
                    return
                # Save raw content for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, message.guild.id, message.channel.id,
                    message.author.id, message.created_at.isoformat(),
                    message.content,
                )
                # Non-pro mode submissions look like results but lack accuracy;
                # ask the user to enable Pro Mode and skip the ingest.
                if (game.name == AKARI_GAME.name
                        and looks_like_non_pro_akari(message.content)):
                    await self._notify_non_pro_mode(message)
                    return
                saved = await self._ingest_message(message, game)
                if saved:
                    self._recompute_game_ratings(message.guild.id, game)
            except Exception:
                logger.error('Error ingesting message %s', message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        if after.guild is None or after.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(after)
        if game is None:
            return
        cleaned = strip_codeblock(after.content)
        invalid_message = self._invalid_minigame_submission_message(game, cleaned)
        is_non_pro = (game.name == AKARI_GAME.name
                      and looks_like_non_pro_akari(after.content))
        if self._is_akari_banned(after.guild.id, after.author.id, game):
            try:
                if game.parse(cleaned) or is_non_pro or invalid_message is not None:
                    await self._notify_banned_submission(after, game)
            except Exception:
                logger.warning('Failed to notify banned edit %s',
                               after.id, exc_info=True)
            return  # leave pre-ban data untouched
        try:
            # Update raw content so future reparse uses the edited version
            cf_common.user_db.update_raw_message(after.id, after.content)
            if invalid_message is not None:
                changed = cf_common.user_db.delete_minigame_result(after.id)
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
                await self._notify_invalid_minigame_submission(after, game, cleaned)
                if changed:
                    self._recompute_game_ratings(after.guild.id, game)
                return
            # An edit into a non-pro shape: drop any prior result for this
            # message and tell the user.  Same skip-the-ingest path on_message
            # uses for fresh non-pro posts.
            if is_non_pro:
                changed = cf_common.user_db.delete_minigame_result(after.id)
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
                await self._notify_non_pro_mode(after)
                if changed:
                    self._recompute_game_ratings(after.guild.id, game)
                return
            # Delete all existing live results for this message, then re-ingest.
            # Handles the case where an edit removes some results from a multi-result message.
            changed = cf_common.user_db.delete_minigame_result(after.id)
            results = game.parse(cleaned)
            if results:
                changed += await self._ingest_message(after, game)
            else:
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
            if changed:
                self._recompute_game_ratings(after.guild.id, game)
        except Exception:
            logger.error('Error handling message edit %s', after.id, exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        try:
            old = cf_common.user_db.get_minigame_result(payload.message_id)
            deleted = cf_common.user_db.delete_minigame_result(payload.message_id)
            deleted += cf_common.user_db.delete_imported_minigame_result(payload.message_id)
            cf_common.user_db.delete_raw_message(payload.message_id)
            if deleted and old is not None and old.game in self.GAMES:
                self._recompute_game_ratings(
                    payload.guild_id, self.GAMES[old.game])
        except Exception:
            logger.error('Error handling message delete %s', payload.message_id, exc_info=True)

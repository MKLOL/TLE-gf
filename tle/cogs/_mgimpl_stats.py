"""Stats dispatch and import-management commands. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplStatsMixin:
    async def _cmd_stats(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        # Akari accepts +decay / +exclude=... / +include=... anywhere in the
        # arg list — strip those before falling into the per-user /
        # per-puzzle dispatch so the remaining tokens are just the selector
        # (or a member name).
        excluded_ids = set()
        included_ids = set()
        test_decay = False
        if game.name == AKARI_GAME.name:
            (remaining, _include_decay, excluded_ids, included_ids,
             _include_inactive, test_decay) = await self._extract_akari_filters(
                ctx, args)
            args = tuple(remaining)
        if game.name == 'akari' and len(args) == 1:
            if _maybe_parse_puzzle_selector(args[0]) is not None:
                await self._cmd_akari_stats_puzzle(
                    ctx, args[0],
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    test_decay=test_decay)
                return

        filter_args = list(args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except MinigameCogError:
                member = ctx.author

        try:
            dlo, dhi, plo, phi = parse_date_args(filter_args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        plotter = self._STATS_PLOTTERS.get(game.name)
        if plotter is None:
            raise MinigameCogError(f'Stats are not available for {game.display_name}.')

        discord_file = plotter(rows, _safe_member_name(member))
        await ctx.send(file=discord_file)

    async def _cmd_import_start(self, ctx, game, channel=None):
        key = (ctx.guild.id, game.name)
        if key in self._import_tasks:
            task = self._import_tasks[key]
            if not task.done():
                raise MinigameCogError(
                    f'A {game.display_name} import is already running.')

        configured_channel_id = self._get_channel(ctx.guild.id, game.name)
        if channel is None and configured_channel_id is not None:
            try:
                channel = await self._resolve_channel(int(configured_channel_id))
            except discord.NotFound:
                pass
        channel = channel or ctx.channel

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name, channel_id=channel.id)
        self._import_status[key] = {
            'state': 'running',
            'channel_id': channel.id,
            'scanned': 0,
            'done': 0,
            'skipped': [],
            'error': None,
            'latest_message_id': None,
            'cleared': deleted,
            'started_at': dt.datetime.now(),
        }
        task = asyncio.create_task(self._run_import(ctx.guild.id, channel.id, game))
        self._import_tasks[key] = task

        # Save reply target so the background task can reply when done
        kvs_key = f'{self._KVS_IMPORT_PREFIX}{ctx.guild.id}:{game.name}'
        cf_common.user_db.kvs_set(kvs_key, f'{ctx.channel.id}:{ctx.message.id}')

        logger.info(
            '%s import started: guild=%s channel=%s cleared=%d',
            game.display_name, ctx.guild.id, channel.id, deleted,
        )
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import started for {channel.mention}. '
            f'Cleared {deleted} imported row(s) first.'))

    async def _cmd_import_status(self, ctx, game):
        key = (ctx.guild.id, game.name)
        status = self._import_status.get(key)
        if status is None:
            raise MinigameCogError(
                f'No {game.display_name} import has been started.')

        elapsed = dt.datetime.now() - status['started_at']
        elapsed_str = str(elapsed).split('.')[0]  # drop microseconds
        lines = [
            f'state: `{status["state"]}`',
            f'channel: <#{status["channel_id"]}>',
            f'messages scanned: **{status["scanned"]}**',
            f'results imported: **{status["done"]}**',
            f'elapsed: `{elapsed_str}`',
        ]
        if status['latest_message_id'] is not None:
            lines.append(f'latest message: `{status["latest_message_id"]}`')
        skipped = status.get('skipped', [])
        if skipped:
            lines.append(f'detected but unparseable: **{len(skipped)}** '
                         f'(IDs: {", ".join(skipped[:10])}{"…" if len(skipped) > 10 else ""})')
        if status['error']:
            lines.append(f'error: `{status["error"]}`')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_import_cancel(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is None or task.done():
            raise MinigameCogError(
                f'No {game.display_name} import is currently running.')
        task.cancel()
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import cancelled.'))

    async def _cmd_import_clear(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is not None and not task.done():
            raise MinigameCogError(
                f'Cancel the running {game.display_name} import before clearing it.')

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        self._import_status.pop(key, None)
        self._recompute_game_ratings(ctx.guild.id, game)
        await ctx.send(embed=discord_common.embed_success(
            f'Deleted {deleted} imported {game.display_name} row(s). '
            f'Raw messages preserved for reparse.'))

    async def _cmd_import_orphans(self, ctx, game):
        """Temporary audit: list imported results that have no live counterpart
        for the same (user, puzzle) — i.e. rows that exist only because of an
        ``import start``.  Handy for spotting junk left behind by a bad import.
        """
        rows = cf_common.user_db.get_import_only_minigame_results(
            ctx.guild.id, game.name)
        if not rows:
            await ctx.send(embed=discord_common.embed_success(
                f'No import-only {game.display_name} results — every imported '
                f'result has a live counterpart.'))
            return

        per_page = 10
        title = (f'{game.display_name} import-only results '
                 f'({len(rows)} total)')
        pages = []
        for page_idx, chunk in enumerate(paginator.chunkify(rows, per_page)):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                name = self._minigame_public_user_name(
                    ctx.guild, game, row.user_id)
                if row.is_perfect:
                    result_str = f'\N{GLOWING STAR} {format_duration(row.time_seconds)}'
                else:
                    result_str = f'{row.accuracy}% {format_duration(row.time_seconds)}'
                date_str = normalize_puzzle_date(row.puzzle_date).isoformat()
                lines.append(
                    f'**{rank}.** `{name}` \N{MIDDLE DOT} '
                    f'#{row.puzzle_number} \N{MIDDLE DOT} {date_str} '
                    f'\N{MIDDLE DOT} {result_str} '
                    f'\N{MIDDLE DOT} msg `{row.message_id}`')
            embed = discord.Embed(
                title=title,
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    @staticmethod
    def _format_winner_value(value):
        """Render a merged-winner ``(time, is_perfect, accuracy)`` tuple."""
        if value is None:
            return '\N{EM DASH}'
        time_seconds, is_perfect, accuracy = value
        result = ('\N{GLOWING STAR}' if is_perfect
                  else f'{accuracy}%')
        return f'{result} {format_duration(time_seconds)}'


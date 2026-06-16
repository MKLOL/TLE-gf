"""Shared here/clear/show/vs/top/streak/remove commands. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplSharedCmdMixin:
    # ── Shared command implementations ──────────────────────────────────

    async def _cmd_here(self, ctx, game):
        cf_common.user_db.set_minigame_channel(ctx.guild.id, game.name, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel set to {ctx.channel.mention}'
        ))

    async def _cmd_clear(self, ctx, game):
        cf_common.user_db.clear_minigame_channel(ctx.guild.id, game.name)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel cleared.'
        ))

    async def _cmd_show(self, ctx, game):
        enabled = self._is_enabled(ctx.guild.id, game.feature_flag)
        channel_id = self._get_channel(ctx.guild.id, game.name)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            f'channel: {channel}',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {game.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @staticmethod
    def _guessgame_puzzle_url(puzzle_number):
        return f'https://guessthe.game/p/{int(puzzle_number)}'

    @staticmethod
    def _format_guessgame_result(row):
        if row is None:
            return 'no result'

        accuracy = int(getattr(row, 'accuracy', 0))
        yellow_pos = int(getattr(row, 'time_seconds', 7))
        if accuracy > 0:
            green_pos = 7 - accuracy
            if green_pos == 1:
                return 'perfect'
            return f'green {green_pos}'
        if yellow_pos < 7:
            return f'yellow {yellow_pos}'
        return 'no green'

    def _make_guessgame_vs_pages(self, ctx, game, member1, member2, stats, matchups, scoring_name):
        title_suffix = f' ({scoring_name.title()})' if scoring_name else ''
        summary_lines = [
            f'`{_safe_member_name(member1)}`: **{_format_score(stats["score1"])}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{_format_score(stats["score2"])}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ]

        pages = []
        per_page = 10
        ordered_matchups = list(reversed(matchups))
        for chunk in paginator.chunkify(ordered_matchups, per_page):
            embed = discord.Embed(
                title=f'{game.display_name} Head to Head{title_suffix}',
                description='\n'.join(summary_lines),
                color=discord_common.random_cf_color(),
            )

            col1 = []
            col2 = []
            for matchup in chunk:
                row1 = matchup['row1']
                row2 = matchup['row2']
                puzzle_number = int(
                    row1.puzzle_number if row1 is not None else row2.puzzle_number
                )
                puzzle_link = f'[#{puzzle_number}]({self._guessgame_puzzle_url(puzzle_number)})'
                col1.append(
                    f'{puzzle_link} {self._format_guessgame_result(row1)}'
                    f' · {_format_score(matchup["score1"])} pts'
                )
                col2.append(
                    f'{puzzle_link} {self._format_guessgame_result(row2)}'
                    f' · {_format_score(matchup["score2"])} pts'
                )

            embed.add_field(
                name=_safe_member_name(member1),
                value='\n'.join(col1),
                inline=True,
            )
            embed.add_field(
                name=_safe_member_name(member2),
                value='\n'.join(col2),
                inline=True,
            )
            pages.append((None, embed))

        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_vs(self, ctx, game, member1, member2, *args):
        self._require_enabled(ctx.guild.id, game)
        self._sync_minigame_results_for_read(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            weekdays = None
            if game.name == QUEENS_GAME.name:
                args, weekdays = _split_queens_weekday_filter(args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        rows1 = self._filter_minigame_banned_rows(ctx.guild.id, game, rows1)
        rows2 = self._filter_minigame_banned_rows(ctx.guild.id, game, rows2)
        if game.name == QUEENS_GAME.name:
            rows1 = _filter_queens_weekday_rows(rows1, weekdays)
            rows2 = _filter_queens_weekday_rows(rows2, weekdays)
        stats = compute_vs(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        suffix_parts = []
        if scoring_name:
            suffix_parts.append(scoring_name.title())
        weekday_label = (
            _format_queens_weekday_filter(weekdays)
            if game.name == QUEENS_GAME.name else '')
        if weekday_label:
            suffix_parts.append(weekday_label)
        title_suffix = f' ({", ".join(suffix_parts)})' if suffix_parts else ''
        name1 = self._minigame_public_user_name(ctx.guild, game, member1.id)
        name2 = self._minigame_public_user_name(ctx.guild, game, member2.id)
        description = '\n'.join([
            f'`{name1}`: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'`{name2}`: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ])
        embed = discord.Embed(
            title=f'{game.display_name} Head to Head{title_suffix}',
            description=description,
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_guessgame_matchups(self, ctx, member1, member2, *args):
        game = GUESSGAME_GAME
        self._require_enabled(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        stats = compute_vs(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        matchups = compute_vs_matchups(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        self._make_guessgame_vs_pages(
            ctx, game, member1, member2, stats, matchups, scoring_name)

    async def _cmd_streak(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
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
        streak = compute_streak(rows)
        longest = compute_longest_streak(rows)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        best = pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        embed = discord.Embed(
            title=f'{game.display_name} Streak',
            description='\n'.join([
                f'`{_safe_member_name(member)}`: **{streak}** consecutive perfect day(s)',
                f'Longest streak: **{longest}** day(s)',
                f'Latest result: **{latest_status}** in **{format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_top(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        self._sync_minigame_results_for_read(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            weekdays = None
            if game.name == QUEENS_GAME.name:
                args, weekdays = _split_queens_weekday_filter(args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, game.name, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, game, rows)
        if game.name == QUEENS_GAME.name:
            rows = self._filter_queens_registered_result_rows(ctx.guild.id, rows)
            rows = _filter_queens_weekday_rows(rows, weekdays)
        winners = compute_top(
            rows,
            is_eligible=scoring.is_eligible_winner,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            winner_result_sort_key_fn=scoring.winner_result_sort_key,
            group_key_fn=scoring.result_group_key,
        )
        if not winners:
            raise MinigameCogError(
                f'No {game.display_name} winners found for this range.')

        suffix_parts = []
        if scoring_name:
            suffix_parts.append(scoring_name.title())
        weekday_label = (
            _format_queens_weekday_filter(weekdays)
            if game.name == QUEENS_GAME.name else '')
        if weekday_label:
            suffix_parts.append(weekday_label)
        title_suffix = f' ({", ".join(suffix_parts)})' if suffix_parts else ''
        pages = []
        per_page = 10
        for page_idx, chunk in enumerate(paginator.chunkify(winners, per_page)):
            lines = []
            for i, (user_id, wins) in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                name = self._minigame_public_user_name(ctx.guild, game, user_id)
                lines.append(f'**#{rank}** `{name}` — **{wins}** wins')
            embed = discord.Embed(
                title=f'{game.display_name} Winners{title_suffix}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_remove(self, ctx, game, member, puzzle_id):
        rc = cf_common.user_db.delete_minigame_result_for_user_puzzle(
            ctx.guild.id, game.name, member.id, puzzle_id)
        if not rc:
            raise MinigameCogError(
                f'No {game.display_name} result found for '
                f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.')
        if game.name == AKARI_GAME.name:
            self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {game.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.'))


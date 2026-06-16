"""Queens add/remove/clear/clean and ratings/rating commands. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplQueensCmdMixin:
    async def _cmd_queens_clear(self, ctx, puzzle_date):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if puzzle_date is None:
            raise MinigameCogError('Usage: `;queens clear DATE/#`.')
        parsed_date = _parse_queens_date_or_number(puzzle_date)
        parsed_number = _queens_puzzle_number_for_date(parsed_date)
        deleted = 0
        unresolved_deleted = 0
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            deleted += cf_common.user_db.delete_minigame_results_for_puzzle(
                ctx.guild.id, QUEENS_GAME.name, puzzle_number)
            unresolved_deleted += (
                cf_common.user_db.delete_minigame_unresolved_results_for_puzzle(
                    ctx.guild.id, QUEENS_GAME.name, puzzle_number))
        if not deleted and not unresolved_deleted:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'{parsed_date.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} registered and {unresolved_deleted} unresolved '
            f'{QUEENS_GAME.display_name} result(s) for '
            f'#{parsed_number} {parsed_date.isoformat()}.'))

    async def _cmd_queens_clean(self, ctx, start_date, end_date=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if start_date is None:
            raise MinigameCogError('Usage: `;queens clean START_DATE [END_DATE]`.')
        parsed_start = _parse_queens_date_or_number(start_date)
        parsed_end = (
            _parse_queens_date_or_number(end_date)
            if end_date is not None
            else parsed_start
        )
        if parsed_end < parsed_start:
            raise MinigameCogError('Queens clean end date cannot be before start date.')

        days = (parsed_end - parsed_start).days + 1
        end_exclusive = parsed_end + dt.timedelta(days=1)
        deleted = cf_common.user_db.delete_minigame_results_for_date_range(
            ctx.guild.id, QUEENS_GAME.name,
            _queens_puzzle_date_text(parsed_start),
            _queens_puzzle_date_text(end_exclusive))
        unresolved_deleted = (
            cf_common.user_db.delete_minigame_unresolved_results_for_date_range(
                ctx.guild.id, QUEENS_GAME.name,
                _queens_puzzle_date_text(parsed_start),
                _queens_puzzle_date_text(end_exclusive)))

        if not deleted and not unresolved_deleted:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found from '
                f'{parsed_start.isoformat()} to {parsed_end.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} registered and {unresolved_deleted} unresolved '
            f'{QUEENS_GAME.display_name} result(s) from '
            f'{parsed_start.isoformat()} to {parsed_end.isoformat()} '
            f'({days} day(s)).'))

    async def _cmd_queens_ratings_recompute(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._sync_queens_materialized_results(ctx.guild.id)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'{QUEENS_GAME.display_name} ratings recomputed.'))

    async def _extract_queens_rating_filters(self, ctx, args):
        args, weekdays = _split_queens_weekday_filter(args)
        args, date_bounds = _split_queens_rating_date_filter(args)
        (remaining, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        if include_decay or test_decay:
            raise MinigameCogError(
                f'{QUEENS_GAME.display_name} ratings do not use decay.')
        return remaining, excluded_ids, included_ids, weekdays, date_bounds

    async def _parse_queens_rating_args(self, ctx, args, *,
                                        member_required=False,
                                        allow_recalculate=False):
        args, recalculate = _split_queens_recalculate_filter(args)
        if recalculate and not allow_recalculate:
            raise MinigameCogError(
                '`+recalculate` is only supported by `;queens rating`.')
        remaining, excluded_ids, included_ids, weekdays, date_bounds = (
            await self._extract_queens_rating_filters(ctx, args))
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return (
            members, excluded_ids, included_ids, weekdays, date_bounds,
            recalculate,
        )

    async def _cmd_queens_ratings(self, ctx, *, show_all=False,
                                  excluded_ids=None, included_ids=None,
                                  weekdays=None, date_bounds=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        if (excluded_ids or included_ids or weekdays is not None
                or date_bounds is not None):
            rows = self._minigame_rating_rows(
                ctx.guild.id, QUEENS_GAME,
                excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds)
        else:
            rows = cf_common.user_db.get_minigame_ratings(
                ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} ratings yet.')
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        linked_ids = set(links_by_user)
        shown = rows if show_all else [row for row in rows if row.user_id in linked_ids]
        if not shown:
            raise MinigameCogError(
                f'No registered {QUEENS_GAME.display_name} players yet. '
                f'Players register with `;queens register LinkedIn Name`.')
        if show_all:
            suffix_parts = ['all']
            weekday_label = _format_queens_weekday_filter(weekdays)
            if weekday_label:
                suffix_parts.append(weekday_label)
            date_label = _format_queens_date_filter(date_bounds)
            if date_label:
                suffix_parts.append(date_label)
            title = (
                f'{QUEENS_GAME.display_name} Ratings '
                f'({", ".join(suffix_parts)})')
        else:
            title = (
                f'{QUEENS_GAME.display_name} Ratings'
                f'{_queens_filter_suffix(weekdays=weekdays, date_bounds=date_bounds)}')
        discord_file = _mg()._get_akari_rating_table_image_file(
            ctx.guild, shown, linked_ids,
            title=title,
            mark_registered=show_all,
            identity_label='LinkedIn',
            identity_fn=self._queens_rating_identity_fn(links_by_user),
            name_fn=self._queens_name_fn(links_by_user))
        await ctx.send(file=discord_file)

    async def _cmd_queens_rating(self, ctx, members, *,
                                 require_registered=True,
                                 excluded_ids=None, included_ids=None,
                                 weekdays=None, date_bounds=None,
                                 recalculate=False):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            for member in members:
                self._require_queens_registered_member(ctx.guild.id, member)

        replay_date_bounds = date_bounds if recalculate else None
        filtered = bool(excluded_ids or included_ids or weekdays is not None
                        or replay_date_bounds is not None)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._minigame_user_data(
                    ctx.guild.id, QUEENS_GAME, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    weekdays=weekdays, date_bounds=replay_date_bounds)
            else:
                row = cf_common.user_db.get_minigame_rating(
                    ctx.guild.id, QUEENS_GAME.name, member.id)
                history = self._minigame_user_history(
                    ctx.guild.id, QUEENS_GAME, member.id)
            if not recalculate:
                history = _filter_queens_rating_date_history(history, date_bounds)
            if row is None:
                raise MinigameCogError(
                    f'No {QUEENS_GAME.display_name} rating for '
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` yet.')
            if not history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no rated '
                    f'{QUEENS_GAME.display_name} days to plot yet.')
            graph_history = _filter_queens_contested_rating_history(history)
            if not graph_history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no contested '
                    f'{QUEENS_GAME.display_name} days to plot yet.')
            per_member.append((member, row, history, graph_history))

        series = [
            (graph_history, self._queens_legend_name(ctx.guild.id, member))
            for member, _row, _history, graph_history in per_member
        ]
        discord_file = _mg().plot_akari_rating(series)

        def _display_rating(row, history):
            return history[-1].rating if date_bounds is not None else row.rating

        def _display_peak(row, history):
            if date_bounds is None:
                return row.peak
            return max(point.rating for point in history)

        def _display_games(row, history):
            if date_bounds is None:
                return row.games
            return sum(1 for point in history
                       if not getattr(point, 'is_decay', False))

        if len(per_member) == 1:
            member, row, history, _graph_history = per_member[0]
            display_name = self._queens_public_user_name(ctx.guild, member.id)
            rating = round(_display_rating(row, history))
            rank = rank_for_rating(rating)
            peak = round(_display_peak(row, history))
            peak_rank = rank_for_rating(peak)
            last_contest = next((h for h in reversed(history)
                                 if h.performance is not None), None)
            last_change_str = (f'{last_contest.delta:+.0f}'
                               if last_contest is not None else '—')
            last_perf_str = (
                f'{round(last_contest.performance)} '
                f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                if last_contest is not None else '—')
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} rating — '
                       f'{display_name}'),
                color=rank.color_embed,
            )
            embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
            embed.add_field(name='Peak', value=f'{peak} ({peak_rank.title_abbr})')
            embed.add_field(name='Games', value=str(_display_games(row, history)))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            _top_member, top_row, top_history, _top_graph_history = max(
                per_member, key=lambda t: _display_rating(t[1], t[2]))
            top_rank = rank_for_rating(
                round(_display_rating(top_row, top_history)))

            def _rating_line(member, row, history):
                rating = round(_display_rating(row, history))
                return (
                    f'**{self._queens_public_user_name(ctx.guild, member.id)}**: '
                    f'{rating} ({rank_for_rating(rating).title_abbr})'
                )

            lines = [
                _rating_line(member, row, history)
                for member, row, history, _graph_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} ratings — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)


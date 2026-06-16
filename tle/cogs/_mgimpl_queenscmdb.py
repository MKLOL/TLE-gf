"""Queens performance/history/show/streak/stats commands. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplQueensCmdBMixin:
    async def _cmd_queens_performance(self, ctx, members, *,
                                      require_registered=True,
                                      excluded_ids=None, included_ids=None,
                                      weekdays=None, date_bounds=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            for member in members:
                self._require_queens_registered_member(ctx.guild.id, member)

        filtered = bool(excluded_ids or included_ids or weekdays is not None)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._minigame_user_data(
                    ctx.guild.id, QUEENS_GAME, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    weekdays=weekdays)
            else:
                row = cf_common.user_db.get_minigame_rating(
                    ctx.guild.id, QUEENS_GAME.name, member.id)
                history = self._minigame_user_history(
                    ctx.guild.id, QUEENS_GAME, member.id)
            history = _filter_queens_rating_date_history(history, date_bounds)
            if row is None:
                raise MinigameCogError(
                    f'No {QUEENS_GAME.display_name} rating for '
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` yet.')
            contest_history = [h for h in history if h.performance is not None]
            if not contest_history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no contested '
                    f'{QUEENS_GAME.display_name} days to plot performance for yet.')
            per_member.append((member, row, history, contest_history))

        series = [
            (
                history,
                self._queens_legend_name(ctx.guild.id, member),
                round(history[-1].rating if date_bounds is not None else row.rating),
            )
            for member, row, history, _contest_history in per_member
        ]
        discord_file = _mg().plot_akari_performance(series)

        if len(per_member) == 1:
            member, _row, _history, contest_history = per_member[0]
            display_name = self._queens_public_user_name(ctx.guild, member.id)
            last_perf = contest_history[-1].performance
            last_rank = rank_for_rating(round(last_perf))
            best_perf = max(h.performance for h in contest_history)
            best_rank = rank_for_rating(round(best_perf))
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} performance — '
                       f'{display_name}'),
                color=last_rank.color_embed,
            )
            embed.add_field(name='Last performance',
                            value=f'{round(last_perf)} ({last_rank.title_abbr})')
            embed.add_field(name='Best performance',
                            value=f'{round(best_perf)} ({best_rank.title_abbr})')
            embed.add_field(name='Contests', value=str(len(contest_history)))
        else:
            top_rank = rank_for_rating(round(max(
                contest_history[-1].performance
                for _member, _row, _history, contest_history in per_member)))
            lines = [
                f'**{self._queens_public_user_name(ctx.guild, member.id)}**: '
                f'last {round(contest_history[-1].performance)} '
                f'({rank_for_rating(round(contest_history[-1].performance)).title_abbr})'
                for member, _row, _history, contest_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} performance — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_queens_history(self, ctx, member, *,
                                  require_registered=True,
                                  excluded_ids=None, included_ids=None,
                                  weekdays=None, date_bounds=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            self._require_queens_registered_member(ctx.guild.id, member)

        history = self._minigame_user_history(
            ctx.guild.id, QUEENS_GAME, member.id,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)
        played_history = [h for h in history if not h.is_decay]
        if not played_history:
            raise MinigameCogError(
                f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no '
                f'{QUEENS_GAME.display_name} days yet.')

        lines = [_format_minigame_history_line(h)
                 for h in reversed(played_history)]
        day_label = 'day' if len(played_history) == 1 else 'days'
        title = (f'{QUEENS_GAME.display_name} rating history — '
                 f'{self._queens_public_user_name(ctx.guild, member.id)} '
                 f'({len(played_history)} {day_label})')
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            embed = discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    async def _cmd_queens_show(self, ctx):
        enabled = self._is_enabled(ctx.guild.id, QUEENS_GAME.feature_flag)
        links = cf_common.user_db.get_minigame_player_links(
            ctx.guild.id, QUEENS_GAME.name)
        rows = cf_common.user_db.get_minigame_unresolved_results_for_guild(
            ctx.guild.id, QUEENS_GAME.name)
        dates = {_format_queens_date(row) for row in rows}
        account = self._get_queens_connection_account(ctx.guild.id)
        account_text = 'not set'
        if account is not None:
            account_text = account['name']
            if account.get('url'):
                account_text += f' <{account["url"]}>'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            'ingest: manual leaderboard import',
            f'connection account: {account_text}',
            f'linked players: **{len(links)}**',
            f'results: **{len(rows)}** across **{len(dates)}** date(s)',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {QUEENS_GAME.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_queens_streak(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        filter_args = list(args)
        filter_args, weekdays = _split_queens_weekday_filter(filter_args)
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
            ctx.guild.id, QUEENS_GAME.name, member.id, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        display_name = self._queens_public_user_name(ctx.guild, member.id)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'`{display_name}`.')

        current, longest, latest = _queens_streak_info(rows)
        latest_status = (
            'no hints & no mistakes'
            if latest.is_perfect
            else 'not clean'
        )
        description = '\n'.join([
            f'`{display_name}`: **{current}** consecutive clean day(s)',
            f'Longest clean streak: **{longest}** day(s)',
            f'Latest result: **{_format_queens_date(latest)}**, **{format_duration(latest.time_seconds)}**, {latest_status}',
        ])
        await ctx.send(embed=discord.Embed(
            title=(f'{QUEENS_GAME.display_name} Streak'
                   f'{_queens_weekday_filter_suffix(weekdays)}'),
            description=description,
            color=discord_common.random_cf_color(),
        ))

    async def _cmd_queens_stats(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        filter_args = list(args)
        filter_args, weekdays = _split_queens_weekday_filter(filter_args)
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
            ctx.guild.id, QUEENS_GAME.name, member.id, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        best = _queens_best_results_by_date(rows)
        display_name = self._queens_public_user_name(ctx.guild, member.id)
        if not best:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'`{display_name}`.')

        results = [best[day] for day in sorted(best)]
        discord_file = _mg().plot_queens_stats(
            results,
            display_name,
            title_suffix=_queens_weekday_filter_suffix(weekdays))
        await ctx.send(file=discord_file)

    async def _cmd_queens_stats_date(self, ctx, date_arg, *,
                                     show_all=False, excluded_ids=None,
                                     included_ids=None, weekdays=None,
                                     date_bounds=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        puzzle_date = _parse_queens_date_or_number(date_arg)
        puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
        day_start = dt.datetime.combine(puzzle_date, dt.time.min).timestamp()
        day_end = dt.datetime.combine(
            puzzle_date + dt.timedelta(days=1), dt.time.min).timestamp()
        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, QUEENS_GAME.name, dlo=day_start, dhi=day_end)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        if not show_all:
            rows = self._filter_queens_registered_result_rows(
                ctx.guild.id, rows, links_by_user=links_by_user)
        if not rows:
            if show_all:
                raise MinigameCogError(
                    f'No {QUEENS_GAME.display_name} results found for '
                    f'`{puzzle_date.isoformat()}`.')
            raise MinigameCogError(
                f'No registered {QUEENS_GAME.display_name} results found for '
                f'`{puzzle_date.isoformat()}`.')

        puzzle_numbers = {int(row.puzzle_number) for row in rows}
        puzzle_info = None
        registrants = None
        if len(puzzle_numbers) == 1:
            puzzle_info = self._minigame_puzzle_change_info(
                ctx.guild.id, QUEENS_GAME, next(iter(puzzle_numbers)),
                excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds)
            registrants = (
                set(puzzle_info.keys())
                if show_all
                else set(links_by_user)
            )
        discord_file = _mg()._get_queens_results_table_image_file(
            ctx.guild, rows,
            f'{QUEENS_GAME.display_name} #{puzzle_number} '
            f'{puzzle_date.isoformat()} Results'
            f'{_queens_filter_suffix(weekdays=weekdays, date_bounds=date_bounds)}',
            puzzle_info=puzzle_info,
            registrants=registrants,
            identity_label='LinkedIn',
            identity_fn=self._queens_rating_identity_fn(links_by_user),
            name_fn=self._queens_name_fn(links_by_user),
            sort_key_fn=lambda row: (
                int(getattr(row, 'time_seconds', 0)),
                int(getattr(row, 'message_id', 0)),
            ))
        await ctx.send(file=discord_file)


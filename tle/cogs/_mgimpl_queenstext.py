"""Queens text-command bodies (admins/links/connection/ban/import) (Minigames cog impl mixin; see minigames.py)."""

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


class ImplQueensTextMixin:

    async def _cmd_queens_admins(self, ctx):
        admin_ids = self._queens_admin_ids(ctx.guild.id)
        if not admin_ids:
            await ctx.send(embed=discord_common.embed_neutral(
                'No extra LinkedIn Queens admins configured.'))
            return
        lines = [
            f'- {_safe_user_name(ctx.guild, user_id)} (`{user_id}`)'
            for user_id in sorted(admin_ids, key=self._user_id_sort_key)
        ]
        await ctx.send(embed=discord_common.embed_neutral(
            'Extra LinkedIn Queens admins:\n' + '\n'.join(lines)))

    async def _cmd_queens_admins_add(self, ctx, member):
        if not self._has_server_mod_role(ctx.author):
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                'can change the LinkedIn Queens admin list.')
        admin_ids = self._queens_admin_ids(ctx.guild.id)
        before = len(admin_ids)
        admin_ids.add(str(member.id))
        self._set_queens_admin_ids(ctx.guild.id, admin_ids)
        if len(admin_ids) == before:
            message = (
                f'`{_safe_member_name(member)}` already has '
                'LinkedIn Queens admin access.')
        else:
            message = (
                f'`{_safe_member_name(member)}` can now run '
                'LinkedIn Queens mod commands.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _cmd_queens_admins_remove(self, ctx, member):
        if not self._has_server_mod_role(ctx.author):
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                'can change the LinkedIn Queens admin list.')
        admin_ids = self._queens_admin_ids(ctx.guild.id)
        removed = str(member.id) in admin_ids
        admin_ids.discard(str(member.id))
        self._set_queens_admin_ids(ctx.guild.id, admin_ids)
        if removed:
            message = (
                f'`{_safe_member_name(member)}` no longer has '
                'LinkedIn Queens admin access.')
        else:
            message = (
                f'`{_safe_member_name(member)}` was not an extra '
                'LinkedIn Queens admin.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _cmd_queens_register_cmd(self, ctx, first, linkedin):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if _is_queens_anonymous_modal_request(first, linkedin):
            await ctx.send(
                embed=discord_common.embed_neutral(
                    'Click the button below to enter your LinkedIn name '
                    'privately. Only you can use this prompt, and your '
                    'LinkedIn name will not be posted in the channel.'),
                view=_QueensAnonymousRegisterView(self, ctx.author.id))
            return
        member, linkedin_text, anonymous = await self._resolve_queens_registration_args(
            ctx, first, linkedin)
        await self._cmd_queens_register(
            ctx, member, linkedin_text, anonymous=anonymous)

    async def _cmd_queens_set_cmd(self, ctx, member, linkedin):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if member is None or not (linkedin or '').strip():
            raise MinigameCogError(
                'Usage: `;queens set [+anon] DiscordUser LinkedIn Name [+anon]`.')
        prefix_anonymous = False
        member_text = member
        linkedin_arg = linkedin.strip()
        if str(member).casefold() in _QUEENS_ANONYMOUS_FLAGS:
            prefix_anonymous = True
            tokens = linkedin_arg.split(maxsplit=1)
            if len(tokens) < 2:
                raise MinigameCogError(
                    'Usage: `;queens set [+anon] DiscordUser LinkedIn Name [+anon]`.')
            member_text, linkedin_arg = tokens
        target = await self._resolve_member(ctx, member_text)
        linkedin_text, suffix_anonymous = _split_queens_anonymous_flag(
            linkedin_arg)
        anonymous = prefix_anonymous or suffix_anonymous
        if not linkedin_text:
            raise MinigameCogError(
                'Usage: `;queens set [+anon] DiscordUser LinkedIn Name [+anon]`.')
        await self._cmd_queens_set(
            ctx, target, linkedin_text, anonymous=anonymous)

    async def _cmd_queens_links(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        rows = cf_common.user_db.get_minigame_player_links(
            ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} links registered.')
        lines = []
        for row in rows:
            display_name = self._queens_public_user_name(
                ctx.guild, row.user_id, {str(row.user_id): row})
            lines.append(
                f'- {display_name}: `{_queens_public_link_name(row)}`')
        pages = []
        for chunk in paginator.chunkify(lines, _QUEENS_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=f'{QUEENS_GAME.display_name} links',
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    async def _cmd_queens_connection(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        account = self._get_queens_connection_account(ctx.guild.id)
        if account is None:
            raise MinigameCogError(
                'No LinkedIn connection account configured yet.')
        await ctx.send(embed=discord_common.embed_neutral(
            self._queens_connection_instruction(ctx.guild.id)))

    async def _cmd_queens_connection_set(self, ctx, linkedin):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        name, external_url = _split_queens_connection_account_text(linkedin)
        self._set_queens_connection_account(ctx.guild.id, name, external_url)
        await ctx.send(embed=discord_common.embed_success(
            self._queens_connection_instruction(ctx.guild.id)))

    async def _cmd_queens_connection_clear(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._clear_queens_connection_account(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            'Cleared the LinkedIn Queens connection account.'))

    async def _cmd_queens_ban(self, ctx, member, reason):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        added = cf_common.user_db.ban_minigame_user(
            ctx.guild.id, QUEENS_GAME.name, member.id, time.time(),
            ctx.author.id, reason)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        if not added:
            raise MinigameCogError(
                f'`{display_name}` is already banned from '
                f'{QUEENS_GAME.display_name}.')
        self._migrate_legacy_queens_results_to_external(ctx.guild.id)
        if link is not None:
            self._delete_queens_materialized_results_for_link(
                ctx.guild.id, link)
        cf_common.user_db.delete_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        lines = [
            f'`{display_name}` is now banned from '
            f'{QUEENS_GAME.display_name}. They will be skipped by imports, '
            'manual adds, and rating recomputes.',
            'Their LinkedIn Queens registration was removed.',
        ]
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    async def _cmd_queens_bans(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        rows = cf_common.user_db.get_minigame_bans(
            ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No active {QUEENS_GAME.display_name} bans.')
        lines = [_format_akari_ban_line(ctx.guild, row) for row in rows]
        title = f'{QUEENS_GAME.display_name} bans ({len(rows)})'
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    async def _cmd_queens_import_preview(self, ctx, puzzle_date, leaderboard):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if puzzle_date is None or leaderboard is None:
            raise MinigameCogError(
                'Usage: `;queens import DATE <pasted leaderboard>`.')
        preview = self._make_queens_import_preview(ctx, puzzle_date, leaderboard)
        self._queens_pending_imports[(ctx.guild.id, ctx.author.id)] = preview
        await ctx.send(embed=discord_common.embed_neutral(
            self._format_queens_import_preview(ctx, preview)))

    async def _cmd_queens_import_confirm(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        key = (ctx.guild.id, ctx.author.id)
        preview = self._queens_pending_imports.pop(key, None)
        if preview is None:
            raise MinigameCogError(
                'No pending Queens import preview. Run `;queens import` first.')
        saved = self._save_queens_import(ctx, preview)
        if not saved.resolved and not saved.unresolved:
            await ctx.send(embed=discord_common.embed_neutral(
                f'No new {QUEENS_GAME.display_name} result(s) for '
                f'#{preview.puzzle_number} {preview.puzzle_date.isoformat()}.'))
            return
        unresolved = (
            f' Stored {saved.unresolved} unresolved result(s) for later registration.'
            if saved.unresolved else ''
        )
        await ctx.send(embed=discord_common.embed_success(
            f'Added {saved.resolved} registered {QUEENS_GAME.display_name} '
            f'result(s) for #{preview.puzzle_number} '
            f'{preview.puzzle_date.isoformat()}.{unresolved}'))


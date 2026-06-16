"""Lifecycle, scheduled Queens update, and shared mod/config helpers. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplCoreMixin:
    async def cog_load(self):
        # ;akari and ;queens are canonical top-level groups; mirror them under
        # ;mg so the nested command paths keep working. Same object in both
        # all_commands dicts -> identical callback dispatch, no parent mutation.
        # Defensive guard: the test harness stubs commands.group, so the
        # group objects don't expose all_commands/get_command — skip in that case.
        if not hasattr(self.minigames, 'all_commands'):
            return
        for group in (self.akari, self.queens):
            if not hasattr(group, 'aliases'):
                continue
            for key in (group.name, *group.aliases):
                if self.minigames.all_commands.get(key) is None:
                    self.minigames.all_commands[key] = group

    async def cog_unload(self):
        import_tasks = list(self._import_tasks.values())
        for task in import_tasks:
            task.cancel()
        if import_tasks:
            await asyncio.gather(*import_tasks, return_exceptions=True)
        connect_tasks = list(self._queens_connect_tasks.values())
        for task in connect_tasks:
            task.cancel()
        if connect_tasks:
            await asyncio.gather(*connect_tasks, return_exceptions=True)
        update_timers = list(self._queens_update_timers.values())
        for task in update_timers:
            task.cancel()
        if update_timers:
            await asyncio.gather(*update_timers, return_exceptions=True)

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._queens_daily_update_check.start()

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _is_enabled(guild_id, feature_flag):
        return cf_common.user_db.get_guild_config(guild_id, feature_flag) == '1'

    @staticmethod
    def _get_channel(guild_id, game_name):
        return cf_common.user_db.get_minigame_channel(guild_id, game_name)

    def _game_for_channel(self, message):
        """Return the GameDef whose configured channel matches, or None."""
        for game in self.GAMES.values():
            if game.manual_ingest_only:
                continue
            if not self._is_enabled(message.guild.id, game.feature_flag):
                continue
            channel_id = self._get_channel(message.guild.id, game.name)
            if channel_id is not None and str(message.channel.id) == str(channel_id):
                return game
        return None

    @staticmethod
    def _require_enabled(guild_id, game):
        if cf_common.user_db.get_guild_config(guild_id, game.feature_flag) != '1':
            raise MinigameCogError(
                f'{game.display_name} is not enabled. '
                f'An admin can enable it with `;meta config enable {game.feature_flag}`.'
            )

    # ── Scheduled Queens update ─────────────────────────────────────────

    @tasks.task_spec(name='QueensDailyUpdateCheck',
                     waiter=tasks.Waiter.fixed_delay(
                         _QUEENS_DAILY_UPDATE_CHECK_INTERVAL))
    async def _queens_daily_update_check(self, _):
        if cf_common.user_db is None:
            return
        now = dt.datetime.now(ZoneInfo(_QUEENS_DAILY_UPDATE_TZ))
        today = now.strftime('%Y-%m-%d')
        for guild in self.bot.guilds:
            try:
                await self._check_queens_daily_update_guild(guild, now, today)
            except Exception:
                logger.warning(
                    'Queens daily update check failed for guild=%s',
                    getattr(guild, 'id', None), exc_info=True)

    async def _check_queens_daily_update_guild(self, guild, now, today):
        if not self._is_enabled(guild.id, QUEENS_GAME.feature_flag):
            return
        channel_id = self._get_channel(guild.id, QUEENS_GAME.name)
        if channel_id is None:
            return
        kvs_key = f'{_QUEENS_DAILY_UPDATE_LAST_PREFIX}{guild.id}'
        target = _queens_daily_update_target_datetime(now)
        if cf_common.user_db.kvs_get(kvs_key) == today:
            next_target = target + dt.timedelta(days=1)
            seconds_until_next = (next_target - now).total_seconds()
            if 0 < seconds_until_next <= _QUEENS_DAILY_UPDATE_PRECISE_WINDOW:
                self._schedule_queens_daily_update_timer(
                    guild, seconds_until_next)
            return

        seconds_until = (target - now).total_seconds()
        pending = self._queens_update_timers.get(guild.id)
        if seconds_until <= 0:
            if pending is not None and not pending.done():
                return
            if await self._send_queens_daily_update(guild):
                cf_common.user_db.kvs_set(kvs_key, today)
        elif seconds_until <= _QUEENS_DAILY_UPDATE_PRECISE_WINDOW:
            self._schedule_queens_daily_update_timer(guild, seconds_until)

    def _schedule_queens_daily_update_timer(self, guild, delay):
        pending = self._queens_update_timers.get(guild.id)
        if pending is None or pending.done():
            logger.info(
                'Scheduling precise Queens daily update for guild=%s in %.0fs',
                guild.id, delay)
            self._queens_update_timers[guild.id] = asyncio.create_task(
                self._precise_queens_daily_update(guild, delay))

    async def _precise_queens_daily_update(self, guild, delay):
        try:
            await asyncio.sleep(delay)
            current_today = dt.datetime.now(
                ZoneInfo(_QUEENS_DAILY_UPDATE_TZ)).strftime('%Y-%m-%d')
            kvs_key = f'{_QUEENS_DAILY_UPDATE_LAST_PREFIX}{guild.id}'
            if cf_common.user_db.kvs_get(kvs_key) == current_today:
                return
            if await self._send_queens_daily_update(guild):
                cf_common.user_db.kvs_set(kvs_key, current_today)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                'Precise Queens daily update failed for guild=%s',
                guild.id, exc_info=True)
        finally:
            self._queens_update_timers.pop(guild.id, None)

    async def _send_queens_daily_update(self, guild):
        channel_id = self._get_channel(guild.id, QUEENS_GAME.name)
        if channel_id is None:
            return False
        try:
            channel = await self._resolve_channel(int(channel_id))
        except Exception:
            logger.warning(
                'Queens daily update channel missing for guild=%s channel=%s',
                guild.id, channel_id, exc_info=True)
            return False

        ctx = _ScheduledCtx(self.bot, guild, channel)
        try:
            await self._cmd_queens_play(
                ctx, import_results=False, send_notice=False)
            await self._cmd_queens_update(ctx, results_day='yesterday')
            return True
        except MinigameCogError as exc:
            message = str(exc)
            if 'rate-limited' in message:
                logger.info(
                    'Queens daily update deferred by rate limit for guild=%s: %s',
                    guild.id, message)
                return False
            try:
                await channel.send(embed=discord_common.embed_alert(message))
            except Exception:
                logger.warning(
                    'Failed to send Queens daily update error for guild=%s',
                    guild.id, exc_info=True)
            return True

    async def _resolve_member(self, ctx, member_text):
        try:
            return await CaseInsensitiveMember().convert(ctx, member_text)
        except commands.BadArgument as exc:
            raise MinigameCogError(str(exc)) from exc

    @staticmethod
    def _resolve_registrar_target(ctx, member):
        """Validate that ``ctx.author`` may (un)register ``member``.

        Anyone can (un)register themselves; only mods/admins can act on someone
        else.  Passing your own member object is treated the same as omitting
        it.  Returns the resolved target.
        """
        if member is None or member.id == ctx.author.id:
            return ctx.author
        is_mod = any(r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
                     for r in ctx.author.roles)
        if not is_mod:
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                f'can register or unregister other users.')
        return member

    @staticmethod
    def _mod_role_error_message():
        return (
            f'You need the `{constants.TLE_ADMIN}` or '
            f'`{constants.TLE_MODERATOR}` role or Queens admin access.')

    @staticmethod
    def _has_server_mod_role(member):
        allowed = {constants.TLE_ADMIN, constants.TLE_MODERATOR}
        return any(r.name in allowed for r in getattr(member, 'roles', []))

    @staticmethod
    def _queens_admin_ids(guild_id):
        if cf_common.user_db is None:
            return set()
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_ADMINS_KEY)
        if not raw:
            return set()
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return set()
        if not isinstance(data, list):
            return set()
        return {
            str(user_id)
            for user_id in data
            if str(user_id).strip()
        }

    @staticmethod
    def _set_queens_admin_ids(guild_id, user_ids):
        user_ids = sorted(
            {str(user_id) for user_id in user_ids},
            key=_mg().Minigames._user_id_sort_key)
        if user_ids:
            cf_common.user_db.set_guild_config(
                guild_id, _QUEENS_ADMINS_KEY, json.dumps(user_ids))
        else:
            cf_common.user_db.delete_guild_config(guild_id, _QUEENS_ADMINS_KEY)

    @staticmethod
    def _user_id_sort_key(user_id):
        try:
            return 0, int(user_id)
        except (TypeError, ValueError):
            return 1, str(user_id)

    def _has_queens_mod_access(self, guild_id, member):
        return (
            self._has_server_mod_role(member)
            or str(getattr(member, 'id', None)) in self._queens_admin_ids(guild_id)
        )

    def _resolve_queens_registrar_target(self, ctx, member):
        if member is None or member.id == ctx.author.id:
            return ctx.author
        if not self._has_queens_mod_access(ctx.guild.id, ctx.author):
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                'or Queens admins can register or unregister other users.')
        return member

    @staticmethod
    def _minigame_banned_user_ids(guild_id, game):
        return {
            str(row.user_id)
            for row in cf_common.user_db.get_minigame_bans(guild_id, game.name)
        }

    def _filter_minigame_banned_rows(self, guild_id, game, rows):
        # Akari has its own ban/opt-out/rating tables; generic bans are for
        # manual minigames such as Queens and must not affect legacy Akari data.
        if game.name == AKARI_GAME.name:
            return rows
        banned = self._minigame_banned_user_ids(guild_id, game)
        if not banned:
            return rows
        return [row for row in rows if str(row.user_id) not in banned]

    def _sync_minigame_results_for_read(self, guild_id, game):
        if game.name == QUEENS_GAME.name:
            self._sync_queens_materialized_results(
                guild_id, migrate_legacy=False)

    @staticmethod
    def _ensure_not_minigame_banned(guild_id, game, user_id, member_name):
        if cf_common.user_db.is_minigame_banned(guild_id, game.name, user_id):
            raise MinigameCogError(
                f'`{member_name}` is banned from {game.display_name}.')

    @staticmethod
    def _get_queens_connection_account(guild_id):
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)
        if raw is None:
            return dict(_QUEENS_DEFAULT_CONNECTION_ACCOUNT)
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return {'name': raw, 'url': None}
        name = data.get('name')
        if not name:
            return None
        return {'name': name, 'url': data.get('url')}

    @staticmethod
    def _set_queens_connection_account(guild_id, name, url):
        cf_common.user_db.set_guild_config(
            guild_id,
            _QUEENS_CONNECTION_ACCOUNT_KEY,
            json.dumps({'name': name, 'url': url}, sort_keys=True),
        )

    @staticmethod
    def _clear_queens_connection_account(guild_id):
        cf_common.user_db.delete_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)

    def _queens_connection_instruction(self, guild_id):
        account = self._get_queens_connection_account(guild_id)
        if account is None:
            return (
                'Ask a moderator to set the LinkedIn account to connect with '
                'using `;queens connection set LinkedIn Name profile_url`.'
            )
        if account.get('url'):
            account_text = f'[this LinkedIn account]({account["url"]})'
        else:
            account_text = 'the configured LinkedIn account'
        return (
            'To join the rating system, send a LinkedIn connection request '
            f'to {account_text}. If you are already connected but not '
            'registered, disconnect on LinkedIn first, then send a new request.'
        )

    async def _resolve_queens_registration_args(self, ctx, first, rest):
        if first is None:
            raise MinigameCogError(
                'Usage: `;queens register [+username DiscordUser] '
                'LinkedIn Name [+anon]`.')
        first = str(first).strip()
        rest = (rest or '').strip()
        target = ctx.author
        linkedin = first if not rest else f'{first} {rest}'

        if first.casefold() == '+username':
            tokens = rest.split(maxsplit=1)
            if len(tokens) < 2:
                raise MinigameCogError(
                    'Usage: `;queens register +username DiscordUser '
                    'LinkedIn Name [+anon]`.')
            target = await self._resolve_member(ctx, tokens[0])
            target = self._resolve_queens_registrar_target(ctx, target)
            linkedin = tokens[1]
        linkedin, anonymous = _split_queens_anonymous_flag(linkedin)
        if not linkedin:
            raise MinigameCogError('A LinkedIn display name is required.')
        return target, linkedin, anonymous


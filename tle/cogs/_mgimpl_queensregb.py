"""Queens unregister and legacy/materialized result migration. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplQueensRegBMixin:
    async def _cmd_queens_unregister(self, ctx, member):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        target = self._resolve_queens_registrar_target(ctx, member)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, target.id)
        self._migrate_legacy_queens_results_to_external(ctx.guild.id)
        if link is not None:
            self._delete_queens_materialized_results_for_link(
                ctx.guild.id, link)
        removed = cf_common.user_db.delete_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, target.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(target)}` is not registered for '
                f'{QUEENS_GAME.display_name}.')
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {QUEENS_GAME.display_name} link for '
            f'`{self._queens_public_user_name(ctx.guild, target.id, {str(target.id): link})}`.'))

    @staticmethod
    def _save_queens_external_result(guild_id, channel_id, entry, puzzle_date,
                                     raw_content):
        puzzle_date = normalize_puzzle_date(puzzle_date)
        cf_common.user_db.save_minigame_unresolved_result(
            guild_id,
            QUEENS_GAME.name,
            normalize_queens_name(entry.linkedin_name),
            entry.linkedin_name,
            channel_id,
            _queens_puzzle_number_for_date(puzzle_date),
            _queens_puzzle_date_text(puzzle_date),
            100 if entry.no_mistakes else 0,
            entry.time_seconds,
            entry.no_hints and entry.no_mistakes,
            raw_content,
        )

    @staticmethod
    def _legacy_queens_entry_matches_row(entry, row):
        return (
            int(entry.time_seconds) == int(row.time_seconds)
            and (100 if entry.no_mistakes else 0) == int(row.accuracy)
            and int(entry.no_hints and entry.no_mistakes) == int(row.is_perfect)
        )

    def _legacy_queens_raw_source_identity(self, row):
        candidates = {}
        for entry in parse_queens_leaderboard(row.raw_content or ''):
            normalized = normalize_queens_name(entry.linkedin_name)
            if normalized == 'you':
                continue
            if self._legacy_queens_entry_matches_row(entry, row):
                candidates[normalized] = entry.linkedin_name
        if len(candidates) != 1:
            return None
        return next(iter(candidates.items()))

    def _legacy_queens_source_identity(self, row, link):
        raw_identity = self._legacy_queens_raw_source_identity(row)
        if raw_identity is not None:
            return raw_identity
        if link is not None:
            return link.normalized_name, link.external_name
        return None

    @staticmethod
    def _queens_source_row_key(normalized_name, row):
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        return (
            normalized_name,
            _queens_puzzle_number_for_date(puzzle_date),
            int(row.accuracy),
            int(row.time_seconds),
            int(row.is_perfect),
        )

    @staticmethod
    def _queens_source_identity_key(normalized_name, puzzle_date):
        puzzle_date = normalize_puzzle_date(puzzle_date)
        return (
            normalized_name,
            _queens_puzzle_number_for_date(puzzle_date),
        )

    def _queens_source_row_keys(self, guild_id):
        return {
            self._queens_source_row_key(row.normalized_name, row)
            for row in cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, QUEENS_GAME.name)
        }

    def _queens_source_identity_keys(self, guild_id):
        return {
            self._queens_source_identity_key(row.normalized_name, row.puzzle_date)
            for row in cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, QUEENS_GAME.name)
        }

    def _is_current_queens_projection_row(self, guild_id, row, link,
                                          source_keys):
        if link is None:
            return False
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        expected_message_id = _queens_result_message_id(
            guild_id, puzzle_date, link.user_id)
        if str(row.message_id) != str(expected_message_id):
            return False
        return self._queens_source_row_key(link.normalized_name, row) in source_keys

    def _delete_queens_materialized_results_for_link(self, guild_id, link):
        deleted = 0
        for row in cf_common.user_db.get_minigame_unresolved_results_for_name(
                guild_id, QUEENS_GAME.name, link.normalized_name):
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            for puzzle_number in _queens_puzzle_numbers_for_date(puzzle_date):
                deleted += cf_common.user_db.delete_minigame_result_for_user_puzzle(
                    guild_id, QUEENS_GAME.name, link.user_id, puzzle_number)
        return deleted

    @staticmethod
    def _same_queens_materialized_result(existing, source, link,
                                         puzzle_number, puzzle_date):
        if existing is None:
            return False
        return (
            str(existing.channel_id) == str(source.channel_id)
            and str(existing.user_id) == str(link.user_id)
            and int(existing.puzzle_number) == int(puzzle_number)
            and _format_queens_date(existing) == _queens_puzzle_date_text(puzzle_date)
            and int(existing.accuracy) == int(source.accuracy)
            and int(existing.time_seconds) == int(source.time_seconds)
            and int(existing.is_perfect) == int(source.is_perfect)
            and str(existing.raw_content) == str(source.raw_content)
        )

    def _migrate_legacy_queens_results_to_external(
            self, guild_id, *, delete_migrated=True):
        links_by_user = self._queens_links_by_user(guild_id)
        source_keys = self._queens_source_row_keys(guild_id)
        source_identity_keys = self._queens_source_identity_keys(guild_id)
        migrated = 0
        rows = cf_common.user_db.get_stored_minigame_results_for_guild(
            guild_id, QUEENS_GAME.name)

        def migration_order(row):
            try:
                message_id = int(row.message_id)
            except (TypeError, ValueError):
                message_id = 0
            storage_order = 0 if row.storage == 'imported' else 1
            return -message_id, storage_order

        for row in sorted(rows, key=migration_order):
            link = links_by_user.get(str(row.user_id))
            if self._is_current_queens_projection_row(
                    guild_id, row, link, source_keys):
                continue
            identity = self._legacy_queens_source_identity(row, link)
            if identity is None:
                continue
            normalized_name, external_name = identity
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            identity_key = self._queens_source_identity_key(
                normalized_name, puzzle_date)
            if identity_key in source_identity_keys:
                if delete_migrated:
                    cf_common.user_db.delete_stored_minigame_result_row(
                        guild_id, QUEENS_GAME.name, row.storage,
                        row.message_id, row.puzzle_number)
                continue
            cf_common.user_db.save_minigame_unresolved_result(
                guild_id,
                QUEENS_GAME.name,
                normalized_name,
                external_name,
                row.channel_id,
                _queens_puzzle_number_for_date(puzzle_date),
                _queens_puzzle_date_text(puzzle_date),
                row.accuracy,
                row.time_seconds,
                row.is_perfect,
                row.raw_content,
            )
            if delete_migrated:
                cf_common.user_db.delete_stored_minigame_result_row(
                    guild_id, QUEENS_GAME.name, row.storage, row.message_id,
                    row.puzzle_number)
            source_keys.add(self._queens_source_row_key(normalized_name, row))
            source_identity_keys.add(identity_key)
            migrated += 1
        return migrated

    def _sync_queens_materialized_results(self, guild_id, *,
                                          migrate_legacy=True):
        if migrate_legacy:
            self._migrate_legacy_queens_results_to_external(guild_id)
        links_by_name = {
            row.normalized_name: row
            for row in cf_common.user_db.get_minigame_player_links(
                guild_id, QUEENS_GAME.name)
        }
        existing_rows = {
            (str(row.message_id), int(row.puzzle_number)): row
            for row in cf_common.user_db.get_live_minigame_results_for_guild(
                guild_id, QUEENS_GAME.name)
        }
        saved = 0
        for row in cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, QUEENS_GAME.name):
            link = links_by_name.get(row.normalized_name)
            if link is None:
                continue
            if cf_common.user_db.is_minigame_banned(
                    guild_id, QUEENS_GAME.name, link.user_id):
                continue
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            message_id = _queens_result_message_id(
                guild_id, puzzle_date, link.user_id)
            puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
            existing = existing_rows.get((str(message_id), int(puzzle_number)))
            if self._same_queens_materialized_result(
                    existing, row, link, puzzle_number, puzzle_date):
                continue
            cf_common.user_db.save_minigame_result(
                message_id,
                guild_id,
                QUEENS_GAME.name,
                row.channel_id,
                link.user_id,
                puzzle_number,
                _queens_puzzle_date_text(puzzle_date),
                row.accuracy,
                row.time_seconds,
                row.is_perfect,
                row.raw_content,
            )
            existing_rows[(str(message_id), int(puzzle_number))] = row
            saved += 1
        return saved

    def _claim_queens_unresolved_results(self, guild_id, user_id,
                                         normalized_name):
        rows = cf_common.user_db.get_minigame_unresolved_results_for_name(
            guild_id, QUEENS_GAME.name, normalized_name)
        del user_id
        self._sync_queens_materialized_results(guild_id, migrate_legacy=False)
        return len(rows)


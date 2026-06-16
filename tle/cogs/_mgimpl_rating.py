"""Rating recompute and per-user/leaderboard rating queries. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplRatingMixin:
    # ── Rating ──────────────────────────────────────────────────────────

    def _recompute_akari_ratings(self, guild_id):
        """Replay all Akari results and overwrite the persisted rating snapshot.

        Pure function of the result tables, so this is always correct after any
        edit/delete/import.  Synchronous and free of ``await`` points, so it runs
        atomically with respect to the event loop (no lock needed).  Only fired
        when an Akari result actually changed, and once (not per row) after an
        import, so the brief CPU cost stays off the hot path.  Never raises — a
        rating failure must not break ingestion.
        """
        try:
            self._recompute_minigame_ratings(guild_id, AKARI_GAME)
        except Exception:
            logger.error('Failed to recompute Akari ratings for guild %s',
                         guild_id, exc_info=True)

    def _recompute_game_ratings(self, guild_id, game):
        if game.rating is None:
            return
        self._recompute_minigame_ratings(guild_id, game)

    @staticmethod
    def _queens_played_day_counts(rows):
        days_by_user = {}
        for row in rows:
            days_by_user.setdefault(str(row.user_id), set()).add(
                _format_queens_date(row))
        return {
            user_id: len(days)
            for user_id, days in days_by_user.items()
        }

    def _with_queens_played_games(self, rows, states):
        counts = self._queens_played_day_counts(rows)
        return {
            user_id: state._replace(
                games=counts.get(str(state.user_id), state.games))
            for user_id, state in states.items()
        }

    def _recompute_minigame_ratings(self, guild_id, game):
        try:
            rating = game.rating
            if rating is None:
                return
            if game.name == QUEENS_GAME.name:
                self._sync_queens_materialized_results(
                    guild_id, migrate_legacy=False)
            rows = cf_common.user_db.get_minigame_results_for_guild(
                guild_id, game.name)
            rows = self._filter_minigame_banned_rows(guild_id, game, rows)
            if game.name == QUEENS_GAME.name:
                rows = self._filter_queens_registered_result_rows(guild_id, rows)
            kwargs = self._rating_compute_kwargs(game)
            states = compute_ratings(rows, **kwargs)
            if game.name == QUEENS_GAME.name:
                states = self._with_queens_played_games(rows, states)
            if game.name == AKARI_GAME.name:
                cf_common.user_db.replace_akari_ratings(
                    guild_id, states.values(), time.time())
            else:
                cf_common.user_db.replace_minigame_ratings(
                    guild_id, game.name, states.values(), time.time())
        except Exception:
            logger.error('Failed to recompute %s ratings for guild %s',
                         game.name, guild_id, exc_info=True)

    @staticmethod
    def _rating_compute_kwargs(game):
        rating = game.rating
        if rating is None:
            return {}
        kwargs = {}
        for name in (
                'start_rating', 'damping', 'decay_base', 'decay_max',
                'decay_grace'):
            value = getattr(rating, name)
            if value is not None:
                kwargs[name] = value
        if rating.current_puzzle_number_fn is not None:
            current_puzzle = rating.current_puzzle_number_fn()
            kwargs['current_puzzle_number'] = current_puzzle
            if rating.max_puzzle_lookahead is not None:
                kwargs['max_puzzle'] = (
                    current_puzzle + rating.max_puzzle_lookahead)
        if rating.rank_fn is not None:
            kwargs['rank_fn'] = rating.rank_fn
        return kwargs

    def _minigame_rating_rows(self, guild_id, game, *, excluded_ids=None,
                              included_ids=None, weekdays=None,
                              date_bounds=None):
        self._sync_minigame_results_for_read(guild_id, game)
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        if game.name == QUEENS_GAME.name:
            rows = self._filter_queens_registered_result_rows(guild_id, rows)
            rows = _filter_queens_weekday_rows(rows, weekdays)
            rows = _filter_queens_rating_date_rows(rows, date_bounds)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        states = compute_ratings(rows, **self._rating_compute_kwargs(game))
        if game.name == QUEENS_GAME.name:
            states = self._with_queens_played_games(rows, states)
        return sorted(
            states.values(),
            key=lambda s: (-s.rating, -s.games, int(s.user_id)),
        )

    def _minigame_user_data(self, guild_id, game, user_id, *,
                            include_decay=False, excluded_ids=None,
                            included_ids=None, weekdays=None,
                            date_bounds=None):
        self._sync_minigame_results_for_read(guild_id, game)
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        if game.name == QUEENS_GAME.name:
            rows = self._filter_queens_registered_result_rows(guild_id, rows)
            rows = _filter_queens_weekday_rows(rows, weekdays)
            rows = _filter_queens_rating_date_rows(rows, date_bounds)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        histories = {}
        states = compute_ratings(
            rows, histories=histories,
            include_decay_in_history=include_decay,
            **self._rating_compute_kwargs(game))
        if game.name == QUEENS_GAME.name:
            states = self._with_queens_played_games(rows, states)
        key = str(user_id)
        return states.get(key), histories.get(key, [])

    def _minigame_user_history(self, guild_id, game, user_id, *,
                               include_decay=False, excluded_ids=None,
                               included_ids=None, weekdays=None,
                               date_bounds=None):
        state, history = self._minigame_user_data(
            guild_id, game, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)
        del state
        return history

    def _minigame_puzzle_change_info(self, guild_id, game, puzzle_number, *,
                                     excluded_ids=None, included_ids=None,
                                     weekdays=None, date_bounds=None):
        self._sync_minigame_results_for_read(guild_id, game)
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        if game.name == QUEENS_GAME.name:
            rows = self._filter_queens_registered_result_rows(guild_id, rows)
            rows = _filter_queens_weekday_rows(rows, weekdays)
            rows = _filter_queens_rating_date_rows(rows, date_bounds)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        histories = {}
        compute_ratings(
            rows, histories=histories,
            **self._rating_compute_kwargs(game))
        info = {}
        for user_id, points in histories.items():
            for point in points:
                if point.puzzle_number == puzzle_number:
                    info[user_id] = _PuzzlePlayerInfo(
                        pre_rating=point.rating - point.delta,
                        delta=point.delta,
                    )
                    break
        return info

    @staticmethod
    def _active_ranking_rows(rows, *, include_inactive=False):
        """Keep only recently-active players for the ranking.

        Hides anyone who hasn't played in the last
        ``AKARI_RANKING_MAX_INACTIVE_DAYS`` days, plus any stale future/garbage
        ``last_puzzle`` (e.g. a troll number lingering until the next recompute).
        With ``include_inactive=True`` the day-cutoff is dropped but the
        garbage-future filter still applies — those rows are never a real
        player.
        """
        current = _mg().expected_puzzle_number(dt.date.today())
        cutoff = constants.AKARI_RANKING_MAX_INACTIVE_DAYS
        lookahead = constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        if include_inactive:
            return [
                row for row in rows
                if -lookahead <= current - int(row.last_puzzle)
            ]
        return [
            row for row in rows
            if -lookahead <= current - int(row.last_puzzle) <= cutoff
        ]

    # ── Queens helpers ─────────────────────────────────────────────────


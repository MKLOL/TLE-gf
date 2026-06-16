"""Akari export/diff and reparse commands. (Minigames cog impl mixin; see minigames.py)."""

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


class ImplExportMixin:
    async def _cmd_akari_export(self, ctx, game):
        """Send a small sqlite snapshot of the two result tables (this game's
        rows only) — the file ``;mg akari diff`` consumes."""
        os.makedirs(constants.TEMP_DIR, exist_ok=True)
        out_path = os.path.join(
            constants.TEMP_DIR, f'{game.name}_snapshot_{ctx.message.id}.db')
        src = cf_common.user_db.conn
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
            dst = sqlite3.connect(out_path)
            try:
                counts = {}
                for tbl in ('minigame_result', 'minigame_import_result'):
                    create = src.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name=?", (tbl,)).fetchone()
                    if not create:
                        raise MinigameCogError(f'`{tbl}` table is missing.')
                    dst.execute(create[0])
                    rows = src.execute(
                        f'SELECT * FROM {tbl} WHERE game=?',
                        (game.name,)).fetchall()
                    if rows:
                        placeholders = ','.join(['?'] * len(rows[0]))
                        dst.executemany(
                            f'INSERT INTO {tbl} VALUES ({placeholders})',
                            [tuple(r) for r in rows])
                    counts[tbl] = len(rows)
                dst.commit()
            finally:
                dst.close()
            await ctx.send(
                content=(f'{game.display_name} snapshot — '
                         f'{counts["minigame_result"]} live + '
                         f'{counts["minigame_import_result"]} imported row(s). '
                         f'Re-upload with `;mg akari diff` to compare later.'),
                file=discord.File(out_path, filename=f'{game.name}_snapshot.db'))
        finally:
            if os.path.exists(out_path):
                os.remove(out_path)

    async def _cmd_akari_diff(self, ctx, game):
        """Diff an uploaded snapshot's merged winners against the live DB.

        Accepts a ``.db``/``.sqlite`` file (or a ``.zip`` containing one) holding
        ``minigame_result`` + ``minigame_import_result`` — e.g. the output of
        ``;mg akari export`` or any backup of the user DB.  Reports the
        merged first-attempt-per-(user, puzzle) winners that were added, removed
        or changed since the snapshot — the rows that actually affect standings.
        """
        attachments = list(getattr(ctx.message, 'attachments', None) or [])
        atts = [a for a in attachments
                if getattr(a, 'filename', '').lower().endswith(
                    ('.db', '.sqlite', '.sqlite3', '.zip'))]
        if not atts:
            raise MinigameCogError(
                'Attach a `.db` snapshot (from `;mg akari export` or a user-DB '
                'backup) — or a `.zip` containing one — to this message.')
        attachment = atts[0]
        size = int(getattr(attachment, 'size', 0) or 0)
        if size and size > _AKARI_DIFF_MAX_BYTES:
            raise MinigameCogError(
                f'Attachment is {size} bytes — refusing anything over '
                f'{_AKARI_DIFF_MAX_BYTES}.')
        raw = await attachment.read()

        os.makedirs(constants.TEMP_DIR, exist_ok=True)
        db_path = os.path.join(
            constants.TEMP_DIR, f'{game.name}_diff_{ctx.message.id}.db')
        try:
            if attachment.filename.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        members = [n for n in zf.namelist()
                                   if n.lower().endswith(
                                       ('.db', '.sqlite', '.sqlite3'))]
                        if len(members) != 1:
                            raise MinigameCogError(
                                f'Zip must contain exactly one `.db` file '
                                f'(found {len(members)}).')
                        db_bytes = zf.read(members[0])
                except zipfile.BadZipFile:
                    raise MinigameCogError('Attachment is not a valid zip.')
            else:
                db_bytes = raw
            with open(db_path, 'wb') as fh:
                fh.write(db_bytes)

            try:
                snap = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            except sqlite3.Error as exc:
                raise MinigameCogError(f'Could not open snapshot: {exc}.')
            try:
                try:
                    present = {r[0] for r in snap.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}
                except sqlite3.DatabaseError:
                    raise MinigameCogError(
                        'Attachment is not a valid SQLite database.')
                missing = {'minigame_result', 'minigame_import_result'} - present
                if missing:
                    raise MinigameCogError(
                        f'Snapshot is missing table(s): {", ".join(sorted(missing))}.')
                old = merged_minigame_winners(snap, ctx.guild.id, game.name)
            finally:
                snap.close()
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

        new = merged_minigame_winners(
            cf_common.user_db.conn, ctx.guild.id, game.name)
        added, removed, changed = diff_merged_winners(old, new)
        total = len(added) + len(removed) + len(changed)
        if total == 0:
            await ctx.send(embed=discord_common.embed_success(
                f'No differences — {len(new)} {game.display_name} merged '
                f'result(s) match the snapshot exactly.'))
            return

        def line(marker, key, old_val, new_val):
            user_id, puzzle_number = key
            name = self._minigame_public_user_name(ctx.guild, game, user_id)
            if old_val is not None and new_val is not None:
                detail = (f'{self._format_winner_value(old_val)} '
                          f'\N{LONG RIGHTWARDS ARROW} '
                          f'{self._format_winner_value(new_val)}')
            elif new_val is not None:
                detail = f'{self._format_winner_value(new_val)} _(new)_'
            else:
                detail = f'{self._format_winner_value(old_val)} _(removed)_'
            return f'{marker} `#{puzzle_number}` `{name}` \N{MIDDLE DOT} {detail}'

        lines = (
            [line('\N{CLOCKWISE RIGHTWARDS AND LEFTWARDS OPEN CIRCLE ARROWS}',
                  k, o, n) for k, o, n in changed]
            + [line('\N{HEAVY MINUS SIGN}', k, o, n) for k, o, n in removed]
            + [line('\N{HEAVY PLUS SIGN}', k, o, n) for k, o, n in added])

        title = (f'{game.display_name} diff vs snapshot — '
                 f'{len(changed)} changed, {len(removed)} removed, '
                 f'{len(added)} added')
        per_page = 12
        pages = []
        for chunk in paginator.chunkify(lines, per_page):
            pages.append((None, discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color())))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    async def _cmd_reparse(self, ctx, game):
        raw_messages = cf_common.user_db.get_raw_messages_for_guild(ctx.guild.id)
        if not raw_messages:
            raise MinigameCogError(
                f'No raw messages stored. Run an import first to populate them.')

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        parsed_count = 0
        skipped = []

        for row in raw_messages:
            if self._is_akari_banned(row.guild_id, row.user_id, game):
                continue  # banned users' raw rows stay in the store but produce no results
            cleaned = strip_codeblock(row.raw_content)
            if self._invalid_minigame_submission_message(game, cleaned) is not None:
                skipped.append(row.message_id)
                await self._notify_invalid_minigame_submission_from_raw(
                    row, game, cleaned)
                continue
            results = game.parse(cleaned)
            if not results:
                if game.detect and game.detect.search(cleaned):
                    skipped.append(row.message_id)
                continue
            puzzle_date_fallback = dt.date.fromisoformat(row.created_at[:10])
            for parsed in results:
                puzzle_date = parsed.puzzle_date or puzzle_date_fallback
                cf_common.user_db.save_imported_minigame_result(
                    row.message_id, row.guild_id, game.name, row.channel_id,
                    row.user_id, parsed.puzzle_number,
                    puzzle_date.isoformat(), parsed.accuracy,
                    parsed.time_seconds, parsed.is_perfect,
                    row.raw_content, commit=False,
                )
                parsed_count += 1
        cf_common.user_db.conn.commit()

        self._recompute_game_ratings(ctx.guild.id, game)

        lines = [
            f'raw messages scanned: **{len(raw_messages)}**',
            f'previous imported rows cleared: **{deleted}**',
            f'results parsed: **{parsed_count}**',
        ]
        if skipped:
            lines.append(
                f'detected but unparseable: **{len(skipped)}** '
                f'(IDs: {", ".join(skipped[:10])}{"…" if len(skipped) > 10 else ""})')
        logger.info(
            '%s reparse: guild=%s raw=%d cleared=%d parsed=%d skipped=%d',
            game.display_name, ctx.guild.id, len(raw_messages), deleted,
            parsed_count, len(skipped),
        )
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

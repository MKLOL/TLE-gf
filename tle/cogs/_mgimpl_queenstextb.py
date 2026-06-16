"""Queens text-command bodies (install/login/settings/backfill) (Minigames cog impl mixin; see minigames.py)."""

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


class ImplQueensTextBMixin:

    async def _cmd_queens_install(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        msg = await ctx.send(embed=discord_common.embed_neutral(
            'Installing scraper dependencies. This downloads ~170 MB and '
            'takes 1–3 minutes.\n\n'
            'Step 1/2: `pip install playwright` …'))

        rc, out = await self._run_install_step(
            [sys.executable, '-m', 'pip', 'install', '--upgrade', 'playwright'],
            timeout=300)
        if rc != 0:
            raise MinigameCogError(
                f'`pip install playwright` failed (rc={rc}). Tail:\n'
                f'```{(out or "(no output)")[-1500:]}```')

        await msg.edit(embed=discord_common.embed_neutral(
            '✓ Step 1/2: `pip install playwright` complete.\n\n'
            'Step 2/2: `playwright install chromium` (~170 MB) …'))

        rc, out = await self._run_install_step(
            [sys.executable, '-m', 'playwright', 'install', 'chromium'],
            timeout=900)
        if rc != 0 and 'does not support' in (out or ''):
            # Host OS isn't in Playwright's hard-coded platform matrix
            # (e.g. Ubuntu 26.04).  Retry forcing the LTS binary.
            await msg.edit(embed=discord_common.embed_neutral(
                f'✓ Step 1/2 complete.\n\n'
                f'Step 2/2: host OS not in Playwright\'s matrix — '
                f'retrying with `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE='
                f'{_QUEENS_PLAYWRIGHT_PLATFORM}` …'))
            rc, out = await self._run_install_step(
                [sys.executable, '-m', 'playwright', 'install', 'chromium'],
                timeout=900,
                extra_env={'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                           _QUEENS_PLAYWRIGHT_PLATFORM})
        if rc != 0:
            raise MinigameCogError(
                f'`playwright install chromium` failed (rc={rc}). Tail:\n'
                f'```{(out or "(no output)")[-1500:]}```')

        await msg.edit(embed=discord_common.embed_success(
            '✓ Playwright + Chromium installed for this bot.\n'
            'Next: upload your LinkedIn session with `;queens login` '
            '(attach `extra/.queens_state.json` generated on your laptop).'))

    async def _cmd_queens_login(self, ctx, linkedin_name):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        attachments = list(getattr(ctx.message, 'attachments', None) or [])
        json_atts = [a for a in attachments
                     if getattr(a, 'filename', '').lower().endswith('.json')]
        if not json_atts:
            raise MinigameCogError(
                'Attach a `.queens_state.json` file (produced by running '
                '`python extra/queens_scrape.py login` on any machine with '
                'a browser) to this message.')
        attachment = json_atts[0]
        size = int(getattr(attachment, 'size', 0) or 0)
        if size and size > _QUEENS_STATE_MAX_BYTES:
            raise MinigameCogError(
                f'Attachment is {size} bytes — refusing anything over '
                f'{_QUEENS_STATE_MAX_BYTES}.')
        raw = await attachment.read()
        try:
            data = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MinigameCogError(
                f'Attachment is not valid JSON: {exc}.')
        cookies = data.get('cookies') if isinstance(data, dict) else None
        if not isinstance(cookies, list):
            raise MinigameCogError(
                'JSON does not look like a Playwright storage_state '
                '(no `cookies` array).')
        has_li_at = any(
            isinstance(c, dict) and c.get('name') == 'li_at'
            for c in cookies)
        if not has_li_at:
            raise MinigameCogError(
                'No `li_at` cookie found — this does not look like a '
                'LinkedIn session.')

        state_path = self._queens_state_path(ctx.guild.id)
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_bytes(raw)
        except OSError as exc:
            raise MinigameCogError(
                f'Could not write session file to `{state_path}`: {exc}.')

        # Clear any stale state from the old design where the uploading
        # mod was registered as the bot's Discord-side avatar.  Going
        # forward, the bot account has no Discord-user mapping; "You"
        # rows in scraped leaderboards are dropped categorically.
        cf_common.user_db.delete_guild_config(
            ctx.guild.id, _QUEENS_IMPORTER_KEY)

        lines = [f'Session saved to `{state_path}`.']

        # Optionally detect + display the LinkedIn account name for
        # transparency.  It's purely informational — no Discord user
        # gets linked to it, no rating consequences.
        if linkedin_name and linkedin_name.strip():
            detected = linkedin_name.strip()
        else:
            detected, err = await self._run_queens_whoami(ctx.guild.id)
            if detected is None:
                lines.append(
                    f'(Could not detect LinkedIn name: {err})')
                detected = None
        if detected:
            cf_common.user_db.set_guild_config(
                ctx.guild.id, _QUEENS_LINKEDIN_NAME_KEY, detected)
            lines.append(f'LinkedIn account: `{detected}`')
        lines.append('Ready — try `;queens play` to verify.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    async def _cmd_queens_settings(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        state_path = self._queens_state_path(ctx.guild.id)
        path_default = state_path == _QUEENS_DEFAULT_STATE_PATH
        state_exists = state_path.exists()
        li_name = cf_common.user_db.get_guild_config(
            ctx.guild.id, _QUEENS_LINKEDIN_NAME_KEY)
        channel_id = self._get_channel(ctx.guild.id, QUEENS_GAME.name)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        last_update = cf_common.user_db.kvs_get(
            f'{_QUEENS_UPDATE_THROTTLE_PREFIX}{ctx.guild.id}')
        last_text = 'never'
        if last_update:
            try:
                last_text = dt.datetime.fromtimestamp(
                    float(last_update), tz=dt.timezone.utc
                ).strftime('%Y-%m-%d %H:%M:%S UTC')
            except (TypeError, ValueError):
                pass
        lines = [
            (f'LinkedIn account: `{li_name}`' if li_name
             else 'LinkedIn account: `unknown` (run `;queens login`)'),
            f'channel: {channel}',
            (f'daily update: `{_QUEENS_DAILY_UPDATE_TIME}` '
             f'{_QUEENS_DAILY_UPDATE_TZ} (`;queens update +yesterday`)'),
            f'state file: `{state_path}`'
            + ('' if not path_default else ' (default)')
            + ('' if state_exists else ' — **missing!**'),
            f'last update: `{last_text}`',
            f'rate limit: `;queens update` once per '
            f'`{_QUEENS_UPDATE_THROTTLE_SECONDS}s`',
        ]
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_queens_backfill(self, ctx, target):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if target is None:
            raise MinigameCogError(
                'Usage: `;queens backfill @user|+all` '
                '(attach `queens_history.json`).')
        data = await self._read_queens_backfill_entries(ctx)
        self._migrate_legacy_queens_results_to_external(
            ctx.guild.id, delete_migrated=False)

        if target.strip().casefold() == '+all':
            result = self._save_queens_backfill_all(ctx, data)
            if not result['valid']:
                raise MinigameCogError(
                    'No valid LinkedIn Queens result entries found in the JSON.')
            saved = result['saved']
            skipped = result['skipped']
            malformed = result['malformed']
            if saved:
                self._sync_queens_materialized_results(ctx.guild.id)
                self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
            lines = [
                f'Backfilled **{saved}** LinkedIn-name result(s).',
                f'- Parsed **{result["valid"]}** valid JSON result(s).',
                f'- Saw **{len(result["registered_names"])}** registered '
                f'LinkedIn name(s) and **{len(result["unresolved_names"])}** '
                'unregistered LinkedIn name(s).',
            ]
            if skipped:
                lines.append(
                    f'- Skipped **{skipped}** already-saved result(s).')
            if malformed:
                lines.append(
                    f'- Ignored **{malformed}** malformed entry/entries.')
            await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))
            return

        member = await self._resolve_member(ctx, target)
        # User must already be registered so we know their LinkedIn name
        # for the match.
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if link is None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not registered for '
                f'{QUEENS_GAME.display_name}. They need to '
                '`;queens register Their LinkedIn Name` first.')

        result = self._save_queens_backfill_for_link(ctx, link, data)
        if not result.matched:
            raise MinigameCogError(
                f'No entries in the JSON match '
                f'`{_safe_member_name(member)}`\'s registered LinkedIn '
                'account.')
        saved = result.saved
        skipped = result.skipped
        malformed = result.malformed
        if saved:
            self._sync_queens_materialized_results(ctx.guild.id)
            self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)

        lines = [
            f'Backfilled **{saved}** result(s) for '
            f'`{_safe_member_name(member)}` '
            f'(LinkedIn: `{_queens_public_link_name(link)}`).',
        ]
        if skipped:
            lines.append(
                f'- Skipped **{skipped}** already-saved result(s).')
        if malformed:
            lines.append(
                f'- Ignored **{malformed}** malformed entry/entries.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))


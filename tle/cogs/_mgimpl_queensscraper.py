"""Queens scraper subprocess plumbing and play/update commands. (Minigames cog impl mixin; see minigames.py)."""

import asyncio
import json
import logging
import os
import pathlib
import sys
import time


from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import (
    QUEENS_GAME,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_CONNECT_TIMEOUT, _QUEENS_STATE_PATH_KEY, _QUEENS_UPDATE_THROTTLE_PREFIX,
    _QUEENS_UPDATE_THROTTLE_SECONDS, _QUEENS_AUTO_PLAY_MIN_SECONDS, _QUEENS_SCRAPER_TIMEOUT,
    _QUEENS_WHOAMI_TIMEOUT, _QUEENS_PLAYWRIGHT_PLATFORM,
    _queens_update_target_date, _queens_public_link_name,
    _format_queens_result, _QUEENS_SCRAPER_SCRIPT, _QUEENS_DEFAULT_STATE_PATH,
)

logger = logging.getLogger(__name__)


class ImplQueensScraperMixin:
    # ── Queens scraper plumbing (used by ;queens play / update / login) ───

    @staticmethod
    def _queens_state_path(guild_id):
        """Resolve the storage_state.json path for this guild.

        Per-guild override stored in guild_config; falls back to the
        scraper's default (``extra/.queens_state.json`` next to the script).
        Returns a ``pathlib.Path``.
        """
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_STATE_PATH_KEY)
        if raw:
            return pathlib.Path(raw).expanduser()
        return _QUEENS_DEFAULT_STATE_PATH

    async def _run_queens_scraper(self, guild_id, *, auto_play,
                                  results_day='today',
                                  min_play_seconds=0):
        """Spawn the scraper's ``fetch`` subprocess.

        ``auto_play=True`` makes the scraper solve today's puzzle if the
        leaderboard isn't visible (used by ``;queens play``).
        ``auto_play=False`` only fetches what's currently visible (used by
        ``;queens update``).

        Returns ``(payload, error_message)``: exactly one is non-None.
        The payload is the parsed JSON dict including the ``status`` field
        (``ok`` / ``not_played`` / ``session_expired`` / ``error``).
        """
        if not _QUEENS_SCRAPER_SCRIPT.exists():
            return None, (
                f'Scraper script missing at `{_QUEENS_SCRAPER_SCRIPT}`.')
        state_path = self._queens_state_path(guild_id)
        cmd = [sys.executable, str(_QUEENS_SCRAPER_SCRIPT),
               '--state', str(state_path), 'fetch', '--json',
               '--day', results_day]
        if auto_play:
            cmd.append('--auto-play')
            if min_play_seconds:
                cmd.extend(['--min-play-seconds', str(min_play_seconds)])
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8',
                     'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                        _QUEENS_PLAYWRIGHT_PLATFORM},
            )
        except FileNotFoundError as exc:
            return None, f'Could not launch scraper: {exc}'
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=_QUEENS_SCRAPER_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return None, (
                f'Scraper timed out after {_QUEENS_SCRAPER_TIMEOUT}s.')
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        if not stdout_text:
            tail = stderr_text or '(no output)'
            return None, f'Scraper produced no output. stderr: `{tail[-800:]}`'
        try:
            payload = json.loads(stdout_text.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return None, (
                f'Could not parse scraper output as JSON: {exc}. '
                f'Tail of stdout: ```{stdout_text[-800:]}```')
        if not isinstance(payload, dict):
            return None, f'Scraper JSON was not an object: `{payload!r}`'
        return payload, None

    async def _run_queens_connect(self, guild_id, names):
        """Accept received LinkedIn invitations whose names match ``names``."""
        state_path = self._queens_state_path(guild_id)
        if not _QUEENS_SCRAPER_SCRIPT.exists():
            return None, f'Scraper script missing at `{_QUEENS_SCRAPER_SCRIPT}`.'
        cmd = [sys.executable, str(_QUEENS_SCRAPER_SCRIPT),
               '--state', str(state_path), 'connect', '--json']
        for name in names:
            cmd.extend(['--name', name])
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8',
                     'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                        _QUEENS_PLAYWRIGHT_PLATFORM},
            )
        except FileNotFoundError as exc:
            return None, f'Could not launch scraper: {exc}'
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=_QUEENS_CONNECT_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return None, (
                f'LinkedIn connection check timed out after '
                f'{_QUEENS_CONNECT_TIMEOUT}s.')
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        if not stdout_text:
            tail = stderr_text or '(no output)'
            return None, f'Connection check produced no output. stderr: `{tail[-800:]}`'
        try:
            payload = json.loads(stdout_text.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return None, (
                f'Could not parse connection check output as JSON: {exc}. '
                f'Tail of stdout: ```{stdout_text[-800:]}```')
        if not isinstance(payload, dict):
            return None, f'Connection check JSON was not an object: `{payload!r}`'
        return payload, None

    async def _run_queens_whoami(self, guild_id):
        """Run the scraper's ``whoami`` subcommand.

        Returns ``(name, error_message)``: exactly one is non-None.
        Same JSON-status conventions as ``_run_queens_scraper``.
        """
        state_path = self._queens_state_path(guild_id)
        if not state_path.exists():
            return None, (
                f'No session file at `{state_path}`. '
                'Upload one with `;queens login` (attach state.json).')
        if not _QUEENS_SCRAPER_SCRIPT.exists():
            return None, f'Scraper script missing at `{_QUEENS_SCRAPER_SCRIPT}`.'
        cmd = [sys.executable, str(_QUEENS_SCRAPER_SCRIPT),
               '--state', str(state_path), 'whoami']
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8',
                     'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                        _QUEENS_PLAYWRIGHT_PLATFORM},
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=_QUEENS_WHOAMI_TIMEOUT)
        except (FileNotFoundError, asyncio.TimeoutError) as exc:
            return None, f'whoami failed: {exc}'
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        if not stdout_text:
            tail = stderr_text or '(no stderr either)'
            return None, f'whoami produced no output. stderr: ```{tail[-600:]}```'
        try:
            payload = json.loads(stdout_text.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return None, f'whoami JSON parse error: {exc}'
        status = payload.get('status')
        if status == 'ok':
            return payload.get('name'), None
        if status == 'session_expired':
            return None, 'Session expired — upload a fresh state file.'
        return None, payload.get('error') or f'whoami status: {status}'

    @staticmethod
    def _queens_status_message(status):
        """Human-readable fallback for an unexpected scraper status string."""
        return {
            'session_expired': (
                'LinkedIn session has expired. A mod needs to run '
                '`;queens login` with a fresh state file.'),
            'session_missing': (
                'No LinkedIn session is saved. A mod needs to run '
                '`;queens login` first.'),
            'not_played': (
                "The bot hasn't solved today's Queens puzzle yet. "
                'Ask a mod to run `;queens play`.'),
        }.get(status, f'Unexpected scraper status: `{status}`')

    async def _do_queens_import(self, ctx, payload, *, source_label,
                                results_day='today'):
        """Apply a scraper payload's ``raw_text`` to the DB additively.

        Used by both ``;queens play`` and ``;queens update``.  Neither
        wipes previously-saved rows; only entries that don't already
        have a row get inserted.  The bot's own ``You`` row is dropped
        on sight via ``skip_importer``.

        Posts a success embed listing every entry that was added (both
        resolved and unresolved).
        """
        raw_text = payload.get('raw_text') or ''
        today_iso = _queens_update_target_date(results_day).isoformat()
        preview = self._make_queens_import_preview(
            ctx, today_iso, raw_text, skip_importer=True)

        new_resolved, new_unresolved = self._filter_new_queens_entries(
            ctx.guild.id, preview)
        if not new_resolved and not new_unresolved:
            await ctx.send(embed=discord_common.embed_neutral(
                f'{source_label} of {QUEENS_GAME.display_name} '
                f'#{preview.puzzle_number} {today_iso}:\n'
                'No new results since the last refresh.'))
            return

        preview = preview._replace(
            resolved=new_resolved, unresolved=new_unresolved)
        self._save_queens_import(ctx, preview, skip_wipe=True)

        await ctx.send(embed=self._format_queens_save_embed(
            ctx, preview, source_label, today_iso))

    def _format_queens_save_embed(self, ctx, preview, source_label, today_iso):
        """Build the success embed listing every entry that was added.

        Resolved entries use the *public* link name (``Anonymous`` for
        anonymously-registered users); unresolved entries show the raw
        scraped name (no Discord user is claiming it).
        """
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        lines = [
            f'{source_label} of {QUEENS_GAME.display_name} '
            f'#{preview.puzzle_number} {today_iso}',
        ]
        if preview.resolved:
            lines.append('')
            lines.append(f'Added **{len(preview.resolved)}** result(s):')
            for index, entry in enumerate(
                    sorted(preview.resolved, key=lambda e: e.time_seconds),
                    start=1):
                discord_name = self._queens_public_user_name(
                    ctx.guild, entry.user_id, links_by_user)
                link = links_by_user.get(str(entry.user_id))
                li_display = (_queens_public_link_name(link)
                              if link else entry.linkedin_name)
                lines.append(
                    f'{index}. {discord_name} — '
                    f'{_format_queens_result(entry, name_override=li_display)}')
        if preview.unresolved:
            lines.append('')
            lines.append(
                f'Added **{len(preview.unresolved)}** unresolved '
                'LinkedIn name(s):')
            for entry in sorted(
                    preview.unresolved, key=lambda e: e.time_seconds)[:20]:
                lines.append(f'- {_format_queens_result(entry)}')
            if len(preview.unresolved) > 20:
                lines.append(
                    f'- ... and {len(preview.unresolved) - 20} more')
        return discord_common.embed_success('\n'.join(lines))

    async def _cmd_queens_play(self, ctx, *, import_results=True,
                               send_notice=True):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        state_path = self._queens_state_path(ctx.guild.id)
        if not state_path.exists():
            raise MinigameCogError(
                f'No LinkedIn session at `{state_path}`. A mod needs to '
                'run `;queens login` (with the state file attached) first.')

        if send_notice:
            await ctx.send(embed=discord_common.embed_neutral(
                'Running the scraper now. If today has not been played yet, '
                f'it waits at least {_QUEENS_AUTO_PLAY_MIN_SECONDS}s before '
                'finishing the puzzle.'))
        payload, error = await self._run_queens_scraper(
            ctx.guild.id,
            auto_play=True,
            min_play_seconds=_QUEENS_AUTO_PLAY_MIN_SECONDS)
        if error is not None:
            raise MinigameCogError(error)
        status = payload.get('status')
        if status != 'ok':
            message = (
                payload.get('error')
                if status == 'error'
                else self._queens_status_message(status)
            )
            raise MinigameCogError(message)
        if import_results:
            await self._do_queens_import(ctx, payload, source_label='Play')
        return payload

    async def _cmd_queens_update(self, ctx, *, results_day='today'):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        kvs_key = f'{_QUEENS_UPDATE_THROTTLE_PREFIX}{ctx.guild.id}'
        last = cf_common.user_db.kvs_get(kvs_key)
        if last:
            try:
                elapsed = time.time() - float(last)
            except (TypeError, ValueError):
                elapsed = _QUEENS_UPDATE_THROTTLE_SECONDS
            if elapsed < _QUEENS_UPDATE_THROTTLE_SECONDS:
                wait = int(_QUEENS_UPDATE_THROTTLE_SECONDS - elapsed) + 1
                raise MinigameCogError(
                    f'`;queens update` is rate-limited. Try again in {wait}s.')

        state_path = self._queens_state_path(ctx.guild.id)
        if not state_path.exists():
            raise MinigameCogError(
                f'No LinkedIn session at `{state_path}`. A mod needs to '
                'run `;queens login` first.')
        await ctx.send('This will take a while')
        # Set the throttle BEFORE the slow subprocess so concurrent users
        # don't both pass the gate.
        cf_common.user_db.kvs_set(kvs_key, str(time.time()))

        payload, error = await self._run_queens_scraper(
            ctx.guild.id, auto_play=False, results_day=results_day)
        if error is not None:
            raise MinigameCogError(error)
        status = payload.get('status')
        if status == 'not_played':
            raise MinigameCogError(self._queens_status_message(status))
        if status != 'ok':
            message = (
                payload.get('error')
                if status == 'error'
                else self._queens_status_message(status)
            )
            raise MinigameCogError(message)
        source_label = (
            'Yesterday update' if results_day == 'yesterday' else 'Update')
        await self._do_queens_import(
            ctx, payload, source_label=source_label, results_day=results_day)


    @staticmethod
    async def _run_install_step(cmd, *, timeout, extra_env=None):
        """Spawn an install subprocess and capture combined stdout+stderr.

        Returns ``(returncode, captured_text)``.  Never raises; timeouts come
        back as ``returncode == -1``.  ``extra_env`` is merged on top of
        ``os.environ`` for the subprocess — used to inject
        ``PLAYWRIGHT_HOST_PLATFORM_OVERRIDE`` on bleeding-edge Ubuntu.
        """
        env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        if extra_env:
            env.update(extra_env)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
        except (FileNotFoundError, PermissionError) as exc:
            return -2, f'Could not launch `{cmd[0]}`: {exc}'
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return -1, '(timed out — try running it manually on the host)'
        return proc.returncode, stdout.decode('utf-8', errors='replace')

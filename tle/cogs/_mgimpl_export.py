"""Akari export/diff and reparse commands. (Minigames cog impl mixin; see minigames.py)."""

import datetime as dt
import io
import logging
import os
import sqlite3
import zipfile

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util.db.minigame_db import (
    merged_minigame_winners, diff_merged_winners,
)

from tle.cogs._minigame_common import (
    strip_codeblock,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError,
)
from tle.cogs._minigame_queens_cog import (
    _AKARI_DIFF_MAX_BYTES,
)

logger = logging.getLogger(__name__)


def _raw_created_timestamp(created_at):
    """Unix timestamp of a raw-message row's ISO ``created_at`` string."""
    ts = dt.datetime.fromisoformat(created_at)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.timestamp()


class ImplExportMixin:
    async def _cmd_akari_export(self, ctx, game):
        """Send a small sqlite snapshot of the two result tables (this
        guild's rows for this game only) — the file ``;mg akari diff``
        consumes."""
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
                        f'SELECT * FROM {tbl} WHERE game=? AND guild_id=?',
                        (game.name, str(ctx.guild.id))).fetchall()
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

        ban_cutoffs = {}
        for row in raw_messages:
            if row.user_id not in ban_cutoffs:
                ban_cutoffs[row.user_id] = self._ingest_ban_cutoff(
                    row.guild_id, row.user_id, game)
            cutoff = ban_cutoffs[row.user_id]
            if (cutoff is not None
                    and _raw_created_timestamp(row.created_at) >= cutoff):
                # Forward-only ban: only post-ban messages are dropped —
                # pre-ban raw rows keep materializing so 'existing results
                # stay rated' survives a reparse.
                continue
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

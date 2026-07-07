"""Queens leaderboard resolution, import preview/save, link helpers. (Minigames cog impl mixin; see minigames.py)."""

import logging

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_common import (
    format_duration,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME, normalize_queens_name, parse_queens_leaderboard,
    parse_queens_time, queens_status_flags,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _safe_member_name,
    _safe_user_name,
)
from tle.cogs._minigame_queens_cog import (
    _QueensResolvedEntry, _QueensImportPreview, _QueensImportSaveResult,
    _parse_queens_date, _queens_puzzle_number_for_date,
    _parse_queens_date_or_number,
    _queens_puzzle_numbers_for_date,
    _queens_public_link_name,
    _clean_queens_linkedin_name, _format_queens_result,
)

logger = logging.getLogger(__name__)


class ImplQueensImportMixin:
    def _resolve_queens_leaderboard(self, ctx, leaderboard, *,
                                    skip_importer=False):
        """Resolve a parsed leaderboard into rated rows + unresolved names.

        ``skip_importer=True`` is the bot-driven mode used by ``;queens play``
        / ``;queens update``: no Discord user is treated as the importer, and
        the "You" row (the bot's own scraper-paced solve) is dropped on sight
        so it never enters the rating pool.  The default ``False`` is the
        manual ``;queens import`` paste path — a human ran the command, their
        Discord-side player_link supplies the "You" row's identity.
        """
        entries = parse_queens_leaderboard(leaderboard)
        if not entries:
            raise MinigameCogError('No LinkedIn Queens leaderboard rows found.')

        importer_link = None
        if not skip_importer:
            importer_link = cf_common.user_db.get_minigame_player_link(
                ctx.guild.id, QUEENS_GAME.name, ctx.author.id)
            if importer_link is None:
                raise MinigameCogError(
                    'Register the importer with `;queens register` before '
                    'importing LinkedIn Queens leaderboard results.')

        resolved = []
        unresolved = []
        seen_users = set()

        for entry in entries:
            normalized = normalize_queens_name(entry.linkedin_name)
            if entry.is_you:
                if skip_importer:
                    # Bot's own row — never imported.
                    continue
                link = importer_link
            else:
                link = cf_common.user_db.get_minigame_player_link_by_name(
                    ctx.guild.id, QUEENS_GAME.name, normalized)
                if link is None:
                    unresolved.append(_QueensResolvedEntry(
                        user_id=None,
                        linkedin_name=entry.linkedin_name,
                        time_seconds=entry.time_seconds,
                        no_hints=entry.no_hints,
                        no_mistakes=entry.no_mistakes,
                    ))
                    continue

            if cf_common.user_db.is_minigame_banned(
                    ctx.guild.id, QUEENS_GAME.name, link.user_id):
                continue
            if link.user_id in seen_users:
                continue
            seen_users.add(link.user_id)
            resolved.append(_QueensResolvedEntry(
                user_id=link.user_id,
                linkedin_name=link.external_name,
                time_seconds=entry.time_seconds,
                no_hints=entry.no_hints,
                no_mistakes=entry.no_mistakes,
            ))

        return resolved, unresolved

    def _make_queens_import_preview(self, ctx, date_text, leaderboard, *,
                                    skip_importer=False):
        puzzle_date = _parse_queens_date(date_text)
        puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
        resolved, unresolved = self._resolve_queens_leaderboard(
            ctx, leaderboard, skip_importer=skip_importer)
        if not resolved and not unresolved:
            raise MinigameCogError(
                'No leaderboard rows matched Queens players.')
        return _QueensImportPreview(
            puzzle_date=puzzle_date,
            puzzle_number=puzzle_number,
            resolved=resolved,
            unresolved=unresolved,
            raw_content=leaderboard,
        )

    def _format_queens_import_preview(self, ctx, preview):
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        lines = [
            f'{QUEENS_GAME.display_name} #{preview.puzzle_number} '
            f'import preview for {preview.puzzle_date.isoformat()}',
            '',
            'Registered:',
        ]
        if preview.resolved:
            for index, entry in enumerate(
                    sorted(preview.resolved, key=lambda e: e.time_seconds), start=1):
                discord_name = self._queens_public_user_name(
                    ctx.guild, entry.user_id, links_by_user)
                link = links_by_user.get(str(entry.user_id))
                li_display = (_queens_public_link_name(link)
                              if link else entry.linkedin_name)
                lines.append(
                    f'{index}. {discord_name} — '
                    f'{_format_queens_result(entry, name_override=li_display)}')
        else:
            lines.append('- none yet')
        if preview.unresolved:
            lines += ['', 'Stored unresolved LinkedIn names:']
            for entry in sorted(preview.unresolved, key=lambda e: e.time_seconds)[:20]:
                lines.append(f'- {_format_queens_result(entry)}')
            if len(preview.unresolved) > 20:
                lines.append(f'- ... and {len(preview.unresolved) - 20} more')
        lines += [
            '',
            'Run `;queens import confirm` to add new results for this date.',
        ]
        return '\n'.join(lines)

    def _filter_new_queens_entries(self, guild_id, preview):
        """Strip entries from ``preview`` that already have identical rows.

        Used by imports to keep saving per-player: exact duplicates are
        skipped, but a changed result for the same LinkedIn name/date is kept
        so the source upsert replaces only that individual result.
        """
        self._migrate_legacy_queens_results_to_external(
            guild_id, delete_migrated=False)
        existing_rows = {}
        for puzzle_number in _queens_puzzle_numbers_for_date(
                preview.puzzle_date):
            for row in cf_common.user_db.get_minigame_unresolved_results_for_puzzle(
                    guild_id, QUEENS_GAME.name, puzzle_number):
                key = self._queens_source_identity_key(
                    row.normalized_name, row.puzzle_date)
                existing_rows.setdefault(key, []).append(row)

        def should_save(entry):
            normalized_name = normalize_queens_name(entry.linkedin_name)
            key = self._queens_source_identity_key(
                normalized_name, preview.puzzle_date)
            rows = existing_rows.get(key, [])
            return not any(
                self._legacy_queens_entry_matches_row(entry, row)
                for row in rows)

        new_resolved = [
            entry for entry in preview.resolved
            if should_save(entry)]
        new_unresolved = [
            entry for entry in preview.unresolved
            if should_save(entry)]

        return new_resolved, new_unresolved

    def _save_queens_import(self, ctx, preview, *, skip_wipe=True):
        del skip_wipe
        new_resolved, new_unresolved = self._filter_new_queens_entries(
            ctx.guild.id, preview)
        preview = preview._replace(
            resolved=new_resolved, unresolved=new_unresolved)
        for entry in preview.resolved:
            self._save_queens_external_result(
                ctx.guild.id, ctx.channel.id, entry, preview.puzzle_date,
                preview.raw_content)
        for entry in preview.unresolved:
            self._save_queens_external_result(
                ctx.guild.id, ctx.channel.id, entry, preview.puzzle_date,
                preview.raw_content)
        self._sync_queens_materialized_results(ctx.guild.id)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        return _QueensImportSaveResult(
            resolved=len(preview.resolved),
            unresolved=len(preview.unresolved),
        )

    @staticmethod
    def _queens_links_by_user(guild_id):
        return {
            str(row.user_id): row
            for row in cf_common.user_db.get_minigame_player_links(
                guild_id, QUEENS_GAME.name)
        }

    def _filter_queens_registered_result_rows(self, guild_id, rows,
                                              *, links_by_user=None):
        if links_by_user is None:
            links_by_user = self._queens_links_by_user(guild_id)
        linked_ids = set(links_by_user)
        return [row for row in rows if row.user_id in linked_ids]

    def _queens_public_user_name(self, guild, user_id, links_by_user=None):
        del links_by_user
        return _safe_user_name(guild, user_id)

    def _queens_name_fn(self, links_by_user):
        return lambda guild, row: self._queens_public_user_name(
            guild, row.user_id, links_by_user)

    def _minigame_public_user_name(self, guild, game, user_id):
        if game.name == QUEENS_GAME.name:
            return self._queens_public_user_name(guild, user_id)
        return _safe_user_name(guild, user_id)

    def _require_queens_registered_member(self, guild_id, member):
        link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member.id)
        if link is None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not registered for '
                f'{QUEENS_GAME.display_name} (`;queens register LinkedIn Name`).')
        # Public rating/performance/history views hide banned players, just
        # like Akari's (whose bans auto-opt the user out of displays); the
        # mod-only debug variants skip this gate.
        self._ensure_not_minigame_banned(
            guild_id, QUEENS_GAME, member.id, _safe_member_name(member))
        return link

    def _queens_rating_identity_fn(self, links_by_user):
        return lambda _guild, row: (
            _queens_public_link_name(links_by_user.get(str(row.user_id)))
            if str(row.user_id) in links_by_user
            else '-'
        )

    def _queens_legend_name(self, guild_id, member):
        link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member.id)
        if link is not None:
            return _queens_public_link_name(link)
        return _safe_member_name(member)

    async def _resolve_queens_linked_player(self, ctx, player_text):
        player_text = str(player_text or '').strip()
        if not player_text:
            raise MinigameCogError(
                'A Discord user or registered LinkedIn name is required.')

        try:
            member = await self._resolve_member(ctx, player_text)
        except MinigameCogError:
            member = None
        if member is not None:
            link = cf_common.user_db.get_minigame_player_link(
                ctx.guild.id, QUEENS_GAME.name, member.id)
            if link is None:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` is not registered for '
                    f'{QUEENS_GAME.display_name}.')
            return (
                str(member.id),
                self._queens_public_user_name(
                    ctx.guild, member.id, {str(member.id): link}),
                link,
            )

        name = _clean_queens_linkedin_name(player_text)
        link = cf_common.user_db.get_minigame_player_link_by_name(
            ctx.guild.id, QUEENS_GAME.name, normalize_queens_name(name))
        if link is None:
            raise MinigameCogError(
                f'Could not find a Discord user or registered LinkedIn name '
                f'for `{discord.utils.escape_mentions(player_text)}`.')
        label = self._queens_public_user_name(
            ctx.guild, link.user_id, {str(link.user_id): link})
        return str(link.user_id), label, link

    @staticmethod
    def _parse_queens_add_args(args):
        tokens = str(args or '').split()
        if len(tokens) < 3:
            raise MinigameCogError(
                'Usage: `;queens add <@user|LinkedIn Name> DATE/# time '
                '[status...]`.')
        for index in range(1, len(tokens) - 1):
            try:
                parsed_date = _parse_queens_date_or_number(tokens[index])
                parse_queens_time(tokens[index + 1])
            except (MinigameCogError, ValueError):
                continue
            player_text = ' '.join(tokens[:index]).strip()
            status = ' '.join(tokens[index + 2:]).strip()
            return (
                player_text,
                parsed_date,
                tokens[index + 1],
                status or 'No hints & no mistakes',
            )
        raise MinigameCogError(
            'Usage: `;queens add <@user|LinkedIn Name> DATE/# time [status...]`.')

    @staticmethod
    def _parse_queens_remove_args(args):
        tokens = str(args or '').split()
        if len(tokens) < 2:
            raise MinigameCogError(
                'Usage: `;queens remove <@user|LinkedIn Name> DATE/#`.')
        try:
            parsed_date = _parse_queens_date_or_number(tokens[-1])
        except MinigameCogError as exc:
            raise MinigameCogError(
                'Usage: `;queens remove <@user|LinkedIn Name> DATE/#`.') from exc
        player_text = ' '.join(tokens[:-1]).strip()
        return player_text, parsed_date

    async def _cmd_queens_add(self, ctx, args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        player_text, parsed_date, time_text, status = (
            self._parse_queens_add_args(args))
        user_id, label, linked = await self._resolve_queens_linked_player(
            ctx, player_text)
        self._ensure_not_minigame_banned(
            ctx.guild.id, QUEENS_GAME, user_id, label)
        parsed_number = _queens_puzzle_number_for_date(parsed_date)
        no_hints, no_mistakes, _status_text = queens_status_flags(status)
        time_seconds = parse_queens_time(time_text)
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            cf_common.user_db.delete_minigame_unresolved_result_for_name_puzzle(
                ctx.guild.id, QUEENS_GAME.name, linked.normalized_name,
                puzzle_number)
            cf_common.user_db.delete_minigame_result_for_user_puzzle(
                ctx.guild.id, QUEENS_GAME.name, user_id, puzzle_number)
        entry = _QueensResolvedEntry(
            user_id=user_id,
            linkedin_name=linked.external_name,
            time_seconds=time_seconds,
            no_hints=no_hints,
            no_mistakes=no_mistakes,
        )
        self._save_queens_external_result(
            ctx.guild.id, ctx.channel.id, entry, parsed_date,
            f'{linked.external_name}\n{status}\n{time_text}')
        self._sync_queens_materialized_results(ctx.guild.id)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Added {QUEENS_GAME.display_name} result for '
            f'`{label}` on #{parsed_number} {parsed_date.isoformat()}: '
            f'**{format_duration(time_seconds)}**.'))

    async def _cmd_queens_remove(self, ctx, args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        player_text, parsed_date = self._parse_queens_remove_args(args)
        user_id, label, linked = await self._resolve_queens_linked_player(
            ctx, player_text)
        rc = 0
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            rc += cf_common.user_db.delete_minigame_unresolved_result_for_name_puzzle(
                ctx.guild.id, QUEENS_GAME.name, linked.normalized_name,
                puzzle_number)
            rc += cf_common.user_db.delete_minigame_result_for_user_puzzle(
                ctx.guild.id, QUEENS_GAME.name, user_id, puzzle_number)
        if not rc:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} result found for '
                f'`{label}` on {parsed_date.isoformat()}.')
        self._sync_queens_materialized_results(ctx.guild.id)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {QUEENS_GAME.display_name} result for '
            f'`{label}` on #{_queens_puzzle_number_for_date(parsed_date)} '
            f'{parsed_date.isoformat()}.'))


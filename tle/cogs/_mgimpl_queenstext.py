"""LinkedIn-game text-command bodies (admins/links/ban/import)."""

import logging
import time

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_helpers import (
    MinigameCogError, _format_akari_ban_line,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_ANONYMOUS_FLAGS, _QUEENS_HISTORY_PER_PAGE,
    _queens_public_link_name,
    _queens_public_link_sort_key,
    _split_queens_anonymous_flag, _is_queens_anonymous_modal_request,
    _QueensAnonymousRegisterView,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = logging.getLogger(__name__)


class ImplQueensTextMixin:

    async def _cmd_queens_admins(self, ctx, game):
        await self._cmd_minigame_admins(
            ctx, game.display_name,
            lambda guild_id: self._linkedin_admin_ids(guild_id, game))

    async def _cmd_queens_admins_add(self, ctx, game, member):
        await self._cmd_minigame_admins_add(
            ctx, member, game.display_name,
            lambda guild_id: self._linkedin_admin_ids(guild_id, game),
            lambda guild_id, ids: self._set_linkedin_admin_ids(
                guild_id, game, ids))

    async def _cmd_queens_admins_remove(self, ctx, game, member):
        await self._cmd_minigame_admins_remove(
            ctx, member, game.display_name,
            lambda guild_id: self._linkedin_admin_ids(guild_id, game),
            lambda guild_id, ids: self._set_linkedin_admin_ids(
                guild_id, game, ids))

    async def _cmd_queens_register_cmd(self, ctx, game, first, linkedin):
        self._require_enabled(ctx.guild.id, game)
        if _is_queens_anonymous_modal_request(first, linkedin):
            await ctx.send(
                embed=discord_common.embed_neutral(
                    'Click the button below to enter your LinkedIn name '
                    'privately. Only you can use this prompt, and your '
                    'LinkedIn name will not be posted in the channel.'),
                view=_QueensAnonymousRegisterView(self, game, ctx.author.id))
            return
        member, linkedin_text, anonymous = await self._resolve_queens_registration_args(
            ctx, game, first, linkedin)
        await self._cmd_queens_register(
            ctx, game, member, linkedin_text, anonymous=anonymous)

    async def _cmd_queens_set_cmd(self, ctx, game, member, linkedin):
        self._require_enabled(ctx.guild.id, game)
        usage = (
            f'Usage: `;{game.name} set [+anon] DiscordUser LinkedIn Name '
            '[+anon]`.')
        if member is None or not (linkedin or '').strip():
            raise MinigameCogError(usage)
        prefix_anonymous = False
        member_text = member
        linkedin_arg = linkedin.strip()
        if str(member).casefold() in _QUEENS_ANONYMOUS_FLAGS:
            prefix_anonymous = True
            tokens = linkedin_arg.split(maxsplit=1)
            if len(tokens) < 2:
                raise MinigameCogError(usage)
            member_text, linkedin_arg = tokens
        target = await self._resolve_member(ctx, member_text)
        linkedin_text, suffix_anonymous = _split_queens_anonymous_flag(
            linkedin_arg)
        anonymous = prefix_anonymous or suffix_anonymous
        if not linkedin_text:
            raise MinigameCogError(usage)
        await self._cmd_queens_set(
            ctx, game, target, linkedin_text, anonymous=anonymous)

    async def _cmd_queens_links(self, ctx, game):
        self._require_enabled(ctx.guild.id, game)
        rows = cf_common.user_db.get_minigame_player_links(
            ctx.guild.id, game.link_key)
        if not rows:
            raise MinigameCogError(
                f'No {self._linkedin_games_label()} links registered.')
        # The database orders by normalized LinkedIn name for lookup
        # efficiency. Re-sort on the public label so anonymous registrations
        # are grouped as "Anonymous" instead of leaking their hidden names
        # through their positions in this list.
        rows = sorted(rows, key=_queens_public_link_sort_key)
        lines = []
        for row in rows:
            display_name = self._queens_public_user_name(
                ctx.guild, row.user_id, {str(row.user_id): row})
            lines.append(
                f'- {display_name}: `{_queens_public_link_name(row)}`')
        pages = []
        for chunk in paginator.chunkify(lines, _QUEENS_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=f'{self._linkedin_games_label()} links',
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    async def _cmd_queens_ban(self, ctx, game, member, reason):
        """Forward-only ban, mirroring Akari's: new results from the user are
        blocked at every entry point (imports, manual adds, channel shares)
        and they disappear from the public ratings board, but their existing
        results stay stored and rated, and their LinkedIn link is kept so the
        name stays claimed and the block is airtight."""
        self._require_enabled(ctx.guild.id, game)
        added = cf_common.user_db.ban_minigame_user(
            ctx.guild.id, game.name, member.id, time.time(),
            ctx.author.id, reason)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        if not added:
            raise MinigameCogError(
                f'`{display_name}` is already banned from '
                f'{game.display_name}.')
        lines = [
            f'`{display_name}` is now banned from '
            f'{game.display_name}. New results from them will be '
            'dropped by imports, manual adds, and channel shares, and they '
            'are hidden from the public ratings board.',
            'Existing results stay stored and rated.',
        ]
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    async def _cmd_queens_bans(self, ctx, game):
        self._require_enabled(ctx.guild.id, game)
        rows = cf_common.user_db.get_minigame_bans(
            ctx.guild.id, game.name)
        if not rows:
            raise MinigameCogError(
                f'No active {game.display_name} bans.')
        lines = [_format_akari_ban_line(ctx.guild, row) for row in rows]
        title = f'{game.display_name} bans ({len(rows)})'
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

    async def _cmd_queens_import_preview(self, ctx, game, puzzle_date,
                                         leaderboard):
        self._require_enabled(ctx.guild.id, game)
        if puzzle_date is None or leaderboard is None:
            raise MinigameCogError(
                f'Usage: `;{game.name} import DATE <pasted leaderboard>`.')
        preview = self._make_queens_import_preview(
            ctx, game, puzzle_date, leaderboard)
        self._queens_pending_imports[
            (ctx.guild.id, game.name, ctx.author.id)] = preview
        await ctx.send(embed=discord_common.embed_neutral(
            self._format_queens_import_preview(ctx, game, preview)))

    async def _cmd_queens_import_confirm(self, ctx, game):
        self._require_enabled(ctx.guild.id, game)
        key = (ctx.guild.id, game.name, ctx.author.id)
        preview = self._queens_pending_imports.pop(key, None)
        if preview is None:
            raise MinigameCogError(
                f'No pending {game.display_name} import preview. Run '
                f'`;{game.name} import` first.')
        saved = self._save_queens_import(ctx, game, preview)
        if not saved.resolved and not saved.unresolved:
            await ctx.send(embed=discord_common.embed_neutral(
                f'No new {game.display_name} result(s) for '
                f'#{preview.puzzle_number} {preview.puzzle_date.isoformat()}.'))
            return
        unresolved = (
            f' Stored {saved.unresolved} unresolved result(s) for later registration.'
            if saved.unresolved else ''
        )
        await ctx.send(embed=discord_common.embed_success(
            f'Added {saved.resolved} registered {game.display_name} '
            f'result(s) for #{preview.puzzle_number} '
            f'{preview.puzzle_date.isoformat()}.{unresolved}'))

"""Queens text-command bodies (admins/links/connection/ban/import) (Minigames cog impl mixin; see minigames.py)."""

import logging
import time

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_queens import (
    QUEENS_GAME,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _safe_member_name,
    _format_akari_ban_line,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_ANONYMOUS_FLAGS, _QUEENS_HISTORY_PER_PAGE,
    _queens_public_link_name,
    _split_queens_anonymous_flag, _is_queens_anonymous_modal_request,
    _split_queens_connection_account_text,
    _QueensAnonymousRegisterView,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = logging.getLogger(__name__)


class ImplQueensTextMixin:

    async def _cmd_queens_admins(self, ctx):
        await self._cmd_minigame_admins(
            ctx, QUEENS_GAME.display_name, self._queens_admin_ids)

    async def _cmd_queens_admins_add(self, ctx, member):
        await self._cmd_minigame_admins_add(
            ctx, member, QUEENS_GAME.display_name,
            self._queens_admin_ids, self._set_queens_admin_ids)

    async def _cmd_queens_admins_remove(self, ctx, member):
        await self._cmd_minigame_admins_remove(
            ctx, member, QUEENS_GAME.display_name,
            self._queens_admin_ids, self._set_queens_admin_ids)

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
        """Forward-only ban, mirroring Akari's: new results from the user are
        blocked at every entry point (imports, manual adds, channel shares)
        and they disappear from the public ratings board, but their existing
        results stay stored and rated, and their LinkedIn link is kept so the
        name stays claimed and the block is airtight."""
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
        lines = [
            f'`{display_name}` is now banned from '
            f'{QUEENS_GAME.display_name}. New results from them will be '
            'dropped by imports, manual adds, and channel shares, and they '
            'are hidden from the public ratings board.',
            'Existing results stay stored and rated.',
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


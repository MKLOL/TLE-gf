"""Akari text command group and subcommands (Minigames cog command mixin; see minigames.py)."""

from typing import Optional

import asyncio
import datetime as dt
import json
import os
import sys
import time

import discord
from discord import app_commands
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import (
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, queens_mod_only,
    _SlashCtx, _safe_member_name, _safe_user_name, _format_akari_ban_line,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE
from tle.cogs._minigame_queens_cog import (
    _QueensAnonymousRegisterView, _QUEENS_ANONYMOUS_FLAGS,
    _QUEENS_HISTORY_PER_PAGE, _QUEENS_UPDATE_THROTTLE_SECONDS,
    _QUEENS_DAILY_UPDATE_TIME, _QUEENS_DAILY_UPDATE_TZ,
    _QUEENS_LINKEDIN_NAME_KEY, _QUEENS_UPDATE_THROTTLE_PREFIX,
    _QUEENS_STATE_PATH_KEY, _QUEENS_DEFAULT_STATE_PATH,
    _QUEENS_PLAYWRIGHT_PLATFORM, _QUEENS_STATE_MAX_BYTES,
    _QUEENS_IMPORTER_KEY, _QUEENS_BACKFILL_MAX_BYTES,
    _is_queens_anonymous_modal_request, _split_queens_anonymous_flag,
    _split_queens_connection_account_text, _queens_public_link_name,
    _parse_queens_update_args,
)
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter,
    _format_queens_weekday_filter, _format_queens_date_filter,
)
from tle.cogs._minigame_slash_consts import (
    _TIMEFRAME_CHOICES, _MODE_CHOICES,
)

logger = __import__('logging').getLogger(__name__)


class AkariCmdsMixin:
    @commands.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
                    invoke_without_command=True)
    async def akari(self, ctx):
        """Daily Akari commands."""
        await ctx.send_help(ctx.command)

    @akari.command(name='here', brief='Set the Daily Akari channel to the current channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_here(self, ctx):
        await self._cmd_here(ctx, AKARI_GAME)

    @akari.command(name='clear', brief='Clear the Daily Akari channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_clear(self, ctx):
        await self._cmd_clear(ctx, AKARI_GAME)

    @akari.command(name='show', brief='Show Daily Akari settings')
    async def akari_show(self, ctx):
        await self._cmd_show(ctx, AKARI_GAME)

    @akari.command(name='register', brief='Restore Daily Akari rating visibility',
                   usage='[@user (mods only)]')
    async def akari_register(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        changed = cf_common.user_db.register_akari_user(
            ctx.guild.id, target.id)
        who = ('You are' if target.id == ctx.author.id
               else f'`{_safe_member_name(target)}` is')
        if changed:
            msg = (f'{who} opted back in to {AKARI_GAME.display_name} ratings.')
        else:
            msg = (f'{who} already visible in {AKARI_GAME.display_name} ratings '
                   f'(everyone is opted in by default).')
        await ctx.send(embed=discord_common.embed_success(msg))

    @akari.command(name='unregister', brief='Opt out of Daily Akari ratings',
                   usage='[@user (mods only)]')
    async def akari_unregister(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        changed = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, target.id, time.time())
        who = ('You are' if target.id == ctx.author.id
               else f'`{_safe_member_name(target)}` is')
        if changed:
            msg = (f'{who} opted out of {AKARI_GAME.display_name} ratings. '
                   f'Results are still recorded; run `;mg akari register` to opt back in.')
        else:
            msg = f'{who} already opted out.'
        await ctx.send(embed=discord_common.embed_success(msg))

    @akari.command(name='ban',
                   brief='(Mod) Block a user from Akari ingestion',
                   usage='@user [reason...]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ban(self, ctx, member: CaseInsensitiveMember, *,
                        reason: str = None):
        added = cf_common.user_db.ban_akari_user(
            ctx.guild.id, member.id, time.time(), ctx.author.id, reason)
        if not added:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is already banned from '
                f'{AKARI_GAME.display_name}.')
        # Auto opt them out so the rating display state stays consistent and
        # the opt-out sticks past any later unban.
        opted_out = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, member.id, time.time())
        lines = [f'`{_safe_member_name(member)}` is now banned from '
                 f'{AKARI_GAME.display_name} ingestion. New results from '
                 f'them will be dropped silently.']
        if opted_out:
            lines.append('Also opted out of ratings.')
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @akari.command(name='unban',
                   brief='(Mod) Lift an Akari ingestion ban',
                   usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_unban(self, ctx, member: CaseInsensitiveMember):
        removed = cf_common.user_db.unban_akari_user(ctx.guild.id, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{AKARI_GAME.display_name}. They are not auto-registered — '
            f'they need to run `;mg akari register` again.'))

    @akari.command(name='bans',
                   brief='(Mod) List Akari ingestion bans')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_bans(self, ctx):
        rows = cf_common.user_db.get_akari_bans(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No active {AKARI_GAME.display_name} bans.')
        lines = [_format_akari_ban_line(ctx.guild, row) for row in rows]
        title = f'{AKARI_GAME.display_name} bans ({len(rows)})'
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            embed = discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    @akari.command(name='vs', brief='Head-to-head comparison',
                   usage='@user1 @user2 [filters...] [raw|all]')
    async def akari_vs(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, AKARI_GAME, member1, member2, *args)

    @akari.command(name='streak', brief='Show current perfect streak',
                   usage='[@user] [filters...]')
    async def akari_streak(self, ctx, *args):
        await self._cmd_streak(ctx, AKARI_GAME, *args)

    @akari.command(name='top', brief='Show winners leaderboard',
                   usage='[filters...] [raw|all]')
    async def akari_top(self, ctx, *args):
        await self._cmd_top(ctx, AKARI_GAME, *args)

    @akari.group(name='stats', brief='Show personal stats with graphs',
                 usage='[@user] [filters...] | [day | puzzle_id | #puzzle_id]',
                 invoke_without_command=True)
    async def akari_stats(self, ctx, *args):
        await self._cmd_stats(ctx, AKARI_GAME, *args)

    @akari_stats.command(name='debug',
                         brief='(Mod) Puzzle results with ratings for ALL players',
                         usage='<puzzle_id|date> [+test] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_stats_debug(self, ctx, *args):
        (remaining, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        if len(remaining) != 1:
            raise MinigameCogError(
                'Usage: `;mg akari stats debug <puzzle_id|date> '
                '[+test] [+exclude=…] [+include=…]`.')
        await self._cmd_akari_stats_puzzle(
            ctx, remaining[0], show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari.command(name='remove', brief='Remove a user result for a puzzle',
                   usage='@user puzzle_id')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, AKARI_GAME, member, puzzle_id)

    @akari.command(name='add', brief='Manually add a result for a user/puzzle',
                   usage='@user puzzle_id <perfect|N%> <time>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_add(self, ctx, member: CaseInsensitiveMember,
                        puzzle_id: int, result: str, time: str):
        await self._cmd_akari_add(ctx, member, puzzle_id, result, time)

    @akari.group(name='import', brief='Manage imported history',
                 invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import(self, ctx):
        await ctx.send_help(ctx.command)

    @akari_import.command(name='start', brief='Rebuild imported history')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, AKARI_GAME, channel)

    @akari_import.command(name='status', brief='Show import status')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_status(self, ctx):
        await self._cmd_import_status(ctx, AKARI_GAME)

    @akari_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, AKARI_GAME)

    @akari_import.command(name='clear', brief='Delete imported history')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, AKARI_GAME)

    @akari_import.command(name='orphans',
                          brief='(Temp, mod) List imported results with no live counterpart')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_orphans(self, ctx):
        await self._cmd_import_orphans(ctx, AKARI_GAME)

    @akari.command(name='reparse', brief='Reparse all stored raw messages')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_reparse(self, ctx):
        await self._cmd_reparse(ctx, AKARI_GAME)

    @akari.command(name='export', brief='(Mod) Download a snapshot of the result tables')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_export(self, ctx):
        await self._cmd_akari_export(ctx, AKARI_GAME)

    @akari.command(name='diff',
                   brief='(Mod) Diff an uploaded snapshot against current results',
                   usage='(attach a .db / .zip snapshot)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_diff(self, ctx):
        await self._cmd_akari_diff(ctx, AKARI_GAME)

    @akari.group(name='ratings', brief='Show Akari rating leaderboard',
                 usage='[+test] [+inactive] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_ratings(self, ctx, *args):
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        await self._cmd_akari_ratings(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive, test_decay=test_decay)

    @akari.group(name='rating',
                 brief='Show registered users\' Akari rating graph',
                 usage='[@user1 @user2 ...] [+decay] [+test] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_rating(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args)
        await self._cmd_akari_rating(
            ctx, members, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari_rating.command(name='debug',
                          brief='(Mod) Rating graph for any user (incl. shadow-rated)',
                          usage='@user1 [@user2 ...] [+decay] [+test] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_rating_debug(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        await self._cmd_akari_rating(
            ctx, members, require_registered=False,
            include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari.group(name='performance', aliases=['perf'],
                 brief='Show registered users\' Akari performance graph',
                 usage='[@user1 @user2 ...] [+test] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_performance(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args)
        await self._cmd_akari_performance(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari_performance.command(name='debug',
                               brief='(Mod) Performance graph for any user (incl. shadow-rated)',
                               usage='@user1 [@user2 ...] [+test] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_performance_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        await self._cmd_akari_performance(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari.group(name='history',
                 brief='Paginated rating delta log for a registered user',
                 usage='[@user] [+test] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_history(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args)
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari_history.command(name='debug',
                           brief='(Mod) Rating delta log for any user (incl. shadow-rated)',
                           usage='@user [+test] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_history_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)

    @akari_ratings.command(name='recompute', brief='(Mod) Rebuild the rating snapshot')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_recompute(self, ctx):
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{AKARI_GAME.display_name} ratings recomputed.'))

    @akari_ratings.command(name='debug', aliases=['all'],
                           brief='(Mod) Leaderboard incl. shadow-rated (unopted-in) users',
                           usage='[+test] [+inactive] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_debug(self, ctx, *args):
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        await self._cmd_akari_ratings_debug(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive, test_decay=test_decay)

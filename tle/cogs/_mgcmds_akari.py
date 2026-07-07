"""Akari text command group and subcommands (Minigames cog command mixin; see minigames.py)."""


import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import (
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, akari_mod_only,
    _safe_member_name, _format_akari_ban_line,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = __import__('logging').getLogger(__name__)


class AkariCmdsMixin:
    @commands.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
                    invoke_without_command=True)
    async def akari(self, ctx):
        """Daily Akari commands."""
        await ctx.send_help(ctx.command)

    @akari.command(name='here', brief='Set the Daily Akari channel to the current channel')
    @akari_mod_only()
    async def akari_here(self, ctx):
        await self._cmd_here(ctx, AKARI_GAME)

    @akari.command(name='clear', brief='Clear the Daily Akari channel')
    @akari_mod_only()
    async def akari_clear(self, ctx, *args):
        # Refuse stray arguments so ``;akari clear 446`` cannot silently
        # unset the channel when ``;akari delete 446`` was meant.
        if args:
            raise MinigameCogError(
                '`;akari clear` unsets the Akari channel and takes no '
                'arguments. To remove results for a date, use '
                '`;akari delete DATE`.')
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
    @akari_mod_only()
    async def akari_ban(self, ctx, member: CaseInsensitiveMember, *,
                        reason: str = None):
        added = cf_common.user_db.ban_akari_user(
            ctx.guild.id, member.id, time.time(), ctx.author.id, reason)
        if not added:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is already banned from '
                f'{AKARI_GAME.display_name}.')
        # Forward-only, mirroring Queens: new results are dropped and the
        # player is hidden from public boards at display time (no opt-out row
        # is written), so unbanning restores them immediately while a genuine
        # self opt-out survives the ban/unban round-trip.
        lines = [f'`{_safe_member_name(member)}` is now banned from '
                 f'{AKARI_GAME.display_name}. New results from them will be '
                 f'dropped, and they are hidden from the public ratings '
                 f'board. Existing results stay rated.']
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @akari.command(name='unban',
                   brief='(Mod) Lift an Akari ingestion ban',
                   usage='@user')
    @akari_mod_only()
    async def akari_unban(self, ctx, member: CaseInsensitiveMember):
        removed = cf_common.user_db.unban_akari_user(ctx.guild.id, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{AKARI_GAME.display_name}. New results count again and they '
            f'reappear on the public boards immediately.'))

    @akari.command(name='bans',
                   brief='(Mod) List Akari ingestion bans')
    @akari_mod_only()
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
    @akari_mod_only()
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
    @akari_mod_only()
    async def akari_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, AKARI_GAME, member, puzzle_id)

    @akari.command(name='add', brief='Manually add a result for a user/puzzle',
                   usage='@user puzzle_id <perfect|N%> <time>')
    @akari_mod_only()
    async def akari_add(self, ctx, member: CaseInsensitiveMember,
                        puzzle_id: int, result: str, time: str):
        await self._cmd_akari_add(ctx, member, puzzle_id, result, time)

    @akari.group(name='import', brief='Manage imported history',
                 invoke_without_command=True)
    @akari_mod_only()
    async def akari_import(self, ctx):
        await ctx.send_help(ctx.command)

    @akari_import.command(name='start', brief='Rebuild imported history')
    @akari_mod_only()
    async def akari_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, AKARI_GAME, channel)

    @akari_import.command(name='status', brief='Show import status')
    @akari_mod_only()
    async def akari_import_status(self, ctx):
        await self._cmd_import_status(ctx, AKARI_GAME)

    @akari_import.command(name='cancel', brief='Cancel a running import')
    @akari_mod_only()
    async def akari_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, AKARI_GAME)

    @akari_import.command(name='clear', brief='Delete imported history')
    @akari_mod_only()
    async def akari_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, AKARI_GAME)

    @akari_import.command(name='orphans',
                          brief='(Temp, mod) List imported results with no live counterpart')
    @akari_mod_only()
    async def akari_import_orphans(self, ctx):
        await self._cmd_import_orphans(ctx, AKARI_GAME)

    @akari.command(name='reparse', brief='Reparse all stored raw messages')
    @akari_mod_only()
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
                 usage='[+weekly] [+test] [+inactive] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]',
                 invoke_without_command=True)
    async def akari_ratings(self, ctx, *args):
        weekly = '+weekly' in args
        args = tuple(arg for arg in args if arg != '+weekly')
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._extract_akari_extended_filters(ctx, args)
        await self._cmd_akari_ratings(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive, test_decay=test_decay,
            weekly=weekly, weekdays=weekdays, date_bounds=date_bounds)

    @akari.group(name='rating',
                 brief='Show registered users\' Akari rating graph',
                 usage='[@user1 @user2 ...] [+decay] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date] [+recalculate]',
                 invoke_without_command=True)
    async def akari_rating(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         recalculate) = await self._parse_akari_rating_filter_args(
            ctx, args, allow_recalculate=True)
        await self._cmd_akari_rating(
            ctx, members, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays,
            date_bounds=date_bounds, recalculate=recalculate)

    @akari_rating.command(name='debug',
                          brief='(Mod) Rating graph for any user (incl. shadow-rated)',
                          usage='@user1 [@user2 ...] [+decay] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date] [+recalculate]')
    @akari_mod_only()
    async def akari_rating_debug(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         recalculate) = await self._parse_akari_rating_filter_args(
            ctx, args, member_required=True, allow_recalculate=True)
        await self._cmd_akari_rating(
            ctx, members, require_registered=False,
            include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays,
            date_bounds=date_bounds, recalculate=recalculate)

    @akari.group(name='performance', aliases=['perf'],
                 brief='Show registered users\' Akari performance graph',
                 usage='[@user1 @user2 ...] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]',
                 invoke_without_command=True)
    async def akari_performance(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._parse_akari_rating_filter_args(ctx, args)
        await self._cmd_akari_performance(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)

    @akari_performance.command(name='debug',
                               brief='(Mod) Performance graph for any user (incl. shadow-rated)',
                               usage='@user1 [@user2 ...] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]')
    @akari_mod_only()
    async def akari_performance_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._parse_akari_rating_filter_args(
            ctx, args, member_required=True)
        await self._cmd_akari_performance(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)

    @akari.group(name='history',
                 brief='Paginated rating delta log for a registered user',
                 usage='[@user] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]',
                 invoke_without_command=True)
    async def akari_history(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._parse_akari_rating_filter_args(ctx, args)
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)

    @akari_history.command(name='debug',
                           brief='(Mod) Rating delta log for any user (incl. shadow-rated)',
                           usage='@user [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]')
    @akari_mod_only()
    async def akari_history_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._parse_akari_rating_filter_args(
            ctx, args, member_required=True)
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)

    @akari_ratings.command(name='recompute', brief='(Mod) Rebuild the rating snapshot')
    @akari_mod_only()
    async def akari_ratings_recompute(self, ctx):
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{AKARI_GAME.display_name} ratings recomputed.'))

    @akari_ratings.command(name='debug', aliases=['all'],
                           brief='(Mod) Leaderboard incl. shadow-rated (unopted-in) users',
                           usage='[+weekly] [+test] [+inactive] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]')
    @akari_mod_only()
    async def akari_ratings_debug(self, ctx, *args):
        weekly = '+weekly' in args
        args = tuple(arg for arg in args if arg != '+weekly')
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._extract_akari_extended_filters(ctx, args)
        await self._cmd_akari_ratings_debug(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive, test_decay=test_decay,
            weekly=weekly, weekdays=weekdays, date_bounds=date_bounds)

    # ── Delegated-admin tier, bulk deletion, per-date results ───────────

    @akari.group(name='admins', aliases=['admin'],
                 brief='Manage extra Daily Akari command admins',
                 invoke_without_command=True)
    @akari_mod_only()
    async def akari_admins(self, ctx):
        await self._cmd_akari_admins(ctx)

    @akari_admins.command(name='add',
                          brief='(Mod) Add an Akari command admin',
                          usage='@user')
    @akari_mod_only()
    async def akari_admins_add(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_akari_admins_add(ctx, member)

    @akari_admins.command(name='remove',
                          brief='(Mod) Remove an Akari command admin',
                          usage='@user')
    @akari_mod_only()
    async def akari_admins_remove(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_akari_admins_remove(ctx, member)

    @akari.command(name='delete',
                   brief='(Mod) Remove all Akari results for a date/puzzle',
                   usage='date|#number')
    @akari_mod_only()
    async def akari_delete(self, ctx, selector: str = None):
        await self._cmd_akari_delete_date(ctx, selector)

    @akari.command(name='clean', aliases=['cleanup'],
                   brief='(Mod) Remove Akari results for an inclusive date range',
                   usage='start-date|#number [end-date|#number]')
    @akari_mod_only()
    async def akari_clean(self, ctx, start_date: str = None,
                          end_date: str = None):
        await self._cmd_akari_clean(ctx, start_date, end_date)

    @akari.group(name='results', brief='Show Akari puzzle/date leaderboard',
                 usage='[date|#number] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]',
                 invoke_without_command=True)
    async def akari_results(self, ctx, *args):
        await self._cmd_akari_results(ctx, args)

    @akari_results.command(name='debug',
                           brief='(Mod) Puzzle/date results with ratings for ALL players',
                           usage='[date|#number] [+test] [+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]')
    @akari_mod_only()
    async def akari_results_debug(self, ctx, *args):
        await self._cmd_akari_results(ctx, args, show_all=True)

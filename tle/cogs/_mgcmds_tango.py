"""Tango text command group and subcommands (Minigames cog command mixin; see minigames.py)."""


from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs._minigame_helpers import (
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, tango_mod_only,
    _safe_member_name,
)
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_queens_filters import _split_queens_improved_filter

logger = __import__('logging').getLogger(__name__)


class TangoCmdsMixin:
    @commands.group(name='tango', aliases=[],
                    brief='LinkedIn Tango commands',
                    invoke_without_command=True)
    async def tango(self, ctx):
        await ctx.send_help(ctx.command)

    @tango.command(name='show', brief='Show LinkedIn Tango settings')
    async def tango_show(self, ctx):
        await self._cmd_queens_show(ctx, TANGO_GAME)

    @tango.group(name='admins', aliases=['admin'],
                  brief='Manage extra LinkedIn Tango command admins',
                  invoke_without_command=True)
    @tango_mod_only()
    async def tango_admins(self, ctx):
        await self._cmd_queens_admins(ctx, TANGO_GAME)

    @tango_admins.command(name='add',
                           brief='(Mod) Add a Tango command admin',
                           usage='@user')
    @tango_mod_only()
    async def tango_admins_add(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_queens_admins_add(ctx, TANGO_GAME, member)

    @tango_admins.command(name='remove',
                           brief='(Mod) Remove a Tango command admin',
                           usage='@user')
    @tango_mod_only()
    async def tango_admins_remove(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_queens_admins_remove(ctx, TANGO_GAME, member)

    @tango.command(name='here', brief='Set the LinkedIn Tango channel to the current channel')
    @tango_mod_only()
    async def tango_here(self, ctx):
        await self._cmd_here(ctx, TANGO_GAME)

    @tango.command(name='clear', brief='Clear the LinkedIn Tango channel')
    @tango_mod_only()
    async def tango_channel_clear(self, ctx, *args):
        # ``clear`` used to be an alias for per-date deletion; refuse stray
        # arguments so an old-style ``;tango clear DATE`` cannot silently
        # unset the channel instead.
        if args:
            raise MinigameCogError(
                '`;tango clear` unsets the Tango channel and takes no '
                'arguments. To remove results for a date, use '
                '`;tango delete DATE`.')
        await self._cmd_clear(ctx, TANGO_GAME)

    @tango.command(name='register',
                    brief='Link a Discord user to a LinkedIn Tango name',
                    usage='[+username DiscordUser] LinkedIn Name [+anon]')
    async def tango_register(self, ctx, first: str = None, *,
                              linkedin: str = None):
        await self._cmd_queens_register_cmd(ctx, TANGO_GAME, first, linkedin)

    @tango.command(name='set',
                    brief='(Mod) Overwrite a Discord user Tango name',
                    usage='[+anon] DiscordUser LinkedIn Name [+anon]')
    @tango_mod_only()
    async def tango_set(self, ctx, member: str = None, *,
                         linkedin: str = None):
        await self._cmd_queens_set_cmd(ctx, TANGO_GAME, member, linkedin)

    @tango.command(name='unregister',
                    brief='Remove a user LinkedIn Tango link',
                    usage='[@user]')
    async def tango_unregister(self, ctx, member: str = None):
        self._require_enabled(ctx.guild.id, TANGO_GAME)
        if member is None:
            target = ctx.author
        else:
            target = await self._resolve_member(ctx, member)
        await self._cmd_queens_unregister(ctx, TANGO_GAME, target)

    @tango.command(name='links', brief='List registered LinkedIn Tango names')
    async def tango_links(self, ctx):
        await self._cmd_queens_links(ctx, TANGO_GAME)

    @tango.command(name='skips',
                    brief='Show skipped days since the first Tango submission',
                    usage='[@user]')
    async def tango_skips(self, ctx, member: CaseInsensitiveMember = None):
        await self._cmd_queens_skips(ctx, TANGO_GAME, member or ctx.author)

    @tango.command(
        name='backfill', aliases=['backill'],
        brief='(Mod) Backfill historical Tango results',
        usage='@user|+all (attach tango_history.json)')
    @tango_mod_only()
    async def tango_backfill(self, ctx, target: str = None):
        await self._cmd_queens_backfill(ctx, TANGO_GAME, target)

    @tango.command(name='ban',
                    brief='(Mod) Block a user from Tango imports/ratings',
                    usage='@user [reason...]')
    @tango_mod_only()
    async def tango_ban(self, ctx, member: CaseInsensitiveMember, *,
                         reason: str = None):
        await self._cmd_queens_ban(ctx, TANGO_GAME, member, reason)

    @tango.command(name='unban',
                    brief='(Mod) Lift a Tango ban',
                    usage='@user')
    @tango_mod_only()
    async def tango_unban(self, ctx, member: CaseInsensitiveMember):
        self._require_enabled(ctx.guild.id, TANGO_GAME)
        removed = cf_common.user_db.unban_minigame_user(
            ctx.guild.id, TANGO_GAME.name, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        self._recompute_minigame_ratings(ctx.guild.id, TANGO_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{TANGO_GAME.display_name}. Their registration was kept, so '
            'new results count again immediately.'))

    @tango.command(name='bans',
                    brief='(Mod) List Tango bans')
    @tango_mod_only()
    async def tango_bans(self, ctx):
        await self._cmd_queens_bans(ctx, TANGO_GAME)

    @tango.command(name='vs', brief='Compare two or more players',
                    usage='@user1 @user2 [@user3 ...] '
                          '[filters...] [+dow=mon,wed|weekday|weekend]')
    async def tango_vs(self, ctx, *arguments):
        members, filters = await self._resolve_vs_arguments(
            ctx, TANGO_GAME, arguments)
        await self._cmd_vs_members(ctx, TANGO_GAME, members, *filters)

    @tango.command(name='week', aliases=['weekly'],
                    brief='Show the weekly server recap',
                    usage='[YYYY-MM-DD|last]')
    async def tango_week(self, ctx, *args):
        await self._cmd_week(ctx, TANGO_GAME, *args)

    @tango.command(name='top', brief='Show outright fastest-result winners',
                    usage='[+ties] [filters...] '
                          '[+dow=mon,wed|weekday|weekend]')
    async def tango_top(self, ctx, *args):
        await self._cmd_top(ctx, TANGO_GAME, *args)

    @tango.command(name='streak', brief='Show current clean streak',
                    usage='[@user] [filters...] [+dow=mon,wed|weekday|weekend]')
    async def tango_streak(self, ctx, *args):
        await self._cmd_queens_streak(ctx, TANGO_GAME, *args)

    @tango.group(name='stats', brief='Show personal Tango stats',
                  usage='[@user] [filters...] [+dow=mon,wed|weekday|weekend]',
                  invoke_without_command=True)
    async def tango_stats(self, ctx, *args):
        await self._cmd_queens_stats(ctx, TANGO_GAME, *args)

    @tango.group(name='results', brief='Show Tango date leaderboard',
                  usage='[date|number] [+unrated] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def tango_results(self, ctx, *args):
        show_unrated = any(
            str(arg).strip().casefold() == '+unrated' for arg in args)
        args = tuple(
            arg for arg in args
            if str(arg).strip().casefold() != '+unrated')
        args, improved = _split_queens_improved_filter(args)
        (remaining, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = (
            await self._extract_queens_rating_filters(ctx, TANGO_GAME, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;tango results [date|number] '
                '[+unrated] [+beta] [+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] '
                '[d>=date] [d<date]`.')
        date_arg = (
            remaining[0] if remaining
            else _queens_current_puzzle_date().isoformat()
        )
        await self._cmd_queens_stats_date(
            ctx, TANGO_GAME, date_arg,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved,
            show_unrated=show_unrated)

    @tango_results.command(name='debug',
                            brief='(Mod) Date results with ratings for ALL players',
                            usage='[date|number] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @tango_mod_only()
    async def tango_results_debug(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (remaining, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = (
            await self._extract_queens_rating_filters(ctx, TANGO_GAME, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;tango results debug [date|number] '
                '[+beta] [+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] '
                '[d>=date] [d<date]`.')
        date_arg = (
            remaining[0] if remaining
            else _queens_current_puzzle_date().isoformat()
        )
        await self._cmd_queens_stats_date(
            ctx, TANGO_GAME, date_arg, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)

    @tango.group(name='import',
                  brief='Preview pasted Tango results or manage imported history',
                  usage='date <pasted leaderboard>',
                  invoke_without_command=True)
    @tango_mod_only()
    async def tango_import(self, ctx, puzzle_date: str = None, *,
                            leaderboard: str = None):
        await self._cmd_queens_import_preview(ctx, TANGO_GAME, puzzle_date, leaderboard)

    @tango_import.command(name='start',
                           brief='Rebuild imported Tango history from channel messages')
    @tango_mod_only()
    async def tango_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, TANGO_GAME, channel)

    @tango_import.command(name='status', brief='Show Tango import status')
    @tango_mod_only()
    async def tango_import_status(self, ctx):
        await self._cmd_import_status(ctx, TANGO_GAME)

    @tango_import.command(name='cancel', brief='Cancel a running Tango import')
    @tango_mod_only()
    async def tango_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, TANGO_GAME)

    @tango_import.command(name='clear', brief='Delete imported Tango history')
    @tango_mod_only()
    async def tango_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, TANGO_GAME)

    @tango_import.command(name='confirm',
                           brief='Save the latest Tango import preview')
    @tango_mod_only()
    async def tango_import_confirm(self, ctx):
        await self._cmd_queens_import_confirm(ctx, TANGO_GAME)

    @tango_import.command(name='orphans',
                           brief='(Mod) List imported results with no live counterpart')
    @tango_mod_only()
    async def tango_import_orphans(self, ctx):
        await self._cmd_import_orphans(ctx, TANGO_GAME)

    @tango.command(name='export', brief='(Mod) Download a snapshot of the result tables')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def tango_export(self, ctx):
        await self._cmd_akari_export(ctx, TANGO_GAME)

    @tango.command(name='diff',
                    brief='(Mod) Diff an uploaded snapshot against current results',
                    usage='(attach a .db / .zip snapshot)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def tango_diff(self, ctx):
        await self._cmd_akari_diff(ctx, TANGO_GAME)

    @tango.command(name='add',
                    brief='Manually add a Tango result',
                    usage='<@user|LinkedIn Name> date|number time [status...]')
    @tango_mod_only()
    async def tango_add(self, ctx, *, args: str = None):
        await self._cmd_queens_add(ctx, TANGO_GAME, args)

    @tango.command(name='remove', brief='Remove a Tango result',
                    usage='<@user|LinkedIn Name> date|number')
    @tango_mod_only()
    async def tango_remove(self, ctx, *, args: str = None):
        await self._cmd_queens_remove(ctx, TANGO_GAME, args)

    # Standard names across both games: ``clear`` unsets the channel,
    # ``delete``/``clean`` remove results (``;tango clear DATE`` was the
    # historical spelling — the channel-clear command above hints at
    # ``delete`` if it gets arguments).
    @tango.command(name='delete',
                    brief='(Mod) Remove all Tango results for a date',
                    usage='date|number')
    @tango_mod_only()
    async def tango_delete(self, ctx, puzzle_date: str = None):
        await self._cmd_queens_clear(ctx, TANGO_GAME, puzzle_date)

    @tango.command(name='clean', aliases=['cleanup'],
                    brief='(Mod) Remove Tango results for an inclusive date range',
                    usage='start-date|number [end-date|number]')
    @tango_mod_only()
    async def tango_clean(self, ctx, start_date: str = None,
                           end_date: str = None):
        await self._cmd_queens_clean(ctx, TANGO_GAME, start_date, end_date)

    @tango.command(name='reparse', brief='(Mod) Reparse all stored raw Tango messages')
    @tango_mod_only()
    async def tango_reparse(self, ctx):
        await self._cmd_reparse(ctx, TANGO_GAME)

    @tango.group(name='ratings', brief='Show Tango rating leaderboard',
                  usage='[+weekly] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def tango_ratings(self, ctx, *args):
        self._require_enabled(ctx.guild.id, TANGO_GAME)
        weekly = '+weekly' in args
        args = tuple(arg for arg in args if arg != '+weekly')
        args, improved = _split_queens_improved_filter(args)
        (remaining, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = (
            await self._extract_queens_rating_filters(ctx, TANGO_GAME, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;tango ratings [+weekly] [+beta] '
                '[+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] [d>=date] [d<date]`.')
        await self._cmd_queens_ratings(
            ctx, TANGO_GAME, excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved,
            weekly=weekly)

    @tango.group(name='rating',
                  brief='Show Tango rating graph',
                  usage='[@user1 @user2 ...] [+decay] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date] [+recalculate]',
                  invoke_without_command=True)
    async def tango_rating(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, recalculate) = await self._parse_queens_rating_args(
            ctx, TANGO_GAME, args, allow_recalculate=True)
        await self._cmd_queens_rating(
            ctx, TANGO_GAME, members, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            recalculate=recalculate, improved=improved)

    @tango_rating.command(name='debug',
                           brief='(Mod) Rating graph for any rated user',
                           usage='@user1 [@user2 ...] [+decay] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date] [+recalculate]')
    @tango_mod_only()
    async def tango_rating_debug(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, recalculate) = (
            await self._parse_queens_rating_args(
                ctx, TANGO_GAME, args, member_required=True, allow_recalculate=True))
        await self._cmd_queens_rating(
            ctx, TANGO_GAME, members, require_registered=False,
            include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            recalculate=recalculate, improved=improved)

    @tango.group(name='performance', aliases=['perf'],
                  brief='Show Tango performance graph',
                  usage='[@user1 @user2 ...] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def tango_performance(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, _recalculate) = (
            await self._parse_queens_rating_args(ctx, TANGO_GAME, args))
        await self._cmd_queens_performance(
            ctx, TANGO_GAME, members,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)

    @tango_performance.command(name='debug',
                                brief='(Mod) Performance graph for any rated user',
                                usage='@user1 [@user2 ...] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @tango_mod_only()
    async def tango_performance_debug(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, _recalculate) = (
            await self._parse_queens_rating_args(
                ctx, TANGO_GAME, args, member_required=True))
        await self._cmd_queens_performance(
            ctx, TANGO_GAME, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)

    @tango.group(name='history',
                  brief='Paginated Tango rating delta log',
                  usage='[@user] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def tango_history(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, _recalculate) = (
            await self._parse_queens_rating_args(ctx, TANGO_GAME, args))
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, TANGO_GAME, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)

    @tango_history.command(name='debug',
                            brief='(Mod) Rating delta log for any rated user',
                            usage='@user [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @tango_mod_only()
    async def tango_history_debug(self, ctx, *args):
        args, improved = _split_queens_improved_filter(args)
        (members, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds, _recalculate) = (
            await self._parse_queens_rating_args(
                ctx, TANGO_GAME, args, member_required=True))
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, TANGO_GAME, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)

    @tango_ratings.command(name='recompute',
                            brief='(Mod) Rebuild the Tango rating snapshot')
    @tango_mod_only()
    async def tango_ratings_recompute(self, ctx):
        await self._cmd_queens_ratings_recompute(ctx, TANGO_GAME)

    @tango_ratings.command(name='debug', aliases=['all'],
                            brief='(Mod) Leaderboard including unregistered rated users',
                            usage='[+weekly] [+beta] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @tango_mod_only()
    async def tango_ratings_debug(self, ctx, *args):
        weekly = '+weekly' in args
        args = tuple(arg for arg in args if arg != '+weekly')
        args, improved = _split_queens_improved_filter(args)
        (remaining, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = (
            await self._extract_queens_rating_filters(ctx, TANGO_GAME, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;tango ratings debug [+weekly] [+beta] '
                '[+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] [d>=date] [d<date]`.')
        await self._cmd_queens_ratings(
            ctx, TANGO_GAME, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved,
            weekly=weekly)

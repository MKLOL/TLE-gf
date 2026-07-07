"""Queens text command group and subcommands (Minigames cog command mixin; see minigames.py)."""


import datetime as dt

from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import (
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, queens_mod_only,
    _safe_member_name,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_UPDATE_THROTTLE_SECONDS,
    _QUEENS_STATE_PATH_KEY, _QUEENS_DEFAULT_STATE_PATH,
    _parse_queens_update_args,
)

logger = __import__('logging').getLogger(__name__)


class QueensCmdsMixin:
    @commands.group(name='queens', aliases=['queen'],
                    brief='LinkedIn Queens commands',
                    invoke_without_command=True)
    async def queens(self, ctx):
        await ctx.send_help(ctx.command)

    @queens.command(name='show', brief='Show LinkedIn Queens settings')
    async def queens_show(self, ctx):
        await self._cmd_queens_show(ctx)

    @queens.group(name='admins', aliases=['admin'],
                  brief='Manage extra LinkedIn Queens command admins',
                  invoke_without_command=True)
    @queens_mod_only()
    async def queens_admins(self, ctx):
        await self._cmd_queens_admins(ctx)

    @queens_admins.command(name='add',
                           brief='(Mod) Add a Queens command admin',
                           usage='@user')
    @queens_mod_only()
    async def queens_admins_add(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_queens_admins_add(ctx, member)

    @queens_admins.command(name='remove',
                           brief='(Mod) Remove a Queens command admin',
                           usage='@user')
    @queens_mod_only()
    async def queens_admins_remove(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_queens_admins_remove(ctx, member)

    @queens.command(name='here', brief='Set the LinkedIn Queens channel to the current channel')
    @queens_mod_only()
    async def queens_here(self, ctx):
        await self._cmd_here(ctx, QUEENS_GAME)

    @queens.command(name='clear', brief='Clear the LinkedIn Queens channel')
    @queens_mod_only()
    async def queens_channel_clear(self, ctx, *args):
        # ``clear`` used to be an alias for per-date deletion; refuse stray
        # arguments so an old-style ``;queens clear DATE`` cannot silently
        # unset the channel instead.
        if args:
            raise MinigameCogError(
                '`;queens clear` unsets the Queens channel and takes no '
                'arguments. To remove results for a date, use '
                '`;queens delete DATE`.')
        await self._cmd_clear(ctx, QUEENS_GAME)

    @queens.command(name='register',
                    brief='Link a Discord user to a LinkedIn Queens name',
                    usage='[+username DiscordUser] LinkedIn Name [+anon]')
    async def queens_register(self, ctx, first: str = None, *,
                              linkedin: str = None):
        await self._cmd_queens_register_cmd(ctx, first, linkedin)

    @queens.command(name='set',
                    brief='(Mod) Link a Discord user without LinkedIn verification',
                    usage='[+anon] DiscordUser LinkedIn Name [+anon]')
    @queens_mod_only()
    async def queens_set(self, ctx, member: str = None, *,
                         linkedin: str = None):
        await self._cmd_queens_set_cmd(ctx, member, linkedin)

    @queens.command(name='unregister',
                    brief='Remove a user LinkedIn Queens link',
                    usage='[@user]')
    async def queens_unregister(self, ctx, member: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if member is None:
            target = ctx.author
        else:
            target = await self._resolve_member(ctx, member)
        await self._cmd_queens_unregister(ctx, target)

    @queens.command(name='links', brief='List registered LinkedIn Queens names')
    async def queens_links(self, ctx):
        await self._cmd_queens_links(ctx)

    @queens.group(name='connection', aliases=['account'],
                  brief='Show or set the LinkedIn account players connect to',
                  invoke_without_command=True)
    async def queens_connection(self, ctx):
        await self._cmd_queens_connection(ctx)

    @queens_connection.command(name='set',
                               brief='(Mod) Set the LinkedIn connection account',
                               usage='LinkedIn Name profile_url')
    @queens_mod_only()
    async def queens_connection_set(self, ctx, *, linkedin: str):
        await self._cmd_queens_connection_set(ctx, linkedin)

    @queens_connection.command(name='clear',
                               brief='(Mod) Clear the LinkedIn connection account')
    @queens_mod_only()
    async def queens_connection_clear(self, ctx):
        await self._cmd_queens_connection_clear(ctx)

    # ── Scraper-driven commands (login / play / update / settings) ─────

    @queens.command(
        name='install',
        brief='(Mod) Install Playwright + Chromium for the scraper')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_install(self, ctx):
        await self._cmd_queens_install(ctx)

    @queens.command(
        name='login',
        brief='(Mod) Upload a fresh LinkedIn session file',
        usage='[LinkedIn Name] (attach extra/.queens_state.json to the message)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_login(self, ctx, *, linkedin_name: str = None):
        await self._cmd_queens_login(ctx, linkedin_name)

    @queens.command(
        name='play',
        brief='(Mod) Solve today\'s puzzle + refresh the leaderboard')
    @queens_mod_only()
    async def queens_play(self, ctx):
        await self._cmd_queens_play(ctx)

    @queens.command(
        name='update',
        brief='Refresh the LinkedIn Queens leaderboard '
              f'(rate-limited to once per {_QUEENS_UPDATE_THROTTLE_SECONDS}s)',
        usage='[+yesterday]')
    async def queens_update(self, ctx, *args):
        results_day = _parse_queens_update_args(args)
        await self._cmd_queens_update(ctx, results_day=results_day)

    @queens.command(
        name='settings',
        brief='Show the LinkedIn Queens scraper config for this guild')
    async def queens_settings(self, ctx):
        await self._cmd_queens_settings(ctx)

    @queens.command(
        name='backfill', aliases=['backill'],
        brief='(Mod) Backfill historical Queens results',
        usage='@user|+all (attach queens_history.json)')
    @queens_mod_only()
    async def queens_backfill(self, ctx, target: str = None):
        await self._cmd_queens_backfill(ctx, target)

    @queens.command(
        name='state-path', aliases=['statepath'],
        brief='(Mod) Override where the scraper looks for state.json',
        usage='/abs/path/to/state.json | clear')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_state_path(self, ctx, *, path: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if path is None:
            raise MinigameCogError(
                'Usage: `;queens state-path /abs/path/to/state.json` '
                'or `;queens state-path clear`.')
        if path.strip().lower() == 'clear':
            cf_common.user_db.delete_guild_config(
                ctx.guild.id, _QUEENS_STATE_PATH_KEY)
            await ctx.send(embed=discord_common.embed_success(
                f'Cleared the override. Default is `{_QUEENS_DEFAULT_STATE_PATH}`.'))
            return
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _QUEENS_STATE_PATH_KEY, path.strip())
        await ctx.send(embed=discord_common.embed_success(
            f'Scraper will use `{path.strip()}` for the session file.'))

    @queens.command(name='ban',
                    brief='(Mod) Block a user from Queens imports/ratings',
                    usage='@user [reason...]')
    @queens_mod_only()
    async def queens_ban(self, ctx, member: CaseInsensitiveMember, *,
                         reason: str = None):
        await self._cmd_queens_ban(ctx, member, reason)

    @queens.command(name='unban',
                    brief='(Mod) Lift a Queens ban',
                    usage='@user')
    @queens_mod_only()
    async def queens_unban(self, ctx, member: CaseInsensitiveMember):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        removed = cf_common.user_db.unban_minigame_user(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{QUEENS_GAME.display_name}. Their registration was kept, so '
            'new results count again immediately.'))

    @queens.command(name='bans',
                    brief='(Mod) List Queens bans')
    @queens_mod_only()
    async def queens_bans(self, ctx):
        await self._cmd_queens_bans(ctx)

    @queens.command(name='vs', brief='Head-to-head comparison',
                    usage='@user1 @user2 [filters...] [+dow=mon,wed|weekday|weekend]')
    async def queens_vs(self, ctx, member1: CaseInsensitiveMember,
                        member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, QUEENS_GAME, member1, member2, *args)

    @queens.command(name='top', brief='Show fastest-result winners',
                    usage='[filters...] [+dow=mon,wed|weekday|weekend]')
    async def queens_top(self, ctx, *args):
        await self._cmd_top(ctx, QUEENS_GAME, *args)

    @queens.command(name='streak', brief='Show current clean streak',
                    usage='[@user] [filters...] [+dow=mon,wed|weekday|weekend]')
    async def queens_streak(self, ctx, *args):
        await self._cmd_queens_streak(ctx, *args)

    @queens.group(name='stats', brief='Show personal Queens stats',
                  usage='[@user] [filters...] [+dow=mon,wed|weekday|weekend]',
                  invoke_without_command=True)
    async def queens_stats(self, ctx, *args):
        await self._cmd_queens_stats(ctx, *args)

    @queens.group(name='results', brief='Show Queens date leaderboard',
                  usage='[date|number] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def queens_results(self, ctx, *args):
        remaining, excluded_ids, included_ids, weekdays, date_bounds = (
            await self._extract_queens_rating_filters(ctx, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;queens results [date|number] '
                '[+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] '
                '[d>=date] [d<date]`.')
        date_arg = remaining[0] if remaining else dt.date.today().isoformat()
        await self._cmd_queens_stats_date(
            ctx, date_arg,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens_results.command(name='debug',
                            brief='(Mod) Date results with ratings for ALL players',
                            usage='[date|number] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @queens_mod_only()
    async def queens_results_debug(self, ctx, *args):
        remaining, excluded_ids, included_ids, weekdays, date_bounds = (
            await self._extract_queens_rating_filters(ctx, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;queens results debug [date|number] '
                '[+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] '
                '[d>=date] [d<date]`.')
        date_arg = remaining[0] if remaining else dt.date.today().isoformat()
        await self._cmd_queens_stats_date(
            ctx, date_arg, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens.group(name='import',
                  brief='Preview pasted Queens results or manage imported history',
                  usage='date <pasted leaderboard>',
                  invoke_without_command=True)
    @queens_mod_only()
    async def queens_import(self, ctx, puzzle_date: str = None, *,
                            leaderboard: str = None):
        await self._cmd_queens_import_preview(ctx, puzzle_date, leaderboard)

    @queens_import.command(name='start',
                           brief='Rebuild imported Queens history from channel messages')
    @queens_mod_only()
    async def queens_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, QUEENS_GAME, channel)

    @queens_import.command(name='status', brief='Show Queens import status')
    @queens_mod_only()
    async def queens_import_status(self, ctx):
        await self._cmd_import_status(ctx, QUEENS_GAME)

    @queens_import.command(name='cancel', brief='Cancel a running Queens import')
    @queens_mod_only()
    async def queens_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, QUEENS_GAME)

    @queens_import.command(name='clear', brief='Delete imported Queens history')
    @queens_mod_only()
    async def queens_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, QUEENS_GAME)

    @queens_import.command(name='confirm',
                           brief='Save the latest Queens import preview')
    @queens_mod_only()
    async def queens_import_confirm(self, ctx):
        await self._cmd_queens_import_confirm(ctx)

    @queens_import.command(name='orphans',
                           brief='(Mod) List imported results with no live counterpart')
    @queens_mod_only()
    async def queens_import_orphans(self, ctx):
        await self._cmd_import_orphans(ctx, QUEENS_GAME)

    @queens.command(name='export', brief='(Mod) Download a snapshot of the result tables')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_export(self, ctx):
        await self._cmd_akari_export(ctx, QUEENS_GAME)

    @queens.command(name='diff',
                    brief='(Mod) Diff an uploaded snapshot against current results',
                    usage='(attach a .db / .zip snapshot)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_diff(self, ctx):
        await self._cmd_akari_diff(ctx, QUEENS_GAME)

    @queens.command(name='add',
                    brief='Manually add a Queens result',
                    usage='<@user|LinkedIn Name> date|number time [status...]')
    @queens_mod_only()
    async def queens_add(self, ctx, *, args: str = None):
        await self._cmd_queens_add(ctx, args)

    @queens.command(name='remove', brief='Remove a Queens result',
                    usage='<@user|LinkedIn Name> date|number')
    @queens_mod_only()
    async def queens_remove(self, ctx, *, args: str = None):
        await self._cmd_queens_remove(ctx, args)

    # Standard names across both games: ``clear`` unsets the channel,
    # ``delete``/``clean`` remove results (``;queens clear DATE`` was the
    # historical spelling — the channel-clear command above hints at
    # ``delete`` if it gets arguments).
    @queens.command(name='delete',
                    brief='(Mod) Remove all Queens results for a date',
                    usage='date|number')
    @queens_mod_only()
    async def queens_delete(self, ctx, puzzle_date: str = None):
        await self._cmd_queens_clear(ctx, puzzle_date)

    @queens.command(name='clean', aliases=['cleanup'],
                    brief='(Mod) Remove Queens results for an inclusive date range',
                    usage='start-date|number [end-date|number]')
    @queens_mod_only()
    async def queens_clean(self, ctx, start_date: str = None,
                           end_date: str = None):
        await self._cmd_queens_clean(ctx, start_date, end_date)

    @queens.command(name='reparse', brief='(Mod) Reparse all stored raw Queens messages')
    @queens_mod_only()
    async def queens_reparse(self, ctx):
        await self._cmd_reparse(ctx, QUEENS_GAME)

    @queens.group(name='ratings', brief='Show Queens rating leaderboard',
                  usage='[+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def queens_ratings(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        remaining, excluded_ids, included_ids, weekdays, date_bounds = (
            await self._extract_queens_rating_filters(ctx, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;queens ratings [+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] [d>=date] [d<date]`.')
        await self._cmd_queens_ratings(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens.group(name='rating',
                  brief='Show Queens rating graph',
                  usage='[@user1 @user2 ...] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date] [+recalculate]',
                  invoke_without_command=True)
    async def queens_rating(self, ctx, *args):
        (members, excluded_ids, included_ids, weekdays, date_bounds,
         recalculate) = await self._parse_queens_rating_args(
            ctx, args, allow_recalculate=True)
        await self._cmd_queens_rating(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            recalculate=recalculate)

    @queens_rating.command(name='debug',
                           brief='(Mod) Rating graph for any rated user',
                           usage='@user1 [@user2 ...] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date] [+recalculate]')
    @queens_mod_only()
    async def queens_rating_debug(self, ctx, *args):
        (members, excluded_ids, included_ids, weekdays, date_bounds,
         recalculate) = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True, allow_recalculate=True))
        await self._cmd_queens_rating(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            recalculate=recalculate)

    @queens.group(name='performance', aliases=['perf'],
                  brief='Show Queens performance graph',
                  usage='[@user1 @user2 ...] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def queens_performance(self, ctx, *args):
        members, excluded_ids, included_ids, weekdays, date_bounds, _recalculate = (
            await self._parse_queens_rating_args(ctx, args))
        await self._cmd_queens_performance(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens_performance.command(name='debug',
                                brief='(Mod) Performance graph for any rated user',
                                usage='@user1 [@user2 ...] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @queens_mod_only()
    async def queens_performance_debug(self, ctx, *args):
        members, excluded_ids, included_ids, weekdays, date_bounds, _recalculate = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True))
        await self._cmd_queens_performance(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens.group(name='history',
                  brief='Paginated Queens rating delta log',
                  usage='[@user] [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]',
                  invoke_without_command=True)
    async def queens_history(self, ctx, *args):
        members, excluded_ids, included_ids, weekdays, date_bounds, _recalculate = (
            await self._parse_queens_rating_args(ctx, args))
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens_history.command(name='debug',
                            brief='(Mod) Rating delta log for any rated user',
                            usage='@user [+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @queens_mod_only()
    async def queens_history_debug(self, ctx, *args):
        members, excluded_ids, included_ids, weekdays, date_bounds, _recalculate = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True))
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

    @queens_ratings.command(name='recompute',
                            brief='(Mod) Rebuild the Queens rating snapshot')
    @queens_mod_only()
    async def queens_ratings_recompute(self, ctx):
        await self._cmd_queens_ratings_recompute(ctx)

    @queens_ratings.command(name='debug', aliases=['all'],
                            brief='(Mod) Leaderboard including unregistered rated users',
                            usage='[+exclude=…] [+include=…] [+dow=mon,wed|weekday|weekend] [d>=date] [d<date]')
    @queens_mod_only()
    async def queens_ratings_debug(self, ctx, *args):
        remaining, excluded_ids, included_ids, weekdays, date_bounds = (
            await self._extract_queens_rating_filters(ctx, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;queens ratings debug [+exclude=…] [+include=…] '
                '[+dow=mon,wed|weekday|weekend] [d>=date] [d<date]`.')
        await self._cmd_queens_ratings(
            ctx, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)

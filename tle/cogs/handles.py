import io
import asyncio
import logging
import math
import random

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import tasks
from tle.util import db

from PIL import ImageFont

from tle.cogs._handles_helpers import (
    HandleCogError,
    _GudgitterRow,
    FONTS,
    rating_to_color,
    get_gudgitters_image,
    get_prettyhandles_image,
    _make_profile_embed,
    _make_pages,
    parse_date,
    _parse_gudgitter_args,
    _HANDLES_PER_PAGE,
    _NAME_MAX_LEN,
    _PAGINATE_WAIT_TIME,
    _PRETTY_HANDLES_PER_PAGE,
    _TOP_DELTAS_COUNT,
    _MAX_RATING_CHANGES_PER_EMBED,
    _UPDATE_HANDLE_STATUS_INTERVAL,
    _DIVISION_RATING_LOW,
    _DIVISION_RATING_HIGH,
    _LEADERBOARD_PER_PAGE,
)
from tle.cogs._handles_gudgitters import GudgittersMixin
from tle.cogs._handles_rankup import RankUpMixin


class Handles(GudgittersMixin, RankUpMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger(self.__class__.__name__)
        self.font = ImageFont.truetype(constants.NOTO_SANS_CJK_BOLD_FONT_PATH, size=26) # font for ;handle pretty
        self.converter = commands.MemberConverter()

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        cf_common.event_sys.add_listener(self._on_rating_changes)
        self._set_ex_users_inactive_task.start()

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        cf_common.user_db.set_inactive([(member.guild.id, member.id)])

    @commands.command(brief='update status, mark guild members as active')
    @commands.has_role(constants.TLE_ADMIN)
    async def _updatestatus(self, ctx):
        gid = ctx.guild.id
        active_ids = [m.id for m in ctx.guild.members]
        cf_common.user_db.reset_status(gid)
        rc = sum(cf_common.user_db.update_status(gid, chunk) for chunk in paginator.chunkify(active_ids, 100))
        await ctx.send(f'{rc} members active with handle')

    @commands.Cog.listener()
    async def on_member_join(self, member):
        rc = cf_common.user_db.update_status(member.guild.id, [member.id])
        if rc == 1:
            handle = cf_common.user_db.get_handle(member.id, member.guild.id)
            await self._update_ranks(member.guild, [(int(member.id), handle)])

    @tasks.task_spec(name='SetExUsersInactive',
                     waiter=tasks.Waiter.fixed_delay(_UPDATE_HANDLE_STATUS_INTERVAL))
    async def _set_ex_users_inactive_task(self, _):
        # To set users inactive in case the bot was dead when they left.
        to_set_inactive = []
        for guild in self.bot.guilds:
            user_id_handle_pairs = cf_common.user_db.get_handles_for_guild(guild.id)
            to_set_inactive += [(guild.id, user_id) for user_id, _ in user_id_handle_pairs
                                if guild.get_member(user_id) is None]
        cf_common.user_db.set_inactive(to_set_inactive)

    @commands.group(brief='Commands that have to do with handles', invoke_without_command=True)
    async def handle(self, ctx):
        """Change or collect information about specific handles on Codeforces"""
        await ctx.send_help(ctx.command)

    @handle.command(brief='Set Codeforces handle of a user', aliases=["link"])
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def set(self, ctx, member: discord.Member, handle: str):
        """Set Codeforces handle of a user."""
        # CF API returns correct handle ignoring case, update to it
        user, = await cf.user.info(handles=[handle])
        await self._set(ctx, member, user)
        embed = _make_profile_embed(member, user, mode='set')
        await ctx.send(embed=embed)

    async def _set(self, ctx, member, user):
        handle = user.handle
        try:
            cf_common.user_db.set_handle(member.id, ctx.guild.id, handle)
        except db.UniqueConstraintFailed:
            raise HandleCogError(f'The handle `{handle}` is already associated with another user.')
        rc = cf_common.user_db.cache_cf_user(user)
        if rc != 1:
            raise HandleCogError('DB update for user {user.handle} failed.')

        roles = [role for role in ctx.guild.roles if role.name == user.rank.title]
        if not roles:
            raise HandleCogError(f'Role for rank `{user.rank.title}` not present in the server')
        role_to_assign = roles[0]
        await self.update_member_rank_role(member, role_to_assign,
                                           reason='New handle set for user')

    @handle.command(brief='Identify yourself', usage='[handle]')
    @cf_common.user_guard(group='handle',
                          get_exception=lambda: HandleCogError('Identification is already running for you'))
    async def identify(self, ctx, handle: str):
        """Link a codeforces account to discord account by submitting a compile error to a random problem"""
        if cf_common.user_db.get_handle(ctx.author.id, ctx.guild.id):
            raise HandleCogError(f'{ctx.author.mention}, you cannot identify when your handle is '
                                 'already set. Ask an Admin or Moderator if you wish to change it')

        if cf_common.user_db.get_user_id(handle, ctx.guild.id):
            raise HandleCogError(f'The handle `{handle}` is already associated with another user. Ask an Admin or Moderator in case of an inconsistency.')

        if handle in cf_common.HandleIsVjudgeError.HANDLES:
            raise cf_common.HandleIsVjudgeError(handle)

        users = await cf.user.info(handles=[handle])
        invoker = str(ctx.author)
        handle = users[0].handle
        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if prob.rating <= 1200]
        problem = random.choice(problems)
        await ctx.send(f'`{invoker}`, submit a compile error to <{problem.url}> within 60 seconds (this will show the bot that you have access to the account)')
        for i in range(4):
            await asyncio.sleep(15)

            subs = await cf.user.status(handle=handle, count=5)
            if any(sub.problem.name == problem.name and sub.verdict == 'COMPILATION_ERROR' for sub in subs):
                user, = await cf.user.info(handles=[handle])
                await self._set(ctx, ctx.author, user)
                embed = _make_profile_embed(ctx.author, user, mode='set')
                await ctx.send(embed=embed)
                return
        await ctx.send(f'Sorry `{invoker}`, can you try again? Remember: The identification process needs you to submit a Compilation error to the mentioned problem!')

    @handle.command(brief='Get handle by Discord username')
    async def get(self, ctx, member: discord.Member):
        """Show Codeforces handle of a user."""
        handle = cf_common.user_db.get_handle(member.id, ctx.guild.id)
        if not handle:
            raise HandleCogError(f'Handle for {member.mention} not found in database')
        user = cf_common.user_db.fetch_cf_user(handle)
        embed = _make_profile_embed(member, user, mode='get')
        await ctx.send(embed=embed)

    @handle.command(brief='Get Discord username by cf handle')
    async def rget(self, ctx, handle: str):
        """Show Discord username of a cf handle."""
        user_id = cf_common.user_db.get_user_id(handle, ctx.guild.id)
        if not user_id:
            raise HandleCogError(f'Discord username for `{handle}` not found in database')
        user = cf_common.user_db.fetch_cf_user(handle)
        member = ctx.guild.get_member(user_id)
        if member is None:
            raise HandleCogError(f'{user_id} not found in the guild')
        embed = _make_profile_embed(member, user, mode='get')
        await ctx.send(embed=embed)

    @handle.command(brief='Unlink handle', aliases=["unlink"])
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def remove(self, ctx, handle: str):
        """Remove Codeforces handle of a user."""
        handle, = await cf_common.resolve_handles(ctx, self.converter, ['-c' + handle])
        user_id = cf_common.user_db.get_user_id(handle, ctx.guild.id)
        if user_id is None:
            raise HandleCogError(f'{handle} not found in database')

        cf_common.user_db.remove_handle(handle, ctx.guild.id)
        member = ctx.guild.get_member(user_id)
        await self.update_member_rank_role(member, role_to_assign=None,
                                           reason='Handle unlinked')
        embed = discord_common.embed_success(f'Removed {handle} from database')
        await ctx.send(embed=embed)

    @handle.command(brief='Resolve redirect of a user\'s handle')
    async def unmagic(self, ctx):
        """Updates handle of the calling user if they have changed handles
        (typically new year's magic)"""
        member = ctx.author
        handle = cf_common.user_db.get_handle(member.id, ctx.guild.id)
        await self._unmagic_handles(ctx, [handle], {handle: member})

    @handle.command(brief='Resolve handles needing redirection')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def unmagic_all(self, ctx):
        """Updates handles of all users that have changed handles
        (typically new year's magic)"""
        user_id_and_handles = cf_common.user_db.get_handles_for_guild(ctx.guild.id)

        handles = []
        rev_lookup = {}
        for user_id, handle in user_id_and_handles:
            member = ctx.guild.get_member(user_id)
            handles.append(handle)
            rev_lookup[handle] = member
        await self._unmagic_handles(ctx, handles, rev_lookup)

    async def _unmagic_handles(self, ctx, handles, rev_lookup):
        handle_cf_user_mapping = await cf.resolve_redirects(handles)
        mapping = {(rev_lookup[handle], handle): cf_user
                   for handle, cf_user in handle_cf_user_mapping.items()}
        summary_embed = await self._fix_and_report(ctx, mapping)
        await ctx.send(embed=summary_embed)

    async def _fix_and_report(self, ctx, redirections):
        fixed = []
        failed = []
        for (member, handle), cf_user in redirections.items():
            if not cf_user:
                failed.append(handle)
            else:
                await self._set(ctx, member, cf_user)
                fixed.append((handle, cf_user.handle))

        # Return summary embed
        lines = []
        if not fixed and not failed:
            return discord_common.embed_success('No handles updated')
        if fixed:
            lines.append('**Fixed**')
            lines += (f'{old} -> {new}' for old, new in fixed)
        if failed:
            lines.append('**Failed**')
            lines += failed
        return discord_common.embed_success('\n'.join(lines))

    @handle.command(brief="Show all handles")
    async def list(self, ctx, *countries):
        """Shows members of the server who have registered their handles and
        their Codeforces ratings. You can additionally specify a list of countries
        if you wish to display only members from those countries. Country data is
        sourced from codeforces profiles. e.g. ;handle list Croatia Slovenia
        """
        countries = [country.title() for country in countries]
        res = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
        users = [(ctx.guild.get_member(user_id), cf_user.handle, cf_user.rating)
                 for user_id, cf_user in res if not countries or cf_user.country in countries]
        users = [(member, handle, rating) for member, handle, rating in users if member is not None]
        if not users:
            raise HandleCogError('No members with registered handles.')

        users.sort(key=lambda x: (1 if x[2] is None else -x[2], x[1]))  # Sorting by (-rating, handle)
        title = 'Handles of server members'
        if countries:
            title += ' from ' + ', '.join(f'`{country}`' for country in countries)
        pages = _make_pages(users, title)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=_PAGINATE_WAIT_TIME,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @handle.command(brief="Show handles, but prettier")
    async def pretty(self, ctx, page_no: int = None):
        """Show members of the server who have registered their handles and their Codeforces
        ratings, in color.
        """
        user_id_cf_user_pairs = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
        user_id_cf_user_pairs.sort(key=lambda p: p[1].rating if p[1].rating is not None else -1,
                                   reverse=True)
        rows = []
        author_idx = None
        for user_id, cf_user in user_id_cf_user_pairs:
            member = ctx.guild.get_member(user_id)
            if member is None:
                continue
            idx = len(rows)
            if member == ctx.author:
                author_idx = idx
            rows.append((idx, member.display_name, cf_user.handle, cf_user.rating))

        if not rows:
            raise HandleCogError('No members with registered handles.')
        max_page = math.ceil(len(rows) / _PRETTY_HANDLES_PER_PAGE) - 1
        if author_idx is None and page_no is None:
            raise HandleCogError(f'Please specify a page number between 0 and {max_page}.')

        msg = None
        if page_no is not None:
            if page_no < 0 or max_page < page_no:
                msg_fmt = 'Page number must be between 0 and {}. Showing page {}.'
                if page_no < 0:
                    msg = msg_fmt.format(max_page, 0)
                    page_no = 0
                else:
                    msg = msg_fmt.format(max_page, max_page)
                    page_no = max_page
            start_idx = page_no * _PRETTY_HANDLES_PER_PAGE
        else:
            msg = f'Showing neighbourhood of user `{ctx.author.display_name}`.'
            num_before = (_PRETTY_HANDLES_PER_PAGE - 1) // 2
            start_idx = max(0, author_idx - num_before)
        rows_to_display = rows[start_idx : start_idx + _PRETTY_HANDLES_PER_PAGE]
        img = get_prettyhandles_image(rows_to_display, self.font)
        buffer = io.BytesIO()
        img.save(buffer, 'png')
        buffer.seek(0)
        await ctx.send(msg, file=discord.File(buffer, 'handles.png'),
                       allowed_mentions=discord.AllowedMentions.none())

    @discord_common.send_error_if(HandleCogError, cf_common.HandleIsVjudgeError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Handles(bot))

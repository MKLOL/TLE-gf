
import asyncio
import time
import logging

import discord

from discord.ext import commands
from discord.ext.commands import cooldown, BucketType

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import discord_common
from tle.util import paginator
from tle.cogs._lockout_helpers import (
    _calc_round_score,
    RoundCogError,
    MAX_ROUND_USERS,
    LOWER_RATING,
    UPPER_RATING,
    MATCH_DURATION,
    MAX_PROBLEMS,
    MAX_ALTS,
    ROUNDS_PER_PAGE,
    AUTO_UPDATE_TIME,
    RECENT_SUBS_LIMIT,
    PROBLEM_STATUS_UNSOLVED,
    PROBLEM_STATUS_TESTING,
    _PAGINATE_WAIT_TIME,
)
from tle.cogs._lockout_impl import RoundImplMixin

logger = logging.getLogger(__name__)


class Round(RoundImplMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.locked = False

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        asyncio.create_task(self._check_ongoing_rounds())

    @commands.group(brief='Commands related to lockout rounds! Type ;round for more details', invoke_without_command=True)
    async def round(self, ctx):
        await ctx.send(embed=self.make_round_embed(ctx))

    @round.command(brief='Set the lockout channel to the current channel (Admin/Mod only)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)  # OK
    async def set_channel(self, ctx):
        """ Sets the lockout round channel to the current channel.
        """
        cf_common.user_db.set_round_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success('Lockout round channel saved successfully'))

    @round.command(brief='Get the lockout channel')
    async def get_channel(self, ctx):
        """ Gets the lockout round channel.
        """
        channel_id = cf_common.user_db.get_round_channel(ctx.guild.id)
        channel = ctx.guild.get_channel(channel_id)
        if channel is None:
            raise RoundCogError('There is no lockout round channel')
        embed = discord_common.embed_success('Current lockout round channel')
        embed.add_field(name='Channel', value=channel.mention)
        await ctx.send(embed=embed)

    @round.command(name="challenge", brief="Challenge multiple users to a round", usage="[@user1 @user2...]")
    async def challenge(self, ctx, *members: discord.Member):
        # check if we are in the correct channel
        self._check_if_correct_channel(ctx)

        members = list(set(members))
        if ctx.author not in members:
            members.append(ctx.author)
        if len(members) > MAX_ROUND_USERS:
            raise RoundCogError(f'{ctx.author.mention} atmost {MAX_ROUND_USERS} users can compete at a time')

        # get handles first. This also checks if discord member has a linked handle!
        handles = cf_common.members_to_handles(members, ctx.guild.id)
        for member in members:
            if not cf_common.user_db.is_duelist(member.id, ctx.guild.id):
                cf_common.user_db.register_duelist(member.id, ctx.guild.id)

        # check for members still in a round
        self._check_if_any_member_is_already_in_round(ctx, members)

        await self._check_if_all_members_ready(ctx, members)

        problem_cnt = await self._get_time_response(self.bot, ctx, f"{ctx.author.mention} enter the number of problems between [1, {MAX_PROBLEMS}]", 30, ctx.author, [1, MAX_PROBLEMS])

        duration = await self._get_time_response(self.bot, ctx, f"{ctx.author.mention} enter the duration of match in minutes between {MATCH_DURATION}", 30, ctx.author, MATCH_DURATION)

        ratings = await self._get_seq_response(self.bot, ctx, f"{ctx.author.mention} enter {problem_cnt} space seperated integers denoting the ratings of problems (between {LOWER_RATING} and {UPPER_RATING})", 60, problem_cnt, ctx.author, [LOWER_RATING, UPPER_RATING])

        points = await self._get_seq_response(self.bot, ctx, f"{ctx.author.mention} enter {problem_cnt} space seperated integer denoting the points of problems (between 100 and 10,000)", 60, problem_cnt, ctx.author, [100, 10000])

        repeat = await self._get_time_response(self.bot, ctx, f"{ctx.author.mention} do you want a new problem to appear when someone solves a problem (type 1 for yes and 0 for no)", 30, ctx.author, [0, 1])

        # pick problems
        submissions = [await cf.user.status(handle=handle) for handle in handles]
        solved = {sub.problem.name for subs in submissions for sub in subs if sub.verdict != 'COMPILATION_ERROR'}
        selected = []
        for rating in ratings:
            problem = await self._pick_problem(handles, solved, rating, selected)
            selected.append(problem)

        await ctx.send(embed=discord.Embed(description="Starting the round...", color=discord.Color.green()))

        cf_common.user_db.create_ongoing_round(ctx.guild.id, int(time.time()), members, ratings, points, selected, duration, repeat)
        round_info = cf_common.user_db.get_round_info(ctx.guild.id, members[0].id)

        await ctx.send(embed=self._round_problems_embed(round_info))

    @round.command(brief="Invalidate a round (Admin/Mod only)", usage="@user")
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)  # OK
    async def _invalidate(self, ctx, member: discord.Member):
        if not cf_common.user_db.check_if_user_in_ongoing_round(ctx.guild.id, member.id):
            raise RoundCogError(f'{member.mention} is not in a round')
        cf_common.user_db.delete_round(ctx.guild.id, member.id)
        await ctx.send(f'Round deleted.')

    @round.command(brief="View problems of your round or for a specific user", usage="[@user]")
    async def problems(self, ctx, member: discord.Member=None):
        # check if we are in the correct channel
        self._check_if_correct_channel(ctx)

        if not member:
            member = ctx.author
        if not cf_common.user_db.check_if_user_in_ongoing_round(ctx.guild.id, member.id):
            raise RoundCogError(f'{member.mention} is not in a round')

        round_info = cf_common.user_db.get_round_info(ctx.guild.id, member.id)
        await ctx.send(embed=self._round_problems_embed(round_info))

    @round.command(brief="Update matches status for the server")
    @cooldown(1, AUTO_UPDATE_TIME, BucketType.guild)
    async def update(self, ctx):
        # check if we are in the correct channel
        self._check_if_correct_channel(ctx)

        await ctx.send(embed=discord.Embed(description="Updating rounds for this server", color=discord.Color.green()))

        await self._update_all_ongoing_rounds(ctx.guild, ctx.channel, False)

    @round.command(name="ongoing", brief="View ongoing rounds")
    async def ongoing(self, ctx):
        data = cf_common.user_db.get_ongoing_rounds(ctx.guild.id)

        if not data:
            raise RoundCogError(f"No ongoing rounds")

        def _make_pages(data, title):
            chunks = paginator.chunkify(data, ROUNDS_PER_PAGE)
            pages = []

            for chunk in chunks:
                msg = ''
                for round in chunk:
                    ranklist = _calc_round_score(list(map(int, round.users.split())), list(map(int, round.status.split())),
                                                    list(map(int, round.times.split())))
                    msg += ' vs '.join([f"[{cf_common.user_db.get_handle(user.id, round.guild) }](https://codeforces.com/profile/{cf_common.user_db.get_handle(user.id, round.guild) }) `Rank {user.rank}` `{user.points} Points`"
                                    for user in ranklist])
                    msg += f"\n**Problem ratings:** {round.rating}"
                    msg += f"\n**Score distribution** {round.points}"
                    timestr = cf_common.pretty_time_format(((round.time + 60 * round.duration) - int(time.time())), shorten=True, always_seconds=True)
                    msg += f"\n**Time left:** {timestr}\n\n"
                embed = discord_common.cf_color_embed(description=msg)
                pages.append((title, embed))

            return pages

        title = 'List of ongoing lockout rounds'
        pages = _make_pages(data, title)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=_PAGINATE_WAIT_TIME,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @round.command(name="recent", brief="Show recent rounds")
    async def recent(self, ctx, user: discord.Member=None):
        data = cf_common.user_db.get_recent_rounds(ctx.guild.id, str(user.id) if user else None)

        if not data:
            raise RoundCogError(f"No recent rounds")

        def _make_pages(data, title):
            chunks = paginator.chunkify(data, ROUNDS_PER_PAGE)
            pages = []

            for chunk in chunks:
                msg = ''
                for round in chunk:
                    ranklist = _calc_round_score(list(map(int, round.users.split())), list(map(int, round.status.split())),
                                                    list(map(int, round.times.split())))
                    msg += ' vs '.join([f"[{cf_common.user_db.get_handle(user.id, round.guild) }](https://codeforces.com/profile/{cf_common.user_db.get_handle(user.id, round.guild) }) `Rank {user.rank}` `{user.points} Points`"
                                    for user in ranklist])
                    msg += f"\n**Problem ratings:** {round.rating}"
                    msg += f"\n**Score distribution** {round.points}"
                    timestr = cf_common.pretty_time_format(min(60*round.duration, round.end_time-round.time), shorten=True, always_seconds=True)
                    msg += f"\n**Duration:** {timestr}\n\n"
                embed = discord_common.cf_color_embed(description=msg)
                pages.append((title, embed))

            return pages

        title = 'List of recent lockout rounds'
        pages = _make_pages(data, title)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=_PAGINATE_WAIT_TIME,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @discord_common.send_error_if(RoundCogError, cf_common.ResolveHandleError,
                                  cf_common.FilterError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Round(bot))

from typing import List
import logging

import discord
from discord.ext import commands


from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    CodeforcesCogError,
    getEloWinProbability as _getEloWinProbability,
    composeRatings as _composeRatings,
    _GITGUD_NO_SKIP_TIME,
    _GITGUD_SCORE_DISTRIB,
    _GITGUD_SCORE_DISTRIB_MIN,
    _GITGUD_SCORE_DISTRIB_MAX,
    _ONE_WEEK_DURATION,
    _GITGUD_MORE_POINTS_START_TIME,
)
from tle.cogs._codeforces_gitgud import CodeforcesGitgudMixin
from tle.cogs._codeforces_problems import CodeforcesProblemsMixin


class Codeforces(CodeforcesGitgudMixin, CodeforcesProblemsMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()
        self.logger = logging.getLogger(self.__class__.__name__)

    @commands.command(brief='Upsolve a problem')
    @cf_common.user_guard(group='gitgud')
    async def upsolve(self, ctx, choice: int = -1):
        """Upsolve: The command ;upsolve lists all problems that you haven't solved in contests you participated
        - Type ;upsolve for listing all available problems.
        - Type ;upsolve <nr> for choosing the problem <nr> as gitgud problem (only possible if you have no active gitgud challenge)
        - After solving the problem you can claim gitgud points for it with ;gotgud
        - If you can't solve the problem or used external help you should skip it with ;nogud (Available after 2 hours)
        - The all-time ranklist can be found with ;gitgudders
        - A monthly ranklist is shown when you type ;monthlygitgudders
        - Another way to gather gitgud points is ;gitgud (only works if you have no active gitgud-Challenge)
        - For help with each of the commands you can type ;help <command> (e.g. ;help gitgudders)

        Point distribution:
        delta  | <-300| -300 | -200 | -100 |  0  |  100 |  200 |>=300
        points |   1  |   2  |   3  |   5  |  8  |  12  |  17  |  23
        """
        await self._upsolve_impl(ctx, choice)

    @commands.command(brief='Recommend a problem',
                      usage='[+tag..] [~tag..] [+divX] [~divX] [rating|rating1-rating2] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    @cf_common.user_guard(group='gitgud')
    async def gimme(self, ctx, *args):
        await self._gimme_impl(ctx, args)

    @commands.command(brief='List solved problems',
                      usage='[handles] [+hardest] [+practice] [+contest] [+rated] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [c+marker..] [i+index..]')
    async def stalk(self, ctx, *args):
        """Print problems solved by user sorted by time (default) or rating.
        All submission types are included by default (practice, contest, etc.)
        Use +rated to show only contests that were actually rated for the user
        (excludes edu rounds and other contests that were unrated for them).
        Prefix -c to force a Codeforces handle (e.g. -ctourist).
        """
        await self._stalk_impl(ctx, args)

    @commands.command(brief='Create a mashup', usage='[handles] [+tag..] [~tag..] [+divX] [~divX] [?[-]delta]')
    async def mashup(self, ctx, *args):
        """Create a mashup contest using problems within -200 and +400 of average rating of handles provided.
        Add tags with "+" before them. Ban tags with "~" before them.
        Prefix -c to force a Codeforces handle (e.g. -ctourist).
        """
        await self._mashup_impl(ctx, args)

    @commands.command(brief='Challenge', aliases=['gitbad'],
                      usage='[rating|rating1-rating2] [+tags] [~tags] [+divX] [~divX]')
    @cf_common.user_guard(group='gitgud')
    async def gitgud(self, ctx, *args):
        """Gitgud: Request a problem with a specific rating with ;gitgud <rating> or within a rating range with ;gitgud <rating1>-<rating2>
        - Points are assigned by difference between problem rating and your current rating (rounded to nearest 100)
        - Filter problems by division with [+divX] [~divX] possible values are div1, div2, div3, div4, edu
        - Filter problems by tags with [+tags] [~tags]
        - Claim gitgud points once problem is solved with ;gotgud
        - If you can't solve the problem or used external help you should skip it with ;nogud (Available after 2 hours)
        - All-time ranklist: ;gitgudders
        - Monthly ranklist: ;monthlygitgudders
        - Another way to gather gitgud points is ;upsolve (only works if there is no active gitgud-Challenge)
        - Get more help with ;help <command> (e.g. ;help gitgudders)

        Point distribution:
        rating diff | <-300| -300 | -200 | -100 |   0  |  100 |  200 |>=300
        no tags     |   1  |   2  |   3  |   5  |   8  |  12  |  17  |  23
        Each penalised tag divides those points by (tag count + 1), floored,
        never below 1.
        """
        await self._gitgud_impl(ctx, args)

    @commands.command(brief='Print user gitgud history')
    async def gitlog(self, ctx, member: discord.Member = None):
        """Displays the list of gitgud problems issued to the specified member, excluding those noguded by admins.
        If the challenge was completed, time of completion and amount of points gained will also be displayed.
        """
        await self._gitlog_impl(ctx, member)

    @commands.command(brief='Print user nogud history')
    async def nogudlog(self, ctx, member: discord.Member = None):
        """Displays the list of nogud problems issued to the specified member, excluding those noguded by admins.
        """
        await self._nogudlog_impl(ctx, member)

    @commands.command(brief='Report challenge completion', aliases=['gotbad'])
    @cf_common.user_guard(group='gitgud')
    async def gotgud(self, ctx):
        await self._gotgud_impl(ctx)

    @commands.command(brief='Skip challenge', aliases=['toobad'])
    @cf_common.user_guard(group='gitgud')
    async def nogud(self, ctx):
        await self._nogud_impl(ctx)

    @commands.command(brief='Force skip a challenge')
    @cf_common.user_guard(group='gitgud')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def _nogud(self, ctx, member: discord.Member):
        await self._force_nogud_impl(ctx, member)

    @commands.command(brief='Recommend a contest', usage='[handles...] [+pattern...]')
    async def vc(self, ctx, *args: str):
        """Recommends a contest based on Codeforces rating of the handle provided.
        Prefix -c to force a Codeforces handle (e.g. -ctourist).
        e.g ;vc mblazev c1729 +global +hello +goodbye +avito"""
        await self._vc_impl(ctx, args)

    @commands.command(brief="Display unsolved rounds closest to completion", usage='[keywords]')
    async def fullsolve(self, ctx, *args: str):
        """Displays a list of contests, sorted by number of unsolved problems.
        Contest names matching any of the provided tags will be considered. e.g ;fullsolve +edu"""
        await self._fullsolve_impl(ctx, args)

    @staticmethod
    def getEloWinProbability(ra: float, rb: float) -> float:
        return _getEloWinProbability(ra, rb)

    @staticmethod
    def composeRatings(left: float, right: float, ratings: List[float]) -> int:
        return _composeRatings(left, right, ratings)

    @commands.command(brief='Calculate team rating', usage='[handles] [+peak]')
    async def teamrate(self, ctx, *args: str):
        """Provides the combined rating of the entire team.
        If +server is provided as the only handle, will display the rating of the entire server.
        Supports multipliers. e.g: ;teamrate gamegame*1000
        Prefix -c to force a Codeforces handle (e.g. -ctourist)."""
        await self._teamrate_impl(ctx, args)

    @discord_common.send_error_if(CodeforcesCogError, cf_common.ResolveHandleError,
                                  cf_common.FilterError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Codeforces(bot))

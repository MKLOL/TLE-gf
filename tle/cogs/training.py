import datetime

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.cogs._training_helpers import (
    FONTS,
    Game,
    TrainingMode,
    TrainingResult,
    TrainingCogError,
    rating_to_color,
    get_fastest_solves_image,
    _TRAINING_MIN_RATING_VALUE,
    _TRAINING_MAX_RATING_VALUE,
)
from tle.cogs._training_impl import TrainingImplMixin


class Training(TrainingImplMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()

    @commands.group(brief='Training commands',
                    invoke_without_command=True)
    async def training(self, ctx):
        """ A training is a game played against the bot. In this game the bot will assign you a codeforces problem that you should solve. If you manage to solve the problem the bot will assign you a harder problem. If you need to skip the problem the bot will lower the difficulty.
            You can start a game by using the ;training start command. The bot will assign you a codeforces problem that you should solve. If you manage to solve the problem you can do ;training solved and the bot will assign you a problem that is 100 points higher rated. If you need editorial / external help or have no idea how to solve it you can do ;training skip. The bot will reduce the difficulty of the next problem by 100 points.
            You may end your training at any time with ;training end
            The game is available in the following modes:
            - infinite: Try to get as high as possible. You are allowed to skip any number of times.
            - survival: Seeking for some thrill? In this mode you only have 3 lives (you can skip 3 problems). How far will you get?
            - time trial: Still bored? Prepare for the ultimate challenge: In this mode you will only have limited time to solve each problem.
                          If you need to skip a problem or if you are too slow at solving the problem you will lose one of your 3 lives.
                          Available difficulty levels: timed15 (15 minutes for each problem), timed30 (30 minutes), timed60 (60 minutes)
                          You get some bonus time if you manage to solve a problem within the time limit.
            For further help on usage of a command do ;help training <command> (e.g. ;help training start)
        """
        await ctx.send_help(ctx.command)

    # User commands start here

    @training.command(brief='Start a training session',
                      usage='[rating] [infinite|survival|timed15|timed30|timed60]')
    @cf_common.user_guard(group='training')
    async def start(self, ctx, *args):
        """ Start your training session
            - Game modes:
              - infinite: Play the game in infinite mode (you can skip at any time) [DEFAULT]
              - survival: Challenge mode with only 3 skips available
              - timed15/timed30/timed60: Challenge mode similar to survival but u only have a limited time to solve your problem.
                                         Slow solves will also reduce your life by 1. Fast solves will increase available time for the next problem.
            - It is possible to change the start rating from 800 to any other valid rating
        """
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        # get cf handle
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        # get user submissions
        submissions = await cf.user.status(handle=handle)

        rating, mode = self._extractArgs(args)

        gamestate = Game(mode)

        # check if start of a new training is possible
        active = await self._getActiveTraining(ctx.author.id)
        self._validateTrainingStatus(ctx, rating, active)

        # Picking a new problem with a certain rating
        problem = await self._pickTrainingProblem(handle, rating, submissions, ctx.author.id)

        # assign new problem
        await self._startTrainingAndAssignProblem(ctx, handle, problem, gamestate)

    @training.command(brief='If you have solved your current problem it will assign a new one')
    @cf_common.user_guard(group='training')
    async def solved(self, ctx, *args):
        """ Use this command if you got AC on the training problem. If game continues the bot will assign a new problem.
        """

        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        # get cf handle
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        # get user submissions
        submissions = await cf.user.status(handle=handle)

        # check game running
        active = await self._getActiveTraining(ctx.author.id)
        self._checkTrainingActive(ctx, active)

        # check if solved
        finish_time = await self._checkIfSolved(ctx, active, handle, submissions)

        # game logic here
        _, issue_time, _, _, _, rating, _, _, _, _ = active
        gamestate = Game(active[6], active[7], active[8], active[9])
        duration = finish_time - issue_time
        success, newRating = gamestate.doSolved(rating, duration)

        # Picking a new problem with a certain rating
        problem = await self._pickTrainingProblem(handle, newRating, submissions, ctx.author.id)

        # Complete old problem
        await self._completeCurrentTrainingProblem(ctx, active, handle, finish_time, duration, gamestate, success)

        # Check if game ends here
        if await self._endTrainingIfDead(ctx, active, handle, gamestate):
            return

        # Assign new problem
        await self._assignNewTrainingProblem(ctx, active, handle, problem, gamestate)

    @training.command(brief='If you want to skip your current problem you can get a new one.')
    @cf_common.user_guard(group='training')
    async def skip(self, ctx):
        """ Use this command if you want to skip your current training problem. If not in infinite mode this will reduce your lives by 1.
        """
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        # get cf handle
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        # get user submissions
        submissions = await cf.user.status(handle=handle)

        # check game running
        active = await self._getActiveTraining(ctx.author.id)
        self._checkTrainingActive(ctx, active)

        # game logic here
        _, issue_time, _, _, _, rating, _, _, _, _ = active
        gamestate = Game(active[6], active[7], active[8], active[9])
        finish_time = datetime.datetime.now().timestamp()
        duration = finish_time - issue_time
        success, newRating = gamestate.doSkip(rating, duration)

        # Picking a new problem with a certain rating
        problem = await self._pickTrainingProblem(handle, newRating, submissions, ctx.author.id)

        # Complete old problem
        await self._completeCurrentTrainingProblem(ctx, active, handle, finish_time, duration, gamestate, success)

        # Check if game ends here
        if await self._endTrainingIfDead(ctx, active, handle, gamestate):
            return

        # Assign new problem
        await self._assignNewTrainingProblem(ctx, active, handle, problem, gamestate)

    @training.command(brief='End your training session.')
    @cf_common.user_guard(group='training')
    async def end(self, ctx):
        """ Use this command to end the current training session.
        """
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))

        # check game running
        active = await self._getActiveTraining(ctx.author.id)
        self._checkTrainingActive(ctx, active)

        # invalidate active problem and finish training
        _, issue_time, _, _, _, rating, _, _, _, _ = active
        gamestate = Game(active[6], active[7], active[8], active[9])
        finish_time = datetime.datetime.now().timestamp()
        duration = finish_time - issue_time
        success, newRating = gamestate.doFinish(rating, duration)

        # Complete old problem
        await self._completeCurrentTrainingProblem(ctx, active, handle, finish_time, duration, gamestate, success)

        # Check if game ends here // should trigger each time
        if await self._endTrainingIfDead(ctx, active, handle, gamestate):
            return

    @training.command(brief='Shows current status of your training session.', usage='[username]')
    async def status(self, ctx, member: discord.Member = None):
        """ Use this command to show the current status of your training session and the current assigned problem.
            If you don't have an active training this will show the stats of your latest training session.
            You can add the discord name of a user to get his status instead.
        """
        member = member or ctx.author
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(member.id),))

        # check game running
        active = await self._getActiveTraining(member.id)
        if active is not None:
            gamestate = Game(active[6], active[7], active[8], active[9])
            await self._postTrainingStatistics(ctx, active, handle, gamestate, False, False)
        else:
            latest = await self._getLatestTraining(member.id)
            if latest is None:
                raise TrainingCogError(
                    "You don't have an active or past training!")
            gamestate = Game(latest[6], latest[7], latest[8], latest[9])
            await self._postTrainingStatistics(ctx, latest, handle, gamestate, False, True)

    @training.command(brief="Show fastest training solves")
    async def fastest(self, ctx, *args):
        """Show a list of fastest solves within a training session for each rating."""
        res = cf_common.user_db.train_get_fastest_solves()

        rankings = []
        index = 0
        for user_id, rating, time in res:
            member = ctx.guild.get_member(int(user_id))
            handle = cf_common.user_db.get_handle(user_id, ctx.guild.id)
            user = cf_common.user_db.fetch_cf_user(handle)
            if user is None:
                continue
            user_rating = user.rating

            discord_handle = ""
            if member is not None:
                discord_handle = member.display_name

            rankings.append((rating, discord_handle, handle, user_rating, time))

        if not rankings:
            raise TrainingCogError('No one has completed a training challenge yet.')
        discord_file = get_fastest_solves_image(rankings)
        await ctx.send(file=discord_file)

    @training.command(brief='Set the training channel to the current channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)  # OK
    async def set_channel(self, ctx):
        """ Sets the training channel to the current channel.
        """
        cf_common.user_db.set_training_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success('Training channel saved successfully'))

    @training.command(brief='Get the training channel')
    async def get_channel(self, ctx):
        """ Gets the training channel.
        """
        channel_id = cf_common.user_db.get_training_channel(ctx.guild.id)
        channel = ctx.guild.get_channel(channel_id)
        if channel is None:
            raise TrainingCogError('There is no training channel')
        embed = discord_common.embed_success('Current training channel')
        embed.add_field(name='Channel', value=channel.mention)
        await ctx.send(embed=embed)

    @discord_common.send_error_if(TrainingCogError, cf_common.ResolveHandleError,
                                  cf_common.FilterError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Training(bot))

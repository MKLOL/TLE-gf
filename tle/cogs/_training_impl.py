"""Implementation mixin for the training cog.

Holds the non-command helper methods of ``Training`` so the cog file stays
small. This is a plain mixin (NOT a ``commands.Cog``); ``Training`` inherits
from it alongside ``commands.Cog``.
"""
import datetime
import random

import discord

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.cogs._training_helpers import (
    TrainingMode,
    TrainingResult,
    TrainingCogError,
    TrainingProblemStatus,
    _TRAINING_MIN_RATING_VALUE,
    _TRAINING_MAX_RATING_VALUE,
)


class TrainingImplMixin:
    def _checkIfCorrectChannel(self, ctx):
        training_channel_id = cf_common.user_db.get_training_channel(
            ctx.guild.id)
        if not training_channel_id or ctx.channel.id != training_channel_id:
            raise TrainingCogError(
                'You must use this command in training channel.')

    async def _getActiveTraining(self, user_id):
        active = cf_common.user_db.get_active_training(user_id)
        return active

    async def _getLatestTraining(self, user_id):
        latest = cf_common.user_db.get_latest_training(user_id)
        return latest

    def _extractArgs(self, args):
        mode = TrainingMode.NORMAL
        rating = 800
        unrecognizedArgs = []
        for arg in args:
            if arg.isdigit():
                rating = int(arg)
            elif arg == "infinite" or arg == "+infinite":
                mode = TrainingMode.NORMAL
            elif arg == "survival" or arg == "+survival":
                mode = TrainingMode.SURVIVAL
            elif arg == "timed15" or arg == "+timed15":
                mode = TrainingMode.TIMED15
            elif arg == "timed30" or arg == "+timed30":
                mode = TrainingMode.TIMED30
            elif arg == "timed60" or arg == "+timed60":
                mode = TrainingMode.TIMED60
            else:
                unrecognizedArgs.append(arg)
        if len(unrecognizedArgs) > 0:
            raise TrainingCogError(
                'Unrecognized arguments: {}'.format(' '.join(unrecognizedArgs)))
        return rating, mode

    def _getStatus(self, success):
        if success == TrainingResult.SOLVED:
            return TrainingProblemStatus.SOLVED
        if success == TrainingResult.TOOSLOW:
            return TrainingProblemStatus.SOLVED_TOO_SLOW
        if success == TrainingResult.SKIPPED:
            return TrainingProblemStatus.SKIPPED
        if success == TrainingResult.INVALIDATED:
            return TrainingProblemStatus.INVALIDATED

    def _getFormattedTimeleft(self, issue_time, time_left):
        if time_left is None:
            return 'Inf'
        now_time = datetime.datetime.now().timestamp()
        time_passed = now_time - issue_time
        if time_passed > time_left:
            return 'Time over'
        else:
            return cf_common.pretty_time_format(int(time_left - time_passed), shorten=True, always_seconds=True)

    def _validateTrainingStatus(self, ctx, rating, active):
        if rating is not None and rating % 100 != 0:
            raise TrainingCogError('Delta must be a multiple of 100.')
        if rating is not None and (rating < _TRAINING_MIN_RATING_VALUE or rating > _TRAINING_MAX_RATING_VALUE):
            raise TrainingCogError(
                f'Start rating must range from {_TRAINING_MIN_RATING_VALUE} to {_TRAINING_MAX_RATING_VALUE}.')

        if active is not None:
            _, _, name, contest_id, index, _, _, _, _, _ = active
            url = f'{cf.CONTEST_BASE_URL}{contest_id}/problem/{index}'
            raise TrainingCogError(
                f'You have an active training problem {name} at {url}')

    def _checkTrainingActive(self, ctx, active):
        if not active:
            raise TrainingCogError(
                'You do not have an active training. You can start one with ;training start')

    async def _pickTrainingProblem(self, handle, rating, submissions, user_id):
        solved = {sub.problem.name for sub in submissions}
        skips = cf_common.user_db.get_training_skips(user_id)
        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if (prob.rating == rating and
                        prob.name not in solved and
                        prob.name not in skips)]

        def check(problem):
            return (not cf_common.is_nonstandard_problem(problem) and
                    not cf_common.is_contest_writer(problem.contestId, handle))

        problems = list(filter(check, problems))
        # TODO: What happens to DB if this one triggers?
        if not problems:
            raise TrainingCogError(
                'No problem to assign. Start of training failed.')
        problems.sort(key=lambda problem: cf_common.cache2.contest_cache.get_contest(
            problem.contestId).startTimeSeconds)

        choice = max(random.randrange(len(problems)) for _ in range(5))
        return problems[choice]

    async def _checkIfSolved(self, ctx, active, handle, submissions):
        _, _, name, contest_id, index, _, _, _, _, _ = active
        ac = [sub for sub in submissions if sub.problem.name ==
              name and sub.verdict == 'OK']
        # order by creation time increasing
        ac.sort(key=lambda y: y[6])

        if len(ac) == 0:
            url = f'{cf.CONTEST_BASE_URL}{contest_id}/problem/{index}'
            raise TrainingCogError(
                f'You haven\'t completed your active training problem {name} at {url}')
        finish_time = int(ac[0].creationTimeSeconds)
        return finish_time

    async def _postProblemFinished(self, ctx, handle, name, contest_id, index, duration, gamestate, success, timeleft):
        if success == TrainingResult.INVALIDATED:
            return
        desc = ''
        text = ''
        color = 0x000000
        if success == TrainingResult.SOLVED:
            desc = f'{handle} solved training problem.'
            text = 'Problem solved.'
            color = 0x008000
        if success == TrainingResult.TOOSLOW:
            timeDiffFormatted = cf_common.pretty_time_format(
                duration-timeleft, shorten=True, always_seconds=True)
            desc = f'{handle} solved training problem but was {timeDiffFormatted} too slow.'
            text = 'Problem solved but not fast enough.'
            color = 0xf98e1b
        if success == TrainingResult.SKIPPED:
            desc = f'{handle} skipped training problem'
            text = 'Problem skipped.'
            color = 0xff3030

        url = f'{cf.CONTEST_BASE_URL}{contest_id}/problem/{index}'
        title = f'{index}. {name}'
        durationFormatted = cf_common.pretty_time_format(
            duration, shorten=True, always_seconds=True)
        embed = discord.Embed(
            title=title, description=desc, url=url, color=color)
        embed.add_field(name='Score', value=gamestate.score)
        embed.add_field(name='Time taken:', value=durationFormatted)
        embed.add_field(
            name='Lives left:', value=gamestate.lives if gamestate.lives is not None else 'Inf')
        await ctx.send(text, embed=embed)

    async def _postProblem(self, ctx, handle, problemName, problemIndex, problemContestId, problemRating, issue_time, gamestate, new: bool = True):
        url = f'{cf.CONTEST_BASE_URL}{problemContestId}/problem/{problemIndex}'
        title = f'{problemIndex}. {problemName}'
        desc = cf_common.cache2.contest_cache.get_contest(
            problemContestId).name
        embed = discord.Embed(title=title, url=url,
                              description=desc, color=0x008000)
        embed.add_field(name='Rating', value=problemRating)
        embed.add_field(
            name='Lives left:', value=gamestate.lives if gamestate.lives is not None else 'Inf')
        timeleftFormatted = self._getFormattedTimeleft(
            issue_time, gamestate.timeleft)
        embed.add_field(name='Time left:', value=timeleftFormatted)

        prefix = 'New' if new else 'Current'
        await ctx.send(f'{prefix} training problem for `{handle}`', embed=embed)

    async def _postTrainingStatistics(self, ctx, active, handle, gamestate, finished=True, past=False):
        training_id = active[0]
        numSkips = cf_common.user_db.train_get_num_skips(training_id)
        numSolves = cf_common.user_db.train_get_num_solves(training_id)
        numSlowSolves = cf_common.user_db.train_get_num_slow_solves(
            training_id)
        maxRating = cf_common.user_db.train_get_max_rating(training_id)
        startRating = cf_common.user_db.train_get_start_rating(training_id)

        text = ''
        title = f'Current training session of `{handle}`'
        color = 0x000080
        if past:
            text = 'You don\'t have an active training session.'
            title = f'Latest training session of `{handle}`'
            color = 0x000000
        if finished:
            title = f'Game over! Training session of `{handle}` ended.'
            color = 0x000040
        embed = discord.Embed(title=title, color=color)
        embed.add_field(name='Game mode',
                        value=gamestate._getModeStr(), inline=False)
        embed.add_field(name='Start rating', value=startRating, inline=True)
        embed.add_field(name='Highest solve', value=maxRating, inline=False)
        embed.add_field(name='Solves', value=numSolves, inline=True)
        embed.add_field(name='Slow solves', value=numSlowSolves, inline=True)
        embed.add_field(name='Skips', value=numSkips, inline=True)
        embed.add_field(name='Score', value=gamestate.score, inline=False)
        await ctx.send(text, embed=embed)
        if not finished and not past:
            _, issue_time, name, contest_id, index, rating, _, _, _, _ = active
            await self._postProblem(ctx, handle, name, index, contest_id, rating, issue_time, gamestate, False)

    async def _startTrainingAndAssignProblem(self, ctx, handle, problem, gamestate):
        # The caller of this function is responsible for calling `_validate_training_status` first.
        user_id = ctx.author.id
        issue_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.new_training(
            user_id, issue_time, problem, gamestate.mode, gamestate.score, gamestate.lives, gamestate.timeleft)
        if rc != 1:
            raise TrainingCogError(
                'Your training has already been added to the database!')

        active = await self._getActiveTraining(user_id)
        await self._postTrainingStatistics(ctx, active, handle, gamestate, False, False)

    async def _assignNewTrainingProblem(self, ctx, active, handle, problem, gamestate):
        training_id, _, _, _, _, _, _, _, _, _ = active
        issue_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.assign_training_problem(
            training_id, issue_time, problem)
        if rc == 1:
            await self._postProblem(ctx, handle, problem.name, problem.index, problem.contestId, problem.rating, issue_time, gamestate)
        if rc == -1:
            raise TrainingCogError(
                'Your training problem has already been added to the database!')

    async def _completeCurrentTrainingProblem(self, ctx, active, handle, finish_time, duration, gamestate, success):
        training_id, _, name, contest_id, index, _, _, _, _, timeleft = active
        status = self._getStatus(success)
        rc = cf_common.user_db.end_current_training_problem(
            training_id, finish_time, status, gamestate.score, gamestate.lives, gamestate.timeleft)
        if rc == 1:
            await self._postProblemFinished(ctx, handle, name, contest_id, index, duration, gamestate, success, timeleft)
        if rc == -1:
            raise TrainingCogError(
                "You already completed your training problem!")
        if rc == -2:
            raise TrainingCogError(
                'You don\'t have an active training session!')

    async def _finishCurrentTraining(self, ctx, active):
        training_id, _, _, _, _, _, _, _, _, _ = active

        rc = cf_common.user_db.finish_training(training_id)
        if rc == -1:
            raise TrainingCogError("You already ended your training!")

    async def _endTrainingIfDead(self, ctx, active, handle, gamestate):
        if not gamestate.alive:
            # show death message
            await self._finishCurrentTraining(ctx, active)
            # end game and post results
            await self._postTrainingStatistics(ctx, active, handle, gamestate, True, False)
            return True
        return False

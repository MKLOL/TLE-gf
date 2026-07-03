"""Gitgud-related implementation mixin for the codeforces cog.

Holds the heavy command bodies for upsolve/gimme/gitgud/gitlog/nogudlog/gotgud/
nogud plus the shared gitgud helpers. This is a plain mixin (NOT a
``commands.Cog``); ``Codeforces`` inherits from it alongside ``commands.Cog``.
"""
import datetime
import random

import discord

from tle import constants
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.db.user_db_conn import Gitgud
from tle.util import paginator
from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudTagPenaltyDelta,
    _gitgudPenalisedTagCount,
    CodeforcesCogError,
    _GITGUD_NO_SKIP_TIME,
    _ONE_WEEK_DURATION,
    _GITGUD_MORE_POINTS_START_TIME,
    _GITGUD_COIN_MULTIPLIER,
)


class CodeforcesGitgudMixin:
    # more points seasons start at April 1st 2023 (timestamp: 1680300000) and is only active in the last 7 days of the month

    # @@@ add issue and finish time constraint (both times need to be within the more points range)
    def _check_more_points_active(self, now_time, start_time, end_time):
        morePointsActive = False
        morePointsTime = end_time - _ONE_WEEK_DURATION
        if start_time >= _GITGUD_MORE_POINTS_START_TIME and now_time >= morePointsTime:
            morePointsActive = True
        return morePointsActive

    def _award_gitgud_coins(self, ctx, user_id, score):
        """Credit the betting wallet with ``_GITGUD_COIN_MULTIPLIER`` coins per
        base gitgud point. The rate is a flat 5x of the *base* score and never
        gets the end-of-month doubling the monthly ranklist points do. Returns
        the coins awarded, or None when there's no guild (e.g. a DM) so the
        caller can omit the wallet line."""
        guild = ctx.guild
        if guild is None:
            return None
        coins = _GITGUD_COIN_MULTIPLIER * score
        start_balance = (constants.BET_START_BALANCE
                         + cf_common.user_db.bet_get_start_bonus(guild.id))
        cf_common.user_db.bet_adjust_balance(
            guild.id, user_id, coins, start_balance,
            actor_id=user_id, action='gitgud', note=f'score={score}')
        return coins

    async def _validate_gitgud_status(self, ctx):
        user_id = ctx.message.author.id
        active = cf_common.user_db.check_challenge(user_id)
        if active is not None:
            _, _, name, contest_id, index, _ = active
            url = f'{cf.CONTEST_BASE_URL}{contest_id}/problem/{index}'
            raise CodeforcesCogError(f'You have an active challenge {name} at {url}')

    async def _gitgud(self, ctx, handle, problem, delta, hidden):
        # The caller of this function is responsible for calling `_validate_gitgud_status` first.
        user_id = ctx.author.id

        issue_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.new_challenge(user_id, issue_time, problem, delta)
        if rc != 1:
            raise CodeforcesCogError('Your challenge has already been added to the database!')

        # Calculate time range of given month (d=) or current month
        now = datetime.datetime.now()
        start_time, end_time = cf_common.get_start_and_end_of_month(now)
        now_time = int(now.timestamp())
        # more points seasons start at April 1st 2023 (timestamp: 1680300000) and is only active in the last 7 days of the month
        morePointsActive = self._check_more_points_active(now_time, start_time, end_time)

        points = _calculateGitgudScoreForDelta(delta)
        monthlypoints = 2 * points if morePointsActive else points

        title = f'{problem.index}. {problem.name}'
        desc = cf_common.cache2.contest_cache.get_contest(problem.contestId).name
        ratingStr = problem.rating if not hidden else '||'+str(problem.rating)+'||'
        pointsStr = points if not hidden else '||'+str(points)+'||'
        monthlyPointsStr = monthlypoints if not hidden else '||'+str(monthlypoints)+'||'
        embed = discord.Embed(title=title, url=problem.url, description=desc)
        embed.add_field(name='Rating', value=ratingStr)
        embed.add_field(name='Alltime points', value=pointsStr)
        embed.add_field(name='Monthly points', value=monthlyPointsStr)
        await ctx.send(f'Challenge problem for `{handle}`', embed=embed)

    async def _upsolve_impl(self, ctx, choice):
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        user = cf_common.user_db.fetch_cf_user(handle)
        rating = round(user.effective_rating, -2)
        rating = max(1100, rating)
        rating = min(3000, rating)
        resp = await cf.user.rating(handle=handle)
        contests = {change.contestId for change in resp}
        submissions = await cf.user.status(handle=handle)
        solved = {sub.problem.name for sub in submissions if sub.verdict == 'OK'}
        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if prob.name not in solved and prob.contestId in contests]

        if not problems:
            raise CodeforcesCogError('Problems not found within the search parameters')

        problems.sort(key=lambda problem: problem.rating)

        if choice > 0 and choice <= len(problems):
            await self._validate_gitgud_status(ctx)
            problem = problems[choice - 1]
            await self._gitgud(ctx, handle, problem, problem.rating - rating, False)
        else:
            problems = problems[:500]

            def make_line(i, prob):
                data = (f'{i + 1}: [{prob.name}]({prob.url}) [{prob.rating}]')
                return data

            def make_page(chunk, pi, num):
                title = f'Select a problem to upsolve (1-{num}):'
                msg = '\n'.join(make_line(10*pi+i, prob) for i, prob in enumerate(chunk))
                embed = discord_common.cf_color_embed(description=msg)
                return title, embed

            pages = [make_page(chunk, pi, len(problems)) for pi, chunk in enumerate(paginator.chunkify(problems, 10))]
            paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _gimme_impl(self, ctx, args):
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        rating = round(cf_common.user_db.fetch_cf_user(handle).effective_rating, -2)
        tags = cf_common.parse_tags(args, prefix='+')
        bantags = cf_common.parse_tags(args, prefix='~')

        srating = round(cf_common.user_db.fetch_cf_user(handle).effective_rating, -2)
        erating = srating
        dlo,dhi = cf_common.parse_daterange(args)
        for arg in args:
            if arg[0:3].isdigit():
                ratings = arg.split("-")
                srating = int(ratings[0])
                if (len(ratings) > 1):
                    erating = int(ratings[1])
                else:
                    erating = srating

        submissions = await cf.user.status(handle=handle)
        solved = {sub.problem.name for sub in submissions if sub.verdict == 'OK'}

        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if prob.rating >= srating and prob.rating <= erating and prob.name not in solved
                    and not cf_common.is_contest_writer(prob.contestId, handle)
                    and prob.matches_all_tags(tags)
                    and not prob.matches_any_tag(bantags)
                    and dlo <= cf_common.cache2.contest_cache.get_contest(prob.contestId).startTimeSeconds < dhi]

        if not problems:
            raise CodeforcesCogError('Problems not found within the search parameters')

        problems.sort(key=lambda problem: cf_common.cache2.contest_cache.get_contest(
            problem.contestId).startTimeSeconds)

        choice = max([random.randrange(len(problems)) for _ in range(3)])
        problem = problems[choice]

        title = f'{problem.index}. {problem.name}'
        desc = cf_common.cache2.contest_cache.get_contest(problem.contestId).name
        embed = discord.Embed(title=title, url=problem.url, description=desc)
        ratingStr = problem.rating if srating == erating else '||'+str(problem.rating)+'||'
        embed.add_field(name='Rating', value=ratingStr)
        if tags:
            tagslist = ', '.join(problem.get_matched_tags(tags))
            embed.add_field(name='Matched tags', value=tagslist)
        await ctx.send(f'Recommended problem for `{handle}`', embed=embed)

    async def _gitgud_impl(self, ctx, args):
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        user = cf_common.user_db.fetch_cf_user(handle)
        user_rating = round(user.effective_rating, -2)
        user_rating = max(800, user_rating)
        user_rating = min(3500, user_rating)
        rating = user_rating
        rating = max(1100, rating)
        rating = min(3000, rating)
        submissions = await cf.user.status(handle=handle)
        solved = {sub.problem.name for sub in submissions}
        noguds = cf_common.user_db.get_noguds(ctx.message.author.id)
        tags = cf_common.parse_tags(args, prefix='+')
        bantags = cf_common.parse_tags(args, prefix='~')
        srating = user_rating
        erating = user_rating
        hidden = False
        for arg in args:
            if arg[0] == "-":
                raise CodeforcesCogError('Wrong rating requested. Remember gitgud now uses rating (800-3500) instead of delta.')
            if arg[0:3].isdigit():
                ratings = arg.split("-")
                srating = int(ratings[0])
                if (len(ratings) > 1):
                    erating = int(ratings[1])
                    hidden = True
                else:
                    erating = srating

        if erating < 800 or srating > 3500:
            raise CodeforcesCogError('Wrong rating requested. Remember gitgud now uses rating (800-3500) instead of delta.')

        await self._validate_gitgud_status(ctx)

        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if prob.rating >= srating and prob.rating <= erating
                    and prob.name not in solved
                    and prob.name not in noguds
                    and prob.matches_all_tags(tags)
                    and not prob.matches_any_tag(bantags)]


        def check(problem):
            return (not cf_common.is_nonstandard_problem(problem) and
                    not cf_common.is_contest_writer(problem.contestId, handle))

        problems = list(filter(check, problems))
        if not problems:
            raise CodeforcesCogError('No problem to assign')

        problems.sort(key=lambda problem: cf_common.cache2.contest_cache.get_contest(
            problem.contestId).startTimeSeconds)

        choice = max(random.randrange(len(problems)) for _ in range(5))

        # Penalised tags divide points by (tag count + 1), rounded up.
        # Hardening division filters such as +div1 and ~div3/~div4/~edu are
        # exempt; other requested tags and bans count (see
        # _gitgudPenalisedTagCount).
        delta = problems[choice].rating - rating
        delta = _gitgudTagPenaltyDelta(delta, _gitgudPenalisedTagCount(tags, bantags))
        await self._gitgud(ctx, handle, problems[choice], delta, hidden)

    async def _gitlog_impl(self, ctx, member):
        def make_line(entry):
            issue, finish, name, contest, index, delta, status = entry
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            line = f'[{name}]({problem.url})\N{EN SPACE}[{problem.rating}]'
            if finish:
                time_str = cf_common.days_ago(finish)
                points = f'{_calculateGitgudScoreForDelta(delta):+}'
                line += f'\N{EN SPACE}{time_str}\N{EN SPACE}[{points}]'
            return line

        def make_page(chunk,score):
            message = discord.utils.escape_mentions(f'Gitgud log for {member.display_name} (total score: {score})')
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        member = member or ctx.author
        data = cf_common.user_db.gitlog(member.id)
        if not data:
            raise CodeforcesCogError(f'{member.mention} has no gitgud history.')
        score = 0
        for entry in data:
            issue, finish, name, contest, index, delta, status = entry
            if finish:
                score+=_calculateGitgudScoreForDelta(delta)


        pages = [make_page(chunk, score) for chunk in paginator.chunkify(data, 10)]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _nogudlog_impl(self, ctx, member):
        def make_line(entry):
            issue, finish, name, contest, index, delta, status = entry
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            line = f'[{name}]({problem.url})\N{EN SPACE}[{problem.rating}]'
            if finish:
                time_str = cf_common.days_ago(finish)
                points = f'{_calculateGitgudScoreForDelta(delta):+}'
                line += f'\N{EN SPACE}{time_str}\N{EN SPACE}[{points}]'
            return line

        def make_page(chunk):
            message = discord.utils.escape_mentions(f'Nogud log for {member.display_name}')
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        member = member or ctx.author
        data = cf_common.user_db.gitlog(member.id)
        if not data:
            raise CodeforcesCogError(f'{member.mention} has no gitgud history.')

        data = [entry for entry in data if entry[1] is None]

        pages = [make_page(chunk) for chunk in paginator.chunkify(data, 10)]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _gotgud_impl(self, ctx):
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        user_id = ctx.message.author.id
        active = cf_common.user_db.check_challenge(user_id)
        if not active:
            raise CodeforcesCogError(f'You do not have an active challenge')

        submissions = await cf.user.status(handle=handle)
        solved = {sub.problem.name for sub in submissions if sub.verdict == 'OK'}

        challenge_id, issue_time, name, contestId, index, delta = active
        if not name in solved:
            raise CodeforcesCogError('You haven\'t completed your challenge.')

        score = _calculateGitgudScoreForDelta(delta)
        finish_time = int(datetime.datetime.now().timestamp())
        rc = cf_common.user_db.complete_challenge(user_id, challenge_id, finish_time, score)

        now = datetime.datetime.now()
        start_time, end_time = cf_common.get_start_and_end_of_month(now)
        now_time = int(now.timestamp())

        morePointsActive = self._check_more_points_active(now_time, start_time, end_time)

        monthlyPoints = 2 * score if morePointsActive else score

        if rc == 1:
            duration = cf_common.pretty_time_format(finish_time - issue_time)
            msg = (f'Challenge completed in {duration}. {handle} gained {score} '
                   f'alltime ranklist points and {monthlyPoints} monthly ranklist points.')
            # Coins are always credited to the betting wallet, but we only
            # mention them to users who are already playing the betting game
            # (have placed at least one bet) — same bar as showing up on the
            # ;bet leaderboard. Everyone else just banks them silently.
            coins = self._award_gitgud_coins(ctx, user_id, score)
            if coins is not None and \
                    cf_common.user_db.bet_has_wagered(ctx.guild.id, user_id):
                msg += f' You also earned {coins} 🪙.'
            await ctx.send(msg)
        else:
            await ctx.send('You have already claimed your points')

    async def _nogud_impl(self, ctx):
        await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        user_id = ctx.message.author.id
        active = cf_common.user_db.check_challenge(user_id)
        if not active:
            raise CodeforcesCogError(f'You do not have an active challenge')

        challenge_id, issue_time, name, contestId, index, delta = active
        finish_time = int(datetime.datetime.now().timestamp())
        if finish_time - issue_time < _GITGUD_NO_SKIP_TIME:
            skip_time = cf_common.pretty_time_format(issue_time + _GITGUD_NO_SKIP_TIME - finish_time)
            await ctx.send(f'Think more. You can skip your challenge in {skip_time}.')
            return
        cf_common.user_db.skip_challenge(user_id, challenge_id, Gitgud.NOGUD)
        await ctx.send(f'Challenge skipped.')

    async def _force_nogud_impl(self, ctx, member):
        active = cf_common.user_db.check_challenge(member.id)
        if not active:
            await ctx.send(f'No active challenge found for user `{member.display_name}`.')
            return
        rc = cf_common.user_db.skip_challenge(member.id, active[0], Gitgud.FORCED_NOGUD)
        if rc == 1:
            await ctx.send(f'Challenge skip forced.')
        else:
            await ctx.send(f'Failed to force challenge skip.')

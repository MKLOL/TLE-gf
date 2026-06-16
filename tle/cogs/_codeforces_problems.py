"""Problem/contest implementation mixin for the codeforces cog.

Holds the heavy command bodies for stalk/mashup/vc/fullsolve/teamrate. This is a
plain mixin (NOT a ``commands.Cog``); ``Codeforces`` inherits from it alongside
``commands.Cog``.
"""
import random
from collections import defaultdict

import discord

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import cache_system2
from tle.cogs._codeforces_helpers import CodeforcesCogError, composeRatings


class CodeforcesProblemsMixin:
    async def _stalk_impl(self, ctx, args):
        (hardest,), args = cf_common.filter_flags(args, ['+hardest'])
        filt = cf_common.SubFilter(False)
        args = filt.parse(args)
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
        # +rated: fetch rating change history to know which contests were rated
        if filt.only_rated:
            for handle in handles:
                try:
                    changes = await cf.user.rating(handle=handle)
                    filt.rated_contest_ids_by_handle[handle.lower()] = {
                        rc.contestId for rc in changes
                    }
                except cf.HandleNotFoundError:
                    filt.rated_contest_ids_by_handle[handle.lower()] = set()
        submissions = [await cf.user.status(handle=handle) for handle in handles]
        submissions = [sub for subs in submissions for sub in subs]
        submissions = filt.filter_subs(submissions)

        if not submissions:
            raise CodeforcesCogError('Submissions not found within the search parameters')

        if hardest:
            submissions.sort(key=lambda sub: (sub.problem.rating or 0, sub.creationTimeSeconds), reverse=True)
        else:
            submissions.sort(key=lambda sub: sub.creationTimeSeconds, reverse=True)

        handlesWithUrl = ['`{}` (https://codeforces.com/profile/{})'.format(handle,handle) for handle in handles]

        def make_line(sub):
            data = (f'[{sub.problem.name}]({sub.problem.url})',
                    f'[{sub.problem.rating if sub.problem.rating else "?"}]',
                    f'({cf_common.days_ago(sub.creationTimeSeconds)})')
            return '\N{EN SPACE}'.join(data)

        def make_page(chunk):

            title = '{} solved problems by {}'.format('Hardest' if hardest else 'Recently',
                                                        ', '.join(handlesWithUrl))
            hist_str = '\n'.join(make_line(sub) for sub in chunk)
            embed = discord_common.cf_color_embed(description=hist_str)
            return title, embed

        pages = [make_page(chunk) for chunk in paginator.chunkify(submissions[:100], 10)]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _mashup_impl(self, ctx, args):
        delta = 100
        handles = [arg for arg in args if arg[0] not in '+~?']
        tags = cf_common.parse_tags(args, prefix='+')
        bantags = cf_common.parse_tags(args, prefix='~')
        deltaStr = [arg[1:] for arg in args if arg[0] == '?' and len(arg) > 1]
        if len(deltaStr) > 1:
            raise CodeforcesCogError('Only one delta argument is allowed')
        if len(deltaStr) == 1:
            try:
                delta += round(int(deltaStr[0]), -2)
            except ValueError:
                raise CodeforcesCogError('delta could not be interpreted as number')

        handles = handles or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
        resp = [await cf.user.status(handle=handle) for handle in handles]
        submissions = [sub for user in resp for sub in user]
        solved = {sub.problem.name for sub in submissions}
        info = await cf.user.info(handles=handles)
        rating = int(round(sum(user.effective_rating for user in info) / len(handles), -2))
        rating += delta
        rating = max(800, rating)
        rating = min(3500, rating)
        problems = [prob for prob in cf_common.cache2.problem_cache.problems
                    if abs(prob.rating - rating) <= 300 and prob.name not in solved
                    and not any(cf_common.is_contest_writer(prob.contestId, handle) for handle in handles)
                    and not cf_common.is_nonstandard_problem(prob)
                    and prob.matches_all_tags(tags)
                    and not prob.matches_any_tag(bantags)]

        if len(problems) < 4:
            raise CodeforcesCogError('Problems not found within the search parameters')

        problems.sort(key=lambda problem: cf_common.cache2.contest_cache.get_contest(
            problem.contestId).startTimeSeconds)

        choices = []
        for i in range(4):
            k = max(random.randrange(len(problems) - i) for _ in range(2))
            for c in choices:
                if k >= c:
                    k += 1
            choices.append(k)
            choices.sort()

        problems = sorted([problems[k] for k in choices], key=lambda problem: problem.rating)
        msg = '\n'.join(f'{"ABCD"[i]}: [{p.name}]({p.url}) [{p.rating}]' for i, p in enumerate(problems))
        str_handles = '`, `'.join(handles)
        embed = discord_common.cf_color_embed(description=msg)
        await ctx.send(f'Mashup contest for `{str_handles}`', embed=embed)

    async def _vc_impl(self, ctx, args):
        markers = [x for x in args if x[0] == '+']
        handles = [x for x in args if x[0] != '+'] or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles, maxcnt=25)
        info = await cf.user.info(handles=handles)
        contests = cf_common.cache2.contest_cache.get_contests_in_phase('FINISHED')

        if not markers:
            divr = sum(user.effective_rating for user in info) / len(handles)
            div1_indicators = ['div1', 'global', 'avito', 'goodbye', 'hello']
            markers = ['div3'] if divr < 1600 else ['div2'] if divr < 2100 else div1_indicators

        recommendations = {contest.id for contest in contests if
                           contest.matches(markers) and
                           not cf_common.is_nonstandard_contest(contest) and
                           not any(cf_common.is_contest_writer(contest.id, handle)
                                       for handle in handles)}

        # Discard contests in which user has non-CE submissions.
        visited_contests = await cf_common.get_visited_contests(handles)
        recommendations -= visited_contests

        if not recommendations:
            raise CodeforcesCogError('Unable to recommend a contest')

        recommendations = list(recommendations)
        recommendations.sort(key=lambda contest: cf_common.cache2.contest_cache.get_contest(contest).startTimeSeconds, reverse=True)
        contests = [cf_common.cache2.contest_cache.get_contest(contest_id) for contest_id in recommendations[:25]]

        def make_line(c):
            return f'[{c.name}]({c.url}) {cf_common.pretty_time_format(c.durationSeconds)}'

        def make_page(chunk):
            str_handles = '`, `'.join(handles)
            message = f'Recommended contest(s) for `{str_handles}`'
            vc_str = '\n'.join(make_line(contest) for contest in chunk)
            embed = discord_common.cf_color_embed(description=vc_str)
            return message, embed

        pages = [make_page(chunk) for chunk in paginator.chunkify(contests, 5)]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _fullsolve_impl(self, ctx, args):
        handle, = await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id),))
        tags = [x for x in args if x[0] == '+']

        problem_to_contests = cf_common.cache2.problemset_cache.problem_to_contests
        contests = [contest for contest in cf_common.cache2.contest_cache.get_contests_in_phase('FINISHED')
                    if (not tags or contest.matches(tags)) and not cf_common.is_nonstandard_contest(contest)]

        # subs_by_contest_id contains contest_id mapped to [list of problem.name]
        subs_by_contest_id = defaultdict(set)
        for sub in await cf.user.status(handle=handle):
            if sub.verdict == 'OK':
                try:
                    contest = cf_common.cache2.contest_cache.get_contest(sub.problem.contestId)
                    problem_id = (sub.problem.name, contest.startTimeSeconds)
                    for contestId in problem_to_contests[problem_id]:
                        subs_by_contest_id[contestId].add(sub.problem.name)
                except cache_system2.ContestNotFound:
                    pass

        contest_unsolved_pairs = []
        for contest in contests:
            num_solved = len(subs_by_contest_id[contest.id])
            try:
                num_problems = len(cf_common.cache2.problemset_cache.get_problemset(contest.id))
                if 0 < num_solved < num_problems:
                    contest_unsolved_pairs.append((contest, num_solved, num_problems))
            except cache_system2.ProblemsetNotCached:
                # In case of recent contents or cetain bugged contests
                pass

        contest_unsolved_pairs.sort(key=lambda p: (p[2] - p[1], -p[0].startTimeSeconds))

        if not contest_unsolved_pairs:
            raise CodeforcesCogError(f'`{handle}` has no contests to fullsolve :confetti_ball:')

        def make_line(entry):
            contest, solved, total = entry
            return f'[{contest.name}]({contest.url})\N{EN SPACE}[{solved}/{total}]'

        def make_page(chunk):
            message = f'Fullsolve list for `{handle}`'
            full_solve_list = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=full_solve_list)
            return message, embed

        pages = [make_page(chunk) for chunk in paginator.chunkify(contest_unsolved_pairs, 10)]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    async def _teamrate_impl(self, ctx, args):
        (is_entire_server, peak), handles = cf_common.filter_flags(args, ['+server', '+peak'])
        handles = handles or ('!' + str(ctx.author.id),)

        def rating(user):
            return user.maxRating if peak else user.rating

        if is_entire_server:
            res = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
            ratings = [(rating(user), 1) for user_id, user in res if user.rating is not None]
            user_str = '+server'
        else:
            def normalize(x):
                return [i.lower() for i in x]
            handle_counts = {}
            parsed_handles = []
            for i in handles:
                parse_str = normalize(i.split('*'))
                if len(parse_str) > 1:
                    try:
                        handle_counts[parse_str[0]] = int(parse_str[1])
                    except ValueError:
                        raise CodeforcesCogError("Can't multiply by non-integer")
                else:
                    handle_counts[parse_str[0]] = 1
                parsed_handles.append(parse_str[0])

            cf_handles = await cf_common.resolve_handles(ctx, self.converter, parsed_handles, mincnt=1, maxcnt=1000)
            cf_handles = normalize(cf_handles)
            cf_to_original = {a: b for a, b in zip(cf_handles, parsed_handles)}
            original_to_cf = {a: b for a, b in zip(parsed_handles, cf_handles)}
            users = await cf.user.info(handles=cf_handles)
            user_strs = []
            for a, b in handle_counts.items():
                if b > 1:
                    user_strs.append(f'{original_to_cf[a]}*{b}')
                elif b == 1:
                    user_strs.append(original_to_cf[a])
                elif b <= 0:
                    raise CodeforcesCogError('How can you have nonpositive members in team?')

            user_str = ', '.join(user_strs)
            ratings = [(rating(user), handle_counts[cf_to_original[user.handle.lower()]])
                       for user in users if user.rating]

        if len(ratings) == 0:
            raise CodeforcesCogError("No CF usernames with ratings passed in.")

        left = -100.0
        right = 10000.0
        teamRating = composeRatings(left, right, ratings)
        embed = discord.Embed(title=user_str, description=teamRating, color=cf.rating2rank(teamRating).color_embed)
        await ctx.send(embed = embed)

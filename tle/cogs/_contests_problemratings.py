from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import discord_common
from tle.util import table

from tle.cogs._contests_helpers import _render_problemratings_image


class ProblemRatingsMixin:
    """Mixin holding the problem-rating estimation commands and the heavy
    estimation computation."""

    @commands.command(brief='Estimation of contest problem ratings', aliases=['probrat'], usage='contest_id')
    async def problemratings(self, ctx, contest_id: int):
        """Estimation of contest problem ratings
        """
        title, url, indices, official_ratings, predicted, from_cache = (
            await self._compute_problem_ratings(ctx, contest_id))
        # Discord shrinks an embed to its title's rendered width and wraps the
        # codeblock if it's wider. Padding the title with Hangul Filler (U+3164)
        # keeps the embed wide enough for the (~25-char) table. NBSP gets
        # stripped by Discord's whitespace normalization; Hangul Filler is
        # treated as a normal character and survives.
        # Hangul Filler is roughly full-width (~1.5x a normal title char),
        # so each one widens the embed by about 1.5 chars worth.
        # Visually-target the embed to ~45 normal-char widths.
        deficit = max(0, 45 - len(title))
        title = title + 'ㅤ' * (deficit * 2 // 3)
        table_pages = self._format_problemratings_table_pages(
            indices, official_ratings, predicted, from_cache=from_cache)
        await discord_common.send_paginated_embeds(ctx, table_pages, title=title, url=url)

    @commands.command(brief='Estimation of contest problem ratings (image)',
                      aliases=['probratimg'], usage='contest_id')
    async def problemratingsimg(self, ctx, contest_id: int):
        """Same as ;probrat but renders as an image (no codeblock alignment issues).
        """
        title, url, indices, official_ratings, predicted, from_cache = (
            await self._compute_problem_ratings(ctx, contest_id))
        image_file = _render_problemratings_image(
            title, indices, official_ratings, predicted, from_cache=from_cache)
        await ctx.send(content=f'<{url}>', file=image_file)

    async def _compute_problem_ratings(self, ctx, contest_id):
        await ctx.send('This will take a while')
        contests = await cf.contest.list()
        reqcontest = [contest for contest in contests if contest.id == contest_id]
        combined = [contest for contest in contests if reqcontest[0].startTimeSeconds == contest.startTimeSeconds]

        # get ranklist of all contests in separate lists
        # get rating_changes of all contests in separate lists
        # for each problem name of original contest
            # find in each ranklist the handles and ratings that had a chance to do the problem
            # calculate rating from these values

        problems = []
        ranklists = []
        rating_cache = dict()
        for contest in combined:
            _, problem, ranklist = await cf.contest.standings(contest_id=contest.id)
            problems.append(problem)
            ranklists.append(ranklist)

            if contest.id == contest_id:
                officialRatings = [prob.rating for prob in problem]
                indicies = [prob.index for prob in problem]
                problemNames = [prob.name for prob in problem]

            #build ratingCache that has all old_rating for all contestants
            try:
                rating_change = await cf.contest.ratingChanges(contest_id=contest.id)
            except cf.RatingChangesUnavailableError as e:
                rating_change = []
            from_cache = False
            if len(rating_change) == 0:
                # get rating of contestants from cache
                # we want to have the rating before the contest we query for
                from_cache = True
                cached_ratings = await cf_common.cache2.rating_changes_cache.get_all_ratings_before_timestamp(reqcontest[0].startTimeSeconds)
                for row in ranklist:
                    member = row.party.members[0].handle
                    # members not in cache are considered new (Unrated)
                    if member in cached_ratings:
                        rating_cache[member] = cached_ratings[member].newRating
                    else:
                        rating_cache[member] = 0
            else:
                for change in rating_change:
                    rating_cache[change.handle] = change.oldRating

        def calculateDifficulty(ratings, solved):
            ans = -1000

            def calcProb(dif):
                prob = 1
                d = 0
                for (r, s) in zip(ratings, solved):
                    p = 1/(1+10**((dif-r)/400))
                    d += p
                    if s:
                        d -= 1
                    prob *= p if s else (1-p)
                return d > 0 and prob < 0.95
            jump = 4096
            while jump >= 1:
                if calcProb(ans+jump):
                    ans += jump
                jump /= 2
            ans = round(ans+1)
            return ans

        predicted = []
        for name in problemNames:
            ratings = []
            solves = []

            for i in range(len(problems)):
                #get index of name in problem list of each contest
                idx = -1
                for j in range(len(problems[i])):
                    if problems[i][j].name == name:
                        idx = j
                if idx == -1: continue
                for row in ranklists[i]:
                    member = row.party.members[0].handle
                    if member in rating_cache:
                        solves.append(min(row.problemResults[idx].points, 1))
                        ratings.append(rating_cache[member])
            predicted.append(calculateDifficulty(ratings, solves))

        url = f'{cf.CONTEST_BASE_URL}{contest_id}'
        title = reqcontest[0].name
        return title, url, indicies, officialRatings, predicted, from_cache

    @staticmethod
    def _format_problemratings_table_pages(indices, official_ratings, predicted, *, from_cache):
        style = table.Style('{:<}  {:>}  {:>}')
        header = ('#', 'Official', 'Predicted (C)' if from_cache else 'Predicted')
        rows = [
            (index, official_ratings[i], predicted[i])
            for i, index in enumerate(indices)
        ]
        return table.format_table_pages(style, header, rows, flexible_cols=(0,))

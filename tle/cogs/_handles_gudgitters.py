import datetime

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.cogs import codeforces as cfc

from tle.cogs._handles_helpers import (
    HandleCogError,
    _GudgitterRow,
    _DIVISION_RATING_HIGH,
    _DIVISION_RATING_LOW,
    _LEADERBOARD_PER_PAGE,
    _PAGINATE_WAIT_TIME,
    _parse_gudgitter_args,
    parse_date,
)


def _get_gudgitters_image(rankings):
    # Resolve through the public ``handles`` module so tests that monkeypatch
    # ``tle.cogs.handles.get_gudgitters_image`` continue to take effect.
    from tle.cogs import handles as _handles_module
    return _handles_module.get_gudgitters_image(rankings)


class GudgittersMixin:
    """Mixin holding the gitgud leaderboard commands and their row/page
    building helpers."""

    @commands.command(brief="Show gudgitters", aliases=["gitgudders", "gitbadders", "ggtext"], usage="[div1|div2|div3] [+all]")
    async def gudgitters(self, ctx, *args):
        """Show the list of users of gitgud with their scores."""
        res = cf_common.user_db.get_gudgitters()
        res.sort(key=lambda r: r[1], reverse=True)

        division, showall = _parse_gudgitter_args(args)
        rows = self._build_gudgitter_rows(ctx, res, division=division, showall=showall)

        if not rows:
            raise HandleCogError('No one has completed a gitgud challenge, send ;gitgud to request and ;gotgud to mark it as complete')
        pages = self._make_gudgitter_pages(ctx, rows, 'GG Leaderboard')
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=_PAGINATE_WAIT_TIME,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    def filter_rating_changes(self, rating_changes):
        rating_changes = [change for change in rating_changes
                    if self.dlo <= change.ratingUpdateTimeSeconds < self.dhi]
        return rating_changes

    def _get_gudgitter_personal_rank_line(self, ctx, rows):
        user_id_str = str(ctx.author.id)
        for i, row in enumerate(rows):
            if row.user_id == user_id_str:
                return f'\nYour rank: **#{i + 1}** with **{row.score}** points'
        return '\nYou are not on this leaderboard yet.'

    def _make_gudgitter_pages(self, ctx, rows, title):
        personal = self._get_gudgitter_personal_rank_line(ctx, rows)
        chunks = paginator.chunkify(rows, _LEADERBOARD_PER_PAGE)
        pages = []
        for page_idx, chunk in enumerate(chunks):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * _LEADERBOARD_PER_PAGE + i + 1
                member = ctx.guild.get_member(int(row.user_id))
                name = member.mention if member is not None else f'`{row.handle}`'
                rating_str = row.rating if row.rating is not None else 'N/A'
                lines.append(
                    f'**#{rank}** {name} — **{row.score}** pts | `{row.handle}` ({rating_str})'
                )
            lines.append(personal)
            embed = discord.Embed(
                title=title,
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        return pages

    def _make_gudgitter_image_rankings(self, ctx, rows, *, limit=20):
        rankings = []
        for i, row in enumerate(rows[:limit]):
            member = ctx.guild.get_member(int(row.user_id))
            display_name = member.display_name if member is not None else ''
            rankings.append((i, display_name, row.handle, row.rating, row.score))
        return rankings

    def _build_gudgitter_rows(self, ctx, entries, *, division=None, showall=False, start_time=None):
        rows = []
        cache = cf_common.cache2.rating_changes_cache if start_time is not None else None
        for user_id, score in entries:
            member = ctx.guild.get_member(int(user_id))
            if not showall and member is None:
                continue
            if score <= 0:
                continue

            handle = cf_common.user_db.get_handle(user_id, ctx.guild.id)
            user = cf_common.user_db.fetch_cf_user(handle)
            if user is None:
                continue

            rating = user.rating
            if start_time is not None:
                rating_changes = cache.get_rating_changes_for_handle(handle)
                rating_changes = [change for change in rating_changes if change.ratingUpdateTimeSeconds < start_time]
                rating_changes.sort(key=lambda change: change.ratingUpdateTimeSeconds)
                if len(rating_changes) < 1:
                    continue
                if rating_changes[-1] is None:
                    continue
                rating = rating_changes[-1].newRating

            if division is not None:
                if rating is None:
                    continue
                if rating < _DIVISION_RATING_LOW[division - 1] or rating > _DIVISION_RATING_HIGH[division - 1]:
                    continue

            rows.append(_GudgitterRow(str(user_id), handle, rating, score))
        return rows

    @commands.command(brief="Show all gudgitters of the month", aliases=["monthlygitgudders_all", "monthlygga", "monthlygitbadders_all", "mgga"], usage="[div1|div2|div3] [d=mmyyyy] [+all]")
    async def monthlygudgitters_all(self, ctx, *args):
        """Show the list of users of gitgud with their scores."""

        # Calculate time range of given month (d=) or current month
        now = datetime.datetime.now()
        for arg in args:
            if arg[0:2] == 'd=':
                now = parse_date(arg[2:])

        start_time, end_time = cf_common.get_start_and_end_of_month(now)

        # more points seasons start at April 1st 2023 (timestamp: 1680300000) and is only active in the last 7 days of the month
        morePointsActive = False
        morePointsTime = end_time - cfc._ONE_WEEK_DURATION
        if start_time >= cfc._GITGUD_MORE_POINTS_START_TIME:
            morePointsActive = True

        division, showall = _parse_gudgitter_args(args)

        # get gitgud of month and calculate scores
        results = cf_common.user_db.get_gudgitters_timerange(start_time, end_time)
        res = {}
        for entry in results:
            res[entry[0]] = 0
        for entry in results:
            if len(entry) >= 3:
                score = cfc._calculateGitgudScoreForDelta(int(entry[1]))
                # @@ add finish time constraint (both times need to be within the more points range)
                res[entry[0]] += 2 * score if morePointsActive and int(entry[2]) >= morePointsTime else score
            else:
                raise HandleCogError(f'Tuple size {len(entry)} for entry {entry[0]}')

        rows = self._build_gudgitter_rows(
            ctx,
            sorted(res.items(), key=lambda item: item[1], reverse=True),
            division=division,
            showall=showall,
            start_time=start_time,
        )

        if not rows:
            raise HandleCogError('No one has completed a gitgud challenge, send ;gitgud to request and ;gotgud to mark it as complete')
        title = f'MGG Leaderboard ({now.strftime("%b %Y")})'
        pages = self._make_gudgitter_pages(ctx, rows, title)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=_PAGINATE_WAIT_TIME,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @commands.command(brief="Show gudgitters as an image", aliases=["gg"], usage="[div1|div2|div3] [+all]")
    async def ggimg(self, ctx, *args):
        """Show the top gitgudders as a color-coded image."""
        res = cf_common.user_db.get_gudgitters()
        res.sort(key=lambda r: r[1], reverse=True)

        division, showall = _parse_gudgitter_args(args)
        rows = self._build_gudgitter_rows(ctx, res, division=division, showall=showall)

        if not rows:
            raise HandleCogError('No one has completed a gitgud challenge, send ;gitgud to request and ;gotgud to mark it as complete')
        rankings = self._make_gudgitter_image_rankings(ctx, rows)
        await ctx.send(file=_get_gudgitters_image(rankings))

    @commands.command(brief="Show gudgitters of the month", aliases=["monthlygg", "monthlygitbadders", "mgg"], usage="[div1|div2|div3] [d=mmyyyy] [+all]")
    async def monthlygitgudders(self, ctx, *args):
        """Show the top monthly gitgudders as a color-coded image."""
        now = datetime.datetime.now()
        for arg in args:
            if arg[0:2] == 'd=':
                now = parse_date(arg[2:])

        start_time, end_time = cf_common.get_start_and_end_of_month(now)

        morePointsActive = False
        morePointsTime = end_time - cfc._ONE_WEEK_DURATION
        if start_time >= cfc._GITGUD_MORE_POINTS_START_TIME:
            morePointsActive = True

        division, showall = _parse_gudgitter_args(args)

        results = cf_common.user_db.get_gudgitters_timerange(start_time, end_time)
        res = {}
        for entry in results:
            res[entry[0]] = 0
        for entry in results:
            if len(entry) >= 3:
                score = cfc._calculateGitgudScoreForDelta(int(entry[1]))
                res[entry[0]] += 2 * score if morePointsActive and int(entry[2]) >= morePointsTime else score
            else:
                raise HandleCogError(f'Tuple size {len(entry)} for entry {entry[0]}')

        rows = self._build_gudgitter_rows(
            ctx,
            sorted(res.items(), key=lambda item: item[1], reverse=True),
            division=division,
            showall=showall,
            start_time=start_time,
        )

        if not rows:
            raise HandleCogError('No one has completed a gitgud challenge, send ;gitgud to request and ;gotgud to mark it as complete')
        rankings = self._make_gudgitter_image_rankings(ctx, rows)
        await ctx.send(file=_get_gudgitters_image(rankings))

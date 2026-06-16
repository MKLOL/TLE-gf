"""Command-body implementations for the country, visualrank, speed and
perftable Graphs subcommands.

Split out of ``tle/cogs/graphs.py`` (via ``_graph_render2``) to keep every
file under the 500-line limit. ``cog`` is the Graphs cog instance (for
``cog.converter`` and ``cog.bot``).
"""

import collections
import io

import discord
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc
from tle.util import paginator

from tle.cogs._graph_helpers import GraphCogError
from tle.cogs._graph_perftable import (
    _build_cfvc_rows,
    _build_rated_rows,
    _build_vc_rows,
    _format_cfvc_table,
    _format_perftable,
)


async def country(cog, ctx, *countries):
    """Plots distribution of server members by countries. When no countries are specified, plots
     a bar graph of all members by country. When one or more countries are specified, plots a
     swarmplot of members by country and rating. Only members with registered handles and
     countries set on Codeforces are considered.
     """
    max_countries = 8
    if len(countries) > max_countries:
        raise GraphCogError(f'At most {max_countries} countries may be specified.')

    users = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
    counter = collections.Counter(user.country for _, user in users if user.country)

    if not countries:
        # list because seaborn complains for tuple.
        countries, counts = map(list, zip(*counter.most_common()))
        plt.clf()
        fig = plt.figure(figsize=(15, 5))
        with sns.axes_style(rc={'xtick.bottom': True}):
            g = sns.barplot(x=countries, y=counts)
            g.set_yscale("log")

        # Show counts on top of bars.
        ax = plt.gca()
        for p in ax.patches:
            x = p.get_x() + p.get_width() / 2
            y = p.get_y() + p.get_height() + 0.5
            ax.text(x, y, int(p.get_height()), horizontalalignment='center', color='#30304f',
                    fontsize='x-small')

        plt.xticks(rotation=40, horizontalalignment='right')
        ax.tick_params(axis='x', length=4, color=ax.spines['bottom'].get_edgecolor())
        plt.xlabel('Country')
        plt.ylabel('Number of members')
        discord_file = gc.get_current_figure_as_file()
        plt.close(fig)
        embed = discord_common.cf_color_embed(title='Distribution of server members by country')
    else:
        countries = [country.title() for country in countries]
        data = [[user.country, user.rating]
                for _, user in users if user.rating and user.country and user.country in countries]
        if not data:
            raise GraphCogError('No rated members from the specified countries are present.')

        color_map = {rating: f'#{cf.rating2rank(rating).color_embed:06x}' for _, rating in data}
        df = pd.DataFrame(data, columns=['Country', 'Rating'])
        column_order = sorted((country for country in countries if counter[country]),
                              key=counter.get, reverse=True)
        plt.clf()
        if len(column_order) <= 5:
            sns.swarmplot(x='Country', y='Rating', hue='Rating', data=df, order=column_order,
                          palette=color_map)
        else:
            # Add ticks and rotate tick labels to avoid overlap.
            with sns.axes_style(rc={'xtick.bottom': True}):
                sns.swarmplot(x='Country', y='Rating', hue='Rating', data=df,
                              order=column_order, palette=color_map)
            plt.xticks(rotation=30, horizontalalignment='right')
            ax = plt.gca()
            ax.tick_params(axis='x', color=ax.spines['bottom'].get_edgecolor())
        plt.legend().remove()
        plt.xlabel('Country')
        plt.ylabel('Rating')
        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='Rating distribution of server members by '
                                                    'country')

    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def visualrank(cog, ctx, contest_id: int, *args: str):
    """Plot rating changes by rank. Add handles to specify a handle in the plot.
    if arguments contains `+server`, it will include just server members and not all codeforces users.
    Specify `+zoom` to zoom to the neighborhood of handles."""
    args = set(args)
    (in_server, zoom), handles = cf_common.filter_flags(args, ['+server', '+zoom'])
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles, mincnt=0, maxcnt=20)

    rating_changes = await cf.contest.ratingChanges(contest_id=contest_id)
    if in_server:
        guild_handles = set(handle for discord_id, handle
                            in cf_common.user_db.get_handles_for_guild(ctx.guild.id))
        rating_changes = [rating_change for rating_change in rating_changes
                          if rating_change.handle in guild_handles or rating_change.handle in handles]

    if not rating_changes:
        raise GraphCogError(f'No rating changes for contest `{contest_id}`')

    users_to_mark = {}
    for rating_change in rating_changes:
        user_delta = rating_change.newRating - rating_change.oldRating
        if rating_change.handle in handles:
            users_to_mark[rating_change.handle] = (rating_change.rank, user_delta)

    ymargin = 50
    xmargin = 50
    if users_to_mark and zoom:
        xmin = min(point[0] for point in users_to_mark.values())
        xmax = max(point[0] for point in users_to_mark.values())
        ymin = min(point[1] for point in users_to_mark.values())
        ymax = max(point[1] for point in users_to_mark.values())
    else:
        ylim = 0
        if users_to_mark:
            ylim = max(abs(point[1]) for point in users_to_mark.values())
        ylim = max(ylim, 200)

        xmin = 0
        xmax = max(rating_change.rank for rating_change in rating_changes)
        ymin = -ylim
        ymax = ylim

    ranks = []
    delta = []
    color = []
    for rating_change in rating_changes:
        user_delta = rating_change.newRating - rating_change.oldRating

        if (xmin - xmargin <= rating_change.rank <= xmax + xmargin
                and ymin - ymargin <= user_delta <= ymax + ymargin):
            ranks.append(rating_change.rank)
            delta.append(user_delta)
            color.append(cf.rating2rank(rating_change.oldRating).color_graph)

    title = rating_changes[0].contestName

    plt.clf()
    fig = plt.figure(figsize=(12, 8))
    plt.title(title)
    plt.xlabel('Rank')
    plt.ylabel('Rating Changes')

    mark_size = 2e4 / len(ranks)
    plt.xlim(xmin - xmargin, xmax + xmargin)
    plt.ylim(ymin - ymargin, ymax + ymargin)
    plt.scatter(ranks, delta, s=mark_size, c=color)

    for handle, point in users_to_mark.items():
        plt.annotate(handle,
                     xy=point,
                     xytext=(0, 0),
                     textcoords='offset points',
                     ha='left',
                     va='bottom',
                     fontsize='large')
        plt.plot(*point,
                 marker='o',
                 markersize=5,
                 color='black')

    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)

    embed = discord_common.cf_color_embed(title=title)
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def speed(cog, ctx, *args):
    """Plot time spent on problems of particular rating during contest."""
    (add_scatter, use_median), args = cf_common.filter_flags(args, ['+scatter', '+median'])
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    if 'PRACTICE' in filt.types:
        filt.types.remove('PRACTICE')  # can't estimate time for practice submissions

    handles, point_size = [], 3
    for arg in args:
        if arg[0:2] == 's=':
            point_size = int(arg[2:])
        else:
            handles.append(arg)

    handles = handles or ['!' + str(ctx.author.id)]
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.status(handle=handle) for handle in handles]
    all_solved_subs = [filt.filter_subs(submissions) for submissions in resp]

    plt.clf()
    plt.xlabel('Rating')
    plt.ylabel('Minutes spent')

    max_time = 0  # for ylim

    for submissions in all_solved_subs:
        scatter_points = []  # only matters if +scatter

        solved_by_contest = collections.defaultdict(lambda: [])
        for submission in submissions:
            # [solve_time, problem rating, problem index] for each solved problem
            solved_by_contest[submission.contestId].append([
                submission.relativeTimeSeconds,
                submission.problem.rating,
                submission.problem.index
            ])

        time_by_rating = collections.defaultdict(lambda: [])
        for events in solved_by_contest.values():
            events.sort()
            solved_subproblems = dict()
            last_ac_time = 0

            for (current_ac_time, rating, problem_index) in events:
                time_to_solve = current_ac_time - last_ac_time
                last_ac_time = current_ac_time

                # if there are subproblems, add total time for previous subproblems to current one
                if len(problem_index) == 2 and problem_index[1].isdigit():
                    time_to_solve += solved_subproblems.get(problem_index[0], 0)
                    solved_subproblems[problem_index[0]] = time_to_solve

                time_by_rating[rating].append(time_to_solve / 60)  # in minutes

        for rating in time_by_rating.keys():
            times = time_by_rating[rating]
            if use_median:
                time_by_rating[rating] = np.median(times)
            else:
                time_by_rating[rating] = sum(times) / len(times)

            if add_scatter:
                for t in times:
                    scatter_points.append([rating, t])
                    max_time = max(max_time, t)

        xs = sorted(time_by_rating.keys())
        ys = [time_by_rating[rating] for rating in xs]

        max_time = max(max_time, max(ys, default=0))
        plt.plot(xs, ys)
        if add_scatter:
            plt.scatter(*zip(*scatter_points), s=point_size)

    labels = [gc.StrWrap(handle) for handle in handles]
    plt.legend(labels)
    plt.ylim(0, max_time + 5)

    # make xticks divisible by 100
    ticks = plt.gca().get_xticks()
    base = ticks[1] - ticks[0]
    plt.gca().get_xaxis().set_major_locator(MultipleLocator(base = max(base // 100 * 100, 100)))
    discord_file = gc.get_current_figure_as_file()
    title = f'Plot of {"median" if use_median else "average"} time spent on a problem'
    embed = discord_common.cf_color_embed(title=title)
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)

    await ctx.send(embed=embed, file=discord_file)


async def perftable(cog, ctx, *args: str):
    """Show a table of contest performances."""
    (data, vc, cfvc), args = cf_common.filter_flags(args, ['+data', '+vc', '+cfvc'])
    filt = cf_common.SubFilter()
    args = filt.parse(args)

    if cfvc:
        await _perftable_cfvc(cog, ctx, args, data, filt)
        return

    if vc:
        rows = await _perftable_vc(cog, ctx, args, filt)
    else:
        rows = await _perftable_rated(cog, ctx, args, filt)

    if not rows:
        raise GraphCogError('No contest data found.')

    if data:
        table_str = _format_perftable(rows)
        if len(table_str) + 8 > 1900:
            await ctx.send(
                file=discord.File(io.StringIO(table_str), filename='performance.txt')
            )
        else:
            await ctx.send(f'```\n{table_str}\n```')
    else:
        title = 'VC Performance' if vc else 'Contest Performance'
        _PER_PAGE = 10
        pages = []
        for k, chunk in enumerate(paginator.chunkify(rows, _PER_PAGE)):
            table_str = _format_perftable(chunk)
            embed = discord_common.cf_color_embed(description=f'```\n{table_str}\n```')
            pages.append((title, embed))
        paginator.paginate(cog.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)


async def _perftable_cfvc(cog, ctx, args, data, filt):
    """Fetch CF virtual participation performances."""
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles, maxcnt=1)
    handle = handles[0]

    await ctx.send(f'Fetching virtual participations for `{handle}`, this may take a moment...')
    rows, missing = await _build_cfvc_rows(handle, filt.dlo, filt.dhi)

    if not rows:
        raise GraphCogError(f'No CF virtual participations found for `{handle}`.')

    warning = '⚠ VC perf is estimated from closest-ranked official contestant.'
    if missing:
        warning += f' {missing} contest(s) skipped (no cached data).'

    if data:
        table_str = _format_cfvc_table(rows)
        content = f'{warning}\n```\n{table_str}\n```'
        if len(content) > 1900:
            await ctx.send(warning)
            await ctx.send(
                file=discord.File(io.StringIO(table_str), filename='vc_performance.txt')
            )
        else:
            await ctx.send(content)
    else:
        title = 'CF Virtual Performance'
        _PER_PAGE = 10
        pages = []
        for k, chunk in enumerate(paginator.chunkify(rows, _PER_PAGE)):
            table_str = _format_cfvc_table(chunk)
            desc = f'{warning}\n```\n{table_str}\n```'
            embed = discord_common.cf_color_embed(description=desc)
            pages.append((title, embed))
        paginator.paginate(cog.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)


async def _perftable_rated(cog, ctx, args, filt):
    """Fetch rated contest performances from CF API."""
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles, maxcnt=1)
    handle = handles[0]

    rating_changes = await cf.user.rating(handle=handle)
    if not rating_changes:
        raise GraphCogError(f'User `{handle}` is not rated.')

    corrected = cf.user.correct_rating_changes(resp=[list(rating_changes)])
    corrected = corrected[0]
    corrected = filt.filter_rating_changes(corrected)
    rating_changes = filt.filter_rating_changes(rating_changes)

    if not corrected:
        raise GraphCogError(f'No contests found for `{handle}` in the given range.')

    return _build_rated_rows(rating_changes, corrected)


async def _perftable_vc(cog, ctx, args, filt):
    """Fetch virtual contest performances from the DB."""
    if args:
        member = await cog.converter.convert(ctx, args[0])
    else:
        member = ctx.author

    rating_history = cf_common.user_db.get_vc_rating_history(member.id)
    if not rating_history:
        raise GraphCogError(f'{member.mention} has no vc history.')

    def get_vc_info(vc_id):
        vc = cf_common.user_db.get_rated_vc(vc_id)
        try:
            contest = cf_common.cache2.contest_cache.get_contest(vc.contest_id)
            name = contest.name
        except Exception:
            name = f'Contest {vc.contest_id}'
        return vc.finish_time, name

    return _build_vc_rows(rating_history, filt.dlo, filt.dhi, get_vc_info)

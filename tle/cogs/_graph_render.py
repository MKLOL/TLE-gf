"""Command-body implementations for the first group of Graphs subcommands.

Each ``async def`` here holds the full logic of a ``;plot`` subcommand and is
called by a thin wrapper in ``tle/cogs/graphs.py``. Split out to keep the cog
file under the 500-line limit. ``cog`` is the Graphs cog instance (used for
``cog.converter`` and ``cog.bot``).
"""

import datetime as dt
import itertools
import math

from matplotlib import pyplot as plt
from matplotlib import dates as mdates

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc

from tle.cogs._graph_helpers import (
    GraphCogError,
    nice_sub_type,
    _plot_rating_by_date,
    _plot_rating_by_contest,
    _classify_submissions,
    _plot_scatter,
    _plot_average,
    _plot_extreme,
)


async def rating(cog, ctx, *args: str):
    """Plots Codeforces rating graph for the handles provided."""
    (zoom, number, peak), args = cf_common.filter_flags(args, ['+zoom' , '+number', '+peak'])
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.rating(handle=handle) for handle in handles]
    resp = [filt.filter_rating_changes(rating_changes) for rating_changes in resp]

    if not any(resp):
        handles_str = ', '.join(f'`{handle}`' for handle in handles)
        if len(handles) == 1:
            message = f'User {handles_str} is not rated'
        else:
            message = f'None of the given users {handles_str} are rated'
        raise GraphCogError(message)

    def max_prefix(user):
        max_rate = 0
        res = []
        for data in user:
            old_rating = data.oldRating
            if old_rating == 0:
                old_rating = 1500
            if data.newRating - old_rating >= 0 and data.newRating >= max_rate:
                max_rate = data.newRating
                res.append(data)
        return(res)

    if peak:
        resp = [max_prefix(user) for user in resp]

    plt.clf()
    plt.axes().set_prop_cycle(gc.rating_color_cycler)
    if number:
        _plot_rating_by_contest(resp)
    else:
        _plot_rating_by_date(resp)
    current_ratings = [rating_changes[-1].newRating if rating_changes else 'Unrated' for rating_changes in resp]
    labels = [gc.StrWrap(f'{handle} ({rating})') for handle, rating in zip(handles, current_ratings)]
    plt.legend(labels, bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=2)

    if not zoom:
        min_rating = 1100
        max_rating = 1800
        for rating_changes in resp:
            for rating in rating_changes:
                min_rating = min(min_rating, rating.newRating)
                max_rating = max(max_rating, rating.newRating)
        plt.ylim(min_rating - 100, max_rating + 200)

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Rating graph on Codeforces')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def performance(cog, ctx, *args: str):
    """Plots Codeforces performance graph for the handles provided."""
    (zoom, peak), args = cf_common.filter_flags(args, ['+zoom' , '+peak'])
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.rating(handle=handle) for handle in handles]
    # extract last rating before corrections
    current_ratings = [rating_changes[-1].newRating if rating_changes else 'Unrated' for rating_changes in resp]
    resp = cf.user.correct_rating_changes(resp=resp)
    resp = [filt.filter_rating_changes(rating_changes) for rating_changes in resp]

    if not any(resp):
        handles_str = ', '.join(f'`{handle}`' for handle in handles)
        if len(handles) == 1:
            message = f'User {handles_str} is not rated'
        else:
            message = f'None of the given users {handles_str} are rated'
        raise GraphCogError(message)

    def max_prefix(user):
        max_rate = 0
        res = []
        for data in user:
            if data.newRating >= max_rate:
                max_rate = data.newRating
                res.append(data)
        return(res)

    if peak:
        resp = [max_prefix(user) for user in resp]

    plt.clf()
    plt.axes().set_prop_cycle(gc.rating_color_cycler)
    _plot_rating_by_date(resp)
    labels = [gc.StrWrap(f'{handle} ({rating})') for handle, rating in zip(handles, current_ratings)]
    plt.legend(labels, bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=2)

    if not zoom:
        min_rating = 1100
        max_rating = 1800
        for rating_changes in resp:
            for rating in rating_changes:
                min_rating = min(min_rating, rating.newRating)
                max_rating = max(max_rating, rating.newRating)
        plt.ylim(min_rating - 100, max_rating + 200)

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Performance graph on Codeforces')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def extreme(cog, ctx, *args: str):
    """Plots pairs of lowest rated unsolved problem and highest rated solved problem for every
    contest that was rated for the given user.
    """
    (solved, unsolved, nolegend), args = cf_common.filter_flags(args, ['+solved', '+unsolved', '+nolegend'])
    legend, = cf_common.negate_flags(nolegend)
    if not solved and not unsolved:
        solved = unsolved = True

    filt = cf_common.SubFilter()
    args = filt.parse(args)

    handles = args or ('!' + str(ctx.author.id),)
    handle, = await cf_common.resolve_handles(ctx, cog.converter, handles)
    ratingchanges = await cf.user.rating(handle=handle)
    if not ratingchanges:
        raise GraphCogError(f'User {handle} is not rated')

    ratingchanges = filt.filter_rating_changes(ratingchanges)
    contest_ids = [change.contestId for change in ratingchanges]

    subs_by_contest_id = {contest_id: [] for contest_id in contest_ids}
    for sub in await cf.user.status(handle=handle):
        if sub.contestId in subs_by_contest_id:
            subs_by_contest_id[sub.contestId].append(sub)

    packed_contest_subs_problemset = [
        (cf_common.cache2.contest_cache.get_contest(contest_id),
         cf_common.cache2.problemset_cache.get_problemset(contest_id),
         subs_by_contest_id[contest_id])
        for contest_id in contest_ids
    ]

    rating = max(ratingchanges, key=lambda change: change.ratingUpdateTimeSeconds).newRating
    _plot_extreme(handle, rating, packed_contest_subs_problemset, solved, unsolved, legend)

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Codeforces extremes graph')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def solved(cog, ctx, *args: str):
    """Shows a histogram of solved problems' rating on Codeforces for the handles provided."""
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.status(handle=handle) for handle in handles]
    all_solved_subs = [filt.filter_subs(submissions) for submissions in resp]

    if not any(all_solved_subs):
        raise GraphCogError(f'There are no problems within the specified parameters.')

    plt.clf()
    plt.xlabel('Problem rating')
    plt.ylabel('Number solved')
    if len(handles) == 1:
        # Display solved problem separately by type for a single user.
        handle, solved_by_type = handles[0], _classify_submissions(all_solved_subs[0])
        all_ratings = [[sub.problem.rating for sub in solved_by_type[sub_type]]
                       for sub_type in filt.types]

        nice_names = nice_sub_type(filt.types)
        labels = [name.format(len(ratings)) for name, ratings in zip(nice_names, all_ratings)]

        step = 100
        # shift the range to center the text
        hist_bins = list(range(filt.rlo - step // 2, filt.rhi + step // 2 + 1, step))
        plt.hist(all_ratings, stacked=True, bins=hist_bins, label=labels)
        total = sum(map(len, all_ratings))
        plt.legend(title=f'{handle}: {total}', title_fontsize=plt.rcParams['legend.fontsize'],
                   loc='upper right')

    else:
        all_ratings = [[sub.problem.rating for sub in solved_subs]
                       for solved_subs in all_solved_subs]
        labels = [gc.StrWrap(f'{handle}: {len(ratings)}')
                  for handle, ratings in zip(handles, all_ratings)]

        step = 200 if filt.rhi - filt.rlo > 3000 // len(handles) else 100
        hist_bins = list(range(filt.rlo - step // 2, filt.rhi + step // 2 + 1, step))
        plt.hist(all_ratings, bins=hist_bins)
        plt.legend(labels, loc='upper right')

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Histogram of problems solved on Codeforces')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def hist(cog, ctx, *args: str):
    """Shows the histogram of problems solved on Codeforces over time for the handles provided"""
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    phase_days = 1
    handles = []
    for arg in args:
        if arg[0:11] == 'phase_days=':
            phase_days = int(arg[11:])
        else:
            handles.append(arg)

    if phase_days < 1:
        raise GraphCogError('Invalid parameters')
    phase_time = dt.timedelta(days=phase_days)

    handles = handles or ['!' + str(ctx.author.id)]
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.status(handle=handle) for handle in handles]
    all_solved_subs = [filt.filter_subs(submissions) for submissions in resp]

    if not any(all_solved_subs):
        raise GraphCogError(f'There are no problems within the specified parameters.')

    plt.clf()
    plt.xlabel('Time')
    plt.ylabel('Number solved')
    if len(handles) == 1:
        handle, solved_by_type = handles[0], _classify_submissions(all_solved_subs[0])
        all_times = [[dt.datetime.fromtimestamp(sub.creationTimeSeconds) for sub in solved_by_type[sub_type]]
                     for sub_type in filt.types]

        nice_names = nice_sub_type(filt.types)
        labels = [name.format(len(times)) for name, times in zip(nice_names, all_times)]

        dlo = min(itertools.chain.from_iterable(all_times)).date()
        dhi = min(dt.datetime.today() + dt.timedelta(days=1), dt.datetime.fromtimestamp(filt.dhi)).date()
        phase_cnt = math.ceil((dhi - dlo) / phase_time)
        plt.hist(
            all_times,
            stacked=True,
            label=labels,
            range=(dhi - phase_cnt * phase_time, dhi),
            bins=min(40, phase_cnt))

        total = sum(map(len, all_times))
        plt.legend(title=f'{handle}: {total}', title_fontsize=plt.rcParams['legend.fontsize'])
    else:
        all_times = [[dt.datetime.fromtimestamp(sub.creationTimeSeconds) for sub in solved_subs]
                     for solved_subs in all_solved_subs]

        # NOTE: matplotlib ignores labels that begin with _
        # https://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.legend
        # Add zero-width space to work around this
        labels = [gc.StrWrap(f'{handle}: {len(times)}')
                  for handle, times in zip(handles, all_times)]

        dlo = min(itertools.chain.from_iterable(all_times)).date()
        dhi = min(dt.datetime.today() + dt.timedelta(days=1), dt.datetime.fromtimestamp(filt.dhi)).date()
        phase_cnt = math.ceil((dhi - dlo) / phase_time)
        plt.hist(
            all_times,
            range=(dhi - phase_cnt * phase_time, dhi),
            bins=min(40 // len(handles), phase_cnt))
        plt.legend(labels)

    # NOTE: In case of nested list, matplotlib decides type using 1st sublist,
    # it assumes float when 1st sublist is empty.
    # Hence explicitly assigning locator and formatter is must here.
    locator = mdates.AutoDateLocator()
    plt.gca().xaxis.set_major_locator(locator)
    plt.gca().xaxis.set_major_formatter(mdates.AutoDateFormatter(locator))

    plt.gcf().autofmt_xdate()
    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Histogram of number of solved problems over time')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def curve(cog, ctx, *args: str):
    """Plots the count of problems solved over time on Codeforces for the handles provided."""
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    handles = args or ('!' + str(ctx.author.id),)
    handles = await cf_common.resolve_handles(ctx, cog.converter, handles)
    resp = [await cf.user.status(handle=handle) for handle in handles]
    all_solved_subs = [filt.filter_subs(submissions) for submissions in resp]

    if not any(all_solved_subs):
        raise GraphCogError(f'There are no problems within the specified parameters.')

    plt.clf()
    plt.xlabel('Time')
    plt.ylabel('Cumulative solve count')

    all_times = [[dt.datetime.fromtimestamp(sub.creationTimeSeconds) for sub in solved_subs]
                 for solved_subs in all_solved_subs]
    for times in all_times:
        cumulative_solve_count = list(range(1, len(times)+1)) + [len(times)]
        timestretched = times + [min(dt.datetime.now(), dt.datetime.fromtimestamp(filt.dhi))]
        plt.plot(timestretched, cumulative_solve_count)

    labels = [gc.StrWrap(f'{handle}: {len(times)}')
              for handle, times in zip(handles, all_times)]

    plt.legend(labels)

    plt.gcf().autofmt_xdate()
    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Curve of number of solved problems over time')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def scatter(cog, ctx, *args):
    """Plot Codeforces rating overlaid on a scatter plot of problems solved.
    Also plots a running average of ratings of problems solved in practice."""
    (nolegend,), args = cf_common.filter_flags(args, ['+nolegend'])
    legend, = cf_common.negate_flags(nolegend)
    filt = cf_common.SubFilter()
    args = filt.parse(args)
    handle, bin_size, point_size = None, 10, 3
    for arg in args:
        if arg[0:2] == 'b=':
            bin_size = int(arg[2:])
        elif arg[0:2] == 's=':
            point_size = int(arg[2:])
        else:
            if handle:
                raise GraphCogError('Only one handle allowed.')
            handle = arg

    if bin_size < 1 or point_size < 1 or point_size > 100:
        raise GraphCogError('Invalid parameters')

    handle = handle or '!' + str(ctx.author.id)
    handle, = await cf_common.resolve_handles(ctx, cog.converter, (handle,))
    rating_resp = [await cf.user.rating(handle=handle)]
    rating_resp = [filt.filter_rating_changes(rating_changes) for rating_changes in rating_resp]
    submissions = filt.filter_subs(await cf.user.status(handle=handle))

    def extract_time_and_rating(submissions):
        return [(dt.datetime.fromtimestamp(sub.creationTimeSeconds), sub.problem.rating)
                for sub in submissions]

    if not any(submissions):
        raise GraphCogError(f'No submissions for user `{handle}`')

    solved_by_type = _classify_submissions(submissions)
    regular = extract_time_and_rating(solved_by_type['CONTESTANT'] +
                                      solved_by_type['OUT_OF_COMPETITION'])
    practice = extract_time_and_rating(solved_by_type['PRACTICE'])
    virtual = extract_time_and_rating(solved_by_type['VIRTUAL'])

    plt.clf()
    _plot_scatter(regular, practice, virtual, point_size)
    labels = []
    if practice:
        labels.append('Practice')
    if regular:
        labels.append('Regular')
    if virtual:
        labels.append('Virtual')
    if legend:
        plt.legend(labels, bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=3)
    _plot_average(practice, bin_size)
    _plot_rating_by_date(rating_resp, mark='')

    # zoom
    ymin, ymax = plt.gca().get_ylim()
    plt.ylim(max(ymin, filt.rlo - 100), min(ymax, filt.rhi + 100))

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title=f'Rating vs solved problem rating for {handle}')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)

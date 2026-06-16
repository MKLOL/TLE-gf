"""Command-body implementations for the distribution and percentile Graphs
subcommands (rating distribution, centile, howgud) plus the ``ratingfor``
standalone command.

Split out of ``tle/cogs/graphs.py`` to keep the cog file under 500 lines.
``cog`` is the Graphs cog instance (for ``cog.converter`` and ``cog.bot``).
"""

import bisect
import time

import discord
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import patches as patches
from matplotlib import lines as mlines

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc

from tle.cogs._graph_helpers import GraphCogError, _rating_at_percentile

# A user is considered active if the duration since his last contest is not more than this
CONTEST_ACTIVE_TIME_CUTOFF = 90 * 24 * 60 * 60  # 90 days


async def _rating_hist(ctx, ratings, mode, binsize, title):
    if mode not in ('log', 'normal'):
        raise GraphCogError('Mode should be either `log` or `normal`')

    ratings = [r for r in ratings if r >= 0]
    assert ratings, 'Cannot histogram plot empty list of ratings'

    assert 100%binsize == 0 # because bins is semi-hardcoded

    bins = 1 + max(ratings) // binsize

    colors = []
    low, high = 0, binsize * bins
    for rank in cf.RATED_RANKS:
        for r in range(max(rank.low, low), min(rank.high, high), binsize):
            colors.append('#' + '%06x' % rank.color_embed)
    assert len(colors) == bins, f'Expected {bins} colors, got {len(colors)}'

    height = [0] * bins
    for r in ratings:
        height[r // binsize] += 1

    csum = 0
    cent = [0]
    users = sum(height)
    for h in height:
        csum += h
        cent.append(round(100 * csum / users))

    x = [k * binsize for k in range(bins)]
    label = [f'{r} ({c})' for r,c in zip(x, cent)]

    l,r = 0,bins-1
    while not height[l]: l += 1
    while not height[r]: r -= 1
    x = x[l:r+1]
    cent = cent[l:r+1]
    label = label[l:r+1]
    colors = colors[l:r+1]
    height = height[l:r+1]

    plt.clf()
    fig = plt.figure(figsize=(15, 5))

    plt.xticks(rotation=45)
    plt.xlim(l * binsize - binsize//2, r * binsize + binsize//2)
    plt.bar(x, height, binsize*0.9, color=colors, linewidth=0, tick_label=label, log=(mode == 'log'))
    plt.xlabel('Rating')
    plt.ylabel('Number of users')

    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)

    embed = discord_common.cf_color_embed(title=title)
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def distrib(cog, ctx):
    """Plots rating distribution of users in this server"""
    def in_purgatory(userid):
        member = ctx.guild.get_member(int(userid))
        return not member or 'Purgatory' in {role.name for role in member.roles}

    res = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
    ratings = [cf_user.rating for user_id, cf_user in res
               if cf_user.rating is not None and not in_purgatory(user_id)]
    await _rating_hist(ctx,
                       ratings,
                       'normal',
                       binsize=100,
                       title='Rating distribution of server members')


async def cfdistrib(cog, ctx, mode: str = 'log', activity='active', contest_cutoff: int = 5):
    """Plots rating distribution of either active or all users on Codeforces, in either normal or log scale.
    Default mode is log, default activity is active (competed in last 90 days)
    Default contest cutoff is 5 (competed at least five times overall)
    """
    if activity not in ['active', 'all']:
        raise GraphCogError('Activity should be either `active` or `all`')

    time_cutoff = int(time.time()) - CONTEST_ACTIVE_TIME_CUTOFF if activity == 'active' else 0
    handles = cf_common.cache2.rating_changes_cache.get_users_with_more_than_n_contests(time_cutoff, contest_cutoff)
    if not handles:
        raise GraphCogError('No Codeforces users meet the specified criteria')

    ratings = [cf_common.cache2.rating_changes_cache.get_current_rating(handle) for handle in handles]
    title = f'Rating distribution of {activity} Codeforces users ({mode} scale)'
    await _rating_hist(ctx,
                       ratings,
                       mode,
                       binsize=100,
                       title=title)


async def centile(cog, ctx, *args: str):
    """Show percentile distribution of codeforces and mark given handles in the plot. If +zoom and handles are given, it zooms to the neighborhood of the handles."""
    (zoom, nomarker, exact), args = cf_common.filter_flags(args, ['+zoom', '+nomarker', '+exact'])
    # Prepare data
    intervals = [(rank.low, rank.high) for rank in cf.RATED_RANKS]
    colors = [rank.color_graph for rank in cf.RATED_RANKS]

    ratings = cf_common.cache2.rating_changes_cache.get_all_ratings()
    ratings = np.array(sorted(ratings))
    n = len(ratings)
    perc = 100*np.arange(n)/n

    users_to_mark = {}
    if not nomarker:
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx,
                                                  cog.converter,
                                                  handles,
                                                  mincnt=0,
                                                  maxcnt=50)
        infos = await cf.user.info(handles=list(set(handles)))

        for info in infos:
            if info.rating is None:
                raise GraphCogError(f'User `{info.handle}` is not rated')
            ix = bisect.bisect_left(ratings, info.rating)
            cent = 100*ix/len(ratings)
            users_to_mark[info.handle] = info.rating,cent

    # Plot
    plt.clf()
    fig,ax = plt.subplots(1)
    ax.plot(ratings, perc, color='#00000099')

    plt.xlabel('Rating')
    plt.ylabel('Percentile')

    for pos in ['right','top','bottom','left']:
        ax.spines[pos].set_visible(False)
    ax.tick_params(axis='both', which='both',length=0)

    # Color intervals by rank
    for interval,color in zip(intervals,colors):
        alpha = '99'
        l,r = interval
        col = color + alpha
        rect = patches.Rectangle((l,-50), r-l, 200,
                                 edgecolor='none',
                                 facecolor=col)
        ax.add_patch(rect)

    if users_to_mark:
        ymin = min(point[1] for point in users_to_mark.values())
        ymax = max(point[1] for point in users_to_mark.values())
        if zoom:
            ymargin = max(0.5, (ymax - ymin) * 0.1)
            ymin -= ymargin
            ymax += ymargin
        else:
            ymin = min(-1.5, ymin - 8)
            ymax = max(101.5, ymax + 8)
    else:
        ymin, ymax = -1.5, 101.5

    if users_to_mark and zoom:
        xmin = min(point[0] for point in users_to_mark.values())
        xmax = max(point[0] for point in users_to_mark.values())
        xmargin = max(20, (xmax - xmin) * 0.1)
        xmin -= xmargin
        xmax += xmargin
    else:
        xmin, xmax = ratings[0], ratings[-1]

    plt.xlim(xmin, xmax)
    plt.ylim(ymin, ymax)

    # Mark users in plot
    for user, point in users_to_mark.items():
        astr = f'{user} ({round(point[1], 2)})' if exact else user
        apos = ('left', 'top') if point[0] <= (xmax + xmin) // 2 else ('right', 'bottom')
        plt.annotate(astr,
                     xy=point,
                     xytext=(0, 0),
                     textcoords='offset points',
                     ha=apos[0],
                     va=apos[1])
        plt.plot(*point,
                 marker='o',
                 markersize=5,
                 color='red',
                 markeredgecolor='darkred')

    # Draw tick lines
    linecolor = '#00000022'
    inf = 10000
    def horz_line(y):
        l = mlines.Line2D([-inf,inf], [y,y], color=linecolor)
        ax.add_line(l)
    def vert_line(x):
        l = mlines.Line2D([x,x], [-inf,inf], color=linecolor)
        ax.add_line(l)
    for y in ax.get_yticks():
        horz_line(y)
    for x in ax.get_xticks():
        vert_line(x)

    # Discord stuff
    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title=f'Rating/percentile relationship')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def howgud(cog, ctx, *members: discord.Member):
    members = members or (ctx.author,)
    if len(members) > 5:
        raise GraphCogError('Please specify at most 5 gudgitters.')

    deltas = [[x[0] for x in cf_common.user_db.howgud(member.id)] for member in members]
    labels = [gc.StrWrap(f'{member.display_name}: {len(delta)}')
              for member, delta in zip(members, deltas)]

    #get bins dynamically
    min_delta = min([min(delta, default=0) for delta in deltas])
    max_delta = max([max(delta, default=0) for delta in deltas])
    hist_bins = list(range(min_delta - 50, max_delta + 50 + 1, 100))

    plt.clf()
    plt.margins(x=0)
    plt.hist(deltas, bins=hist_bins, rwidth=1)
    plt.xlabel('Problem delta')
    plt.ylabel('Number solved')
    plt.legend(labels, prop=gc.fontprop)

    discord_file = gc.get_current_figure_as_file()
    embed = discord_common.cf_color_embed(title='Histogram of gudgitting')
    discord_common.attach_image(embed, discord_file)
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed, file=discord_file)


async def ratingfor(cog, ctx, percentile: float):
    """Look up the Codeforces rating at the given percentile (0–100)."""
    if not (0 <= percentile <= 100):
        raise GraphCogError('Percentile must be between 0 and 100.')

    if not getattr(cf_common, '_initialize_done', False):
        raise GraphCogError(
            'Bot is still warming up after restart — rating cache is not '
            'loaded yet. Try again in a few seconds.'
        )

    ratings = cf_common.cache2.rating_changes_cache.get_all_ratings()
    rating = _rating_at_percentile(ratings, percentile)
    if rating is None:
        raise GraphCogError(
            'Rating cache is empty. An admin needs to run '
            '`;cache ratingchanges missing` to populate it.'
        )

    rank = cf.rating2rank(rating)
    color = rank.color_embed if rank.color_embed is not None else 0xffaa10
    embed = discord.Embed(
        title=f'Rating at {percentile:g} percentile',
        description=f'**{rating}** ({rank.title}) — computed over {len(ratings)} rated users.',
        color=color,
    )
    discord_common.set_author_footer(embed, ctx.author)
    await ctx.send(embed=embed)

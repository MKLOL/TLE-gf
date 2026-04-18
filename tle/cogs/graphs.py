import bisect
import collections
import datetime as dt
import io
import time
import itertools
import math
import datetime

from typing import List

import discord
import numpy as np
import pandas as pd
import seaborn as sns
from discord.ext import commands
from matplotlib import pyplot as plt
from matplotlib import patches as patches
from matplotlib import lines as mlines
from matplotlib import dates as mdates
from matplotlib.ticker import MultipleLocator

from tle import constants
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc
from tle.util import paginator
from tle.util import table

pd.plotting.register_matplotlib_converters()

# A user is considered active if the duration since his last contest is not more than this
CONTEST_ACTIVE_TIME_CUTOFF = 90 * 24 * 60 * 60 # 90 days

class GraphCogError(commands.CommandError):
    pass


def _rating_at_percentile(ratings, percentile):
    """Return the rating at the given percentile of a sorted-or-unsorted list.

    Percentile is 0–100 inclusive. The convention matches `;plot centile`:
    a user with rating r sits at percentile 100 * (count below r) / n, so
    percentile p maps to the rating at sorted index floor(p/100 * n),
    clamped into range. Returns None if ratings is empty.
    """
    if not (0 <= percentile <= 100):
        raise ValueError('percentile must be between 0 and 100')
    n = len(ratings)
    if n == 0:
        return None
    sorted_ratings = sorted(ratings)
    idx = int(percentile / 100 * n)
    if idx >= n:
        idx = n - 1
    return sorted_ratings[idx]

def nice_sub_type(types):
    nice_map = {'CONTESTANT':'Contest: {}',
                'OUT_OF_COMPETITION':'Unofficial: {}',
                'VIRTUAL':'Virtual: {}',
                'PRACTICE':'Practice: {}'}
    return [nice_map[t] for t in types]

def _plot_rating(plot_data, mark):
    for ratings, when in plot_data:
        plt.plot(when,
                 ratings,
                 linestyle='-',
                 marker=mark,
                 markersize=3,
                 markerfacecolor='white',
                 markeredgewidth=0.5)
    gc.plot_rating_bg(cf.RATED_RANKS)

def _plot_rating_by_date(resp, mark='o'):
    def gen_plot_data():
        for rating_changes in resp:
            ratings, times = [], []
            for rating_change in rating_changes:
                ratings.append(rating_change.newRating)
                times.append(dt.datetime.fromtimestamp(rating_change.ratingUpdateTimeSeconds))
            yield (ratings, times)

    _plot_rating(gen_plot_data(), mark)
    plt.gcf().autofmt_xdate()

def _plot_rating_by_contest(resp, mark='o'):
    def gen_plot_data():
        for rating_changes in resp:
            ratings, indices = [], []
            index = 1
            for rating_change in rating_changes:
                ratings.append(rating_change.newRating)
                indices.append(index)
                index += 1
            yield (ratings, indices)

    _plot_rating(gen_plot_data(), mark)


def _classify_submissions(submissions):
    solved_by_type = {sub_type: [] for sub_type in cf.Party.PARTICIPANT_TYPES}
    for submission in submissions:
        solved_by_type[submission.author.participantType].append(submission)
    return solved_by_type


def _plot_scatter(regular, practice, virtual, point_size):
    for contest in [practice, regular, virtual]:
        if contest:
            times, ratings = zip(*contest)
            plt.scatter(times, ratings, zorder=10, s=point_size)


def _running_mean(x, bin_size):
    n = len(x)

    cum_sum = [0] * (n + 1)
    for i in range(n):
        cum_sum[i + 1] = x[i] + cum_sum[i]

    res = [0] * (n - bin_size + 1)
    for i in range(bin_size, n + 1):
        res[i - bin_size] = (cum_sum[i] - cum_sum[i - bin_size]) / bin_size

    return res


def _get_extremes(contest, problemset, submissions):

    def in_contest(sub):
        return (sub.author.participantType == 'CONTESTANT' or
                (cf_common.is_rated_for_onsite_contest(contest) and
                 sub.author.participantType == 'OUT_OF_COMPETITION'))

    problemset = [prob for prob in problemset if prob.rating is not None]
    submissions = [sub for sub in submissions
                   if in_contest(sub) and sub.problem.rating is not None]
    solved = {sub.problem.index: sub.problem.rating for sub in submissions if
              sub.verdict == 'OK'}
    max_solved = max(solved.values(), default=None)
    min_unsolved = min((prob.rating for prob in problemset if prob.index not in solved),
                       default=None)
    return min_unsolved, max_solved


def _plot_extreme(handle, rating, packed_contest_subs_problemset, solved, unsolved, legend):
    extremes = [
        (dt.datetime.fromtimestamp(contest.end_time), _get_extremes(contest, problemset, subs))
        for contest, problemset, subs in packed_contest_subs_problemset
    ]
    regular = []
    fullsolves = []
    nosolves = []
    for t, (mn, mx) in extremes:
        if mn and mx:
            regular.append((t, mn, mx))
        elif mx:
            fullsolves.append((t, mx))
        elif mn:
            nosolves.append((t, mn))
        else:
            # No rated problems in the contest, which means rating is not yet available for
            # problems in this contest. Skip this data point.
            pass

    solvedcolor = 'tab:orange'
    unsolvedcolor = 'tab:blue'
    linecolor = '#00000022'
    outlinecolor = '#00000022'

    def scatter_outline(*args, **kwargs):
        plt.scatter(*args, **kwargs)
        kwargs['zorder'] -= 1
        kwargs['color'] = outlinecolor
        if kwargs['marker'] == '*':
            kwargs['s'] *= 3
        elif kwargs['marker'] == 's':
            kwargs['s'] *= 1.5
        else:
            kwargs['s'] *= 2
        if 'alpha' in kwargs:
            del kwargs['alpha']
        if 'label' in kwargs:
            del kwargs['label']
        plt.scatter(*args, **kwargs)

    plt.clf()
    if regular:
        time_scatter, plot_min, plot_max = zip(*regular)
        if unsolved:
            scatter_outline(time_scatter, plot_min, zorder=10,
                            s=14, marker='o', color=unsolvedcolor,
                            label='Easiest unsolved')
        if solved:
            scatter_outline(time_scatter, plot_max, zorder=10,
                            s=14, marker='o', color=solvedcolor,
                            label='Hardest solved')

        ax = plt.gca()
        if solved and unsolved:
            for t, mn, mx in regular:
                ax.add_line(mlines.Line2D((t, t), (mn, mx), color=linecolor))

    if fullsolves:
        scatter_outline(*zip(*fullsolves), zorder=15,
                        s=42, marker='*',
                        color=solvedcolor)
    if nosolves:
        scatter_outline(*zip(*nosolves), zorder=15,
                        s=32, marker='X',
                        color=unsolvedcolor)

    if not regular and not fullsolves and not nosolves:
        raise GraphCogError(f'No plot extreme possible. User probably only participated in contests that have no problem ratings yet.')

    if legend:
        plt.legend(title=f'{handle}: {rating}', title_fontsize=plt.rcParams['legend.fontsize'],
                   loc='upper left').set_zorder(20)
    gc.plot_rating_bg(cf.RATED_RANKS)
    plt.gcf().autofmt_xdate()


def _plot_average(practice, bin_size, label: str = ''):
    if len(practice) > bin_size:
        sub_times, ratings = map(list, zip(*practice))

        sub_timestamps = [sub_time.timestamp() for sub_time in sub_times]
        mean_sub_timestamps = _running_mean(sub_timestamps, bin_size)
        mean_sub_times = [dt.datetime.fromtimestamp(timestamp) for timestamp in mean_sub_timestamps]
        mean_ratings = _running_mean(ratings, bin_size)

        plt.plot(mean_sub_times,
                 mean_ratings,
                 linestyle='-',
                 marker='',
                 markerfacecolor='white',
                 markeredgewidth=0.5,
                 label=label)


_CONTEST_NAME_MAX = 30


def _truncate_name(name):
    if len(name) > _CONTEST_NAME_MAX:
        return name[:_CONTEST_NAME_MAX - 3] + '...'
    return name


def _build_rated_rows(rating_changes, corrected):
    """Build performance rows from original and corrected rating changes."""
    rows = []
    for i, (orig, corr) in enumerate(zip(rating_changes, corrected)):
        delta = orig.newRating - orig.oldRating
        rows.append({
            'idx': i + 1,
            'contest': _truncate_name(orig.contestName),
            'rank': orig.rank,
            'old': orig.oldRating,
            'new': orig.newRating,
            'delta': delta,
            'perf': corr.newRating,
        })
    return rows


def _build_vc_rows(rating_history, dlo, dhi, get_vc_info):
    """Build performance rows from VC rating history.

    get_vc_info(vc_id) -> (finish_time, contest_name)
    """
    rows = []
    ratingbefore = 1500
    idx = 0
    for vc_id, rating in rating_history:
        finish_time, contest_name = get_vc_info(vc_id)
        if not (dlo <= finish_time < dhi):
            ratingbefore = rating
            continue
        idx += 1
        delta = rating - ratingbefore
        perf = ratingbefore + (rating - ratingbefore) * 4
        rows.append({
            'idx': idx,
            'contest': _truncate_name(contest_name),
            'rank': None,
            'old': ratingbefore,
            'new': rating,
            'delta': delta,
            'perf': perf,
        })
        ratingbefore = rating
    return rows


def _estimate_perf_from_cache(contest_id, virtual_rank):
    """Estimate performance by finding the closest-ranked official contestant in cache.

    Returns estimated performance or None if no cached data.
    """
    changes = cf_common.cache2.rating_changes_cache.get_rating_changes_for_contest(contest_id)
    if not changes:
        return None
    # Sort by rank (should already be, but be safe)
    changes.sort(key=lambda c: c.rank)
    # Binary search for closest rank
    lo, hi = 0, len(changes) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if changes[mid].rank < virtual_rank:
            lo = mid + 1
        else:
            hi = mid
    # Check neighbors for closest
    best = changes[lo]
    if lo > 0 and abs(changes[lo - 1].rank - virtual_rank) < abs(best.rank - virtual_rank):
        best = changes[lo - 1]
    return best.oldRating + 4 * (best.newRating - best.oldRating)


async def _build_cfvc_rows(handle, dlo=0, dhi=10**10, source=cf.RANKLIST_SOURCE_STANDINGS):
    """Build performance rows for CF virtual participations (not TLE VCs).

    Caching strategy:
      - cfvc_cache stores (handle, contest_id, rank) — the per-user data that
        requires an API call to contest.standings.
      - Performance is computed on-the-fly from the shared rating_changes_cache,
        so all users benefit from the same cached rating data.
      - Only contest.standings calls for NEW virtual participations are made;
        already-cached ranks are reused instantly.

    Returns (rows, missing_count).
    """
    submissions = await cf.user.status(handle=handle)
    virtual_cids = set()
    for sub in submissions:
        if sub.author.participantType == 'VIRTUAL':
            cid = sub.contestId
            if cid is not None and cid < cf.GYM_ID_THRESHOLD:
                virtual_cids.add(cid)

    if not virtual_cids:
        return [], 0

    # Load cached ranks (per-user)
    cached_cids = cf_common.user_db.get_cfvc_cached_contest_ids(handle)
    cached_ranks = cf_common.user_db.get_cfvc_cache(handle)
    rank_by_cid = {cid: rank for cid, rank in cached_ranks}

    # Only fetch standings for contests not yet cached
    uncached_cids = sorted(virtual_cids - cached_cids)
    new_entries = []  # (contest_id, rank) to save

    missing = 0

    for cid in uncached_cids:
        try:
            _contest, _problems, ranklist = await cf.contest.standings(
                contest_id=cid, handles=[handle], show_unofficial=True, source=source)
        except Exception:
            missing += 1
            continue

        virtual_row = None
        for row in ranklist:
            if row.party.participantType == 'VIRTUAL':
                virtual_row = row
                break
        if virtual_row is None:
            missing += 1
            continue

        new_entries.append((cid, virtual_row.rank))
        rank_by_cid[cid] = virtual_row.rank

    # Persist newly fetched ranks
    if new_entries:
        cf_common.user_db.save_cfvc_cache(handle, new_entries)

    # Build rows — compute perf on-the-fly from shared rating changes cache
    rows = []
    for cid in sorted(virtual_cids):
        if cid not in rank_by_cid:
            continue
        rank = rank_by_cid[cid]

        # Date filter using contest start time from contest cache
        try:
            contest = cf_common.cache2.contest_cache.get_contest(cid)
            if contest.startTimeSeconds is not None:
                if not (dlo <= contest.startTimeSeconds < dhi):
                    continue
            contest_name = contest.name
        except Exception:
            contest_name = f'Contest {cid}'

        # Perf from shared rating changes cache (benefits all users)
        perf = _estimate_perf_from_cache(cid, rank)
        if perf is None:
            missing += 1
            continue

        rows.append({
            'idx': len(rows) + 1,
            'contest': _truncate_name(contest_name),
            'rank': rank,
            'old': None,
            'new': None,
            'delta': None,
            'perf': perf,
        })

    return rows, missing


def _format_cfvc_table(rows):
    """Format CF virtual contest rows (rank + perf only, no rating columns)."""
    style = table.Style('{:>}  {:<}  {:>}  {:>}')
    t = table.Table(style)
    t += table.Header('#', 'Contest', 'Rank', 'Perf')
    t += table.Line()
    for row in rows:
        t += table.Data(row['idx'], row['contest'], row['rank'], row['perf'])
    return str(t)


def _format_perftable(rows):
    """Format performance rows into a string table."""
    has_rank = any(r['rank'] is not None for r in rows)
    if has_rank:
        style = table.Style('{:>}  {:<}  {:>}  {:>}  {:>}  {:>}  {:>}')
    else:
        style = table.Style('{:>}  {:<}  {:>}  {:>}  {:>}  {:>}')

    t = table.Table(style)
    if has_rank:
        t += table.Header('#', 'Contest', 'Rank', 'Old', 'New', 'Δ', 'Perf')
    else:
        t += table.Header('#', 'Contest', 'Old', 'New', 'Δ', 'Perf')
    t += table.Line()
    for row in rows:
        delta_str = f'+{row["delta"]}' if row['delta'] >= 0 else str(row['delta'])
        if has_rank:
            rank_str = str(row['rank']) if row['rank'] is not None else '-'
            t += table.Data(row['idx'], row['contest'], rank_str,
                            row['old'], row['new'], delta_str, row['perf'])
        else:
            t += table.Data(row['idx'], row['contest'],
                            row['old'], row['new'], delta_str, row['perf'])
    return str(t)


class Graphs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()

    @commands.group(brief='Graphs for analyzing Codeforces activity',
                    invoke_without_command=True)
    async def plot(self, ctx):
        """Plot various graphs. Wherever Codeforces handles are accepted it is possible to
        use a server member's Discord name or @mention directly.
        Prefix -c to force a Codeforces handle (e.g. -ctourist)."""
        await ctx.send_help('plot')

    @plot.command(brief='Plot Codeforces rating graph', usage='[+zoom] [+number] [+peak] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def rating(self, ctx, *args: str):
        """Plots Codeforces rating graph for the handles provided."""

        (zoom, number, peak), args = cf_common.filter_flags(args, ['+zoom' , '+number', '+peak'])
        filt = cf_common.SubFilter()
        args = filt.parse(args)
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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


    @plot.command(brief='Plot Codeforces performance graph', aliases=['perf'], usage='[+zoom] [+peak] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def performance(self, ctx, *args: str):
        """Plots Codeforces performance graph for the handles provided."""

        (zoom, peak), args = cf_common.filter_flags(args, ['+zoom' , '+peak'])
        filt = cf_common.SubFilter()
        args = filt.parse(args)
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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




    @plot.command(brief='Plot Codeforces extremes graph',
                  usage='[handles] [+solved] [+unsolved] [+nolegend] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def extreme(self, ctx, *args: str):
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
        handle, = await cf_common.resolve_handles(ctx, self.converter, handles)
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

    @plot.command(brief="Show histogram of solved problems' rating on CF",
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [c+marker..] [i+index..]')
    async def solved(self, ctx, *args: str):
        """Shows a histogram of solved problems' rating on Codeforces for the handles provided.
        e.g. ;plot solved meooow +contest +virtual +outof +dp"""
        filt = cf_common.SubFilter()
        args = filt.parse(args)
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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

    @plot.command(brief='Show histogram of solved problems on CF over time',
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [phase_days=] [c+marker..] [i+index..]')
    async def hist(self, ctx, *args: str):
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
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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

    @plot.command(brief='Plot count of solved CF problems over time',
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [c+marker..] [i+index..]')
    async def curve(self, ctx, *args: str):
        """Plots the count of problems solved over time on Codeforces for the handles provided."""
        filt = cf_common.SubFilter()
        args = filt.parse(args)
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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

    @plot.command(brief='Show history of problems solved by rating',
                  aliases=['chilli'], usage='[handle] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [b=10] [s=3] [c+marker..] [i+index..] [+nolegend]')
    async def scatter(self, ctx, *args):
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
        handle, = await cf_common.resolve_handles(ctx, self.converter, (handle,))
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

    async def _rating_hist(self, ctx, ratings, mode, binsize, title):
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

    @plot.command(brief='Show server rating distribution')
    async def distrib(self, ctx):
        """Plots rating distribution of users in this server"""
        def in_purgatory(userid):
            member = ctx.guild.get_member(int(userid))
            return not member or 'Purgatory' in {role.name for role in member.roles}

        res = cf_common.user_db.get_cf_users_for_guild(ctx.guild.id)
        ratings = [cf_user.rating for user_id, cf_user in res
                   if cf_user.rating is not None and not in_purgatory(user_id)]
        await self._rating_hist(ctx,
                                ratings,
                                'normal',
                                binsize=100,
                                title='Rating distribution of server members')

    @plot.command(brief='Show Codeforces rating distribution', usage='[normal/log] [active/all] [contest_cutoff=5]')
    async def cfdistrib(self, ctx, mode: str = 'log', activity = 'active', contest_cutoff: int = 5):
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
        await self._rating_hist(ctx,
                                ratings,
                                mode,
                                binsize=100,
                                title=title)

    @plot.command(brief='Show percentile distribution on codeforces', usage='[+zoom] [+nomarker] [handles...] [+exact]')
    async def centile(self, ctx, *args: str):
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
                                                      self.converter,
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

    @plot.command(brief='Plot histogram of gudgiting')
    async def howgud(self, ctx, *members: discord.Member):
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

    @plot.command(brief='Plot distribution of server members by country')
    async def country(self, ctx, *countries):
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

    @plot.command(brief='Show rating changes by rank', usage='contest_id [+server] [+zoom] [handles..]')
    async def visualrank(self, ctx, contest_id: int, *args: str):
        """Plot rating changes by rank. Add handles to specify a handle in the plot.
        if arguments contains `+server`, it will include just server members and not all codeforces users.
        Specify `+zoom` to zoom to the neighborhood of handles."""

        args = set(args)
        (in_server, zoom), handles = cf_common.filter_flags(args, ['+server', '+zoom'])
        handles = await cf_common.resolve_handles(ctx, self.converter, handles, mincnt=0, maxcnt=20)

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

    @plot.command(brief='Show speed of solving problems by rating',
                  usage='[handles...] [+contest] [+virtual] [+outof] [+scatter] [+median] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [s=3]')
    async def speed(self, ctx, *args):
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
        handles = await cf_common.resolve_handles(ctx, self.converter, handles)
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

    @plot.command(brief='Show performance table for rated contests',
                  aliases=['ptable'],
                  usage='[+data] [+vc] [+cfvc] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def perftable(self, ctx, *args: str):
        """Show a table of contest performances.

        By default shows rated contest performances from Codeforces.
        Use +vc to show TLE virtual contest performances (accepts @mentions).
        Use +cfvc to show Codeforces virtual participations (rank + estimated perf).
        Use +data for a plain-text dump (uploaded as file if too long).
        Prefix -c to force a Codeforces handle (e.g. -ctourist)."""

        (data, vc, cfvc), args = cf_common.filter_flags(args, ['+data', '+vc', '+cfvc'])
        filt = cf_common.SubFilter()
        args = filt.parse(args)

        if cfvc:
            await self._perftable_cfvc(ctx, args, data, filt)
            return

        if vc:
            rows = await self._perftable_vc(ctx, args, filt)
        else:
            rows = await self._perftable_rated(ctx, args, filt)

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
            paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                               set_pagenum_footers=True, author_id=ctx.author.id)

    async def _perftable_cfvc(self, ctx, args, data, filt):
        """Fetch CF virtual participation performances."""
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles, maxcnt=1)
        handle = handles[0]

        await ctx.send(f'Fetching virtual participations for `{handle}`, this may take a moment...')
        source = cf_common.get_cf_ranklist_source(ctx.guild.id)
        rows, missing = await _build_cfvc_rows(handle, filt.dlo, filt.dhi, source=source)

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
            paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                               set_pagenum_footers=True, author_id=ctx.author.id)

    async def _perftable_rated(self, ctx, args, filt):
        """Fetch rated contest performances from CF API."""
        handles = args or ('!' + str(ctx.author.id),)
        handles = await cf_common.resolve_handles(ctx, self.converter, handles, maxcnt=1)
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

    async def _perftable_vc(self, ctx, args, filt):
        """Fetch virtual contest performances from the DB."""
        if args:
            member = await self.converter.convert(ctx, args[0])
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

    @commands.command(brief='Show rating at a given percentile',
                      aliases=['ratingat', 'ratat'],
                      usage='<percentile>')
    async def ratingfor(self, ctx, percentile: float):
        """Look up the Codeforces rating at the given percentile (0–100).

        Example: ;ratingfor 99.5 — what rating puts you in the top 0.5%?
        """
        if not (0 <= percentile <= 100):
            raise GraphCogError('Percentile must be between 0 and 100.')

        ratings = cf_common.cache2.rating_changes_cache.get_all_ratings()
        rating = _rating_at_percentile(ratings, percentile)
        if rating is None:
            raise GraphCogError('No rating data available.')

        rank = cf.rating2rank(rating)
        color = rank.color_embed if rank.color_embed is not None else 0xffaa10
        embed = discord.Embed(
            title=f'Rating at {percentile:g} percentile',
            description=f'**{rating}** ({rank.title}) — computed over {len(ratings)} rated users.',
            color=color,
        )
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed)

    @discord_common.send_error_if(GraphCogError, cf_common.ResolveHandleError,
                                  cf_common.FilterError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Graphs(bot))

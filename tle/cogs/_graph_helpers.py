"""Plotting and data primitives for the Graphs cog.

These module-level helpers were split out of ``tle/cogs/graphs.py`` to keep
every file under the 500-line limit. ``graphs.py`` re-exports the public
symbols it needs, so existing imports such as
``from tle.cogs.graphs import _rating_at_percentile`` keep working.
"""

import datetime as dt

from discord.ext import commands
from matplotlib import pyplot as plt
from matplotlib import lines as mlines

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import graph_common as gc


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

"""Rating/performance line plots for minigames (Akari & Queens)."""

from matplotlib import pyplot as plt

from tle.util import graph_common as gc
from tle.util.akari_rating import AKARI_RANKS
from tle.cogs._minigame_common import normalize_puzzle_date


def _plot_akari_multi(series, legend_entries):
    """Shared body for the rating and performance graphs.

    ``series`` is a list of ``(dates, values, marker_indices)`` triples — one
    triple per user plotted.  ``marker_indices=None`` means "marker on every
    point" (the default look); a list restricts markers to those indices so
    decay days only contribute to the line.  ``legend_entries`` is a list of
    ``(display_name, legend_value)`` pairs in the same order as ``series``.

    Paints the Akari tier bands once underneath all lines and sets the
    y-window to span every series' values.  Lines pick consecutive colours
    from the rating colour cycle — same palette as CF's rating graph.
    """
    plt.clf()
    plt.axes().set_prop_cycle(gc.rating_color_cycler)
    all_values = []
    for dates, values, marker_indices in series:
        markevery = (list(marker_indices)
                     if marker_indices is not None else None)
        plt.plot(dates, values, linestyle='-', marker='o', markersize=3,
                 markerfacecolor='white', markeredgewidth=0.5,
                 markevery=markevery)
        all_values.extend(values)

    plt.ylim(min(min(all_values) - 50, 1100), max(max(all_values) + 50, 1500))
    gc.plot_rating_bg(AKARI_RANKS)

    plt.gcf().autofmt_xdate()
    labels = [gc.StrWrap(f'{name} ({round(value)})')
              for name, value in legend_entries]
    # One legend column for a single user (preserves the original look);
    # scale up modestly for multi-user so labels don't stack vertically and
    # eat the plot area.
    ncol = min(len(labels), 3) if len(labels) > 1 else 1
    plt.legend(labels, bbox_to_anchor=(0, 1, 1, 0), loc='lower left',
               mode='expand', ncol=ncol)

    return gc.get_current_figure_as_file()


def plot_akari_rating(series):
    """Plot Daily Akari rating over time for one or more users.

    ``series`` is a list of ``(history, display_name)`` pairs.  Each
    ``history`` is the list of :class:`HistoryPoint` returned by
    ``compute_ratings(histories=...)`` for that user.  Default mode draws a
    line + markers per user; ``+decay`` histories (containing
    ``is_decay=True`` points) anchor markers only on played days so the
    inactivity slope is visible without losing the played-day emphasis.

    Single-user is just the trivial ``len(series) == 1`` case — it still
    looks like the previous single-user graph.
    """
    plotted = []
    legend_entries = []
    for history, display_name in series:
        dates = [normalize_puzzle_date(h.puzzle_date) for h in history]
        ratings = [h.rating for h in history]
        has_decay = any(getattr(h, 'is_decay', False) for h in history)
        marker_indices = (
            [i for i, h in enumerate(history) if not getattr(h, 'is_decay', False)]
            if has_decay else None
        )
        plotted.append((dates, ratings, marker_indices))
        legend_entries.append((display_name, ratings[-1]))
    return _plot_akari_multi(plotted, legend_entries)


def plot_akari_performance(series):
    """Plot per-contest performance over time for one or more users.

    ``series`` is a list of ``(history, display_name, current_rating)``.
    Solo days (no field → ``performance=None``) are dropped per user.
    Raises ``ValueError`` if *every* user is solo-only (nothing to plot).

    The legend shows each user's *current rating*, not the latest
    performance point, to match the look of :func:`plot_akari_rating`.
    """
    plotted = []
    legend_entries = []
    for history, display_name, current_rating in series:
        points = [(normalize_puzzle_date(h.puzzle_date), h.performance)
                  for h in history if h.performance is not None]
        if not points:
            continue  # skip users with no contest days
        dates, perfs = zip(*points)
        plotted.append((list(dates), list(perfs), None))
        legend_entries.append((display_name, current_rating))
    if not plotted:
        raise ValueError('No contest days to plot performance for.')
    return _plot_akari_multi(plotted, legend_entries)

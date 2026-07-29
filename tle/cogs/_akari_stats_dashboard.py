"""Light seven-day player dashboard for Daily Akari."""

import datetime as dt
import statistics

from matplotlib import dates as mdates
from matplotlib import pyplot as plt

from tle.util import graph_common as gc
from tle.cogs._minigame_stats_text import (
    draw_player_name,
    safe_player_name as _safe_player_name,
)
from tle.cogs._minigame_common import (
    compute_longest_streak,
    compute_streak,
    format_duration,
    normalize_puzzle_date,
    pick_best_results,
)


_BG = '#F3F7F5'
_PANEL = '#FFFFFF'
_GRID = '#DCE6E1'
_TEXT = '#18251F'
_MUTED = '#66756D'
_GREEN = '#16845B'
_GREEN_DARK = '#0C6444'
_BLUE = '#356B9E'
_AMBER = '#A46100'
_RED = '#C63C55'
_WEEKDAYS = ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN')


def _percentile(values, fraction):
    values = sorted(float(value) for value in values)
    if not values:
        return 0.0
    position = (len(values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _chart_time_cap(values):
    """Choose a robust chart ceiling so one slow run cannot flatten the plot."""
    values = [max(0, int(value)) for value in values]
    if len(values) < 4:
        return max(values, default=1), False
    median = statistics.median(values)
    q3 = _percentile(values, .75)
    robust_limit = max(1, median * 3, q3 * 1.5)
    maximum = max(values)
    return min(maximum, robust_limit), maximum > robust_limit


def _best_results_by_date(results):
    return pick_best_results(
        results,
        group_key_fn=lambda row: normalize_puzzle_date(row.puzzle_date),
    )


def _akari_dashboard_data(results, weekdays=None, as_of_date=None):
    """Prepare renderer-independent Akari dashboard values."""
    best = _best_results_by_date(results)
    ordered = [best[day] for day in sorted(best)]
    latest = ordered[-1]
    first_day = normalize_puzzle_date(ordered[0].puzzle_date)
    latest_day = normalize_puzzle_date(latest.puzzle_date)
    logical_today = as_of_date or dt.date.today()
    current_week = logical_today - dt.timedelta(days=logical_today.weekday())
    if latest_day >= current_week - dt.timedelta(days=7):
        anchor = logical_today
        view_date = logical_today
    else:
        anchor = latest_day
        view_date = latest_day
    week_start = anchor - dt.timedelta(days=anchor.weekday())
    week_days = [week_start + dt.timedelta(days=offset) for offset in range(7)]
    week_rows = {day: best.get(day) for day in week_days}
    previous_rows = [
        best.get(day - dt.timedelta(days=7))
        for day in week_days
        if weekdays is None or day.weekday() in weekdays
    ]
    previous_rows = [row for row in previous_rows if row is not None]

    clean_results = [row for row in ordered if bool(row.is_perfect)]
    clean_times = [
        int(row.time_seconds) for row in clean_results
        if int(row.time_seconds) > 0
    ]
    clean_count = len(clean_results)

    weekday_stats = []
    for weekday in range(7):
        rows = [
            row for row in ordered
            if normalize_puzzle_date(row.puzzle_date).weekday() == weekday
        ]
        clean_rows = [row for row in rows if bool(row.is_perfect)]
        day_clean_times = [
            int(row.time_seconds) for row in clean_rows
            if int(row.time_seconds) > 0
        ]
        weekday_stats.append({
            'count': len(rows),
            'clean': len(clean_rows),
            'clean_rate': (
                100 * len(clean_rows) / len(rows) if rows else 0),
            'clean_median': (
                statistics.median(day_clean_times)
                if day_clean_times else None),
        })

    return {
        'results': ordered,
        'clean_results': clean_results,
        'total': len(ordered),
        'clean': clean_count,
        'clean_rate': 100 * clean_count / len(ordered),
        'clean_times': clean_times,
        'best_clean_time': min(clean_times) if clean_times else None,
        'median_clean_time': (
            statistics.median(clean_times) if clean_times else None),
        'current_streak': compute_streak(ordered, weekdays),
        'longest_streak': compute_longest_streak(ordered, weekdays),
        'first_day': first_day,
        'latest_day': latest_day,
        'view_date': view_date,
        'week_start': week_start,
        'week_days': week_days,
        'week_rows': week_rows,
        'previous_rows': previous_rows,
        'weekday_stats': weekday_stats,
    }


def _style_axis(ax):
    ax.set_facecolor(_PANEL)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(colors=_MUTED, labelsize=8, length=0)


def _add_kpi(fig, x, label, value, detail, color):
    ax = fig.add_axes((x, .745, .215, .115))
    _style_axis(ax)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.text(.06, .77, label, transform=ax.transAxes, color=_MUTED,
            fontsize=8, fontweight='bold', va='center')
    ax.text(.06, .42, value, transform=ax.transAxes, color=color,
            fontsize=20, fontweight='bold', va='center')
    ax.text(.06, .12, detail, transform=ax.transAxes, color=_TEXT,
            fontsize=8, va='center')
    ax.axvline(0, color=color, linewidth=5)


def _week_status(row, day, view_date):
    if row is not None:
        if bool(row.is_perfect):
            return 'CLEAN', _GREEN
        return f'{int(row.accuracy)}% ACC', _AMBER
    if day > view_date:
        return 'UP NEXT', _MUTED
    if day == view_date:
        return 'OPEN', _BLUE
    return 'MISSED', _RED


def _draw_week_strip(fig, data, weekdays):
    allowed = set(range(7) if weekdays is None else weekdays)
    for index, day in enumerate(data['week_days']):
        ax = fig.add_axes((.04 + index * .132, .535, .118, .145))
        _style_axis(ax)
        ax.set_xticks([])
        ax.set_yticks([])
        row = data['week_rows'][day]
        if index not in allowed:
            status, color = 'OFF', _MUTED
            time_text = '—'
        else:
            status, color = _week_status(row, day, data['view_date'])
            time_text = (
                format_duration(row.time_seconds) if row is not None
                and int(row.time_seconds) > 0 else '—'
            )
        ax.axhline(1, color=color, linewidth=5)
        ax.text(.08, .78, _WEEKDAYS[index], transform=ax.transAxes,
                color=_TEXT, fontsize=9, fontweight='bold')
        ax.text(.92, .78, day.strftime('%d'), transform=ax.transAxes,
                color=_MUTED, fontsize=8, ha='right')
        ax.text(.08, .43, time_text, transform=ax.transAxes,
                color=color if row is not None else _MUTED,
                fontsize=14, fontweight='bold')
        ax.text(.08, .12, status, transform=ax.transAxes, color=color,
                fontsize=7, fontweight='bold')


def _rolling_average(values, window):
    return [
        sum(values[index - window + 1:index + 1]) / window
        for index in range(window - 1, len(values))
    ]


def _draw_trend(fig, data):
    ax = fig.add_axes((.04, .09, .575, .375))
    _style_axis(ax)
    rows = [
        row for row in data['results'][-35:]
        if int(row.time_seconds) > 0
    ]
    times = [int(row.time_seconds) for row in rows]
    cap, clipped = _chart_time_cap(times)

    clean_rows = [row for row in rows if bool(row.is_perfect)]
    partial_rows = [row for row in rows if not bool(row.is_perfect)]
    clean_dates = [
        normalize_puzzle_date(row.puzzle_date) for row in clean_rows]
    partial_dates = [
        normalize_puzzle_date(row.puzzle_date) for row in partial_rows]
    clean_times = [int(row.time_seconds) for row in clean_rows]
    partial_times = [int(row.time_seconds) for row in partial_rows]
    clean_plotted = [min(value, cap) for value in clean_times]
    partial_plotted = [min(value, cap) for value in partial_times]

    if clean_rows:
        ax.scatter(
            clean_dates, clean_plotted, color=_GREEN, s=34, alpha=.92,
            edgecolors=_PANEL, linewidths=.6, label='Clean', zorder=4)
    if partial_rows:
        ax.scatter(
            partial_dates, partial_plotted, color=_AMBER, marker='x', s=34,
            linewidths=1.2, label='Imperfect', zorder=3)
    if len(clean_rows) >= 3:
        window = min(7, max(3, len(clean_rows) // 3))
        rolling = _rolling_average(clean_plotted, window)
        ax.plot(
            clean_dates[window - 1:], rolling, color=_GREEN_DARK,
            linewidth=2.2, label=f'{window}-clean pace', zorder=5)
    elif len(clean_rows) > 1:
        ax.plot(clean_dates, clean_plotted, color=_GREEN_DARK, linewidth=1,
                alpha=.65, zorder=2)

    if clipped:
        outlier_dates = [
            normalize_puzzle_date(row.puzzle_date)
            for row in rows if int(row.time_seconds) > cap
        ]
        ax.scatter(outlier_dates, [cap] * len(outlier_dates),
                   marker='v', color=_RED, s=50, zorder=6)
        ax.text(.98, .04, f'{len(outlier_dates)} slow outlier(s) clipped',
                transform=ax.transAxes, color=_RED, fontsize=7, ha='right')
    if len(clean_rows) <= 1:
        ax.text(.5, .88, 'A few more clean runs unlock the pace line',
                transform=ax.transAxes, color=_MUTED, fontsize=8, ha='center')

    all_dates = clean_dates + partial_dates
    ax.set_ylim(max(1, cap * 1.15), 0)
    ax.grid(axis='y', color=_GRID, linewidth=.7, alpha=.8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    try:
        from matplotlib.ticker import FuncFormatter
    except ImportError:
        FuncFormatter = None
    if FuncFormatter is not None:
        ax.yaxis.set_major_formatter(
            FuncFormatter(lambda value, _position: format_duration(value)))
    if len(all_dates) == 1:
        ax.set_xlim(
            all_dates[0] - dt.timedelta(days=3),
            all_dates[0] + dt.timedelta(days=3))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    else:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(minticks=3, maxticks=6))
    ax.set_ylabel('TIME  ·  FASTER ↑', color=_MUTED,
                  fontsize=8, fontweight='bold')
    ax.set_title('PACE LAB  ·  LAST 35 RESULTS', color=_TEXT, fontsize=10,
                 fontweight='bold', loc='left', pad=13)
    if rows:
        legend = ax.legend(loc='upper right', frameon=False, fontsize=8)
        for text in legend.get_texts():
            text.set_color(_MUTED)


def _weekday_color(clean_rate):
    if clean_rate >= 80:
        return _GREEN
    if clean_rate >= 50:
        return _BLUE
    return _AMBER


def _draw_weekday_dna(fig, data):
    ax = fig.add_axes((.66, .09, .30, .375))
    _style_axis(ax)
    stats = data['weekday_stats']
    strengths = [
        max(.025, item['clean_rate'] / 100) if item['count'] else 0
        for item in stats
    ]
    colors = [
        _weekday_color(item['clean_rate']) if item['count'] else _GRID
        for item in stats
    ]
    bars = ax.barh(range(7), strengths, height=.57, color=colors)
    ax.set_yticks(range(7))
    ax.set_yticklabels(_WEEKDAYS, color=_TEXT, fontsize=8,
                       fontweight='bold')
    ax.invert_yaxis()
    ax.set_xlim(0, 1.43)
    ax.set_xticks([])
    for bar, item, strength in zip(bars, stats, strengths):
        y = bar.get_y() + bar.get_height() / 2
        if not item['count']:
            ax.text(.04, y, 'NO DATA', va='center', color=_MUTED,
                    fontsize=7)
            continue
        label = f'{item["clean"]}/{item["count"]} CLEAN'
        ax.text(.04, y, label, va='center',
                color=_PANEL if strength >= .39 else _TEXT,
                fontsize=7, fontweight='bold')
        median = item['clean_median']
        median_text = (
            f'{format_duration(median)} MEDIAN' if median is not None
            else 'NO CLEAN PACE')
        ax.text(1.40, y, median_text, va='center', ha='right',
                color=_TEXT, fontsize=7)
    ax.set_title('DAY DNA  ·  CLEAN RATE + PACE', color=_TEXT, fontsize=10,
                 fontweight='bold', loc='left', pad=13)


def _duration_or_dash(value):
    return format_duration(value) if value is not None else '—'


def plot_akari_stats(results, display_name, weekdays=None, *,
                     as_of_date=None):
    """Render a light, Discord-friendly seven-day Akari dashboard."""
    data = _akari_dashboard_data(results, weekdays, as_of_date)
    fig = plt.figure(figsize=(16, 10), facecolor=_BG)

    header = fig.add_axes((.04, .89, .92, .075))
    header.set_facecolor(_BG)
    header.axis('off')
    header.text(0, .72, 'AKARI  /  7-DAY PLAYER DASHBOARD',
                transform=header.transAxes, color=_GREEN, fontsize=10,
                fontweight='bold')
    draw_player_name(
        header, display_name, xy=(0, .08), transform=header.transAxes,
        color=_TEXT, fontsize=24, max_width_px=760)
    header.text(1, .18, f'LATEST WEEK  ·  {data["week_start"]:%b %d, %Y}',
                transform=header.transAxes, color=_MUTED, fontsize=9,
                fontweight='bold', ha='right')

    _add_kpi(
        fig, .04, 'RUNS LOGGED', str(data['total']),
        f'since {data["first_day"]:%b %d, %Y}', _BLUE)
    _add_kpi(
        fig, .275, 'CLEAN RATE', f'{data["clean_rate"]:.0f}%',
        f'{data["clean"]}/{data["total"]} clean results', _GREEN)
    _add_kpi(
        fig, .51, 'CLEAN PERSONAL BEST',
        _duration_or_dash(data['best_clean_time']),
        f'median {_duration_or_dash(data["median_clean_time"])}', _AMBER)
    _add_kpi(
        fig, .745, 'CLEAN STREAK', str(data['current_streak']),
        f'longest {data["longest_streak"]} days', _RED)

    allowed = set(range(7) if weekdays is None else weekdays)
    eligible_days = [
        day for day in data['week_days']
        if day.weekday() in allowed and day <= data['view_date']
    ]
    week_rows = [
        data['week_rows'][day] for day in eligible_days
        if data['week_rows'][day] is not None
    ]
    selected_days = [
        day for day in data['week_days']
        if day.weekday() in allowed
    ]
    progress_total = len(eligible_days) or len(selected_days)
    week_clean_rows = [row for row in week_rows if bool(row.is_perfect)]
    week_summary = (
        f'{len(week_rows)}/{progress_total} PLAYED  ·  '
        f'{len(week_clean_rows)} CLEAN'
    )
    week_clean_times = [
        int(row.time_seconds) for row in week_clean_rows
        if int(row.time_seconds) > 0
    ]
    if week_clean_times:
        week_summary += f'  ·  {format_duration(min(week_clean_times))} BEST'
    previous_clean_times = [
        int(row.time_seconds) for row in data['previous_rows']
        if bool(row.is_perfect) and int(row.time_seconds) > 0
    ]
    if len(week_clean_times) >= 2 and len(previous_clean_times) >= 2:
        change = round(
            statistics.median(previous_clean_times)
            - statistics.median(week_clean_times))
        if change:
            direction = 'FASTER' if change > 0 else 'SLOWER'
            week_summary += (
                f'  ·  {format_duration(abs(change))} '
                f'{direction} VS LAST WEEK')
    fig.text(.04, .695, week_summary, color=_TEXT, fontsize=9,
             fontweight='bold')

    _draw_week_strip(fig, data, weekdays)
    _draw_trend(fig, data)
    _draw_weekday_dna(fig, data)
    fig.text(
        .04, .035,
        'CLEAN TIMES DRIVE THE PACE LINE  ·  WEEK RUNS MONDAY → SUNDAY',
        color=_MUTED, fontsize=7, fontweight='bold')

    plt.sca(header)
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file

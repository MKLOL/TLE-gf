"""Seven-day player dashboard for LinkedIn Queens."""

import datetime as dt
import os
import statistics
import unicodedata

from matplotlib import dates as mdates
from matplotlib import pyplot as plt

from tle.util import graph_common as gc
from tle.cogs._minigame_common import format_duration, normalize_puzzle_date
from tle.cogs._minigame_queens_cog import (
    _queens_best_results_by_date, _queens_current_puzzle_date,
    _queens_streak_info,
)


_BG = '#090B14'
_PANEL = '#121625'
_GRID = '#272D43'
_TEXT = '#F7F5FF'
_MUTED = '#9198AE'
_PURPLE = '#A78BFA'
_PURPLE_DARK = '#6D4AD2'
_TEAL = '#39D6B4'
_AMBER = '#FFBE5C'
_RED = '#FF718B'
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
    """Choose a robust cap so one extreme result cannot flatten a chart."""
    values = [max(0, int(value)) for value in values]
    if len(values) < 4:
        return max(values, default=1), False
    median = statistics.median(values)
    q3 = _percentile(values, .75)
    # Tukey's usual outer fence is too permissive for tiny histories because
    # one outlier influences Q3 heavily. This tighter ceiling keeps the normal
    # runs legible while the clipped marker still calls out the real outlier.
    robust_limit = max(1, median * 3, q3 * 1.5)
    maximum = max(values)
    return min(maximum, robust_limit), maximum > robust_limit


def _safe_player_name(value, limit=42):
    """Keep headings readable and omit emoji glyphs matplotlib cannot render."""
    cleaned = ''.join(
        char for char in str(value)
        if ord(char) <= 0xFFFF
        and unicodedata.category(char) != 'So'
        and char not in ('\u200d', '\ufe0f')
    ).strip()
    cleaned = cleaned or 'Player'
    return cleaned if len(cleaned) <= limit else f'{cleaned[:limit - 1]}…'


def _font_kwargs():
    font_path = getattr(gc.fontprop, 'get_file', lambda: None)()
    if font_path and os.path.isfile(font_path):
        return {'fontproperties': gc.fontprop}
    try:
        from matplotlib import font_manager
        installed = {font.name for font in font_manager.fontManager.ttflist}
    except (AttributeError, ImportError):
        installed = set()
    for family in (
            'Noto Sans CJK JP', 'Arial Unicode MS', 'Hiragino Sans GB',
            'DejaVu Sans'):
        if family in installed:
            return {'fontfamily': family}
    return {'fontfamily': 'sans-serif'}


def _queens_dashboard_data(results, weekdays=None, as_of_date=None):
    """Prepare stable, renderer-independent dashboard values."""
    best = _queens_best_results_by_date(results)
    ordered = [best[day] for day in sorted(best)]
    current, longest, latest = _queens_streak_info(ordered, weekdays)
    latest_day = normalize_puzzle_date(latest.puzzle_date)
    logical_today = as_of_date or _queens_current_puzzle_date()
    current_week = logical_today - dt.timedelta(days=logical_today.weekday())
    # Active players see this week's tracker. Historical/inactive selections
    # remain anchored to their latest populated week instead of seven blanks.
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
    times = [
        int(row.time_seconds) for row in ordered
        if int(row.time_seconds) > 0
    ]
    clean = sum(bool(row.is_perfect) for row in ordered)

    weekday_stats = []
    for weekday in range(7):
        rows = [
            row for row in ordered
            if normalize_puzzle_date(row.puzzle_date).weekday() == weekday
        ]
        day_times = [
            int(row.time_seconds) for row in rows
            if int(row.time_seconds) > 0
        ]
        weekday_stats.append({
            'count': len(rows),
            'median': statistics.median(day_times) if day_times else None,
            'clean_rate': (
                100 * sum(bool(row.is_perfect) for row in rows) / len(rows)
                if rows else 0
            ),
        })

    return {
        'results': ordered,
        'total': len(ordered),
        'times': times,
        'clean': clean,
        'clean_rate': 100 * clean / len(ordered),
        'no_mistakes': sum(int(row.accuracy) == 100 for row in ordered),
        'best_time': min(times) if times else None,
        'median_time': statistics.median(times) if times else None,
        'current_streak': current,
        'longest_streak': longest,
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
            return 'CLEAN', _TEAL
        if int(row.accuracy) == 100:
            return 'NO MISTAKES', _PURPLE
        return 'COMPLETED', _AMBER
    if day > view_date:
        return 'UP NEXT', _MUTED
    if day == view_date:
        return 'OPEN', _PURPLE
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
    dates = [normalize_puzzle_date(row.puzzle_date) for row in rows]
    times = [int(row.time_seconds) for row in rows]
    cap, clipped = _chart_time_cap(times)
    plotted = [min(time, cap) for time in times]
    colors = [_TEAL if row.is_perfect else _PURPLE for row in rows]
    ax.scatter(dates, plotted, c=colors, s=30, alpha=.9,
               edgecolors=_BG, linewidths=.5, zorder=3)
    if len(rows) >= 5:
        window = min(7, max(3, len(rows) // 3))
        # Average the displayed values: a clipped outlier should be visible,
        # but must not pin several subsequent pace points to the chart floor.
        rolling = _rolling_average(plotted, window)
        ax.plot(dates[window - 1:], rolling, color=_AMBER,
                linewidth=2.2, label=f'{window}-run pace', zorder=4)
    elif len(rows) > 1:
        ax.plot(dates, plotted, color=_PURPLE_DARK, linewidth=1,
                alpha=.65, zorder=2)
    if clipped:
        outlier_dates = [
            day for day, value in zip(dates, times) if value > cap
        ]
        ax.scatter(outlier_dates, [cap] * len(outlier_dates),
                   marker='v', color=_RED, s=50, zorder=5)
        ax.text(.98, .04, f'{len(outlier_dates)} slow outlier(s) clipped',
                transform=ax.transAxes, color=_RED, fontsize=7, ha='right')
    if len(rows) <= 1:
        ax.text(.5, .88, 'A few more runs unlock the pace line',
                transform=ax.transAxes, color=_MUTED, fontsize=8, ha='center')
    ax.set_ylim(max(1, cap * 1.15), 0)
    ax.grid(axis='y', color=_GRID, linewidth=.7, alpha=.8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    if len(dates) == 1:
        ax.set_xlim(
            dates[0] - dt.timedelta(days=3),
            dates[0] + dt.timedelta(days=3))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    else:
        ax.xaxis.set_major_locator(
            mdates.AutoDateLocator(minticks=3, maxticks=6))
    ax.set_ylabel('TIME  ·  FASTER ↑', color=_MUTED,
                  fontsize=8, fontweight='bold')
    ax.set_title('PACE LAB  ·  LAST 35 RUNS', color=_TEXT, fontsize=10,
                 fontweight='bold', loc='left', pad=13)
    if len(rows) >= 5:
        legend = ax.legend(loc='upper right', frameon=False, fontsize=8)
        for text in legend.get_texts():
            text.set_color(_MUTED)


def _weekday_color(clean_rate):
    if clean_rate >= 80:
        return _TEAL
    if clean_rate >= 45:
        return _PURPLE
    return _AMBER


def _draw_weekday_dna(fig, data):
    ax = fig.add_axes((.66, .09, .30, .375))
    _style_axis(ax)
    stats = data['weekday_stats']
    medians = [item['median'] for item in stats if item['median'] is not None]
    fastest = min(medians, default=1)
    strengths = [
        max(.08, min(1, fastest / item['median']))
        if item['median'] else 0
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
    ax.set_xlim(0, 1.42)
    ax.set_xticks([])
    for bar, item, strength in zip(bars, stats, strengths):
        y = bar.get_y() + bar.get_height() / 2
        if not item['count']:
            ax.text(.04, y, 'NO DATA', va='center', color=_MUTED,
                    fontsize=7)
            continue
        ax.text(.04, y, f'{item["clean_rate"]:.0f}% clean',
                va='center', color=_BG if strength >= .43 else _TEXT,
                fontsize=7, fontweight='bold')
        ax.text(1.39, y,
                f'{format_duration(item["median"])}  ·  {item["count"]}x',
                va='center', ha='right', color=_TEXT, fontsize=7)
    ax.set_title('DAY DNA  ·  SPEED + CLEAN RATE', color=_TEXT, fontsize=10,
                 fontweight='bold', loc='left', pad=13)


def _duration_or_dash(value):
    return format_duration(value) if value is not None else '—'


def plot_queens_stats(results, display_name, *, title_suffix='',
                      weekdays=None, as_of_date=None):
    """Render a dark, Discord-friendly seven-day Queens dashboard."""
    data = _queens_dashboard_data(results, weekdays, as_of_date)
    fig = plt.figure(figsize=(16, 10), facecolor=_BG)

    header = fig.add_axes((.04, .89, .92, .075))
    header.set_facecolor(_BG)
    header.axis('off')
    header.text(0, .72, 'QUEENS  /  7-DAY PLAYER DASHBOARD',
                transform=header.transAxes, color=_PURPLE, fontsize=10,
                fontweight='bold')
    header.text(0, .08, _safe_player_name(display_name),
                transform=header.transAxes, color=_TEXT, fontsize=24,
                **_font_kwargs())
    header.text(1, .18, f'LATEST WEEK  ·  {data["week_start"]:%b %d, %Y}',
                transform=header.transAxes, color=_MUTED, fontsize=9,
                fontweight='bold', ha='right')
    if title_suffix:
        header.text(1, .65, str(title_suffix).strip(),
                    transform=header.transAxes, color=_AMBER, fontsize=8,
                    ha='right')

    _add_kpi(
        fig, .04, 'RUNS LOGGED', str(data['total']),
        f'{data["no_mistakes"]} with no mistakes', _PURPLE)
    _add_kpi(
        fig, .275, 'CLEAN RATE', f'{data["clean_rate"]:.0f}%',
        f'{data["clean"]} no-hint clean solves', _TEAL)
    _add_kpi(
        fig, .51, 'PERSONAL BEST', _duration_or_dash(data['best_time']),
        f'median {_duration_or_dash(data["median_time"])}', _AMBER)
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
    previous_rows = data['previous_rows']
    week_best = min(
        (int(row.time_seconds) for row in week_rows
         if int(row.time_seconds) > 0),
        default=None)
    selected_days = [
        day for day in data['week_days']
        if day.weekday() in allowed
    ]
    progress_total = len(eligible_days) or len(selected_days)
    week_summary = (
        f'{len(week_rows)}/{progress_total} PLAYED  ·  '
        f'{sum(bool(row.is_perfect) for row in week_rows)} CLEAN'
    )
    if week_best is not None:
        week_summary += f'  ·  {format_duration(week_best)} BEST'
    current_times = [
        int(row.time_seconds) for row in week_rows
        if int(row.time_seconds) > 0
    ]
    previous_times = [
        int(row.time_seconds) for row in previous_rows
        if int(row.time_seconds) > 0
    ]
    if len(current_times) >= 2 and len(previous_times) >= 2:
        change = round(
            statistics.median(previous_times)
            - statistics.median(current_times))
        if change:
            direction = 'FASTER' if change > 0 else 'SLOWER'
            week_summary += f'  ·  {abs(change)}s {direction} VS LAST WEEK'
    fig.text(.04, .695, week_summary, color=_TEXT, fontsize=9,
             fontweight='bold')

    _draw_week_strip(fig, data, weekdays)
    _draw_trend(fig, data)
    _draw_weekday_dna(fig, data)
    fig.text(
        .04, .035,
        'CLEAN = NO HINTS + NO MISTAKES  ·  WEEK RUNS MONDAY → SUNDAY',
        color=_MUTED, fontsize=7, fontweight='bold')

    # graph_common derives the save background from the current axes. Keep
    # the header current so the canvas uses the intended darker outer color,
    # leaving the KPI/week/chart panels visibly raised from it.
    plt.sca(header)
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file

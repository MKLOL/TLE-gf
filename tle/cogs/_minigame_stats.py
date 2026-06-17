"""Stats plotting for minigames (Akari, Queens & GuessThe.Game)."""

import datetime as dt
from collections import Counter

import numpy as np
from matplotlib import pyplot as plt
from matplotlib import dates as mdates

from tle.util import graph_common as gc
from tle.cogs._minigame_common import (
    compute_streak, compute_longest_streak, pick_best_results,
    normalize_puzzle_date, format_duration,
)
# Re-exported so callers (and the test suite) keep importing the rating /
# performance line plots from this module after they moved to _stats_plots.
from tle.cogs._minigame_stats_plots import (  # noqa: F401
    _plot_akari_multi, plot_akari_rating, plot_akari_performance,
)

# ── Akari ──────────────────────────────────────────────────────────────

_AKARI_TIME_BINS = [0, 30, 60, 90, 120, 180, 300, 600, float('inf')]
_AKARI_TIME_LABELS = ['<30s', '30-60s', '1-1.5m', '1.5-2m', '2-3m', '3-5m', '5-10m', '10m+']
_WEEKDAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def _bin_times(times):
    counts = [0] * len(_AKARI_TIME_LABELS)
    for t in times:
        for i in range(len(_AKARI_TIME_BINS) - 1):
            if _AKARI_TIME_BINS[i] <= t < _AKARI_TIME_BINS[i + 1]:
                counts[i] += 1
                break
    return counts


def plot_akari_stats(rows, display_name):
    """Generate a multi-panel Akari stats image. Returns a discord.File."""
    best = pick_best_results(rows)
    results = sorted(best.values(), key=lambda r: normalize_puzzle_date(r.puzzle_date))

    total = len(results)
    perfects = [r for r in results if r.is_perfect]
    imperfects = [r for r in results if not r.is_perfect]
    perfect_count = len(perfects)
    perfect_rate = perfect_count / total * 100 if total else 0

    all_times = [r.time_seconds for r in results]
    perfect_times = [r.time_seconds for r in perfects]

    streak = compute_streak(rows)
    longest = compute_longest_streak(rows)

    best_time = min(perfect_times) if perfect_times else min(all_times) if all_times else 0
    avg_time = sum(perfect_times) / len(perfect_times) if perfect_times else 0
    median_time = float(np.median(perfect_times)) if perfect_times else 0

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(f'{display_name} — Akari Stats', fontsize=16, fontweight='bold', y=0.97)

    # ── Panel 1: Summary text ──
    ax = axes[0, 0]
    ax.axis('off')
    lines = [
        f'Total puzzles:  {total}',
        f'Perfect:  {perfect_count}  ({perfect_rate:.0f}%)',
        f'Imperfect:  {len(imperfects)}',
        '',
        f'Best time (perfect):  {format_duration(best_time)}',
        f'Avg time (perfect):  {format_duration(avg_time)}',
        f'Median time (perfect):  {format_duration(median_time)}',
        '',
        f'Current streak:  {streak}',
        f'Longest streak:  {longest}',
    ]
    ax.text(0.08, 0.92, '\n'.join(lines), transform=ax.transAxes,
            fontsize=13, verticalalignment='top', fontfamily='monospace',
            linespacing=1.6)
    ax.set_title('Overview', fontsize=13, fontweight='bold')

    # ── Panel 2: Time distribution histogram ──
    ax = axes[0, 1]
    counts = _bin_times(perfect_times if perfect_times else all_times)
    label = 'perfect solves' if perfect_times else 'all solves'
    colors = ['#4CAF50' if c > 0 else '#ccc' for c in counts]
    bars = ax.bar(range(len(counts)), counts, color=colors, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(len(_AKARI_TIME_LABELS)))
    ax.set_xticklabels(_AKARI_TIME_LABELS, rotation=30, ha='right', fontsize=9)
    ax.set_ylabel('Count')
    ax.set_title(f'Time Distribution ({label})', fontsize=13, fontweight='bold')
    for bar, c in zip(bars, counts):
        if c > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    str(c), ha='center', va='bottom', fontsize=9)

    # ── Panel 3: Accuracy distribution ──
    ax = axes[1, 0]
    accuracy_counts = Counter()
    for r in results:
        if r.accuracy == 100:
            accuracy_counts['100%'] += 1
        elif r.accuracy >= 90:
            accuracy_counts['90-99%'] += 1
        elif r.accuracy >= 80:
            accuracy_counts['80-89%'] += 1
        else:
            accuracy_counts['<80%'] += 1
    labels_acc = ['100%', '90-99%', '80-89%', '<80%']
    vals = [accuracy_counts.get(l, 0) for l in labels_acc]
    acc_colors = ['#4CAF50', '#8BC34A', '#FFC107', '#FF5722']
    bars = ax.bar(labels_acc, vals, color=acc_colors, edgecolor='white', linewidth=0.5)
    ax.set_ylabel('Count')
    ax.set_title('Accuracy Distribution', fontsize=13, fontweight='bold')
    for bar, v in zip(bars, vals):
        if v > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    str(v), ha='center', va='bottom', fontsize=9)

    # ── Panel 4: Time trend (rolling average) ──
    ax = axes[1, 1]
    if len(results) >= 3:
        dates = [normalize_puzzle_date(r.puzzle_date) for r in results]
        times = [r.time_seconds for r in results]
        window = min(7, len(times))
        rolling = np.convolve(times, np.ones(window) / window, mode='valid')
        rolling_dates = dates[window - 1:]
        ax.plot(rolling_dates, rolling, color='#2196F3', linewidth=2, label=f'{window}-day avg')
        ax.scatter(dates, times, color='#90CAF9', s=12, alpha=0.5, zorder=2, label='Individual')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')
        ax.set_ylabel('Seconds')
        ax.legend(fontsize=9)
    else:
        ax.text(0.5, 0.5, 'Need 3+ results\nfor trend', transform=ax.transAxes,
                ha='center', va='center', fontsize=12, color='#888')
        ax.set_xticks([])
        ax.set_yticks([])
    ax.set_title('Time Trend', fontsize=13, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file


# ── Queens ─────────────────────────────────────────────────────────────

def _queens_streak_info(results):
    best = {
        normalize_puzzle_date(row.puzzle_date): row
        for row in results
    }
    if not best:
        return 0, 0, None

    latest_day = max(best)
    current = 0
    day = latest_day
    while day in best and best[day].is_perfect:
        current += 1
        day -= dt.timedelta(days=1)

    longest = 0
    run = 0
    previous_day = None
    for day in sorted(best):
        if best[day].is_perfect:
            is_consecutive = (
                previous_day is not None
                and day == previous_day + dt.timedelta(days=1)
            )
            run = run + 1 if is_consecutive else 1
            longest = max(longest, run)
        else:
            run = 0
        previous_day = day

    return current, longest, best[latest_day]


def _empty_panel(ax, text):
    ax.text(0.5, 0.5, text, transform=ax.transAxes,
            ha='center', va='center', fontsize=12, color='#888')
    ax.set_xticks([])
    ax.set_yticks([])


def plot_queens_stats(results, display_name, *, title_suffix=''):
    """Generate a multi-panel Queens stats image. Returns a discord.File."""
    results = sorted(
        results,
        key=lambda row: normalize_puzzle_date(row.puzzle_date))

    total = len(results)
    clean = [row for row in results if row.is_perfect]
    no_mistakes = [row for row in results if int(row.accuracy) == 100]
    times = [int(row.time_seconds) for row in results]
    current, longest, latest = _queens_streak_info(results)

    weekday_rows = [[] for _ in range(7)]
    for row in results:
        weekday_rows[normalize_puzzle_date(row.puzzle_date).weekday()].append(row)
    weekday_counts = [len(rows) for rows in weekday_rows]
    weekday_medians = [
        float(np.median([row.time_seconds for row in rows])) if rows else 0
        for rows in weekday_rows
    ]
    active_weekdays = [
        (index, weekday_counts[index], weekday_medians[index])
        for index in range(7) if weekday_counts[index]
    ]
    most_active = max(active_weekdays, key=lambda item: item[1], default=None)
    fastest = min(active_weekdays, key=lambda item: item[2], default=None)

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(
        f'{display_name} — Queens Stats{title_suffix}',
        fontsize=16, fontweight='bold', y=0.97)

    ax = axes[0, 0]
    ax.axis('off')
    if times:
        lines = [
            f'Queens days:  {total}',
            f'Clean:  {len(clean)}',
            f'No mistakes:  {len(no_mistakes)}',
            '',
            f'Best time:  {format_duration(min(times))}',
            f'Avg time:  {format_duration(sum(times) / len(times))}',
            f'Median time:  {format_duration(float(np.median(times)))}',
            '',
            f'Current clean streak:  {current}',
            f'Longest clean streak:  {longest}',
            f'Latest:  {normalize_puzzle_date(latest.puzzle_date).isoformat()}',
        ]
        if most_active is not None:
            lines.extend([
                '',
                f'Most active day:  {_WEEKDAY_LABELS[most_active[0]]} ({most_active[1]})',
                f'Fastest weekday:  {_WEEKDAY_LABELS[fastest[0]]} ({format_duration(fastest[2])})',
            ])
        ax.text(0.08, 0.92, '\n'.join(lines), transform=ax.transAxes,
                fontsize=12, verticalalignment='top', fontfamily='monospace',
                linespacing=1.45)
    else:
        _empty_panel(ax, 'No Queens results')
    ax.set_title('Overview', fontsize=13, fontweight='bold')

    ax = axes[0, 1]
    counts = _bin_times(times)
    colors = ['#7E57C2' if count else '#ccc' for count in counts]
    bars = ax.bar(range(len(counts)), counts, color=colors,
                  edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(len(_AKARI_TIME_LABELS)))
    ax.set_xticklabels(_AKARI_TIME_LABELS, rotation=30, ha='right', fontsize=9)
    ax.set_ylabel('Count')
    ax.set_title('Time Distribution', fontsize=13, fontweight='bold')
    for bar, count in zip(bars, counts):
        if count:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    str(count), ha='center', va='bottom', fontsize=9)

    ax = axes[1, 0]
    bar_colors = ['#26A69A' if count else '#d0d0d0'
                  for count in weekday_counts]
    bars = ax.bar(_WEEKDAY_LABELS, weekday_medians, color=bar_colors,
                  edgecolor='white', linewidth=0.5)
    ax.set_ylabel('Median seconds')
    ax.set_title('Weekday Speed', fontsize=13, fontweight='bold')
    for bar, count, median in zip(bars, weekday_counts, weekday_medians):
        if count:
            ax.text(bar.get_x() + bar.get_width() / 2, median + 0.3,
                    f'n={count}', ha='center', va='bottom', fontsize=8)

    ax = axes[1, 1]
    if len(results) >= 3:
        dates = [normalize_puzzle_date(row.puzzle_date) for row in results]
        window = min(7, len(times))
        rolling = np.convolve(times, np.ones(window) / window, mode='valid')
        rolling_dates = dates[window - 1:]
        ax.plot(rolling_dates, rolling, color='#5C6BC0',
                linewidth=2, label=f'{window}-day avg')
        ax.scatter(dates, times, color='#90CAF9', s=18,
                   alpha=0.65, zorder=2, label='Result')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')
        ax.set_ylabel('Seconds')
        ax.legend(fontsize=9)
    else:
        _empty_panel(ax, 'Need 3+ results\nfor trend')
    ax.set_title('Time Trend', fontsize=13, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file


# ── GuessThe.Game ──────────────────────────────────────────────────────

_GG_GUESS_LABELS = ['1st', '2nd', '3rd', '4th', '5th', '6th', 'X']
_GG_GUESS_COLORS = ['#4CAF50', '#8BC34A', '#CDDC39', '#FFC107', '#FF9800', '#FF5722', '#9E9E9E']


def _guess_position(row):
    """Convert accuracy back to guess position (1-based), or 0 for no green."""
    if row.accuracy > 0:
        return 7 - row.accuracy  # accuracy = 7 - pos, so pos = 7 - accuracy
    return 0  # no green


def plot_guessgame_stats(rows, display_name):
    """Generate a multi-panel GuessThe.Game stats image. Returns a discord.File."""
    best = pick_best_results(rows)
    results = sorted(best.values(), key=lambda r: normalize_puzzle_date(r.puzzle_date))

    total = len(results)
    greens = [r for r in results if r.accuracy > 0]
    green_count = len(greens)
    green_rate = green_count / total * 100 if total else 0
    perfect_count = sum(1 for r in results if r.is_perfect)
    perfect_rate = perfect_count / total * 100 if total else 0

    streak = compute_streak(rows)
    longest = compute_longest_streak(rows)

    # Guess distribution
    guess_counts = [0] * 7  # positions 1-6 + X(no green)
    for r in results:
        pos = _guess_position(r)
        if pos >= 1:
            guess_counts[pos - 1] += 1
        else:
            guess_counts[6] += 1

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(f'{display_name} — GuessThe.Game Stats', fontsize=16, fontweight='bold', y=0.97)

    # ── Panel 1: Summary text ──
    ax = axes[0, 0]
    ax.axis('off')
    avg_pos = sum(_guess_position(r) for r in greens) / green_count if green_count else 0
    lines = [
        f'Total games:  {total}',
        f'Games won (green):  {green_count}  ({green_rate:.0f}%)',
        f'Perfect (1st guess):  {perfect_count}  ({perfect_rate:.0f}%)',
        '',
        f'Avg guess position:  {avg_pos:.1f}',
        '',
        f'Current perfect streak:  {streak}',
        f'Longest perfect streak:  {longest}',
    ]
    ax.text(0.08, 0.92, '\n'.join(lines), transform=ax.transAxes,
            fontsize=13, verticalalignment='top', fontfamily='monospace',
            linespacing=1.6)
    ax.set_title('Overview', fontsize=13, fontweight='bold')

    # ── Panel 2: Guess distribution ──
    ax = axes[0, 1]
    bars = ax.bar(_GG_GUESS_LABELS, guess_counts, color=_GG_GUESS_COLORS,
                  edgecolor='white', linewidth=0.5)
    ax.set_ylabel('Count')
    ax.set_title('Guess Distribution', fontsize=13, fontweight='bold')
    for bar, c in zip(bars, guess_counts):
        if c > 0:
            pct = c / total * 100
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    f'{c}\n({pct:.0f}%)', ha='center', va='bottom', fontsize=9)

    # ── Panel 3: Win rate over time (rolling) ──
    ax = axes[1, 0]
    if len(results) >= 5:
        dates = [normalize_puzzle_date(r.puzzle_date) for r in results]
        wins = [1 if r.accuracy > 0 else 0 for r in results]
        window = min(10, len(wins))
        rolling = np.convolve(wins, np.ones(window) / window, mode='valid')
        rolling_dates = dates[window - 1:]
        ax.plot(rolling_dates, [v * 100 for v in rolling], color='#4CAF50', linewidth=2,
                label=f'{window}-game rolling')
        ax.axhline(y=green_rate, color='#888', linestyle='--', linewidth=1, label='Overall avg')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')
        ax.set_ylabel('Win %')
        ax.set_ylim(-5, 105)
        ax.legend(fontsize=9)
    else:
        ax.text(0.5, 0.5, 'Need 5+ results\nfor trend', transform=ax.transAxes,
                ha='center', va='center', fontsize=12, color='#888')
        ax.set_xticks([])
        ax.set_yticks([])
    ax.set_title('Win Rate Trend', fontsize=13, fontweight='bold')

    # ── Panel 4: Accuracy over time (rolling avg guess position) ──
    ax = axes[1, 1]
    if len(greens) >= 5:
        green_dates = [normalize_puzzle_date(r.puzzle_date) for r in results if r.accuracy > 0]
        positions = [_guess_position(r) for r in results if r.accuracy > 0]
        window = min(10, len(positions))
        rolling = np.convolve(positions, np.ones(window) / window, mode='valid')
        rolling_dates = green_dates[window - 1:]
        ax.plot(rolling_dates, rolling, color='#FF9800', linewidth=2,
                label=f'{window}-game rolling')
        ax.axhline(y=avg_pos, color='#888', linestyle='--', linewidth=1, label='Overall avg')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')
        ax.set_ylabel('Avg Guess Position')
        ax.set_ylim(0.5, 6.5)
        ax.invert_yaxis()
        ax.legend(fontsize=9)
    else:
        ax.text(0.5, 0.5, 'Need 5+ wins\nfor trend', transform=ax.transAxes,
                ha='center', va='center', fontsize=12, color='#888')
        ax.set_xticks([])
        ax.set_yticks([])
    ax.set_title('Guess Position Trend', fontsize=13, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file

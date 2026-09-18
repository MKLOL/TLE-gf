"""Weekly server recap panel for Daily Akari and LinkedIn Queens.

Styled to sit alongside the seven-day player dashboards, so `;akari week` and
`;akari stats` read as the same family. The per-game accent palette is passed
in rather than hardcoded: Akari is green, Queens purple.
"""

from matplotlib import pyplot as plt

from tle.util import graph_common as gc
from tle.cogs._minigame_stats_text import draw_player_name
from tle.cogs._minigame_common import format_duration
from tle.cogs._minigame_weekly import top_and_bottom


_BG = '#F4F6FA'
_PANEL = '#FFFFFF'
_TEXT = '#172033'
_MUTED = '#667085'
_RED = '#C63C55'
_GREEN = '#16845B'

_WEEKDAYS = ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN')

AKARI_PALETTE = {'accent': '#16845B', 'dark': '#0C6444', 'alt': '#356B9E',
                 'warm': '#A46100'}
QUEENS_PALETTE = {'accent': '#6842C2', 'dark': '#49309B', 'alt': '#007A68',
                  'warm': '#9A5A00'}


def _style_axis(ax):
    ax.set_facecolor(_PANEL)
    ax.grid(False)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)


def _panel(fig, rect, title, accent):
    ax = fig.add_axes(rect)
    _style_axis(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.text(.03, 1.045, title, transform=ax.transAxes, color=_TEXT,
            fontsize=10, fontweight='bold', va='bottom')
    ax.axvline(0, color=accent, linewidth=5)
    return ax


def _draw_kpi_strip(fig, entries, accent):
    """One banded row of value/label pairs, instead of four tall cards."""
    ax = fig.add_axes((.04, .875, .92, .05))
    _style_axis(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axvline(0, color=accent, linewidth=5)
    width = 1 / max(1, len(entries))
    for index, (value, label, color) in enumerate(entries):
        x = width * index + .022
        ax.text(x, .5, value, va='center', color=color, fontsize=15,
                fontweight='bold')
        # Place the label after the value without measuring text: values here
        # are short, so a width proportional to their length keeps pairs tight.
        ax.text(x + .022 + .011 * len(str(value)), .46, label, va='center',
                color=_MUTED, fontsize=8, fontweight='bold')


def _names(user_ids, name_fn, limit=2):
    """Join winner names, keeping tied days readable at a glance."""
    shown = [name_fn(user_id) for user_id in user_ids[:limit]]
    extra = len(user_ids) - len(shown)
    joined = ' + '.join(shown)
    return f'{joined} +{extra}' if extra > 0 else joined


def _draw_days(fig, recap, name_fn, palette):
    ax = _panel(fig, (.04, .375, .44, .44),
                'DAY BY DAY  ·  WINNERS', palette['accent'])
    step = 1 / 7
    for index, day in enumerate(recap.days):
        y = 1 - step * (index + .5)
        ax.text(.04, y, _WEEKDAYS[day.date.weekday()], va='center',
                color=_MUTED, fontsize=8, fontweight='bold')
        ax.text(.135, y, f'{day.date:%m-%d}', va='center', color=_MUTED,
                fontsize=8)
        if not day.winner_ids:
            if day.date > recap.today:
                label = 'up next'
            elif not day.participants:
                label = 'no results'
            else:
                label = 'no winner'
            ax.text(.26, y, label, va='center', color=_MUTED, fontsize=9,
                    style='italic')
            continue
        # Tied days list several names, so the winner column is the widest
        # thing here; keep its budget clear of the TIED badge that follows it.
        draw_player_name(
            ax, _names(day.winner_ids, name_fn), xy=(.26, y - .022),
            transform=ax.transData, color=_TEXT, fontsize=10,
            max_width_px=230, fontweight='bold')
        if day.tied:
            ax.text(.71, y, 'TIED', va='center', color=palette['warm'],
                    fontsize=7, fontweight='bold')
        if day.best_row is not None:
            ax.text(.855, y, format_duration(day.best_row.time_seconds),
                    va='center', ha='right', color=palette['dark'],
                    fontsize=9, fontweight='bold')
        ax.text(.97, y, f'{day.participants}p', va='center', ha='right',
                color=_MUTED, fontsize=8)


def _draw_leaders(fig, recap, name_fn, palette):
    ax = _panel(fig, (.53, .55, .43, .265),
                'WEEK LEADERBOARD  ·  WINS', palette['alt'])
    # recap.leaders already arrives ordered by (total wins, solo wins), which
    # is the ranking this panel shows.
    leaders = recap.leaders[:6]
    if not leaders:
        ax.text(.04, .5, 'Nobody won a day this week.', va='center',
                color=_MUTED, fontsize=9, style='italic')
        return
    step = 1 / max(4, len(leaders))
    for index, (user_id, solo, tied) in enumerate(leaders):
        y = 1 - step * (index + .5)
        ax.text(.04, y, f'#{index + 1}', va='center', color=palette['alt'],
                fontsize=9, fontweight='bold')
        draw_player_name(
            ax, name_fn(user_id), xy=(.13, y - .028), transform=ax.transData,
            color=_TEXT, fontsize=10, max_width_px=250)
        total = solo + tied
        ax.text(.80, y, f'{total} win' + ('s' if total != 1 else ''),
                va='center', ha='right', color=_TEXT, fontsize=9,
                fontweight='bold')
        ax.text(.98, y, f'{solo} solo · {tied} tied', va='center', ha='right',
                color=_MUTED, fontsize=8)


def _draw_superlatives(fig, recap, name_fn, palette):
    ax = _panel(fig, (.53, .375, .43, .115), 'STANDOUTS', palette['warm'])
    if not recap.superlatives:
        ax.text(.04, .5, 'Not enough results for standouts.', va='center',
                color=_MUTED, fontsize=9, style='italic')
        return
    entries = recap.superlatives[:3]
    step = 1 / max(2, len(entries))
    for index, entry in enumerate(entries):
        y = 1 - step * (index + .5)
        ax.text(.04, y, entry.label.upper(), va='center', color=_MUTED,
                fontsize=8, fontweight='bold')
        draw_player_name(
            ax, name_fn(entry.user_id), xy=(.35, y - .04),
            transform=ax.transData, color=_TEXT, fontsize=9, max_width_px=200)
        value = format_duration(entry.value)
        # The caption is right-aligned at the panel edge, so the value stops
        # well short of it — the longest caption must not run under the value.
        ax.text(.76, y, value, va='center', ha='right', color=palette['warm'],
                fontsize=9, fontweight='bold')
        ax.text(.98, y, entry.detail, va='center', ha='right', color=_MUTED,
                fontsize=7)


def _draw_rating_panel(fig, rect, title, changes, name_fn, accent, empty):
    """Three biggest gains over three biggest losses for one rating ladder."""
    ax = _panel(fig, rect, title, accent)
    if not changes:
        ax.text(.03, .5, empty, va='center', color=_MUTED, fontsize=8.5,
                style='italic')
        return
    best, worst = top_and_bottom(changes)
    rows = ([(entry, True) for entry in best]
            + [(entry, False) for entry in worst])
    step = 1 / 6
    for index, (entry, gaining) in enumerate(rows):
        y = 1 - step * (index + .5)
        color = _GREEN if entry.delta >= 0 else _RED
        ax.text(.03, y, '▲' if gaining else '▼', va='center', color=color,
                fontsize=7)
        draw_player_name(
            ax, name_fn(entry.user_id), xy=(.09, y - .028),
            transform=ax.transData, color=_TEXT, fontsize=9, max_width_px=190)
        ax.text(.83, y, f'{round(entry.new)}', va='center', ha='right',
                color=_TEXT, fontsize=9, fontweight='bold')
        ax.text(.98, y, f'{entry.delta:+.0f}', va='center', ha='right',
                color=color, fontsize=9, fontweight='bold')


def _draw_pace(fig, rect, recap, name_fn, accent):
    """Speed relative to each day's field, so easy-day-only weeks cannot win."""
    ax = _panel(fig, rect, 'BEST PACE  ·  FULL WEEK ONLY', accent)
    entries = recap.paces[:6]
    if not entries:
        ax.text(.03, .5, 'Nobody played every day this week.', va='center',
                color=_MUTED, fontsize=8.5, style='italic')
        return
    step = 1 / 6
    for index, entry in enumerate(entries):
        y = 1 - step * (index + .5)
        ax.text(.03, y, f'#{index + 1}', va='center', color=accent,
                fontsize=8, fontweight='bold')
        draw_player_name(
            ax, name_fn(entry.user_id), xy=(.13, y - .028),
            transform=ax.transData, color=_TEXT, fontsize=9, max_width_px=190)
        ax.text(.85, y, f'{entry.pace:.2f}×', va='center', ha='right',
                color=accent, fontsize=9, fontweight='bold')
        # Everyone here played the same days, so the day count says nothing;
        # the wall-clock average is the useful companion to the multiple.
        ax.text(.99, y, format_duration(round(entry.seconds)), va='center',
                ha='right', color=_MUTED, fontsize=8)


def _draw_ratings(fig, recap, name_fn, palette):
    # Three equal columns across the same band the two rating panels used.
    width, gap = .2833, .035
    left, middle, right = .04, .04 + width + gap, .04 + 2 * (width + gap)
    band = .225
    _draw_rating_panel(
        fig, (left, .08, width, band), 'DAILY RATING  ·  THIS WEEK',
        recap.daily_ratings, name_fn, palette['dark'],
        'No daily rating movement this week.')
    weekly_title = ('WEEKLY RATING  ·  NOT RATED YET' if recap.in_progress
                    else 'WEEKLY RATING  ·  THIS WEEK')
    _draw_rating_panel(
        fig, (middle, .08, width, band), weekly_title,
        recap.ratings, name_fn, palette['alt'],
        'Not rated until Sunday closes.' if recap.in_progress
        else 'No rated movement (needs two players).')
    _draw_pace(fig, (right, .08, width, band), recap, name_fn,
               palette['warm'])


def _personal_summary(recap, name_fn):
    personal = recap.personal
    if personal is None:
        return 'YOU HAVE NO RESULTS THIS WEEK'
    parts = [
        f'{personal.days_played} PLAYED',
        f'{personal.perfects} CLEAN',
    ]
    if personal.solo_wins or personal.tied_wins:
        parts.append(f'{personal.solo_wins} SOLO WIN(S)'
                     f' · {personal.tied_wins} TIED')
    if personal.best_row is not None:
        parts.append(f'{format_duration(personal.best_row.time_seconds)} BEST')
    if personal.rank:
        parts.append(f'#{personal.rank} ON THE BOARD')
    return f'YOUR WEEK  ·  {"  ·  ".join(parts)}'


def plot_weekly_recap(recap, name_fn, palette=None):
    """Render the weekly recap panel and return it as a Discord file."""
    palette = palette or AKARI_PALETTE
    fig = plt.figure(figsize=(16, 9), facecolor=_BG)

    header = fig.add_axes((.04, .935, .92, .05))
    header.set_facecolor(_BG)
    header.axis('off')
    header.text(0, .78, f'{recap.display_name.upper()}  /  WEEKLY SERVER RECAP',
                transform=header.transAxes, color=palette['accent'],
                fontsize=9, fontweight='bold')
    header.text(0, .1,
                f'{recap.week_start:%b %d} – {recap.week_end:%b %d, %Y}',
                transform=header.transAxes, color=_TEXT, fontsize=20,
                fontweight='bold')
    status = 'IN PROGRESS' if recap.in_progress else 'FINAL'
    # recap.leaders is ordered by (total wins, solo wins), so the leader is
    # simply its first entry and the headline number is that total — reading
    # the solo count here would name one player and count another's wins.
    if recap.leaders:
        user_id, solo, tied = recap.leaders[0]
        total = solo + tied
        status = (f'{name_fn(user_id)} leads · {total} '
                  f'win{"s" if total != 1 else ""}  ·  {status}')
    header.text(1, .3, status, transform=header.transAxes,
                color=palette['warm'] if recap.in_progress else _MUTED,
                fontsize=9, fontweight='bold', ha='right')

    active_days = sum(1 for day in recap.days if day.participants)
    solo_days = sum(1 for day in recap.days
                    if day.winner_ids and not day.tied)
    _draw_kpi_strip(fig, [
        (str(recap.player_count), 'PLAYERS', palette['accent']),
        (str(recap.result_count), 'RESULTS', palette['accent']),
        (f'{active_days}/7', 'DAYS PLAYED', palette['alt']),
        (str(solo_days), 'DECIDED OUTRIGHT', palette['alt']),
        (str(sum(1 for day in recap.days if day.tied)), 'TIED DAYS',
         palette['warm']),
    ], palette['accent'])

    _draw_days(fig, recap, name_fn, palette)
    _draw_leaders(fig, recap, name_fn, palette)
    _draw_superlatives(fig, recap, name_fn, palette)
    _draw_ratings(fig, recap, name_fn, palette)

    fig.text(.04, .03, _personal_summary(recap, name_fn), color=_TEXT,
             fontsize=9, fontweight='bold')
    fig.text(.96, .03, 'WEEK RUNS MONDAY → SUNDAY', color=_MUTED,
             fontsize=7, fontweight='bold', ha='right')

    plt.sca(header)
    discord_file = gc.get_current_figure_as_file()
    plt.close(fig)
    return discord_file

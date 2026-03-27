"""Shared types and computation functions for the minigames system."""

import datetime as dt
import time
from dataclasses import dataclass
from typing import Callable, Optional

from tle.util import codeforces_common as cf_common

_NO_TIME_BOUND = 10 ** 10
_TIMELINE_KEYWORDS = {'week', 'month', 'year'}


@dataclass(frozen=True)
class ParsedResult:
    """Parsed result from a daily puzzle game message."""
    puzzle_number: int
    puzzle_date: dt.date
    accuracy: int
    time_seconds: int
    is_perfect: bool


@dataclass(frozen=True)
class GameDef:
    """Definition of a daily puzzle minigame.

    To add a new game, create a ``GameDef`` with a parser that converts a
    Discord message body into a ``ParsedResult`` (or ``None`` if the message
    doesn't match).  Then register it in ``Minigames.GAMES`` and add thin
    command wrappers in ``minigames.py``.
    """
    name: str               # short key used in DB, e.g. 'akari'
    display_name: str       # human-readable, e.g. 'Daily Akari'
    feature_flag: str       # guild config key, e.g. 'akari'
    aliases: tuple          # command aliases, e.g. ('dailyakari',)
    parse: Callable[[str], Optional[ParsedResult]]


# ── Helpers ─────────────────────────────────────────────────────────────

def normalize_puzzle_date(value):
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def result_key(row):
    return normalize_puzzle_date(row.puzzle_date), row.puzzle_number


def result_sort_key(row):
    return (
        int(bool(row.is_perfect)),
        int(getattr(row, 'accuracy', 0)),
        -int(getattr(row, 'time_seconds', 0)),
        int(getattr(row, 'message_id', 0)),
    )


def pick_best_results(rows):
    best = {}
    for row in rows:
        key = result_key(row)
        prev = best.get(key)
        if prev is None or result_sort_key(row) > result_sort_key(prev):
            best[key] = row
    return best


def format_duration(total_seconds):
    minutes, seconds = divmod(int(total_seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}:{minutes:02d}:{seconds:02d}'
    return f'{minutes}:{seconds:02d}'


# ── Scoring ─────────────────────────────────────────────────────────────

def score_matchup(row1, row2):
    """Default scoring: perfect beats non-perfect; among perfects, faster wins."""
    if row1.is_perfect and row2.is_perfect:
        if row1.time_seconds < row2.time_seconds:
            return 1.0, 0.0
        if row1.time_seconds > row2.time_seconds:
            return 0.0, 1.0
        return 0.5, 0.5
    if row1.is_perfect and not row2.is_perfect:
        return 1.0, 0.0
    if row2.is_perfect and not row1.is_perfect:
        return 0.0, 1.0
    return 0.5, 0.5


def compute_vs(rows1, rows2):
    best1 = pick_best_results(rows1)
    best2 = pick_best_results(rows2)
    common = sorted(set(best1) & set(best2))

    score1, score2 = 0.0, 0.0
    wins1, wins2, ties = 0, 0, 0

    for key in common:
        pts1, pts2 = score_matchup(best1[key], best2[key])
        score1 += pts1
        score2 += pts2
        if pts1 == pts2:
            ties += 1
        elif pts1 > pts2:
            wins1 += 1
        else:
            wins2 += 1

    return {
        'common_count': len(common),
        'score1': score1, 'score2': score2,
        'wins1': wins1, 'wins2': wins2, 'ties': ties,
    }


def compute_streak(rows):
    best_by_day = {}
    for row in rows:
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        prev = best_by_day.get(puzzle_date)
        if prev is None or result_sort_key(row) > result_sort_key(prev):
            best_by_day[puzzle_date] = row

    if not best_by_day:
        return 0

    current_day = max(best_by_day)
    streak = 0
    while True:
        row = best_by_day.get(current_day)
        if row is None or not row.is_perfect:
            break
        streak += 1
        current_day -= dt.timedelta(days=1)
    return streak


def compute_top(rows):
    best_by_user_puzzle = {}
    for row in rows:
        key = (str(row.user_id), result_key(row))
        prev = best_by_user_puzzle.get(key)
        if prev is None or result_sort_key(row) > result_sort_key(prev):
            best_by_user_puzzle[key] = row

    best_per_puzzle = {}
    for (_, puzzle_key), row in best_by_user_puzzle.items():
        if not row.is_perfect:
            continue
        entry = best_per_puzzle.get(puzzle_key)
        if entry is None or row.time_seconds < entry['time_seconds']:
            best_per_puzzle[puzzle_key] = {'time_seconds': row.time_seconds, 'rows': [row]}
        elif row.time_seconds == entry['time_seconds']:
            entry['rows'].append(row)

    wins_by_user = {}
    for entry in best_per_puzzle.values():
        for row in entry['rows']:
            user_id = str(row.user_id)
            wins_by_user[user_id] = wins_by_user.get(user_id, 0) + 1

    return sorted(wins_by_user.items(), key=lambda item: (-item[1], int(item[0])))


# ── Argument parsing ────────────────────────────────────────────────────

def parse_date_args(args):
    """Parse timeline filter arguments.  Returns ``(dlo, dhi)`` timestamps.

    Raises ``ValueError`` on unrecognized arguments.
    """
    dlo = 0
    dhi = _NO_TIME_BOUND

    for arg in args:
        lower = arg.lower()
        if lower in _TIMELINE_KEYWORDS:
            now = dt.datetime.now()
            if lower == 'week':
                monday = now - dt.timedelta(days=now.weekday())
                dlo = time.mktime(monday.replace(hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'month':
                dlo = time.mktime(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'year':
                dlo = time.mktime(now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
        elif lower.startswith('d>='):
            dlo = max(dlo, cf_common.parse_date(arg[3:]))
        elif lower.startswith('d<'):
            dhi = min(dhi, cf_common.parse_date(arg[2:]))
        else:
            raise ValueError(f'Unrecognized filter: `{arg}`.')
    return dlo, dhi

"""Queens weekday / date-bound filter parsing and label formatting.

Split out of ``_minigame_queens_cog`` to keep both modules small.
``minigames.py`` re-exports these by name.
"""

import datetime as dt
import re

from tle.cogs._minigame_common import (
    normalize_puzzle_date, parse_date_args, _NO_TIME_BOUND,
)
from tle.cogs._minigame_helpers import MinigameCogError

_QUEENS_WEEKDAY_NAMES = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
_QUEENS_WEEKDAY_ALIASES = {
    'mon': 0,
    'monday': 0,
    'tue': 1,
    'tues': 1,
    'tuesday': 1,
    'wed': 2,
    'wednesday': 2,
    'thu': 3,
    'thur': 3,
    'thurs': 3,
    'thursday': 3,
    'fri': 4,
    'friday': 4,
    'sat': 5,
    'saturday': 5,
    'sun': 6,
    'sunday': 6,
}


def _parse_queens_weekday_filter_arg(arg):
    text = str(arg).strip()
    match = re.fullmatch(
        r'\+(?:dow|day|days|weekday|weekdays)=(.+)', text, re.IGNORECASE)
    if not match:
        return None
    weekdays = set()
    for raw_part in match.group(1).split(','):
        part = raw_part.strip().casefold()
        if not part:
            continue
        if part == 'all':
            weekdays.update(range(7))
        elif part == 'weekday':
            weekdays.update(range(5))
        elif part == 'weekend':
            weekdays.update((5, 6))
        elif part in _QUEENS_WEEKDAY_ALIASES:
            weekdays.add(_QUEENS_WEEKDAY_ALIASES[part])
        else:
            raise MinigameCogError(
                f'Unknown Queens weekday `{raw_part}`. Use names like '
                '`mon`, `tuesday`, `weekday`, or `weekend`.')
    if not weekdays:
        raise MinigameCogError(
            'Queens weekday filter cannot be empty. Example: `+dow=mon,wed`.')
    return weekdays


def _split_queens_weekday_filter(args):
    remaining = []
    weekdays = None
    for arg in args:
        parsed = _parse_queens_weekday_filter_arg(arg)
        if parsed is None:
            remaining.append(arg)
            continue
        if weekdays is None:
            weekdays = set()
        weekdays.update(parsed)
    return remaining, weekdays


def _filter_queens_weekday_rows(rows, weekdays):
    if weekdays is None:
        return rows
    return [
        row for row in rows
        if normalize_puzzle_date(row.puzzle_date).weekday() in weekdays
    ]


def _split_queens_rating_date_filter(args):
    import tle.util.codeforces_common as cf_common
    remaining = []
    date_args = []
    for arg in args:
        text = str(arg).strip()
        lower = text.lower()
        if lower.startswith('d>=') or lower.startswith('d<'):
            date_args.append(text)
        else:
            remaining.append(arg)
    if not date_args:
        return remaining, None
    try:
        return remaining, parse_date_args(date_args)
    except (ValueError, cf_common.ParamParseError) as e:
        raise MinigameCogError(str(e)) from e


def _split_queens_recalculate_filter(args):
    remaining = []
    recalculate = False
    for arg in args:
        if str(arg).strip().lower() == '+recalculate':
            recalculate = True
        else:
            remaining.append(arg)
    return remaining, recalculate


def _filter_queens_rating_date_rows(rows, date_bounds):
    if date_bounds is None:
        return rows
    dlo, dhi, plo, phi = date_bounds
    filtered = []
    for row in rows:
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        timestamp = dt.datetime.combine(puzzle_date, dt.time.min).timestamp()
        puzzle_number = int(row.puzzle_number)
        if timestamp < dlo or timestamp >= dhi:
            continue
        if puzzle_number < plo:
            continue
        if phi and puzzle_number >= phi:
            continue
        filtered.append(row)
    return filtered


def _filter_queens_rating_date_history(history, date_bounds):
    if date_bounds is None:
        return history
    dlo, dhi, plo, phi = date_bounds
    filtered = []
    for point in history:
        puzzle_date = normalize_puzzle_date(point.puzzle_date)
        timestamp = dt.datetime.combine(puzzle_date, dt.time.min).timestamp()
        puzzle_number = int(point.puzzle_number)
        if timestamp < dlo or timestamp >= dhi:
            continue
        if puzzle_number < plo:
            continue
        if phi and puzzle_number >= phi:
            continue
        filtered.append(point)
    return filtered


def _format_queens_weekday_filter(weekdays):
    if weekdays is None:
        return ''
    if set(weekdays) == set(range(7)):
        return 'all days'
    if set(weekdays) == set(range(5)):
        return 'weekdays'
    if set(weekdays) == {5, 6}:
        return 'weekend'
    return '/'.join(_QUEENS_WEEKDAY_NAMES[index] for index in sorted(weekdays))


def _queens_weekday_filter_suffix(weekdays):
    label = _format_queens_weekday_filter(weekdays)
    return f' ({label})' if label else ''


def _format_queens_date_filter(date_bounds):
    if date_bounds is None:
        return ''
    dlo, dhi, _plo, _phi = date_bounds
    parts = []
    if dlo:
        parts.append(
            f'from {dt.datetime.fromtimestamp(dlo).date().isoformat()}')
    if dhi < _NO_TIME_BOUND:
        parts.append(
            f'before {dt.datetime.fromtimestamp(dhi).date().isoformat()}')
    return ', '.join(parts)


def _queens_filter_suffix(*, weekdays=None, date_bounds=None):
    parts = [
        part for part in (
            _format_queens_weekday_filter(weekdays),
            _format_queens_date_filter(date_bounds),
        )
        if part
    ]
    return f' ({", ".join(parts)})' if parts else ''


def _filter_queens_contested_rating_history(history):
    """Return graph history with solo contest days omitted entirely."""
    return [
        point for point in history
        if point.performance is not None
    ]

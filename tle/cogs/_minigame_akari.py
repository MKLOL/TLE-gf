"""Daily Akari game definition for the minigames system."""

import datetime as dt
import re

from tle.cogs._minigame_common import ParsedResult, GameDef, ScoringDef


_HEADER_RE = re.compile(r'^Daily\s+Akari\b', re.IGNORECASE)
_HEADER_NUM_RE = re.compile(r'\b(\d+)\s*$')
_DATE_RE = re.compile(
    r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|[A-Za-z]+ \d{1,2}, \d{4})'
)
_TIME_RE = re.compile(r'🕓\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)')
_ACCURACY_RE = re.compile(r'(\d{1,3})%')

# Known anchor for inferring puzzle numbers from dates (1 puzzle per day).
_ANCHOR_DATE = dt.date(2026, 3, 27)
_ANCHOR_NUMBER = 446


def _puzzle_number_from_date(puzzle_date):
    return _ANCHOR_NUMBER + (puzzle_date - _ANCHOR_DATE).days


def _parse_time(time_text):
    parts = [int(part) for part in time_text.split(':')]
    if len(parts) == 2:
        minutes, seconds = parts
        return minutes * 60 + seconds
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f'Unrecognized time format: {time_text}')


def _parse_date(date_text):
    # MM/DD/YYYY is tried before DD/MM/YYYY — matches dailyakari.com's format
    cleaned = date_text.strip().replace('/', '-')
    formats = (
        '%Y-%m-%d',
        '%m-%d-%Y',
        '%d-%m-%Y',
        '%B %d, %Y',
        '%b %d, %Y',
    )
    for fmt in formats:
        try:
            return dt.datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    raise ValueError(f'Unrecognized date format: {date_text}')


def parse_akari_message(content):
    """Parse a Daily Akari result message.  Returns a list with one ``ParsedResult``, or ``[]``."""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 3:
        return []

    # Search for the "Daily Akari" header anywhere in the message
    # (users may prepend a URL, commentary, or invisible Unicode chars).
    header_idx = None
    header_line = None
    for i, line in enumerate(lines):
        if _HEADER_RE.match(line):
            header_idx = i
            header_line = line
            break
    if header_idx is None:
        return []

    # Need at least a date line and a stats line after the header
    if header_idx + 2 >= len(lines):
        return []

    date_match = _DATE_RE.search(lines[header_idx + 1])
    if date_match is None:
        return []

    stats_line = None
    for line in lines[header_idx + 2:]:
        if '🕓' in line:
            stats_line = line
            break
    if stats_line is None:
        return []

    time_match = _TIME_RE.search(stats_line)
    if time_match is None:
        return []

    is_perfect = 'perfect' in stats_line.lower() or '🌟' in stats_line
    accuracy_match = _ACCURACY_RE.search(stats_line)
    if is_perfect:
        accuracy = 100
    elif accuracy_match is not None:
        accuracy = int(accuracy_match.group(1))
    else:
        return []

    try:
        puzzle_date = _parse_date(date_match.group(1))
        time_seconds = _parse_time(time_match.group(1))
    except ValueError:
        return []

    num_match = _HEADER_NUM_RE.search(header_line)
    puzzle_number = int(num_match.group(1)) if num_match else _puzzle_number_from_date(puzzle_date)

    return [ParsedResult(
        puzzle_number=puzzle_number,
        puzzle_date=puzzle_date,
        accuracy=accuracy,
        time_seconds=time_seconds,
        is_perfect=is_perfect,
    )]


def akari_raw_score_matchup(row1, row2):
    """Raw-time scoring: faster result wins, accuracy ignored."""
    if row1.time_seconds < row2.time_seconds:
        return 1.0, 0.0
    if row1.time_seconds > row2.time_seconds:
        return 0.0, 1.0
    return 0.5, 0.5


def akari_raw_is_eligible_winner(_row):
    return True


def akari_raw_best_result_sort_key(row):
    return (
        -int(getattr(row, 'time_seconds', 0)),
        -int(getattr(row, 'message_id', 0)),
    )


def akari_raw_winner_result_sort_key(row):
    return (-int(getattr(row, 'time_seconds', 0)),)


AKARI_GAME = GameDef(
    name='akari',
    display_name='Daily Akari',
    feature_flag='akari',
    parse=parse_akari_message,
    detect=_HEADER_RE,
    scoring_variants={
        'raw': ScoringDef(
            score_matchup=akari_raw_score_matchup,
            is_eligible_winner=akari_raw_is_eligible_winner,
            best_result_sort_key=akari_raw_best_result_sort_key,
            winner_result_sort_key=akari_raw_winner_result_sort_key,
        ),
    },
)

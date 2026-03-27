"""Daily Akari game definition for the minigames system."""

import datetime as dt
import re

from tle.cogs._minigame_common import ParsedResult, GameDef


_FIRST_LINE_RE = re.compile(r'^Daily\s+Akari\b.*?\b(\d+)\s*$', re.IGNORECASE)
_DATE_RE = re.compile(
    r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|[A-Za-z]+ \d{1,2}, \d{4})'
)
_TIME_RE = re.compile(r'🕓\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)')
_ACCURACY_RE = re.compile(r'(\d{1,3})%')


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
    """Parse a Daily Akari result message.  Returns ``ParsedResult`` or ``None``."""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 3:
        return None

    first_match = _FIRST_LINE_RE.match(lines[0])
    if first_match is None:
        return None

    date_match = _DATE_RE.search(lines[1])
    if date_match is None:
        return None

    stats_line = None
    for line in lines[2:]:
        if '🕓' in line:
            stats_line = line
            break
    if stats_line is None:
        return None

    time_match = _TIME_RE.search(stats_line)
    if time_match is None:
        return None

    is_perfect = 'perfect' in stats_line.lower() or '🌟' in stats_line
    accuracy_match = _ACCURACY_RE.search(stats_line)
    if is_perfect:
        accuracy = 100
    elif accuracy_match is not None:
        accuracy = int(accuracy_match.group(1))
    else:
        return None

    try:
        puzzle_date = _parse_date(date_match.group(1))
        time_seconds = _parse_time(time_match.group(1))
    except ValueError:
        return None

    return ParsedResult(
        puzzle_number=int(first_match.group(1)),
        puzzle_date=puzzle_date,
        accuracy=accuracy,
        time_seconds=time_seconds,
        is_perfect=is_perfect,
    )


AKARI_GAME = GameDef(
    name='akari',
    display_name='Daily Akari',
    feature_flag='akari',
    aliases=('dailyakari',),
    parse=parse_akari_message,
)

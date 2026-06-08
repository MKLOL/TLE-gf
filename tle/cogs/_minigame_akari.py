"""Daily Akari game definition for the minigames system."""

import datetime as dt
import re
from types import SimpleNamespace

from tle import constants
from tle.cogs._minigame_common import (
    ParsedResult, GameDef, RatingDef, ScoringDef, default_score_matchup,
)


_HEADER_RE = re.compile(r'^Daily\s+Akari\b', re.IGNORECASE)
_HEADER_NUM_RE = re.compile(r'\b(\d+)\s*$')
_DATE_RE = re.compile(
    r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|[A-Za-z]+ \d{1,2}, \d{4})'
)
_TIME_RE = re.compile(r'🕓\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)')
_ACCURACY_RE = re.compile(r'(\d{1,3})%')
# Non-pro mode signature: dailyakari.com shows "✅ Solved!" instead of an
# accuracy percentage / "🌟 Perfect!" when the player hasn't enabled Pro Mode.
# The message is otherwise shaped like a real submission (header + date + time),
# so the cog can recognise it and ask the user to enable Pro Mode.
_SOLVED_RE = re.compile(r'(✅|solved)', re.IGNORECASE)

# Known anchor for inferring puzzle numbers from dates (1 puzzle per day).
_ANCHOR_DATE = dt.date(2026, 3, 27)
_ANCHOR_NUMBER = 446


def _puzzle_number_from_date(puzzle_date):
    return _ANCHOR_NUMBER + (puzzle_date - _ANCHOR_DATE).days


def expected_puzzle_number(on_date):
    """The Daily Akari puzzle number expected on ``on_date`` (one puzzle/day).

    Used to reject troll/garbage puzzle numbers far beyond the real one.
    """
    return _puzzle_number_from_date(on_date)


def puzzle_date_for(puzzle_number):
    """Inverse of :func:`expected_puzzle_number` — date for a puzzle number.

    Used when manually adding a result (``;mg akari add``) so the saved row's
    ``puzzle_date`` matches what a real ingested message would have used.
    """
    return _ANCHOR_DATE + dt.timedelta(days=int(puzzle_number) - _ANCHOR_NUMBER)


def current_puzzle_number():
    return expected_puzzle_number(dt.date.today())


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


def looks_like_non_pro_akari(content):
    """True iff this looks like a non-pro mode Daily Akari submission.

    Mirrors the structural checks in :func:`parse_akari_message` (header,
    date, stats line with ``🕓``) and adds: the stats line carries a
    ``✅``/``Solved`` completion marker but *no* accuracy ``%`` and *no*
    ``perfect``/``🌟``.  This is precisely the case where the normal parser
    returns ``[]`` even though the user clearly submitted a result — so the
    cog can recognise it and reply asking them to enable Pro Mode.
    """
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 3:
        return False

    header_idx = None
    for i, line in enumerate(lines):
        if _HEADER_RE.match(line):
            header_idx = i
            break
    if header_idx is None or header_idx + 2 >= len(lines):
        return False

    if _DATE_RE.search(lines[header_idx + 1]) is None:
        return False

    stats_line = None
    for line in lines[header_idx + 2:]:
        if '🕓' in line:
            stats_line = line
            break
    if stats_line is None:
        return False

    # Already parseable shapes — let the real parser handle them.
    if (_ACCURACY_RE.search(stats_line)
            or 'perfect' in stats_line.lower()
            or '🌟' in stats_line):
        return False

    return _SOLVED_RE.search(stats_line) is not None


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


_AKARI_MISSING_RESULT = SimpleNamespace(is_missing=True)


def akari_all_score_matchup(row1, row2):
    if getattr(row1, 'is_missing', False):
        if getattr(row2, 'is_missing', False):
            return 0.5, 0.5
        return 0.0, 1.0
    if getattr(row2, 'is_missing', False):
        return 1.0, 0.0
    return default_score_matchup(row1, row2)


AKARI_GAME = GameDef(
    name='akari',
    display_name='Daily Akari',
    feature_flag='akari',
    parse=parse_akari_message,
    detect=_HEADER_RE,
    rating=RatingDef(
        start_rating=constants.AKARI_START_RATING,
        damping=constants.AKARI_RATING_DAMPING,
        decay_base=constants.AKARI_DECAY_BASE,
        decay_max=constants.AKARI_DECAY_MAX,
        decay_grace=constants.AKARI_DECAY_GRACE,
        current_puzzle_number_fn=current_puzzle_number,
        max_puzzle_lookahead=constants.AKARI_MAX_PUZZLE_LOOKAHEAD,
    ),
    scoring_variants={
        'raw': ScoringDef(
            score_matchup=akari_raw_score_matchup,
            is_eligible_winner=akari_raw_is_eligible_winner,
            best_result_sort_key=akari_raw_best_result_sort_key,
            winner_result_sort_key=akari_raw_winner_result_sort_key,
        ),
        'all': ScoringDef(
            score_matchup=akari_all_score_matchup,
            is_eligible_winner=lambda _row: True,
            missing_is_loss=True,
            missing_result=_AKARI_MISSING_RESULT,
        ),
    },
)

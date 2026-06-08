"""LinkedIn Queens leaderboard parsing for the minigames system."""

import re
from collections import namedtuple

from tle.cogs._minigame_common import GameDef, RatingDef


_TIME_RE = re.compile(r'^\d{1,2}:\d{2}(?::\d{2})?$')
_RANK_RE = re.compile(r'^\d+$')
_URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)
_DETECT_RE = re.compile(r'Queens|No hints|No mistakes|\b\d{1,2}:\d{2}\b', re.IGNORECASE)

QueensLeaderboardEntry = namedtuple(
    'QueensLeaderboardEntry',
    'linkedin_name time_seconds no_hints no_mistakes status_text',
)


def normalize_queens_name(name):
    return ' '.join(str(name).strip().casefold().split())


def parse_queens_time(time_text):
    parts = [int(part) for part in time_text.split(':')]
    if len(parts) == 2:
        minutes, seconds = parts
        return minutes * 60 + seconds
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f'Unrecognized time format: {time_text}')


def _is_status_line(line):
    lowered = line.casefold()
    return (
        'hint' in lowered
        or 'mistake' in lowered
        or '\U0001f913' in line
        or '\U0001f48e' in line
    )


def queens_status_flags(status):
    if isinstance(status, str):
        status_text = status
    else:
        status_text = ' '.join(line for line in status if _is_status_line(line))
    lowered = status_text.casefold()
    no_hints = 'no hints' in lowered or '\U0001f913' in status_text
    no_mistakes = 'no mistakes' in lowered or '\U0001f48e' in status_text
    return no_hints, no_mistakes, status_text


def _candidate_name(lines):
    candidates = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped == 'You':
            candidates.append(stripped)
            continue
        if _RANK_RE.match(stripped) or _TIME_RE.match(stripped):
            continue
        if _URL_RE.search(stripped) or _is_status_line(stripped):
            continue
        candidates.append(stripped)

    collapsed = []
    for candidate in candidates:
        if not collapsed or collapsed[-1] != candidate:
            collapsed.append(candidate)

    real_names = [name for name in collapsed if name != 'You']
    if real_names:
        return real_names[-1]
    if 'You' in collapsed:
        return 'You'
    return None


def parse_queens_leaderboard(content):
    """Parse a pasted LinkedIn Queens leaderboard into result entries.

    LinkedIn's copied leaderboard is noisy: names are repeated, rank numbers may
    appear before tied groups, and the current user can appear as ``You``.  This
    parser treats each time line as the end of one entry, then scans the block
    since the previous time for the closest real name and status badges.
    """
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    entries = []
    block_start = 0

    for index, line in enumerate(lines):
        if not _TIME_RE.match(line):
            continue
        block = lines[block_start:index]
        block_start = index + 1
        name = _candidate_name(block)
        if name is None:
            continue
        no_hints, no_mistakes, status_text = queens_status_flags(block)
        entries.append(QueensLeaderboardEntry(
            linkedin_name=name,
            time_seconds=parse_queens_time(line),
            no_hints=no_hints,
            no_mistakes=no_mistakes,
            status_text=status_text,
        ))

    return entries


def queens_time_score_matchup(row1, row2):
    if row1.time_seconds < row2.time_seconds:
        return 1.0, 0.0
    if row1.time_seconds > row2.time_seconds:
        return 0.0, 1.0
    return 0.5, 0.5


def queens_best_result_sort_key(row):
    return (-int(getattr(row, 'time_seconds', 0)), -int(getattr(row, 'message_id', 0)))


def queens_winner_result_sort_key(row):
    return -int(getattr(row, 'time_seconds', 0))


def queens_result_group_key(row):
    return int(row.puzzle_number)


def rank_queens_participants(rows):
    ordered = sorted(rows, key=lambda row: int(row.time_seconds))
    ranks = {}
    current_rank = 0
    prev_time = None
    for index, row in enumerate(ordered):
        time_seconds = int(row.time_seconds)
        if prev_time is None or time_seconds != prev_time:
            current_rank = index + 1
            prev_time = time_seconds
        ranks[str(row.user_id)] = current_rank
    return ranks


QUEENS_GAME = GameDef(
    name='queens',
    display_name='LinkedIn Queens',
    feature_flag='queens',
    parse=lambda _content: [],
    detect=_DETECT_RE,
    score_matchup=queens_time_score_matchup,
    is_eligible_winner=lambda _row: True,
    best_result_sort_key=queens_best_result_sort_key,
    winner_result_sort_key=queens_winner_result_sort_key,
    result_group_key=queens_result_group_key,
    manual_ingest_only=True,
    rating=RatingDef(rank_fn=rank_queens_participants),
)

"""Parsing shared by every LinkedIn daily puzzle (Queens, Tango, ...).

LinkedIn's games all publish the same two artefacts: a share message whose
header is ``<Game> #<n> | m:ss`` (the time sometimes wraps onto the next
line), and a copy-pasteable leaderboard of display names, badges, and times.
Only the game word in the header and the calendar anchor differ, so a game
module builds its share parser with :func:`make_share_parser` and reuses
everything else here unchanged.  ``_minigame_queens`` re-exports these under
their historical ``queens_*`` names for existing importers.
"""

import datetime as dt
import re
from collections import namedtuple
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from tle.cogs._minigame_common import ParsedResult, normalize_puzzle_date


# LinkedIn rolls every daily puzzle over at midnight Pacific, so that — not
# the host's local midnight — is what "today's puzzle" means for every
# LinkedIn game.
_LINKEDIN_TIME_ZONE = ZoneInfo('America/Los_Angeles')


def linkedin_current_puzzle_date(now=None):
    """The LinkedIn puzzle date currently in progress (midnight Pacific)."""
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    return now.astimezone(_LINKEDIN_TIME_ZONE).date()


@dataclass(frozen=True)
class LinkedInDef:
    """Identity and calendar settings for a LinkedIn daily puzzle.

    LinkedIn's games (Queens, Tango, ...) share one shape: a numbered daily
    puzzle that rolls over at midnight Pacific, a share message of the form
    ``<Game> #<n> | m:ss``, and a copy-pasteable leaderboard of display names.
    Results are therefore keyed by LinkedIn display name in
    ``minigame_unresolved_result`` and projected onto Discord users through
    ``minigame_player_link``.  That link lives in a namespace shared by every
    LinkedIn game — the same person has one LinkedIn profile — so registering
    once resolves all of them.  Only the calendar anchor and the delegated
    admin key are per game.
    """
    anchor_date: dt.date        # a known puzzle date ...
    anchor_number: int          # ... and the puzzle number published that day
    admins_key: str             # guild_config key for extra command admins
    link_namespace: str = 'linkedin'
    # Queens once stored ``date.toordinal()`` as the puzzle number; those rows
    # must still be found when deleting or re-rating by date.
    legacy_ordinal_numbers: bool = False

    def date_for_number(self, puzzle_number):
        return self.anchor_date + dt.timedelta(
            days=int(puzzle_number) - self.anchor_number)

    def number_for_date(self, puzzle_date):
        puzzle_date = normalize_puzzle_date(puzzle_date)
        return self.anchor_number + (puzzle_date - self.anchor_date).days

    def puzzle_numbers_for_date(self, puzzle_date):
        """Every puzzle number a stored row for this date might carry."""
        puzzle_date = normalize_puzzle_date(puzzle_date)
        numbers = [self.number_for_date(puzzle_date)]
        if self.legacy_ordinal_numbers:
            legacy_number = puzzle_date.toordinal()
            if legacy_number != numbers[0]:
                numbers.append(legacy_number)
        return numbers

    def current_puzzle_number(self):
        """The puzzle in progress on LinkedIn's clock (gates rating decay)."""
        return self.number_for_date(linkedin_current_puzzle_date())


_TIME_RE = re.compile(r'^\d{1,2}:\d{2}(?::\d{2})?$')
_SHARE_TIME_RE = re.compile(r'(?<!\d)(\d{1,2}:\d{2}(?::\d{2})?)(?!\d)')
_RANK_RE = re.compile(r'^\d+$')
_URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)
# LinkedIn ramps every game up through each Monday-Sunday puzzle week.  Map
# the four weekday bands directly to evenly spaced weekly-rating levels:
# Easy, Medium, Hard, Very Hard.
LINKEDIN_WEEKDAY_DIFFICULTIES = (1, 1, 2, 2, 3, 3, 4)

LinkedInLeaderboardEntry = namedtuple(
    'LinkedInLeaderboardEntry',
    'linkedin_name time_seconds no_hints no_mistakes status_text is_you',
)


def share_header_re(game_word):
    """Header regex for one game's share message, e.g. ``Queens #774 | 1:26``."""
    return re.compile(rf'\b{game_word}\s*#\s*(\d+)\b(.*)', re.IGNORECASE)


def share_detect_re(game_word):
    """Loose detector used only to log near-miss messages in a game channel."""
    return re.compile(
        rf'{game_word}|No hints|No mistakes|\b\d{{1,2}}:\d{{2}}\b',
        re.IGNORECASE)


def normalize_linkedin_name(name):
    return ' '.join(str(name).strip().casefold().split())


def linkedin_weekly_difficulty_map(rows):
    """Return static LinkedIn weekday difficulties for every represented week.

    Fill all seven puzzle numbers even when the database has results for only
    some days.  The weekly scorer normalizes over the whole week, so leaving a
    missing day at its neutral fallback would distort that week's weights.
    """
    difficulties = {}
    for row in rows:
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        monday_number = int(row.puzzle_number) - puzzle_date.weekday()
        for offset, difficulty in enumerate(LINKEDIN_WEEKDAY_DIFFICULTIES):
            difficulties[monday_number + offset] = difficulty
    return difficulties


def parse_linkedin_time(time_text):
    parts = [int(part) for part in time_text.split(':')]
    if len(parts) == 2:
        minutes, seconds = parts
        return minutes * 60 + seconds
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f'Unrecognized time format: {time_text}')


def make_share_parser(header_re, calendar):
    """Build a share-message parser for one game.

    ``header_re`` must capture the puzzle number in group 1 and the rest of
    the header line in group 2; ``calendar`` is the game's ``LinkedInDef``.

    Example accepted shapes::

        Queens #774 | 1:26
        No mistakes & no hints
        lnkd.in/queens.

        Tango #695
        0:08 🌗
        lnkd.in/tango.

    Status text is intentionally ignored; channel shares count as clean.
    """
    def parse(content):
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        for index, line in enumerate(lines):
            header = header_re.search(line)
            if header is None:
                continue
            puzzle_number = int(header.group(1))
            time_text = header.group(2) or ''
            if not _SHARE_TIME_RE.search(time_text):
                time_text = '\n'.join(lines[index + 1:])
            time_match = _SHARE_TIME_RE.search(time_text)
            if time_match is None:
                return []
            try:
                time_seconds = parse_linkedin_time(time_match.group(1))
            except ValueError:
                return []
            return [ParsedResult(
                puzzle_number=puzzle_number,
                puzzle_date=calendar.date_for_number(puzzle_number),
                accuracy=100,
                time_seconds=time_seconds,
                is_perfect=True,
            )]
        return []
    return parse


def _is_status_line(line):
    lowered = line.casefold()
    return (
        'hint' in lowered
        or 'mistake' in lowered
        or '\U0001f913' in line
        or '\U0001f48e' in line
    )


def linkedin_status_flags(status):
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
        return real_names[-1], 'You' in collapsed
    if 'You' in collapsed:
        return 'You', True
    return None, False


def parse_linkedin_leaderboard(content):
    """Parse a pasted LinkedIn leaderboard into result entries.

    LinkedIn's copied leaderboard is noisy: names are repeated, rank numbers may
    appear before tied groups, and the current user can appear as ``You``.  This
    parser treats each time line as the end of one entry, then scans the block
    since the previous time for the closest real name and status badges.  The
    format is identical across LinkedIn's games.
    """
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    entries = []
    block_start = 0

    for index, line in enumerate(lines):
        if not _TIME_RE.match(line):
            continue
        block = lines[block_start:index]
        block_start = index + 1
        name, is_you = _candidate_name(block)
        if name is None:
            continue
        no_hints, no_mistakes, status_text = linkedin_status_flags(block)
        entries.append(LinkedInLeaderboardEntry(
            linkedin_name=name,
            time_seconds=parse_linkedin_time(line),
            no_hints=no_hints,
            no_mistakes=no_mistakes,
            status_text=status_text,
            is_you=is_you,
        ))

    return entries


# ── Scoring shared by time-only LinkedIn games ───────────────────────────

def linkedin_time_score_matchup(row1, row2):
    if row1.time_seconds < row2.time_seconds:
        return 1.0, 0.0
    if row1.time_seconds > row2.time_seconds:
        return 0.0, 1.0
    return 0.5, 0.5


def linkedin_best_result_sort_key(row):
    return (-int(getattr(row, 'time_seconds', 0)), -int(getattr(row, 'message_id', 0)))


def linkedin_winner_result_sort_key(row):
    return -int(getattr(row, 'time_seconds', 0))


def linkedin_result_group_key(row):
    return normalize_puzzle_date(row.puzzle_date)


def rank_linkedin_participants(rows):
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

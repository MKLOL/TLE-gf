"""Shared types and computation functions for the minigames system."""

import datetime as dt
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from tle.util import codeforces_common as cf_common

_NO_TIME_BOUND = 10 ** 10
_TIMELINE_KEYWORDS = {'week', 'month', 'year'}

_CODEBLOCK_RE = re.compile(r'^```[^\n]*\n?(.*?)```\s*$', re.DOTALL)


def strip_codeblock(text):
    """Remove Discord code-block / inline-code markers from *text*.

    Handles triple-backtick blocks (with optional language tag) and
    per-line or whole-message single backticks so that parsers always
    see clean content.
    """
    m = _CODEBLOCK_RE.match(text.strip())
    if m:
        return m.group(1)
    return text.replace('`', '')


@dataclass(frozen=True)
class ParsedResult:
    """Parsed result from a daily puzzle game message."""
    puzzle_number: int
    puzzle_date: Optional[dt.date] = None  # None = cog fills from message timestamp
    accuracy: int = 0
    time_seconds: int = 0
    is_perfect: bool = False


@dataclass(frozen=True)
class ScoringDef:
    """Scoring/ranking behavior for a game command mode."""
    score_matchup: Optional[Callable] = None
    is_eligible_winner: Optional[Callable] = None
    best_result_sort_key: Optional[Callable] = None
    winner_result_sort_key: Optional[Callable] = None
    result_group_key: Optional[Callable] = None


@dataclass(frozen=True)
class GameDef:
    """Definition of a daily puzzle minigame.

    To add a new game, create a ``GameDef`` with a parser that converts a
    Discord message body into a list of ``ParsedResult`` (empty list if the
    message doesn't match).  Then register it in ``Minigames.GAMES`` and add
    thin command wrappers in ``minigames.py``.
    """
    name: str               # short key used in DB, e.g. 'akari'
    display_name: str       # human-readable, e.g. 'Daily Akari'
    feature_flag: str       # guild config key, e.g. 'akari'
    parse: Callable[[str], List[ParsedResult]]
    detect: Optional[re.Pattern] = None  # loose pattern to detect game content (for logging)
    # Optional per-game overrides (defaults = Akari-style scoring)
    score_matchup: Optional[Callable] = None
    is_eligible_winner: Optional[Callable] = None
    best_result_sort_key: Optional[Callable] = None
    winner_result_sort_key: Optional[Callable] = None
    result_group_key: Optional[Callable] = None
    missing_is_loss: bool = False  # if True, missing puzzle = automatic loss in VS
    missing_result: object = None  # synthetic result for missing puzzles (used with score_fn)
    scoring_variants: Dict[str, ScoringDef] = field(default_factory=dict)


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


def winner_result_sort_key(row):
    return result_sort_key(row)[:3]


def pick_best_results(rows, sort_key_fn=None, group_key_fn=None):
    if sort_key_fn is None:
        sort_key_fn = result_sort_key
    if group_key_fn is None:
        group_key_fn = result_key
    best = {}
    for row in rows:
        key = group_key_fn(row)
        prev = best.get(key)
        if prev is None or sort_key_fn(row) > sort_key_fn(prev):
            best[key] = row
    return best


def format_duration(total_seconds):
    minutes, seconds = divmod(int(total_seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}:{minutes:02d}:{seconds:02d}'
    return f'{minutes}:{seconds:02d}'


# ── Scoring ─────────────────────────────────────────────────────────────

def default_score_matchup(row1, row2):
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


def default_is_eligible_winner(row):
    """Default eligibility for top leaderboard: only perfect results."""
    return bool(row.is_perfect)


def resolve_scoring(game, args):
    """Split trailing scoring selector from ``args`` and resolve its config."""
    variant = ScoringDef(
        score_matchup=game.score_matchup,
        is_eligible_winner=game.is_eligible_winner,
        best_result_sort_key=game.best_result_sort_key,
        winner_result_sort_key=game.winner_result_sort_key,
        result_group_key=game.result_group_key,
    )
    if args:
        mode = args[-1].lower()
        override = game.scoring_variants.get(mode)
        if override is not None:
            variant = ScoringDef(
                score_matchup=override.score_matchup or variant.score_matchup,
                is_eligible_winner=override.is_eligible_winner or variant.is_eligible_winner,
                best_result_sort_key=override.best_result_sort_key or variant.best_result_sort_key,
                winner_result_sort_key=override.winner_result_sort_key or variant.winner_result_sort_key,
                result_group_key=override.result_group_key or variant.result_group_key,
            )
            return args[:-1], mode, variant
    return args, None, variant


def compute_vs_matchups(rows1, rows2, score_fn=None, missing_is_loss=False,
                        best_result_sort_key_fn=None, group_key_fn=None,
                        missing_result=None):
    if score_fn is None:
        score_fn = default_score_matchup
    best1 = pick_best_results(
        rows1, sort_key_fn=best_result_sort_key_fn, group_key_fn=group_key_fn)
    best2 = pick_best_results(
        rows2, sort_key_fn=best_result_sort_key_fn, group_key_fn=group_key_fn)

    if missing_is_loss:
        puzzles = sorted(set(best1) | set(best2))
    else:
        puzzles = sorted(set(best1) & set(best2))

    matchups = []

    for key in puzzles:
        r1 = best1.get(key)
        r2 = best2.get(key)
        if r1 is None and r2 is None:
            continue
        if r1 is None:
            if missing_result is not None:
                pts1, pts2 = score_fn(missing_result, r2)
            else:
                pts1, pts2 = 0.0, 1.0
        elif r2 is None:
            if missing_result is not None:
                pts1, pts2 = score_fn(r1, missing_result)
            else:
                pts1, pts2 = 1.0, 0.0
        else:
            pts1, pts2 = score_fn(r1, r2)
        matchups.append({
            'key': key,
            'row1': r1,
            'row2': r2,
            'score1': pts1,
            'score2': pts2,
        })

    return matchups


def compute_vs(rows1, rows2, score_fn=None, missing_is_loss=False,
               best_result_sort_key_fn=None, group_key_fn=None,
               missing_result=None):
    matchups = compute_vs_matchups(
        rows1, rows2,
        score_fn=score_fn,
        missing_is_loss=missing_is_loss,
        best_result_sort_key_fn=best_result_sort_key_fn,
        group_key_fn=group_key_fn,
        missing_result=missing_result,
    )

    score1, score2 = 0.0, 0.0
    wins1, wins2, ties = 0, 0, 0

    for matchup in matchups:
        pts1 = matchup['score1']
        pts2 = matchup['score2']
        score1 += pts1
        score2 += pts2
        if pts1 == pts2:
            ties += 1
        elif pts1 > pts2:
            wins1 += 1
        else:
            wins2 += 1

    return {
        'common_count': len(matchups),
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


def compute_longest_streak(rows):
    """Return the longest run of consecutive perfect days across all data."""
    best_by_day = {}
    for row in rows:
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        prev = best_by_day.get(puzzle_date)
        if prev is None or result_sort_key(row) > result_sort_key(prev):
            best_by_day[puzzle_date] = row

    if not best_by_day:
        return 0

    longest = 0
    current = 0
    prev_day = None
    for day in sorted(best_by_day):
        row = best_by_day[day]
        if row.is_perfect and (prev_day is None or day == prev_day + dt.timedelta(days=1)):
            current += 1
        elif row.is_perfect:
            current = 1
        else:
            current = 0
        longest = max(longest, current)
        prev_day = day
    return longest


def compute_top(rows, is_eligible=None, best_result_sort_key_fn=None,
                winner_result_sort_key_fn=None, group_key_fn=None):
    if is_eligible is None:
        is_eligible = default_is_eligible_winner
    if best_result_sort_key_fn is None:
        best_result_sort_key_fn = result_sort_key
    if winner_result_sort_key_fn is None:
        winner_result_sort_key_fn = winner_result_sort_key
    if group_key_fn is None:
        group_key_fn = result_key
    best_by_user_puzzle = {}
    for row in rows:
        key = (str(row.user_id), group_key_fn(row))
        prev = best_by_user_puzzle.get(key)
        if prev is None or best_result_sort_key_fn(row) > best_result_sort_key_fn(prev):
            best_by_user_puzzle[key] = row

    best_per_puzzle = {}
    for (_, puzzle_key), row in best_by_user_puzzle.items():
        if not is_eligible(row):
            continue
        entry = best_per_puzzle.get(puzzle_key)
        row_key = winner_result_sort_key_fn(row)
        if entry is None or row_key > entry['sort_key']:
            best_per_puzzle[puzzle_key] = {'sort_key': row_key, 'rows': [row]}
        elif row_key == entry['sort_key']:
            entry['rows'].append(row)

    wins_by_user = {}
    for entry in best_per_puzzle.values():
        for row in entry['rows']:
            user_id = str(row.user_id)
            wins_by_user[user_id] = wins_by_user.get(user_id, 0) + 1

    return sorted(wins_by_user.items(), key=lambda item: (-item[1], int(item[0])))


# ── Argument parsing ────────────────────────────────────────────────────

def parse_date_args(args):
    """Parse timeline and puzzle-number filter arguments.

    Returns ``(dlo, dhi, plo, phi)`` where dlo/dhi are timestamps and
    plo/phi are puzzle-number bounds (0 = unbounded).

    Raises ``ValueError`` on unrecognized arguments.
    """
    dlo = 0
    dhi = _NO_TIME_BOUND
    plo = 0
    phi = 0

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
        elif lower.startswith('p>='):
            plo = max(plo, int(arg[3:]))
        elif lower.startswith('p<'):
            val = int(arg[2:])
            phi = min(phi, val) if phi > 0 else val
        else:
            raise ValueError(f'Unrecognized filter: `{arg}`.')
    return dlo, dhi, plo, phi

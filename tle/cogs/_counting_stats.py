"""Pure aggregation and compact rendering for counting-channel statistics.

The counting cog owns Discord and persistence concerns.  This module accepts
either mapping rows or attribute-based rows so it can be shared by the cog,
the SQLite layer, and focused tests without importing discord.py.
"""

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Optional, Tuple


_MISSING = object()
_BASE_LABELS = {10: 'DEC', 2: 'BIN', 16: 'HEX'}
_BASE_ORDER = (10, 2, 16)
_MAX_LEADERBOARD_ROWS = 10
_MAX_STORED_NAME = 80


@dataclass(frozen=True)
class AuthorTotal:
    """One author and their success or miss total."""

    user_id: str
    author_name: str
    count: int


@dataclass(frozen=True)
class BaseTotal:
    """Number of successful counts written in one radix."""

    radix: int
    count: int


@dataclass(frozen=True)
class SuccessGap:
    """Elapsed time between two consecutive successful numbers."""

    seconds: float
    from_number: Optional[int]
    to_number: Optional[int]


@dataclass(frozen=True)
class GapSummary:
    """Aggregate timing information for consecutive successful numbers."""

    sample_count: int
    fastest: Optional[SuccessGap]
    longest: Optional[SuccessGap]
    average_seconds: Optional[float]


@dataclass(frozen=True)
class SameUserStreak:
    """A run of consecutive successful numbers from one author."""

    user_id: str
    author_name: str
    length: int
    start_number: Optional[int]
    end_number: Optional[int]


@dataclass(frozen=True)
class CountingStats:
    """Structured counting statistics ready for a Discord-facing renderer."""

    current_count: int
    total_successes: int
    total_attempts: int
    accuracy_percent: float
    unique_counters: int
    top_success_authors: Tuple[AuthorTotal, ...]
    most_misses: Tuple[AuthorTotal, ...]
    base_usage: Tuple[BaseTotal, ...]
    gaps: GapSummary
    longest_same_user_streak: Optional[SameUserStreak]
    current_same_user_streak: Optional[SameUserStreak] = None


@dataclass(frozen=True)
class _Attempt:
    accepted: bool
    user_id: Optional[str]
    author_name: str
    created_at: Optional[float]
    number: Optional[int]
    radix: Optional[int]
    position: int


def build_counting_stats(rows, current_count=None, *, leaderboard_limit=3):
    """Public aggregation entry point used by the counting cog."""
    return summarize_counting_attempts(
        rows, current_count, leaderboard_limit=leaderboard_limit)


def summarize_counting_attempts(attempts, current_count=None, *,
                                leaderboard_limit=3):
    """Return :class:`CountingStats` for mapping or attribute-based rows.

    ``expected_value`` is preferred for the successful number, with ``number``
    accepted as a compatibility fallback.  Base usage counts successful rows;
    unique counters likewise means authors with at least one success.  Timing
    and streak metrics use chronological successful rows and do not bridge a
    discontinuity in their recorded numbers.
    """
    rows = tuple(_normalize_attempt(row, index)
                 for index, row in enumerate(attempts))
    successes = tuple(row for row in rows if row.accepted)
    failures = tuple(row for row in rows if not row.accepted)
    names = _latest_names(rows)
    limit = min(_MAX_LEADERBOARD_ROWS,
                max(0, _coerce_int(leaderboard_limit, 3)))

    success_counts = Counter(
        row.user_id for row in successes if row.user_id is not None)
    miss_counts = Counter(
        row.user_id for row in failures if row.user_id is not None)
    base_counts = Counter(
        row.radix for row in successes if row.radix in _BASE_LABELS)
    ordered = sorted(successes, key=_success_order_key)
    longest_streak, current_streak = _same_user_streaks(ordered, names)

    total_attempts = len(rows)
    total_successes = len(successes)
    accuracy = (100.0 * total_successes / total_attempts
                if total_attempts else 0.0)
    derived_count = max(
        (row.number for row in successes if row.number is not None),
        default=0)

    return CountingStats(
        current_count=max(
            0, _coerce_int(current_count, derived_count)
            if current_count is not None else derived_count),
        total_successes=total_successes,
        total_attempts=total_attempts,
        accuracy_percent=accuracy,
        unique_counters=len(success_counts),
        top_success_authors=_rank_authors(
            success_counts, names, limit),
        most_misses=_rank_authors(miss_counts, names, limit),
        base_usage=tuple(
            BaseTotal(radix, base_counts[radix]) for radix in _BASE_ORDER),
        gaps=_summarize_gaps(ordered),
        longest_same_user_streak=longest_streak,
        current_same_user_streak=current_streak,
    )


def _normalize_attempt(row, position):
    number = _field(row, 'expected_value', _MISSING)
    if number is _MISSING or number is None:
        number = _field(row, 'number', None)
    return _Attempt(
        accepted=_accepted(_field(row, 'accepted', False)),
        user_id=_user_id(_field(row, 'user_id', None)),
        author_name=_clean_name(_field(row, 'author_name', None)),
        created_at=_timestamp(_field(row, 'created_at', None)),
        number=_optional_int(number),
        radix=_optional_int(_field(row, 'radix', None)),
        position=position,
    )


def _field(row, name, default):
    if isinstance(row, Mapping):
        return row.get(name, default)
    try:
        return getattr(row, name)
    except (AttributeError, TypeError):
        return default


def _accepted(value):
    if isinstance(value, str):
        return value.strip().casefold() in {
            '1', 'true', 'yes', 'accepted', 'ok', 'success'}
    return bool(value)


def _user_id(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _coerce_int(value, default):
    converted = _optional_int(value)
    return default if converted is None else converted


def _timestamp(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(value, (int, float)):
        converted = float(value)
        return converted if math.isfinite(converted) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        converted = float(text)
    except ValueError:
        try:
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return converted if math.isfinite(converted) else None


def _clean_name(value):
    text = ' '.join(str(value or '').split())
    text = ''.join(char for char in text if char.isprintable())
    return text[:_MAX_STORED_NAME] or 'Unknown'


def _latest_names(rows):
    latest = {}
    for row in rows:
        if row.user_id is None or row.author_name == 'Unknown':
            continue
        key = (row.created_at is not None,
               row.created_at if row.created_at is not None else row.position,
               row.position)
        if row.user_id not in latest or key > latest[row.user_id][0]:
            latest[row.user_id] = (key, row.author_name)
    return {user_id: value[1] for user_id, value in latest.items()}


def _rank_authors(counts, names, limit):
    ranked = sorted(
        counts.items(),
        key=lambda item: (
            -item[1], names.get(item[0], item[0]).casefold(), item[0]))
    return tuple(
        AuthorTotal(user_id, names.get(user_id, f'User {user_id}'), count)
        for user_id, count in ranked[:limit])


def _success_order_key(row):
    return (row.created_at is None,
            row.created_at if row.created_at is not None else math.inf,
            row.number if row.number is not None else math.inf,
            row.position)


def _numbers_are_consecutive(left, right):
    if left.number is None or right.number is None:
        return True
    return right.number == left.number + 1


def _summarize_gaps(successes):
    gaps = []
    for left, right in zip(successes, successes[1:]):
        if (not _numbers_are_consecutive(left, right)
                or left.created_at is None or right.created_at is None):
            continue
        seconds = right.created_at - left.created_at
        if seconds < 0:
            continue
        gaps.append(SuccessGap(seconds, left.number, right.number))
    if not gaps:
        return GapSummary(0, None, None, None)
    fastest = min(gaps, key=lambda gap: gap.seconds)
    longest = max(gaps, key=lambda gap: gap.seconds)
    return GapSummary(
        len(gaps), fastest, longest,
        sum(gap.seconds for gap in gaps) / len(gaps))


def _same_user_streaks(successes, names):
    """Return the longest and current runs, ignoring unsuccessful attempts."""
    best = None
    current = []
    for row in successes:
        continues = (
            current and row.user_id is not None
            and row.user_id == current[-1].user_id
            and _numbers_are_consecutive(current[-1], row))
        if continues:
            current.append(row)
        else:
            candidate = _make_streak(current, names)
            if candidate is not None and (best is None
                                          or candidate.length > best.length):
                best = candidate
            current = [row] if row.user_id is not None else []
    candidate = _make_streak(current, names)
    if candidate is not None and (best is None or candidate.length > best.length):
        best = candidate
    return best, candidate


def _make_streak(rows, names):
    if not rows:
        return None
    user_id = rows[0].user_id
    return SameUserStreak(
        user_id=user_id,
        author_name=names.get(user_id, f'User {user_id}'),
        length=len(rows),
        start_number=rows[0].number,
        end_number=rows[-1].number,
    )


from tle.cogs._counting_stats_render import (  # noqa: E402,F401
    format_counting_stats, format_duration, render_counting_stats,
)

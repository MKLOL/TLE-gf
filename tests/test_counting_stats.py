from collections import namedtuple
from datetime import datetime, timezone
from types import MappingProxyType, SimpleNamespace

import pytest

from tle.cogs._counting_stats import (
    build_counting_stats,
    format_counting_stats,
    format_duration,
    render_counting_stats,
)


Attempt = namedtuple(
    'Attempt',
    'accepted user_id created_at expected_value radix author_name',
)


def _attempt(accepted, user_id, created_at, number, radix, name):
    return Attempt(accepted, str(user_id), created_at, number, radix, name)


def test_build_counting_stats_computes_requested_fun_stats():
    rows = [
        _attempt(True, 1, 0, 1, 10, 'Alice'),
        _attempt(False, 2, 2, 2, 10, 'Bob'),
        _attempt(True, 1, 10, 2, 2, 'Alice'),
        _attempt(True, 1, 14, 3, 16, 'Alice'),
        _attempt(False, 2, 15, 4, 10, 'Bob'),
        _attempt(False, 2, 16, 4, 2, 'Bob'),
        _attempt(True, 2, 34, 4, 10, 'Bob'),
        _attempt(True, 3, 40, 5, 2, 'Cara'),
    ]

    stats = build_counting_stats(rows, current_count=5)

    assert stats.current_count == 5
    assert stats.total_successes == 5
    assert stats.total_attempts == 8
    assert stats.accuracy_percent == 62.5
    assert stats.unique_counters == 3
    assert [(row.author_name, row.count)
            for row in stats.top_success_authors] == [
                ('Alice', 3), ('Bob', 1), ('Cara', 1)]
    assert [(row.author_name, row.count) for row in stats.most_misses] == [
        ('Bob', 3)]
    assert [(row.radix, row.count) for row in stats.base_usage] == [
        (10, 2), (2, 2), (16, 1)]

    assert stats.gaps.sample_count == 4
    assert stats.gaps.fastest.seconds == 4
    assert (stats.gaps.fastest.from_number,
            stats.gaps.fastest.to_number) == (2, 3)
    assert stats.gaps.longest.seconds == 20
    assert (stats.gaps.longest.from_number,
            stats.gaps.longest.to_number) == (3, 4)
    assert stats.gaps.average_seconds == 10

    streak = stats.longest_same_user_streak
    assert (streak.author_name, streak.length) == ('Alice', 3)
    assert (streak.start_number, streak.end_number) == (1, 3)

    current = stats.current_same_user_streak
    assert (current.author_name, current.length) == ('Cara', 1)
    assert (current.start_number, current.end_number) == (5, 5)


def test_current_streak_uses_latest_run_and_ignores_trailing_misses():
    rows = [
        _attempt(True, 1, 0, 1, 10, 'Alice'),
        _attempt(True, 1, 1, 2, 10, 'Alice'),
        _attempt(True, 1, 2, 3, 10, 'Alice'),
        _attempt(True, 2, 3, 4, 10, 'Bob'),
        _attempt(False, 1, 4, 5, 10, 'Alice'),
        _attempt(True, 2, 5, 5, 16, 'Bob'),
        _attempt(False, 1, 6, 6, 10, 'Alice'),
    ]

    stats = build_counting_stats(reversed(rows))

    assert stats.longest_same_user_streak.author_name == 'Alice'
    assert stats.longest_same_user_streak.length == 3
    current = stats.current_same_user_streak
    assert (current.user_id, current.author_name, current.length) == ('2', 'Bob', 2)
    assert (current.start_number, current.end_number) == (4, 5)
    assert '**Current same-user streak:** Bob — 2 (#4–#5)' in \
        render_counting_stats(stats)


def test_rows_can_be_mappings_or_objects_and_number_is_a_fallback():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [
        MappingProxyType({
            'accepted': 'yes', 'user_id': 3,
            'created_at': '2025-01-01T00:00:15Z', 'number': 3,
            'radix': '16', 'author_name': 'Cara',
        }),
        SimpleNamespace(
            accepted=1, user_id=1, created_at=start,
            number=1, radix=10, author_name='Alice'),
        {
            'accepted': 'true', 'user_id': 2,
            'created_at': str(start.timestamp() + 10), 'number': 2,
            'radix': 2, 'author_name': 'Bob',
        },
    ]

    stats = build_counting_stats(rows)

    assert stats.current_count == 3
    assert stats.total_successes == 3
    assert stats.gaps.fastest.seconds == 5
    assert stats.gaps.longest.seconds == 10
    assert stats.gaps.average_seconds == 7.5


def test_gaps_and_streaks_do_not_bridge_number_discontinuities():
    rows = [
        _attempt(True, 1, 0, 1, 10, 'Alice'),
        _attempt(True, 1, 100, 3, 10, 'Alice'),
    ]

    stats = build_counting_stats(rows)

    assert stats.gaps.sample_count == 0
    assert stats.gaps.fastest is None
    assert stats.gaps.longest is None
    assert stats.gaps.average_seconds is None
    assert stats.longest_same_user_streak.length == 1
    assert stats.current_same_user_streak.length == 1
    assert stats.current_same_user_streak.start_number == 3


def test_failures_do_not_break_a_consecutive_success_streak():
    rows = [
        _attempt(True, 1, 0, 1, 10, 'Alice'),
        _attempt(False, 2, 2, 2, 10, 'Bob'),
        _attempt(True, 1, 4, 2, 10, 'Alice'),
    ]

    stats = build_counting_stats(rows)

    assert stats.longest_same_user_streak.length == 2
    assert stats.current_same_user_streak == stats.longest_same_user_streak
    assert stats.gaps.sample_count == 1


def test_empty_and_unidentified_rows_are_safe():
    empty = build_counting_stats([], current_count='7')

    assert empty.current_count == 7
    assert empty.total_attempts == 0
    assert empty.accuracy_percent == 0
    assert empty.unique_counters == 0
    assert empty.longest_same_user_streak is None
    assert empty.current_same_user_streak is None
    _, fields = format_counting_stats(empty)
    assert ('🔥 Current same-user streak', 'No successes yet', False) in fields

    unknown = build_counting_stats([{
        'accepted': True, 'expected_value': 8, 'created_at': 'bad',
        'radix': None, 'author_name': None,
    }])
    assert unknown.current_count == 8
    assert unknown.total_successes == 1
    assert unknown.unique_counters == 0
    assert unknown.top_success_authors == ()
    assert unknown.longest_same_user_streak is None
    assert unknown.current_same_user_streak is None

    rows = [_attempt(True, 1, 0, 7, 10, 'Alice'), {
        'accepted': True, 'expected_value': 8, 'created_at': 1,
    }]
    assert build_counting_stats(rows).current_same_user_streak is None


def test_rankings_are_deterministic_and_respect_limit():
    rows = [
        _attempt(True, 2, 0, 1, 10, 'Bob'),
        _attempt(True, 1, 1, 2, 10, 'Alice'),
        _attempt(False, 2, 2, 3, 10, 'Bob'),
        _attempt(False, 1, 3, 3, 10, 'Alice'),
    ]

    stats = build_counting_stats(rows, 2, leaderboard_limit=1)

    assert [(row.author_name, row.count)
            for row in stats.top_success_authors] == [('Alice', 1)]
    assert [(row.author_name, row.count)
            for row in stats.most_misses] == [('Alice', 1)]


def test_embed_format_api_is_bounded_and_neutralizes_names():
    rows = [
        _attempt(True, 1, 0, 1, 10, '@everyone **Alice**\nsecond line'),
        _attempt(False, 1, 1, 2, 10, '@everyone **Alice**\nsecond line'),
        _attempt(True, 1, 2, 2, 2, '@everyone **Alice**\nsecond line'),
    ]
    stats = build_counting_stats(rows, 2)

    description, fields = format_counting_stats(
        stats, max_description_chars=80, max_field_chars=60)

    assert len(description) <= 80
    assert len(fields) == 6
    assert all(len(value) <= 60 for _, value, _ in fields)
    rendered = '\n'.join([description] + [value for _, value, _ in fields])
    assert '@\u200beveryone' in rendered
    assert r'\*\*Alice\*\*' in rendered
    assert '\nsecond line' not in rendered

    compact = render_counting_stats(stats, max_chars=100)
    assert len(compact) <= 100
    assert compact.startswith('**Current count:** 2')


@pytest.mark.parametrize(('seconds', 'expected'), [
    (0, '0s'),
    (0.5, '0.5s'),
    (9.6, '10s'),
    (65, '1m 05s'),
    (3661, '1h 01m'),
    (90000, '1d 1h'),
    (None, '—'),
])
def test_format_duration(seconds, expected):
    assert format_duration(seconds) == expected

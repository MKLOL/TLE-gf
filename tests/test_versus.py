"""Tests for the versus cog's pure computation logic.

The DB/API-integration tests (handle aliases, rating-change fetching) live in
``test_versus_db`` to keep this module under the 500-line limit; the shared
``_make_rc`` fake is in ``tests/versus_test_utils``.
"""

import time

from tests.versus_test_utils import _make_rc
from tle.cogs.versus import (
    _compute_versus_stats,
    _filter_changes_by_date,
    _is_stale,
    _list_shared_contests,
    _parse_versus_args,
)


class TestComputeVersusStats:
    def test_basic_two_users(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(2, 'alice', 5)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(2, 'bob', 3)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 2
        # Contest 1: alice rank 10 < bob rank 20 → alice wins
        # Contest 2: bob rank 3 < alice rank 5 → bob wins
        assert wins['alice'] == 1
        assert wins['bob'] == 1

    def test_three_users_placements(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 100)],
            'b': [_make_rc(1, 'b', 50)],
            'c': [_make_rc(1, 'c', 200)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        # b=50 < a=100 < c=200 → b is 1st, a is 2nd, c is 3rd
        assert wins['b'] == 1
        assert wins['a'] == 0
        assert wins['c'] == 0
        assert placements['b'][1] == 1
        assert placements['a'][2] == 1
        assert placements['c'][3] == 1

    def test_no_shared_contests(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(2, 'bob', 20)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0
        assert wins['alice'] == 0
        assert wins['bob'] == 0

    def test_tie_no_win_awarded(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(1, 'bob', 10)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        # Same rank → tie → no one gets a win
        assert wins['alice'] == 0
        assert wins['bob'] == 0
        # Both get 1st place (competition ranking)
        assert placements['alice'][1] == 1
        assert placements['bob'][1] == 1

    def test_competition_ranking_three_way(self):
        """Two users tie for 1st, third user gets 3rd (not 2nd)."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5)],
            'b': [_make_rc(1, 'b', 5)],
            'c': [_make_rc(1, 'c', 20)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 1
        assert wins['a'] == 0  # Tie, no win
        assert wins['b'] == 0
        assert wins['c'] == 0
        # a and b both get 1st, c gets 3rd (competition ranking skips 2nd)
        assert placements['a'][1] == 1
        assert placements['b'][1] == 1
        assert placements['c'][3] == 1
        assert placements['c'].get(2, 0) == 0  # 2nd place not assigned

    def test_partial_overlap(self):
        """Only contests where 2+ users participated are counted."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10), _make_rc(3, 'a', 1)],
            'b': [_make_rc(1, 'b', 10), _make_rc(3, 'b', 2)],
            'c': [_make_rc(2, 'c', 5)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        # Shared: contest 1 (a, b), contest 2 (a, c), contest 3 (a, b) = 3 contests
        assert total == 3
        # Contest 1: a=5 beats b=10 → a wins
        # Contest 2: c=5 beats a=10 → c wins
        # Contest 3: a=1 beats b=2 → a wins
        assert wins['a'] == 2
        assert wins['b'] == 0
        assert wins['c'] == 1

    def test_empty_changes(self):
        handles = ['a', 'b']
        all_changes = {'a': [], 'b': []}
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0

    def test_missing_handle_in_changes(self):
        handles = ['a', 'b']
        all_changes = {'a': [_make_rc(1, 'a', 5)]}  # 'b' missing entirely
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 0

    def test_multiple_contests_accumulate(self):
        handles = ['x', 'y']
        all_changes = {
            'x': [_make_rc(i, 'x', 10) for i in range(1, 6)],
            'y': [_make_rc(i, 'y', 20) for i in range(1, 6)],
        }
        wins, placements, total = _compute_versus_stats(handles, all_changes)
        assert total == 5
        assert wins['x'] == 5  # x always has better rank
        assert wins['y'] == 0
        assert placements['x'][1] == 5
        assert placements['y'][2] == 5


class TestListSharedContests:
    def test_basic_two_users(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(2, 'alice', 5)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(2, 'bob', 3)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert len(rows) == 2
        by_id = {r['contest_id']: r for r in rows}
        assert by_id[1]['ranks'] == {'alice': 10, 'bob': 20}
        assert by_id[2]['ranks'] == {'alice': 5, 'bob': 3}
        assert by_id[1]['name'] == 'Contest 1'

    def test_sorted_newest_first(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10), _make_rc(3, 'alice', 5), _make_rc(2, 'alice', 7)],
            'bob':   [_make_rc(1, 'bob', 20),   _make_rc(3, 'bob', 3),   _make_rc(2, 'bob', 9)],
        }
        rows = _list_shared_contests(handles, all_changes)
        # Time field is 1000000 + contest_id, so newest=3, then 2, then 1
        assert [r['contest_id'] for r in rows] == [3, 2, 1]

    def test_no_shared_contests(self):
        handles = ['alice', 'bob']
        all_changes = {
            'alice': [_make_rc(1, 'alice', 10)],
            'bob':   [_make_rc(2, 'bob', 20)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert rows == []

    def test_empty_inputs(self):
        assert _list_shared_contests(['a', 'b'], {}) == []
        assert _list_shared_contests(['a', 'b'], {'a': [], 'b': []}) == []

    def test_partial_overlap_only_shared_included(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10)],            # contest 1 shared with a
            'c': [_make_rc(3, 'c', 1)],             # contest 3 only c — excluded
        }
        rows = _list_shared_contests(handles, all_changes)
        assert {r['contest_id'] for r in rows} == {1}
        assert rows[0]['ranks'] == {'a': 5, 'b': 10}

    def test_strict_requires_all_handles(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 5)],
            'c': [_make_rc(2, 'c', 20)],
        }
        non_strict = _list_shared_contests(handles, all_changes, strict=False)
        assert {r['contest_id'] for r in non_strict} == {1, 2}

        strict = _list_shared_contests(handles, all_changes, strict=True)
        assert [r['contest_id'] for r in strict] == [2]
        assert strict[0]['ranks'] == {'a': 10, 'b': 5, 'c': 20}

    def test_missing_handle_in_changes_ok(self):
        handles = ['a', 'b']
        all_changes = {'a': [_make_rc(1, 'a', 5)]}  # 'b' missing entirely
        assert _list_shared_contests(handles, all_changes) == []

    def test_contest_name_preserved(self):
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(42, 'a', 1)],
            'b': [_make_rc(42, 'b', 2)],
        }
        rows = _list_shared_contests(handles, all_changes)
        assert rows[0]['name'] == 'Contest 42'
        assert rows[0]['time'] == 1000000 + 42

    def test_ranks_isolated_per_contest(self):
        """Make sure the ranks dict isn't shared between rows."""
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 7)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 12)],
        }
        rows = _list_shared_contests(handles, all_changes)
        by_id = {r['contest_id']: r for r in rows}
        # Mutating one shouldn't affect the other
        by_id[1]['ranks']['mutated'] = 99
        assert 'mutated' not in by_id[2]['ranks']


class TestVersusDateFilters:
    def test_parse_date_filters_removes_date_args(self):
        strict, date_from, date_to, handles = _parse_versus_args(
            ('+all', 'alice', 'd>=2024', 'bob', 'd<2025'))

        assert strict is True
        assert handles == ['alice', 'bob']
        assert date_from > 0
        assert date_to < 10**10
        assert date_from < date_to

    def test_filter_changes_by_date_is_inclusive_exclusive(self):
        all_changes = {
            'alice': [
                _make_rc(1, 'alice', 10, update_time=99),
                _make_rc(2, 'alice', 20, update_time=100),
                _make_rc(3, 'alice', 30, update_time=199),
                _make_rc(4, 'alice', 40, update_time=200),
            ],
            'bob': [
                _make_rc(2, 'bob', 15, update_time=100),
                _make_rc(4, 'bob', 35, update_time=200),
            ],
        }

        filtered = _filter_changes_by_date(all_changes, 100, 200)

        assert [rc.contestId for rc in filtered['alice']] == [2, 3]
        assert [rc.contestId for rc in filtered['bob']] == [2]


class TestStrictMode:
    def test_strict_requires_all_handles(self):
        """With strict=True, only contests where ALL handles participated count."""
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10), _make_rc(2, 'b', 5)],
            'c': [_make_rc(2, 'c', 20)],
        }
        # Non-strict: contest 1 (a,b) + contest 2 (a,b,c) = 2
        wins, placements, total = _compute_versus_stats(handles, all_changes, strict=False)
        assert total == 2

        # Strict: only contest 2 has all 3
        wins, placements, total = _compute_versus_stats(handles, all_changes, strict=True)
        assert total == 1
        # Contest 2: b=5 < a=10 < c=20 → b wins
        assert wins['b'] == 1
        assert wins['a'] == 0
        assert wins['c'] == 0

    def test_strict_no_shared_contests(self):
        handles = ['a', 'b', 'c']
        all_changes = {
            'a': [_make_rc(1, 'a', 5)],
            'b': [_make_rc(1, 'b', 10)],
            'c': [_make_rc(2, 'c', 20)],
        }
        # Non-strict: contest 1 has a,b → 1
        wins, _, total = _compute_versus_stats(handles, all_changes, strict=False)
        assert total == 1

        # Strict: no contest has all 3
        wins, _, total = _compute_versus_stats(handles, all_changes, strict=True)
        assert total == 0

    def test_strict_two_users_same_as_default(self):
        """With 2 users, strict and non-strict are equivalent."""
        handles = ['a', 'b']
        all_changes = {
            'a': [_make_rc(1, 'a', 5), _make_rc(2, 'a', 10)],
            'b': [_make_rc(1, 'b', 10)],
        }
        _, _, total_default = _compute_versus_stats(handles, all_changes, strict=False)
        _, _, total_strict = _compute_versus_stats(handles, all_changes, strict=True)
        # Contest 1 has both, contest 2 has only a → both modes give 1
        assert total_default == 1
        assert total_strict == 1


class TestIsStale:
    def test_none_is_stale(self):
        assert _is_stale(None) is True

    def test_recent_is_fresh(self):
        assert _is_stale(time.time() - 100) is False

    def test_old_is_stale(self):
        # 15 days ago, outside rename season
        assert _is_stale(time.time() - 15 * 86400) is True

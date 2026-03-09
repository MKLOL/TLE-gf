"""Tests for the versus cog's pure computation logic."""

import collections
import pytest

# We need a lightweight RatingChange-like namedtuple for testing
RatingChange = collections.namedtuple(
    'RatingChange',
    'contestId contestName handle rank ratingUpdateTimeSeconds oldRating newRating'
)


def _make_rc(contest_id, handle, rank):
    """Helper to create a minimal RatingChange for testing."""
    return RatingChange(
        contestId=contest_id,
        contestName=f'Contest {contest_id}',
        handle=handle,
        rank=rank,
        ratingUpdateTimeSeconds=1000000 + contest_id,
        oldRating=1500,
        newRating=1500,
    )


# Import the pure functions under test
from tle.cogs.versus import _compute_versus_stats, _normalize_handles


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


class TestNormalizeHandles:
    def _make_cache(self, canonical_handles):
        """Create a fake cache with handle_rating_cache mapping."""
        class FakeCache:
            pass
        cache = FakeCache()
        cache.handle_rating_cache = {h: 1500 for h in canonical_handles}
        return cache

    def test_lowercase_input_resolved(self):
        cache = self._make_cache(['Dragos', 'Nifeshe'])
        result = _normalize_handles(['dragos', 'nifeshe'], cache)
        assert result == ['Dragos', 'Nifeshe']

    def test_mixed_case_input(self):
        cache = self._make_cache(['Tourist', 'Petr'])
        result = _normalize_handles(['tourist', 'PETR'], cache)
        assert result == ['Tourist', 'Petr']

    def test_already_correct_case(self):
        cache = self._make_cache(['Dragos'])
        result = _normalize_handles(['Dragos'], cache)
        assert result == ['Dragos']

    def test_unknown_handle_unchanged(self):
        cache = self._make_cache(['Alice'])
        result = _normalize_handles(['alice', 'unknown_user'], cache)
        assert result == ['Alice', 'unknown_user']

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
from tle.cogs.versus import _compute_versus_stats, _normalize_handles, _alias_is_stale, _resolve_aliases, _get_all_changes
import asyncio
import time


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


class TestAliasIsStale:
    def test_none_is_stale(self):
        assert _alias_is_stale(None) is True

    def test_recent_is_fresh(self):
        assert _alias_is_stale(time.time() - 100) is False

    def test_old_is_stale(self):
        # 15 days ago, outside rename season
        assert _alias_is_stale(time.time() - 15 * 86400) is True


class TestHandleAliasDb:
    """Test the handle_alias table in CacheDbConn."""

    def _make_db(self):
        import sqlite3
        from tle.util.db.cache_db_conn import CacheDbConn
        db = CacheDbConn.__new__(CacheDbConn)
        db.db_file = ':memory:'
        db.conn = sqlite3.connect(':memory:')
        db.create_tables()
        return db

    def test_no_alias_returns_none(self):
        db = self._make_db()
        aliases, resolved_at = db.get_handle_aliases('unknown')
        assert aliases is None
        assert resolved_at is None

    def test_save_and_get_aliases(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({
            'Friedrich': 'LMeyling',
            'LMeyling': 'LMeyling',
        }, now)

        aliases, resolved_at = db.get_handle_aliases('Friedrich')
        assert aliases == {'Friedrich', 'LMeyling'}
        assert resolved_at == now

        aliases, resolved_at = db.get_handle_aliases('LMeyling')
        assert aliases == {'Friedrich', 'LMeyling'}

    def test_self_alias(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({'alice': 'alice'}, now)
        aliases, _ = db.get_handle_aliases('alice')
        assert aliases == {'alice'}

    def test_triple_chain(self):
        db = self._make_db()
        now = int(time.time())
        db.save_handle_aliases({
            'A': 'C',
            'B': 'C',
            'C': 'C',
        }, now)
        aliases, _ = db.get_handle_aliases('A')
        assert aliases == {'A', 'B', 'C'}


class TestResolveAliases:
    """Test _resolve_aliases with mocked CF API."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _make_db(self):
        import sqlite3
        from tle.util.db.cache_db_conn import CacheDbConn
        db = CacheDbConn.__new__(CacheDbConn)
        db.db_file = ':memory:'
        db.conn = sqlite3.connect(':memory:')
        db.create_tables()
        return db

    def test_caches_after_api_call(self, monkeypatch):
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'Friedrich', 50),
            _make_rc(2, 'Friedrich', 40),
            _make_rc(3, 'LMeyling', 30),
        ]

        call_count = 0
        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                nonlocal call_count
                call_count += 1
                return api_response

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': type('E', (Exception,), {}),
        })())

        db = self._make_db()

        # First call — hits API
        aliases = self._run(_resolve_aliases('LMeyling', db))
        assert 'Friedrich' in aliases
        assert 'LMeyling' in aliases
        assert call_count == 1

        # Second call — uses cache, no API
        aliases = self._run(_resolve_aliases('LMeyling', db))
        assert 'Friedrich' in aliases
        assert call_count == 1  # Still 1, no new API call

    def test_handle_not_found(self, monkeypatch):
        import tle.cogs.versus as versus_mod

        class FakeNotFound(Exception):
            pass

        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                raise FakeNotFound()

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': FakeNotFound,
        })())

        db = self._make_db()
        aliases = self._run(_resolve_aliases('nobody', db))
        assert aliases == {'nobody'}

    def test_lookup_by_old_handle(self, monkeypatch):
        """Looking up the old handle also finds the new one."""
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'OldName', 50),
            _make_rc(2, 'NewName', 30),
        ]

        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                return api_response

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': type('E', (Exception,), {}),
        })())

        db = self._make_db()
        aliases = self._run(_resolve_aliases('OldName', db))
        assert aliases == {'OldName', 'NewName'}

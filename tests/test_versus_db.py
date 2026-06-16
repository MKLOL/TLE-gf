"""DB/API-integration tests for the versus cog.

Split out of ``test_versus`` (handle-alias cache + rating-change fetching) to
keep each module under the 500-line limit. Pure-computation tests live in
``test_versus``; the shared ``_make_rc`` fake is in ``tests/versus_test_utils``.
"""

import asyncio
import time

from tests.versus_test_utils import _make_rc
from tle.cogs.versus import _get_rating_changes


class TestHandleAliasDb:
    """Test the handle_alias table in CacheDbConn."""

    def _make_db(self):
        import sqlite3
        from tle.util.db.cache_db_conn import CacheDbConn
        db = CacheDbConn.__new__(CacheDbConn)
        db.db_file = ':memory:'
        db.conn = sqlite3.connect(':memory:')
        db.create_tables()
        db._run_upgrades()
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


class TestGetRatingChanges:
    """Test _get_rating_changes with mocked CF API and real in-memory DB."""

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
        db._run_upgrades()
        return db

    def test_api_called_and_cached(self, monkeypatch):
        """First call hits API, second uses cache."""
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
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

        # First call — hits API, writes to cache
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 3
        assert call_count == 1

        # Second call — fresh cache, no API
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 3
        assert call_count == 1  # Still 1, no new API call

    def test_handle_not_found(self, monkeypatch):
        """Unknown handle returns empty and doesn't crash."""
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
        changes = self._run(_get_rating_changes('nobody', db))
        assert changes == []

    def test_old_handle_rows_cleaned_up(self, monkeypatch):
        """When API returns data under new handle, old handle rows are removed."""
        import tle.cogs.versus as versus_mod

        # API returns all contests under current handle "LMeyling"
        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
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

        # Pre-populate old handle rows (as if bot cached them before rename)
        db.save_rating_changes([
            _make_rc(1, 'Friedrich', 50),
            _make_rc(2, 'Friedrich', 40),
        ])
        # Verify old rows exist
        old_rows = db.get_rating_changes_for_handle('Friedrich')
        assert len(old_rows) == 2

        # Now resolve "Friedrich" — API returns data under "LMeyling"
        # Simulate: queried handle is "Friedrich", API returns "LMeyling"
        changes = self._run(_get_rating_changes('Friedrich', db))
        assert len(changes) == 2
        # All returned under current handle
        assert all(c.handle == 'LMeyling' for c in changes)

        # Old rows should be cleaned up
        old_rows = db.get_rating_changes_for_handle('Friedrich')
        assert len(old_rows) == 0

    def test_old_handle_404_uses_alias(self, monkeypatch):
        """If old handle 404s but we know its current name, use that."""
        import tle.cogs.versus as versus_mod

        class FakeNotFound(Exception):
            pass

        call_count = 0
        class FakeUser:
            @staticmethod
            async def rating(*, handle):
                nonlocal call_count
                call_count += 1
                if handle == 'Friedrich':
                    raise FakeNotFound()
                return [_make_rc(1, 'LMeyling', 50), _make_rc(2, 'LMeyling', 40)]

        monkeypatch.setattr(versus_mod, 'cf', type('cf', (), {
            'user': FakeUser(),
            'HandleNotFoundError': FakeNotFound,
        })())

        db = self._make_db()

        # First, resolve "LMeyling" to populate alias table and cache
        changes = self._run(_get_rating_changes('LMeyling', db))
        assert len(changes) == 2
        assert call_count == 1

        # Now query "Friedrich" — it 404s, but alias table knows Friedrich→LMeyling
        # First set up the alias manually (as if a previous resolution found it)
        now = int(time.time())
        db.conn.execute(
            'INSERT OR REPLACE INTO handle_alias (handle, current_handle, resolved_at) '
            'VALUES (?, ?, ?)', ('Friedrich', 'LMeyling', now)
        )
        db.conn.commit()

        changes = self._run(_get_rating_changes('Friedrich', db))
        assert len(changes) == 2
        assert all(c.handle == 'LMeyling' for c in changes)

    def test_no_duplicate_rows_after_rename(self, monkeypatch):
        """Ensure no duplicate contest entries exist after resolving a rename."""
        import tle.cogs.versus as versus_mod

        api_response = [
            _make_rc(1, 'LMeyling', 50),
            _make_rc(2, 'LMeyling', 40),
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

        # Pre-populate old handle rows
        db.save_rating_changes([_make_rc(1, 'Friedrich', 50), _make_rc(2, 'Friedrich', 40)])

        # Resolve under new handle
        self._run(_get_rating_changes('Friedrich', db))

        # Check: contest 1 should only have ONE entry (LMeyling), not two
        contest_rows = db.get_rating_changes_for_contest(1)
        handles_in_contest = [r.handle for r in contest_rows]
        assert 'Friedrich' not in handles_in_contest
        assert 'LMeyling' in handles_in_contest
        assert len(handles_in_contest) == 1

"""Tests for the generic UpgradeRegistry."""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.upgrades import UpgradeRegistry


def namedtuple_factory(cursor, row):
    fields = [col[0] if col[0].isidentifier() else f'col_{i}'
              for i, col in enumerate(cursor.description)]
    Row = namedtuple("Row", fields)
    return Row(*row)


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    yield conn
    conn.close()


@pytest.fixture
def registry():
    return UpgradeRegistry(version_table='db_version')


class TestEnsureVersionTable:
    def test_creates_table(self, db, registry):
        registry.ensure_version_table(db)
        # Table should exist and be empty
        result = db.execute('SELECT COUNT(*) as cnt FROM db_version').fetchone()
        assert result.cnt == 0

    def test_idempotent(self, db, registry):
        registry.ensure_version_table(db)
        registry.ensure_version_table(db)
        result = db.execute('SELECT COUNT(*) as cnt FROM db_version').fetchone()
        assert result.cnt == 0


class TestGetSetVersion:
    def test_get_returns_none_when_empty(self, db, registry):
        registry.ensure_version_table(db)
        assert registry.get_current_version(db) is None

    def test_get_returns_none_when_no_table(self, db, registry):
        # No version table at all
        assert registry.get_current_version(db) is None

    def test_set_then_get(self, db, registry):
        registry.ensure_version_table(db)
        registry.set_version(db, '1.0.0')
        assert registry.get_current_version(db) == '1.0.0'

    def test_set_overwrites(self, db, registry):
        registry.ensure_version_table(db)
        registry.set_version(db, '1.0.0')
        registry.set_version(db, '2.0.0')
        assert registry.get_current_version(db) == '2.0.0'
        # Should only have one row
        result = db.execute('SELECT COUNT(*) as cnt FROM db_version').fetchone()
        assert result.cnt == 1


class TestRegister:
    def test_register_decorator(self, registry):
        @registry.register('1.0.0', 'Test')
        def upgrade(db):
            pass
        assert len(registry.upgrades) == 1
        assert registry.upgrades[0][0] == '1.0.0'
        assert registry.upgrades[0][1] == 'Test'

    def test_latest_version_empty(self):
        reg = UpgradeRegistry()
        assert reg.latest_version is None

    def test_latest_version(self, registry):
        @registry.register('1.0.0', 'First')
        def up1(db):
            pass
        @registry.register('2.0.0', 'Second')
        def up2(db):
            pass
        assert registry.latest_version == '2.0.0'


class TestRun:
    def test_run_all_from_scratch(self, db, registry):
        calls = []

        @registry.register('1.0.0', 'First')
        def up1(conn):
            calls.append('1.0.0')

        @registry.register('2.0.0', 'Second')
        def up2(conn):
            calls.append('2.0.0')

        registry.run(db)
        assert calls == ['1.0.0', '2.0.0']
        assert registry.get_current_version(db) == '2.0.0'

    def test_run_skips_already_applied(self, db, registry):
        calls = []

        @registry.register('1.0.0', 'First')
        def up1(conn):
            calls.append('1.0.0')

        @registry.register('2.0.0', 'Second')
        def up2(conn):
            calls.append('2.0.0')

        # Pre-stamp version to 1.0.0
        registry.ensure_version_table(db)
        registry.set_version(db, '1.0.0')

        registry.run(db)
        assert calls == ['2.0.0']
        assert registry.get_current_version(db) == '2.0.0'

    def test_run_nothing_when_up_to_date(self, db, registry):
        calls = []

        @registry.register('1.0.0', 'First')
        def up1(conn):
            calls.append('1.0.0')

        registry.ensure_version_table(db)
        registry.set_version(db, '1.0.0')

        registry.run(db)
        assert calls == []

    def test_run_raises_on_unknown_version(self, db, registry):
        """Bug #3 fix: unknown version should raise, not re-run all upgrades."""
        @registry.register('1.0.0', 'First')
        def up1(conn):
            pass

        registry.ensure_version_table(db)
        registry.set_version(db, '9.9.9')  # Unknown version

        with pytest.raises(RuntimeError, match='not recognized'):
            registry.run(db)

    def test_run_stops_on_failure(self, db, registry):
        calls = []

        @registry.register('1.0.0', 'First')
        def up1(conn):
            calls.append('1.0.0')

        @registry.register('2.0.0', 'Broken')
        def up2(conn):
            raise ValueError('boom')

        @registry.register('3.0.0', 'Third')
        def up3(conn):
            calls.append('3.0.0')

        with pytest.raises(ValueError, match='boom'):
            registry.run(db)

        # 1.0.0 ran, 2.0.0 failed, 3.0.0 never ran
        assert calls == ['1.0.0']
        # Version should be stamped at 1.0.0 (last successful)
        assert registry.get_current_version(db) == '1.0.0'

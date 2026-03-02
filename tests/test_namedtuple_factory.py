"""Tests for namedtuple_factory — particularly the SELECT 1 bug fix."""
import sqlite3
from collections import namedtuple

import pytest

from tle.util.db.user_db_conn import namedtuple_factory


@pytest.fixture
def db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    conn.execute('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)')
    conn.execute("INSERT INTO test_table VALUES (1, 'foo', 42)")
    conn.execute("INSERT INTO test_table VALUES (2, 'bar', 99)")
    conn.commit()
    yield conn
    conn.close()


class TestNamedtupleFactory:
    def test_normal_select(self, db):
        row = db.execute('SELECT id, name, value FROM test_table WHERE id = 1').fetchone()
        assert row.id == 1
        assert row.name == 'foo'
        assert row.value == 42

    def test_select_1_returns_row(self, db):
        """Bug #1: SELECT 1 used to crash because '1' is not a valid identifier."""
        row = db.execute('SELECT 1 FROM test_table LIMIT 1').fetchone()
        assert row is not None
        # The column gets aliased to col_0
        assert row.col_0 == 1

    def test_select_1_no_match_returns_none(self, db):
        row = db.execute('SELECT 1 FROM test_table WHERE id = 999').fetchone()
        assert row is None

    def test_select_count_star(self, db):
        """COUNT(*) produces a non-identifier column name in some SQLite versions."""
        row = db.execute('SELECT COUNT(*) FROM test_table').fetchone()
        assert row is not None
        # Access by position
        assert row[0] == 2

    def test_mixed_identifier_and_non_identifier(self, db):
        row = db.execute('SELECT name, 1, value FROM test_table WHERE id = 1').fetchone()
        assert row.name == 'foo'
        assert row.col_1 == 1
        assert row.value == 42

    def test_aliased_column(self, db):
        row = db.execute('SELECT COUNT(*) as cnt FROM test_table').fetchone()
        assert row.cnt == 2

    def test_fetchall(self, db):
        rows = db.execute('SELECT id, name FROM test_table ORDER BY id').fetchall()
        assert len(rows) == 2
        assert rows[0].name == 'foo'
        assert rows[1].name == 'bar'

    def test_select_1_exists_pattern(self, db):
        """The pattern used in check_exists_starboard_message and friends."""
        exists = db.execute('SELECT 1 FROM test_table WHERE id = 1').fetchone() is not None
        assert exists is True

        not_exists = db.execute('SELECT 1 FROM test_table WHERE id = 999').fetchone() is not None
        assert not_exists is False

"""Tests for the DB upgrade functions: fresh-db detection, later versions, end-to-end."""
import sqlite3

import pytest

from tle.util.db.user_db_conn import namedtuple_factory
from tests.migrations_test_utils import (
    db, make_registry_with_upgrades, create_legacy_tables,
)


# =====================================================================
# Fresh DB detection logic
# =====================================================================

class TestFreshDbDetection:
    def test_fresh_db_stamps_latest(self, db):
        """On a fresh DB, the latest version should be stamped without running upgrades."""
        # Create all tables (simulating create_tables())
        create_legacy_tables(db)
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id TEXT, emoji TEXT, threshold INTEGER, color INTEGER,
                channel_id TEXT, PRIMARY KEY (guild_id, emoji)
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id TEXT, starboard_msg_id TEXT, guild_id TEXT,
                emoji TEXT, author_id TEXT, star_count INTEGER DEFAULT 0,
                channel_id TEXT, PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        db.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id TEXT, key TEXT, value TEXT, PRIMARY KEY (guild_id, key)
            )
        ''')
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        current = reg.get_current_version(db)
        assert current is None

        # Legacy table is empty -> fresh DB
        has_legacy = db.execute('SELECT 1 FROM starboard LIMIT 1').fetchone() is not None
        assert has_legacy is False

        reg.set_version(db, reg.latest_version)
        assert reg.get_current_version(db) == '1.4.0'

    def test_preupgrade_db_detected(self, db):
        """A DB with legacy starboard data should be detected as pre-upgrade."""
        create_legacy_tables(db)
        db.execute("INSERT INTO starboard VALUES ('111', '999')")
        db.commit()

        reg = make_registry_with_upgrades()
        reg.ensure_version_table(db)
        current = reg.get_current_version(db)
        assert current is None

        has_legacy = db.execute('SELECT 1 FROM starboard LIMIT 1').fetchone() is not None
        assert has_legacy is True


class TestUpgrade124:
    def test_creates_rating_tables(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0
        upgrade_1_24_0(db)
        # Both tables exist with the expected columns (no error = present).
        db.execute('SELECT guild_id, user_id, registered_at '
                   'FROM minigame_registrant').fetchall()
        db.execute('SELECT guild_id, user_id, rating, games, peak, last_delta, '
                   'updated_at FROM akari_rating').fetchall()

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0
        upgrade_1_24_0(db)
        upgrade_1_24_0(db)  # CREATE ... IF NOT EXISTS -> safe to re-run


class TestUpgrade125:
    def test_adds_decay_columns(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0, upgrade_1_25_0
        upgrade_1_24_0(db)   # creates akari_rating without the decay columns
        upgrade_1_25_0(db)   # adds skip_streak / last_puzzle
        db.execute(
            "INSERT INTO akari_rating (guild_id, user_id, rating, games, peak, "
            "last_delta, skip_streak, last_puzzle, updated_at) "
            "VALUES ('1', '9', 1300, 2, 1310, -1.5, 4, 500, 123.0)")
        row = db.execute(
            'SELECT skip_streak, last_puzzle FROM akari_rating').fetchone()
        assert row.skip_streak == 4
        assert row.last_puzzle == 500

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0, upgrade_1_25_0
        upgrade_1_24_0(db)
        upgrade_1_25_0(db)
        upgrade_1_25_0(db)  # ALTER guarded by try/except -> safe to re-run


class TestUpgrade126:
    def test_renames_table_and_preserves_rows(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0, upgrade_1_26_0
        upgrade_1_24_0(db)  # creates the legacy minigame_registrant table
        db.execute(
            "INSERT INTO minigame_registrant (guild_id, user_id, registered_at) "
            "VALUES ('1', '999', 123.0), ('1', '888', 124.0)")
        upgrade_1_26_0(db)
        # Old table is gone; new table has the rows.
        rows = db.execute(
            'SELECT guild_id, user_id, registered_at FROM akari_registrant '
            'ORDER BY user_id').fetchall()
        assert [(r.guild_id, r.user_id, r.registered_at) for r in rows] == [
            ('1', '888', 124.0),
            ('1', '999', 123.0),
        ]
        legacy_exists = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='minigame_registrant'").fetchone()
        assert legacy_exists is None

    def test_no_legacy_table_is_a_noop(self, db):
        # Fresh DBs created via user_db_conn.py never had minigame_registrant —
        # the migration must skip the copy/drop without failing.
        from tle.util.db.user_db_upgrades import upgrade_1_26_0
        upgrade_1_26_0(db)
        # akari_registrant exists and is empty.
        rows = db.execute('SELECT user_id FROM akari_registrant').fetchall()
        assert rows == []

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_24_0, upgrade_1_26_0
        upgrade_1_24_0(db)
        db.execute(
            "INSERT INTO minigame_registrant (guild_id, user_id, registered_at) "
            "VALUES ('1', '999', 123.0)")
        upgrade_1_26_0(db)
        upgrade_1_26_0(db)  # legacy table absent on second pass — must not raise
        rows = db.execute('SELECT user_id FROM akari_registrant').fetchall()
        assert {r.user_id for r in rows} == {'999'}


class TestUpgrade130:
    def test_creates_generic_minigame_tables_and_copies_akari_ratings(self, db):
        from tle.util.db.user_db_upgrades import (
            upgrade_1_24_0, upgrade_1_25_0, upgrade_1_30_0,
        )
        upgrade_1_24_0(db)
        upgrade_1_25_0(db)
        db.execute(
            "INSERT INTO akari_rating (guild_id, user_id, rating, games, peak, "
            "last_delta, skip_streak, last_puzzle, updated_at) "
            "VALUES ('1', '9', 1300, 2, 1310, -1.5, 4, 500, 123.0)")

        upgrade_1_30_0(db)

        db.execute('SELECT guild_id, game, user_id, external_name, '
                   'normalized_name, external_url, linked_at, linked_by '
                   'FROM minigame_player_link').fetchall()
        row = db.execute(
            'SELECT guild_id, game, user_id, rating, games, peak, last_delta, '
            'skip_streak, last_puzzle, updated_at FROM minigame_rating').fetchone()
        assert row.guild_id == '1'
        assert row.game == 'akari'
        assert row.user_id == '9'
        assert row.rating == 1300
        assert row.skip_streak == 4
        assert row.last_puzzle == 500

    def test_idempotent_without_akari_rating_table(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_30_0
        upgrade_1_30_0(db)
        upgrade_1_30_0(db)
        db.execute('SELECT * FROM minigame_player_link').fetchall()
        db.execute('SELECT * FROM minigame_rating').fetchall()


class TestUpgrade131:
    def test_creates_generic_minigame_ban_table(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_31_0
        upgrade_1_31_0(db)
        db.execute(
            "INSERT INTO minigame_ban "
            "(guild_id, game, user_id, banned_at, banned_by, reason) "
            "VALUES ('1', 'queens', '9', 123.0, '7', 'spam')")
        row = db.execute(
            'SELECT guild_id, game, user_id, banned_at, banned_by, reason '
            'FROM minigame_ban').fetchone()
        assert row.guild_id == '1'
        assert row.game == 'queens'
        assert row.user_id == '9'
        assert row.reason == 'spam'

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_31_0
        upgrade_1_31_0(db)
        upgrade_1_31_0(db)
        db.execute('SELECT * FROM minigame_ban').fetchall()


class TestUpgrade138:
    def test_creates_akari_difficulty_cache(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_38_0
        upgrade_1_38_0(db)
        db.execute(
            'INSERT INTO akari_puzzle_difficulty '
            '(puzzle_number, difficulty, fetched_at) VALUES (529, 4, 123.0)')
        row = db.execute(
            'SELECT puzzle_number, difficulty, fetched_at '
            'FROM akari_puzzle_difficulty').fetchone()
        assert (row.puzzle_number, row.difficulty) == (529, 4)

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_38_0
        upgrade_1_38_0(db)
        upgrade_1_38_0(db)


class TestUpgrade132:
    def test_creates_unresolved_minigame_result_table(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_32_0
        upgrade_1_32_0(db)
        db.execute(
            "INSERT INTO minigame_unresolved_result "
            "(guild_id, game, normalized_name, external_name, channel_id, "
            "puzzle_number, puzzle_date, accuracy, time_seconds, is_perfect, "
            "raw_content) VALUES "
            "('1', 'queens', 'alice', 'Alice', '9', 123, '2026-06-08', "
            "100, 5, 1, 'raw')")
        row = db.execute(
            'SELECT guild_id, game, normalized_name, external_name, '
            'channel_id, puzzle_number, puzzle_date, accuracy, time_seconds, '
            'is_perfect, raw_content '
            'FROM minigame_unresolved_result').fetchone()
        assert row.guild_id == '1'
        assert row.game == 'queens'
        assert row.normalized_name == 'alice'
        assert row.external_name == 'Alice'
        assert row.time_seconds == 5

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_32_0
        upgrade_1_32_0(db)
        upgrade_1_32_0(db)
        db.execute('SELECT * FROM minigame_unresolved_result').fetchall()


class TestUserDbConnUpgradeEndToEnd:
    @staticmethod
    def _seed_legacy_akari_rating(dbfile):
        raw = sqlite3.connect(dbfile)
        raw.execute('''
            CREATE TABLE akari_rating (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        raw.execute(
            "INSERT INTO akari_rating "
            "(guild_id, user_id, rating, games, peak, last_delta, "
            "skip_streak, last_puzzle, updated_at) "
            "VALUES ('1', '9', 1300, 2, 1310, -1.5, 4, 500, 123.0)")
        return raw

    def test_opening_versioned_129_db_copies_akari_ratings(self, tmp_path):
        from tle.util.db.user_db_conn import UserDbConn
        from tle.util.db.user_db_upgrades import registry

        dbfile = tmp_path / 'user.db'
        raw = self._seed_legacy_akari_rating(dbfile)
        raw.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        raw.execute("INSERT INTO db_version (version) VALUES ('1.29.0')")
        raw.commit()
        raw.close()

        conn = UserDbConn(str(dbfile))
        try:
            row = conn.conn.execute(
                'SELECT guild_id, game, user_id, rating, games, peak, '
                'last_delta, skip_streak, last_puzzle, updated_at '
                'FROM minigame_rating WHERE guild_id = ? AND game = ? '
                'AND user_id = ?',
                ('1', 'akari', '9'),
            ).fetchone()
            assert row is not None
            assert row.rating == 1300
            assert row.skip_streak == 4
            assert row.last_puzzle == 500
            assert conn.get_akari_rating('1', '9').rating == 1300
            assert registry.get_current_version(conn.conn) == registry.latest_version
        finally:
            conn.conn.close()

    def test_unversioned_db_with_akari_rating_rows_still_reads_them(self, tmp_path):
        from tle.util.db.user_db_conn import UserDbConn
        from tle.util.db.user_db_upgrades import registry

        dbfile = tmp_path / 'user.db'
        raw = self._seed_legacy_akari_rating(dbfile)
        raw.commit()
        raw.close()

        conn = UserDbConn(str(dbfile))
        try:
            row = conn.get_akari_rating('1', '9')
            assert row.rating == 1300
            assert row.skip_streak == 4
            assert registry.get_current_version(conn.conn) == registry.latest_version
        finally:
            conn.conn.close()


class TestUpgrade127:
    def test_creates_ban_table(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_27_0
        upgrade_1_27_0(db)
        # Insert + read to verify the schema is queryable.
        db.execute(
            "INSERT INTO akari_ban (guild_id, user_id, banned_at, banned_by, reason) "
            "VALUES ('1', '999', 100.0, '7', 'spam')")
        row = db.execute(
            'SELECT user_id, banned_at, banned_by, reason '
            'FROM akari_ban').fetchone()
        assert row.user_id == '999'
        assert row.banned_at == 100.0
        assert row.banned_by == '7'
        assert row.reason == 'spam'

    def test_idempotent(self, db):
        from tle.util.db.user_db_upgrades import upgrade_1_27_0
        upgrade_1_27_0(db)
        upgrade_1_27_0(db)  # CREATE … IF NOT EXISTS — safe to re-run


class TestFreshDbSchema:
    """A fresh DB stamps the latest version WITHOUT running migrations, so every
    migration table/column must also be created by create_tables()."""

    def test_fresh_userdbconn_has_rating_tables(self):
        from tle.util.db.user_db_conn import UserDbConn
        from tle.util.db.user_db_upgrades import registry

        conn = UserDbConn(':memory:')
        try:
            # These would raise "no such table"/"no such column" if only the
            # 1.24.0/1.25.0/1.26.0/1.27.0 migrations (which a fresh DB never
            # runs) created them.
            conn.conn.execute('SELECT guild_id, user_id, registered_at '
                              'FROM akari_registrant').fetchall()
            conn.conn.execute('SELECT guild_id, user_id, rating, games, peak, '
                              'last_delta, skip_streak, last_puzzle, updated_at '
                              'FROM akari_rating').fetchall()
            conn.conn.execute('SELECT guild_id, user_id, banned_at, banned_by, '
                              'reason FROM akari_ban').fetchall()
            conn.conn.execute('SELECT guild_id, game, user_id, external_name, '
                              'normalized_name, external_url, linked_at, '
                              'linked_by FROM minigame_player_link').fetchall()
            conn.conn.execute('SELECT guild_id, game, user_id, rating, games, '
                              'peak, last_delta, skip_streak, last_puzzle, '
                              'updated_at FROM minigame_rating').fetchall()
            conn.conn.execute('SELECT guild_id, game, normalized_name, '
                              'external_name, channel_id, puzzle_number, '
                              'puzzle_date, accuracy, time_seconds, '
                              'is_perfect, raw_content '
                              'FROM minigame_unresolved_result').fetchall()
            conn.conn.execute('SELECT guild_id, game, user_id, banned_at, '
                              'banned_by, reason FROM minigame_ban').fetchall()
            conn.conn.execute('SELECT puzzle_number, difficulty, fetched_at '
                              'FROM akari_puzzle_difficulty').fetchall()
            # And the legacy table must NOT be created by the fresh path.
            legacy = conn.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='minigame_registrant'").fetchone()
            assert legacy is None
            assert registry.get_current_version(conn.conn) == registry.latest_version
        finally:
            conn.conn.close()

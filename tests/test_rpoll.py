"""Tests for rating-weighted polls (rpoll) — DB methods, upgrade, and embed building."""
import sqlite3
import time

import pytest

from tle.util.db.user_db_conn import namedtuple_factory
from tle.cogs.rpoll import _build_poll_embed, _build_results_summary, _parse_duration, MAX_OPTIONS, _DEFAULT_DURATION


# ---------------------------------------------------------------------------
# Fake DB that has rpoll tables + enough CF user/handle tables for rating lookup
# ---------------------------------------------------------------------------

class FakeRpollDb:
    """Minimal in-memory DB with rpoll tables and CF user cache for testing."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_tables()

    def _create_tables(self):
        self.conn.execute('''
            CREATE TABLE rpoll (
                poll_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                message_id  TEXT,
                question    TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  REAL NOT NULL,
                anonymous   INTEGER NOT NULL DEFAULT 0,
                expires_at  REAL NOT NULL DEFAULT 0,
                closed      INTEGER NOT NULL DEFAULT 0
            )
        ''')
        self.conn.execute('''
            CREATE TABLE rpoll_option (
                poll_id       INTEGER NOT NULL,
                option_index  INTEGER NOT NULL,
                label         TEXT NOT NULL,
                PRIMARY KEY (poll_id, option_index)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE rpoll_vote (
                poll_id       INTEGER NOT NULL,
                user_id       TEXT NOT NULL,
                option_index  INTEGER NOT NULL,
                rating        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (poll_id, user_id, option_index)
            )
        ''')
        # Minimal CF tables for get_rpoll_user_rating
        self.conn.execute('''
            CREATE TABLE user_handle (
                user_id   TEXT,
                guild_id  TEXT,
                handle    TEXT,
                active    INTEGER,
                PRIMARY KEY (user_id, guild_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE cf_user_cache (
                handle    TEXT PRIMARY KEY,
                first_name TEXT, last_name TEXT, country TEXT, city TEXT,
                organization TEXT, contribution INTEGER,
                rating INTEGER, maxRating INTEGER,
                last_online_time INTEGER, registration_time INTEGER,
                friend_of_count INTEGER, title_photo TEXT
            )
        ''')
        self.conn.commit()

    def _fetchone(self, query, params=(), row_factory=None):
        old = self.conn.row_factory
        if row_factory is not None:
            self.conn.row_factory = row_factory
        result = self.conn.execute(query, params).fetchone()
        self.conn.row_factory = old
        return result

    def _fetchall(self, query, params=(), row_factory=None):
        old = self.conn.row_factory
        if row_factory is not None:
            self.conn.row_factory = row_factory
        result = self.conn.execute(query, params).fetchall()
        self.conn.row_factory = old
        return result

    # Import the actual methods from UserDbConn
    from tle.util.db.user_db_conn import UserDbConn as _UC
    create_rpoll = _UC.create_rpoll
    set_rpoll_message_id = _UC.set_rpoll_message_id
    get_rpoll = _UC.get_rpoll
    get_rpoll_by_message_id = _UC.get_rpoll_by_message_id
    get_rpoll_options = _UC.get_rpoll_options
    toggle_rpoll_vote = _UC.toggle_rpoll_vote
    get_rpoll_totals = _UC.get_rpoll_totals
    get_rpoll_voters = _UC.get_rpoll_voters
    get_rpoll_vote_count = _UC.get_rpoll_vote_count
    get_rpoll_user_rating = _UC.get_rpoll_user_rating
    get_all_active_rpolls = _UC.get_all_active_rpolls
    close_rpoll = _UC.close_rpoll
    get_expired_unclosed_rpolls = _UC.get_expired_unclosed_rpolls
    get_handle = _UC.get_handle
    fetch_cf_user = _UC.fetch_cf_user

    def _seed_cf_user(self, user_id, guild_id, handle, rating):
        """Helper: link a Discord user to a CF handle with a rating."""
        self.conn.execute(
            'INSERT OR REPLACE INTO user_handle (user_id, guild_id, handle, active) VALUES (?, ?, ?, 1)',
            (str(user_id), str(guild_id), handle)
        )
        self.conn.execute(
            'INSERT OR REPLACE INTO cf_user_cache '
            '(handle, first_name, last_name, country, city, organization, contribution, '
            ' rating, maxRating, last_online_time, registration_time, friend_of_count, title_photo) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (handle, '', '', '', '', '', 0, rating, rating, 0, 0, 0, '')
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


GUILD = 111111111111111111
CHANNEL = 222222222222222222


@pytest.fixture
def db():
    d = FakeRpollDb()
    yield d
    d.close()


# =====================================================================
# DB: create_rpoll
# =====================================================================

class TestCreateRpoll:
    def test_creates_poll_and_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Best algo?', ['BFS', 'DFS'], 'user1', 1000.0)
        assert pid is not None
        poll = db.get_rpoll(pid)
        assert poll.question == 'Best algo?'
        assert poll.guild_id == str(GUILD)
        assert poll.message_id is None

        opts = db.get_rpoll_options(pid)
        assert len(opts) == 2
        assert opts[0].label == 'BFS'
        assert opts[0].option_index == 0
        assert opts[1].label == 'DFS'
        assert opts[1].option_index == 1

    def test_auto_increments_poll_id(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['C', 'D'], 'u', 2.0)
        assert p2 > p1

    def test_five_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B', 'C', 'D', 'E'], 'u', 1.0)
        opts = db.get_rpoll_options(pid)
        assert len(opts) == 5
        assert [o.label for o in opts] == ['A', 'B', 'C', 'D', 'E']

    def test_default_expires_at(self, db):
        now = 1000.0
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', now)
        poll = db.get_rpoll(pid)
        assert poll.expires_at == now + 86400

    def test_custom_expires_at(self, db):
        now = 1000.0
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', now, expires_at=now + 3600)
        poll = db.get_rpoll(pid)
        assert poll.expires_at == now + 3600

    def test_poll_starts_open(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        poll = db.get_rpoll(pid)
        assert poll.closed == 0


# =====================================================================
# DB: set/get message_id
# =====================================================================

class TestMessageId:
    def test_set_and_get(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 999888777)
        poll = db.get_rpoll(pid)
        assert poll.message_id == '999888777'

    def test_get_by_message_id(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 999888777)
        poll = db.get_rpoll_by_message_id(999888777)
        assert poll is not None
        assert poll.poll_id == pid

    def test_get_by_message_id_not_found(self, db):
        assert db.get_rpoll_by_message_id(12345) is None


# =====================================================================
# DB: toggle_rpoll_vote
# =====================================================================

class TestToggleVote:
    def test_vote_adds(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        added = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert added is True

    def test_vote_toggles_off(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        removed = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert removed is False

    def test_vote_toggle_on_again(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        added = db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        assert added is True

    def test_multiple_options_same_user(self, db):
        """User can vote for multiple options simultaneously."""
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B', 'C'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user1', 1, 1500)
        totals = db.get_rpoll_totals(pid)
        totals_map = {r.option_index: r.total_rating for r in totals}
        assert totals_map[0] == 1500
        assert totals_map[1] == 1500

    def test_multiple_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'user1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'user2', 0, 2000)
        db.toggle_rpoll_vote(pid, 'user3', 1, 1800)
        totals = db.get_rpoll_totals(pid)
        totals_map = {r.option_index: r.total_rating for r in totals}
        assert totals_map[0] == 3500
        assert totals_map[1] == 1800


# =====================================================================
# DB: get_rpoll_totals
# =====================================================================

class TestRpollTotals:
    def test_empty_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        totals = db.get_rpoll_totals(pid)
        assert len(totals) == 0

    def test_sums_ratings(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        db.toggle_rpoll_vote(pid, 'u3', 1, 2400)
        totals = db.get_rpoll_totals(pid)
        totals_map = {r.option_index: r.total_rating for r in totals}
        assert totals_map[0] == 3000
        assert totals_map[1] == 2400

    def test_after_unvote_total_decreases(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1800)
        # u1 un-votes
        db.toggle_rpoll_vote(pid, 'u1', 0, 1200)
        totals = db.get_rpoll_totals(pid)
        totals_map = {r.option_index: r.total_rating for r in totals}
        assert totals_map[0] == 1800

    def test_zero_rating_counts(self, db):
        """Users without CF handle vote with rating=0."""
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 0)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1500)
        totals = db.get_rpoll_totals(pid)
        totals_map = {r.option_index: r.total_rating for r in totals}
        assert totals_map[0] == 1500


# =====================================================================
# DB: get_rpoll_vote_count
# =====================================================================

class TestVoteCount:
    def test_no_votes(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_vote_count(pid) == 0

    def test_counts_distinct_users(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 1, 1500)  # Same user, two options
        db.toggle_rpoll_vote(pid, 'u2', 0, 2000)
        assert db.get_rpoll_vote_count(pid) == 2

    def test_unvote_decreases_count(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)  # Un-vote
        assert db.get_rpoll_vote_count(pid) == 0


# =====================================================================
# DB: get_rpoll_user_rating
# =====================================================================

class TestUserRating:
    def test_linked_user(self, db):
        db._seed_cf_user('user1', GUILD, 'tourist', 3800)
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 3800

    def test_unlinked_user(self, db):
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 0

    def test_linked_but_no_rating(self, db):
        """User linked but CF profile has no rating (e.g., never competed)."""
        db._seed_cf_user('user1', GUILD, 'newbie', None)
        rating = db.get_rpoll_user_rating('user1', GUILD)
        assert rating == 0

    def test_different_guilds(self, db):
        db._seed_cf_user('user1', GUILD, 'tourist', 3800)
        # Different guild, no link
        rating = db.get_rpoll_user_rating('user1', 999999)
        assert rating == 0


# =====================================================================
# DB: get_all_active_rpolls
# =====================================================================

class TestActiveRpolls:
    def test_returns_only_posted_polls(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.set_rpoll_message_id(p1, 111)
        # p2 has no message_id
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].poll_id == p1

    def test_empty_when_none_posted(self, db):
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert len(db.get_all_active_rpolls()) == 0

    def test_excludes_closed_polls(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.set_rpoll_message_id(pid, 111)
        db.close_rpoll(pid)
        assert len(db.get_all_active_rpolls()) == 0

    def test_includes_open_excludes_closed(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.set_rpoll_message_id(p1, 111)
        db.set_rpoll_message_id(p2, 222)
        db.close_rpoll(p1)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].poll_id == p2


# =====================================================================
# DB: close_rpoll
# =====================================================================

class TestCloseRpoll:
    def test_closes_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll(pid).closed == 0
        db.close_rpoll(pid)
        assert db.get_rpoll(pid).closed == 1

    def test_close_idempotent(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', 1.0)
        db.close_rpoll(pid)
        db.close_rpoll(pid)  # Should not error
        assert db.get_rpoll(pid).closed == 1


# =====================================================================
# DB: get_expired_unclosed_rpolls
# =====================================================================

class TestExpiredUnclosedRpolls:
    def test_no_expired_polls(self, db):
        now = time.time()
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', now, expires_at=now + 3600)
        assert db.get_expired_unclosed_rpolls() == []

    def test_finds_expired_poll(self, db):
        past = time.time() - 100
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400,
                              expires_at=past)
        db.set_rpoll_message_id(pid, 111)
        expired = db.get_expired_unclosed_rpolls()
        assert len(expired) == 1
        assert expired[0].poll_id == pid

    def test_ignores_closed_polls(self, db):
        past = time.time() - 100
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400,
                              expires_at=past)
        db.set_rpoll_message_id(pid, 111)
        db.close_rpoll(pid)
        assert db.get_expired_unclosed_rpolls() == []

    def test_ignores_polls_without_message_id(self, db):
        past = time.time() - 100
        db.create_rpoll(GUILD, CHANNEL, 'Q', ['A', 'B'], 'u', past - 86400,
                        expires_at=past)
        # No set_rpoll_message_id call
        assert db.get_expired_unclosed_rpolls() == []

    def test_mixed_expired_and_active(self, db):
        now = time.time()
        past = now - 100
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', past - 86400,
                             expires_at=past)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', now,
                             expires_at=now + 3600)
        db.set_rpoll_message_id(p1, 111)
        db.set_rpoll_message_id(p2, 222)
        expired = db.get_expired_unclosed_rpolls()
        assert len(expired) == 1
        assert expired[0].poll_id == p1


# =====================================================================
# DB upgrade 1.5.0
# =====================================================================

class TestUpgrade150:
    def test_creates_tables(self):
        from tle.util.db.user_db_upgrades import registry
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        # Verify tables exist
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert 'rpoll' in tables
        assert 'rpoll_option' in tables
        assert 'rpoll_vote' in tables
        conn.close()

    def test_idempotent(self):
        from tle.util.db.user_db_upgrades import registry
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        # Running again should not error
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        conn.close()


class TestUpgrade180:
    def test_adds_anonymous_column(self):
        from tle.util.db.user_db_upgrades import registry
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        # Start from 1.4.0 so 1.5.0 creates rpoll tables, then 1.8.0 adds anonymous
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        # Verify anonymous column exists with default 0
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Q?', 'u', 1.0)
        )
        conn.commit()
        row = conn.execute('SELECT anonymous FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.anonymous == 0
        conn.close()

    def test_existing_polls_get_default_anonymous(self):
        """Pre-existing polls should get anonymous=0 after upgrade."""
        from tle.util.db.user_db_upgrades import registry
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        # Create rpoll tables at 1.5.0, then upgrade to 1.8.0
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        # Reset to 1.7.0 to test the 1.8.0 upgrade path
        conn.execute('UPDATE db_version SET version = ?', ('1.7.0',))
        conn.commit()
        # Insert a poll before the upgrade
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Old poll', 'u', 1.0)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT anonymous FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.anonymous == 0
        conn.close()


class TestUpgrade190:
    def _make_pre_190_db(self):
        """Create a DB at version 1.8.0 (rpoll tables exist, no expiry columns)."""
        from tle.util.db.user_db_upgrades import registry
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        registry.ensure_version_table(conn)
        registry.set_version(conn, '1.4.0')
        registry.run(conn)
        # Roll back to 1.8.0
        conn.execute('UPDATE db_version SET version = ?', ('1.8.0',))
        conn.commit()
        return conn

    def test_adds_expires_at_and_closed_columns(self):
        from tle.util.db.user_db_upgrades import registry
        conn = self._make_pre_190_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Q?', 'u', time.time())
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at > 0
        assert row.closed in (0, 1)
        conn.close()

    def test_old_expired_poll_gets_closed(self):
        from tle.util.db.user_db_upgrades import registry
        conn = self._make_pre_190_db()
        old_time = time.time() - 200000  # Way in the past
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, message_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            ('1', '2', '999', 'Old Q', 'u', old_time)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at == old_time + 86400
        assert row.closed == 1
        conn.close()

    def test_recent_poll_stays_open(self):
        from tle.util.db.user_db_upgrades import registry
        conn = self._make_pre_190_db()
        now = time.time()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, message_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            ('1', '2', '999', 'Recent Q', 'u', now)
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT expires_at, closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.expires_at == now + 86400
        assert row.closed == 0
        conn.close()

    def test_unposted_poll_gets_closed(self):
        from tle.util.db.user_db_upgrades import registry
        conn = self._make_pre_190_db()
        conn.execute(
            'INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?)',
            ('1', '2', 'Unposted Q', 'u', time.time())
        )
        conn.commit()
        registry.run(conn)
        row = conn.execute('SELECT closed FROM rpoll WHERE poll_id = 1').fetchone()
        assert row.closed == 1
        conn.close()


# =====================================================================
# Duration parsing
# =====================================================================

class TestParseDuration:
    def test_minutes(self):
        assert _parse_duration('+30m') == 30 * 60

    def test_hours(self):
        assert _parse_duration('+2h') == 2 * 3600

    def test_days(self):
        assert _parse_duration('+1d') == 86400

    def test_single_minute(self):
        assert _parse_duration('+1m') == 60

    def test_large_hours(self):
        assert _parse_duration('+48h') == 48 * 3600

    def test_invalid_no_plus(self):
        assert _parse_duration('30m') is None

    def test_invalid_unit(self):
        assert _parse_duration('+5x') is None

    def test_invalid_no_number(self):
        assert _parse_duration('+h') is None

    def test_not_a_duration(self):
        assert _parse_duration('+anon') is None

    def test_empty(self):
        assert _parse_duration('') is None


# =====================================================================
# Embed building
# =====================================================================

class TestBuildPollEmbed:
    def test_basic_embed(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        embed = _build_poll_embed('Best algo?', options, {}, 0)
        assert embed.title == 'Best algo?'
        assert '0 votes' in embed.footer['text']
        assert 'BFS' in embed.description
        assert 'DFS' in embed.description

    def test_with_totals(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 3400, 1: 5200}
        embed = _build_poll_embed('Q?', options, totals, 5)
        assert '**3400**' in embed.description
        assert '**5200**' in embed.description
        assert '5 votes' in embed.footer['text']

    def test_missing_option_defaults_zero(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 1500}  # Option 1 has no votes
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert '**0**' in embed.description  # B shows 0

    def test_singular_vote(self):
        embed = _build_poll_embed('Q?', [(0, 'A')], {}, 1)
        assert '1 vote' in embed.footer['text']
        assert 'votes' not in embed.footer['text']

    def test_number_emojis_in_description(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert '1\N{COMBINING ENCLOSING KEYCAP}' in embed.description
        assert '2\N{COMBINING ENCLOSING KEYCAP}' in embed.description
        assert '3\N{COMBINING ENCLOSING KEYCAP}' in embed.description

    def test_percentages_shown_when_totals_nonzero(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 3000, 1: 7000}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert '(30%)' in embed.description
        assert '(70%)' in embed.description

    def test_no_percentages_when_all_zero(self):
        options = [(0, 'A'), (1, 'B')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert '%' not in embed.description

    def test_one_option_has_all_rating(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 2000}
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert '(100%)' in embed.description
        assert '(0%)' in embed.description

    def test_percentages_round(self):
        """3 equal votes: 33% + 33% + 33% (rounding)."""
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        totals = {0: 1000, 1: 1000, 2: 1000}
        embed = _build_poll_embed('Q?', options, totals, 3)
        assert '(33%)' in embed.description

    def test_zero_rating_votes_no_percentages(self):
        """All voters have 0 rating — no percentages shown."""
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 0, 1: 0}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert '%' not in embed.description

    def test_leader_shown(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 3000, 1: 1000}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert 'Leader: **BFS** (+2000)' in embed.description

    def test_tied_shown(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 1500, 1: 1500}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert 'Tied:' in embed.description
        assert '**BFS**' in embed.description
        assert '**DFS**' in embed.description

    def test_three_way_tie(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        totals = {0: 1000, 1: 1000, 2: 1000}
        embed = _build_poll_embed('Q?', options, totals, 3)
        assert 'Tied:' in embed.description
        assert '**A**' in embed.description
        assert '**B**' in embed.description
        assert '**C**' in embed.description

    def test_no_leader_when_all_zero(self):
        options = [(0, 'A'), (1, 'B')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert 'Leader' not in embed.description
        assert 'Tied' not in embed.description

    def test_leader_with_zero_second(self):
        """One option has votes, others have zero."""
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 2000}
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert 'Leader: **A** (+2000)' in embed.description


# =====================================================================
# Embed: expiry and closed state
# =====================================================================

class TestBuildPollEmbedExpiry:
    def test_shows_expiry_timestamp(self):
        expires_at = 1700000000.0
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, expires_at=expires_at)
        assert f'<t:{int(expires_at)}:R>' in embed.description

    def test_closed_shows_ended(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, closed=True)
        assert 'Poll has ended.' in embed.description

    def test_closed_overrides_expiry(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0,
                                  expires_at=1700000000.0, closed=True)
        assert 'Poll has ended.' in embed.description
        assert '<t:' not in embed.description

    def test_no_expiry_no_closed(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0)
        assert 'Poll has ended' not in embed.description
        assert 'Ends' not in embed.description


# =====================================================================
# Poll isolation
# =====================================================================

class TestPollIsolation:
    def test_votes_dont_cross_polls(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.toggle_rpoll_vote(p1, 'u1', 0, 2000)
        db.toggle_rpoll_vote(p2, 'u1', 0, 2000)
        t1 = db.get_rpoll_totals(p1)
        t2 = db.get_rpoll_totals(p2)
        assert len(t1) == 1
        assert len(t2) == 1

    def test_vote_count_per_poll(self, db):
        p1 = db.create_rpoll(GUILD, CHANNEL, 'Q1', ['A', 'B'], 'u', 1.0)
        p2 = db.create_rpoll(GUILD, CHANNEL, 'Q2', ['A', 'B'], 'u', 2.0)
        db.toggle_rpoll_vote(p1, 'u1', 0, 1500)
        db.toggle_rpoll_vote(p1, 'u2', 0, 1500)
        db.toggle_rpoll_vote(p2, 'u3', 0, 1500)
        assert db.get_rpoll_vote_count(p1) == 2
        assert db.get_rpoll_vote_count(p2) == 1


class TestGetRpollVoters:
    @pytest.fixture
    def db(self):
        d = FakeRpollDb()
        yield d
        d.close()

    def test_no_voters(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        assert db.get_rpoll_voters(pid) == []

    def test_single_voter(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        voters = db.get_rpoll_voters(pid)
        assert len(voters) == 1
        assert voters[0].option_index == 0
        assert voters[0].user_id == 'u1'

    def test_multiple_voters_multiple_options(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B', 'C'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u2', 0, 1200)
        db.toggle_rpoll_vote(pid, 'u3', 1, 1800)
        db.toggle_rpoll_vote(pid, 'u1', 2, 1500)  # u1 votes for two options
        voters = db.get_rpoll_voters(pid)
        # Should be ordered by option_index
        assert len(voters) == 4
        option_0 = [v for v in voters if v.option_index == 0]
        option_1 = [v for v in voters if v.option_index == 1]
        option_2 = [v for v in voters if v.option_index == 2]
        assert {v.user_id for v in option_0} == {'u1', 'u2'}
        assert {v.user_id for v in option_1} == {'u3'}
        assert {v.user_id for v in option_2} == {'u1'}

    def test_unvote_removes_from_voters(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)
        db.toggle_rpoll_vote(pid, 'u1', 0, 1500)  # toggle off
        assert db.get_rpoll_voters(pid) == []


class TestBuildPollEmbedVoters:
    def test_no_voters_no_voter_section(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0)
        assert 'A:' not in embed.description

    def test_voters_shown(self):
        voters_map = {0: [111, 222], 1: [333]}
        embed = _build_poll_embed(
            'Q?', [(0, 'Alpha'), (1, 'Beta')],
            {0: 3000, 1: 1500}, 3, voters_map
        )
        assert '<@111>' in embed.description
        assert '<@222>' in embed.description
        assert '<@333>' in embed.description
        assert 'Alpha: <@111>, <@222>' in embed.description
        assert 'Beta: <@333>' in embed.description

    def test_empty_option_not_shown(self):
        voters_map = {0: [111]}
        embed = _build_poll_embed(
            'Q?', [(0, 'A'), (1, 'B')],
            {0: 1500, 1: 0}, 1, voters_map
        )
        assert 'A: <@111>' in embed.description
        # B has no voters, should not appear in voter section
        lines = embed.description.split('\n')
        voter_lines = [l for l in lines if l.startswith('B:')]
        assert voter_lines == []


# =====================================================================
# Anonymous polls
# =====================================================================

class TestAnonymousPoll:
    def test_create_anonymous_poll(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        poll = db.get_rpoll(pid)
        assert poll.anonymous == 1

    def test_create_non_anonymous_poll_default(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0)
        poll = db.get_rpoll(pid)
        assert poll.anonymous == 0

    def test_anonymous_flag_in_get_by_message_id(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        db.set_rpoll_message_id(pid, 12345)
        poll = db.get_rpoll_by_message_id(12345)
        assert poll.anonymous == 1

    def test_anonymous_flag_in_active_rpolls(self, db):
        pid = db.create_rpoll(GUILD, CHANNEL, 'Q?', ['A', 'B'], 'u', 1.0, anonymous=True)
        db.set_rpoll_message_id(pid, 12345)
        active = db.get_all_active_rpolls()
        assert len(active) == 1
        assert active[0].anonymous == 1


class TestBuildResultsSummary:
    def test_basic_summary(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals_map = {0: 3400, 1: 1600}
        result = _build_results_summary(options, totals_map, 5)
        assert 'Poll ended' in result
        assert '**BFS** 68%' in result
        assert '**DFS** 32%' in result
        assert '5 votes' in result

    def test_zero_totals(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_summary(options, {}, 0)
        assert '**A** 0' in result
        assert '**B** 0' in result
        assert '0 votes' in result

    def test_singular_vote(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_summary(options, {0: 1500}, 1)
        assert '1 vote)' in result

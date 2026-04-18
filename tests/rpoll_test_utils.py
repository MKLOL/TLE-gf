"""Shared test helpers for rpoll tests."""
import sqlite3

import pytest

from tle.util.db.user_db_conn import namedtuple_factory


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
                closed      INTEGER NOT NULL DEFAULT 0,
                formula     TEXT NOT NULL DEFAULT 'sum'
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
        self.conn.execute('''
            CREATE TABLE challenge (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id       TEXT NOT NULL,
                issue_time    REAL NOT NULL,
                finish_time   REAL,
                problem_name  TEXT NOT NULL,
                contest_id    INTEGER NOT NULL,
                p_index       INTEGER NOT NULL,
                rating_delta  INTEGER NOT NULL,
                status        INTEGER NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE user_challenge (
                user_id              TEXT PRIMARY KEY,
                active_challenge_id  INTEGER,
                issue_time           REAL,
                score                INTEGER NOT NULL,
                num_completed        INTEGER NOT NULL,
                num_skipped          INTEGER NOT NULL
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
    get_rpoll_vote_ratings = _UC.get_rpoll_vote_ratings
    get_rpoll_voter_ids = _UC.get_rpoll_voter_ids
    update_rpoll_voter_rating = _UC.update_rpoll_voter_rating
    get_gudgitter_score = _UC.get_gudgitter_score
    get_gudgitters_timerange_for_user = _UC.get_gudgitters_timerange_for_user
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

    def _seed_gudgitter_score(self, user_id, score):
        self.conn.execute(
            'INSERT OR REPLACE INTO user_challenge '
            '(user_id, active_challenge_id, issue_time, score, num_completed, num_skipped) '
            'VALUES (?, NULL, NULL, ?, 0, 0)',
            (str(user_id), score)
        )
        self.conn.commit()

    def _seed_monthly_gitgud_entry(self, user_id, issue_time, finish_time, rating_delta, problem_name='P'):
        self.conn.execute(
            'INSERT INTO challenge '
            '(user_id, issue_time, finish_time, problem_name, contest_id, p_index, rating_delta, status) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (str(user_id), issue_time, finish_time, problem_name, 1, 0, rating_delta, 0)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


GUILD = 111111111111111111
CHANNEL = 222222222222222222


@pytest.fixture
def db():
    database = FakeRpollDb()
    yield database
    database.close()

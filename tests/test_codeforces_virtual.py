"""`;virtual` — pure helpers, solve rules and storage."""
import sqlite3

from tle.cogs import _codeforces_virtual as virtual
from tle.util import codeforces_common as cf_common

from tests.subfilter_common import _problem
from tests.virtual_common import (  # noqa: F401
    CONTESTS, DURATION, GUILD, HANDLE, NOW, USER, _Db, _session, _sub)

# --- pure helpers ---------------------------------------------------------

class TestHelpers:
    def test_division_markers_follow_the_vc_split(self):
        assert virtual.division_markers(1500) == ['div3']
        assert virtual.division_markers(1600) == ['div2']
        assert 'div1' in virtual.division_markers(2100)

    def test_base_rating_is_rounded_and_clamped(self):
        assert virtual.gitgud_base_rating(1849) == 1800
        assert virtual.gitgud_base_rating(900) == 1100
        assert virtual.gitgud_base_rating(3400) == 3000

    def test_expiry_is_start_grace_plus_contest(self):
        assert virtual.expiry(NOW, DURATION) == NOW + virtual._START_GRACE + DURATION
        assert virtual.start_deadline(NOW) == NOW + virtual._START_GRACE

    def test_the_offer_rounds_the_length(self):
        assert virtual._rough_hours(2 * 3600) == 'about 2h00'
        assert virtual._rough_hours(2 * 3600 + 10 * 60) == 'about 2h00'
        assert virtual._rough_hours(2 * 3600 + 20 * 60) == 'about 2h30'

    def test_eligible_contests(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'is_contest_writer',
                            lambda cid, handle: cid == 3)
        picked = virtual.eligible_contests(CONTESTS, ['div2'], {1}, HANDLE)
        # 1 visited, 3 written by the user, 4 April Fools, 2/5 wrong division
        assert [c.id for c in picked] == []
        picked = virtual.eligible_contests(CONTESTS, ['div2'], set(), HANDLE)
        assert [c.id for c in picked] == [1]


class TestVirtualSolves:
    def _solved(self, subs, **kw):
        return [p.index for p in virtual.virtual_solves(
            subs, _session(**kw), DURATION)]

    def test_a_clean_virtual_solve_counts(self):
        assert self._solved([_sub('B')]) == ['B']

    def test_only_this_contest(self):
        assert self._solved([_sub('A', contest_id=2)]) == []

    def test_only_virtual_participation(self):
        assert self._solved([_sub('A', ptype='CONTESTANT'),
                             _sub('B', ptype='PRACTICE')]) == []

    def test_team_virtuals_do_not_count(self):
        assert self._solved([_sub('A', members=2)]) == []

    def test_a_virtual_started_before_confirming_is_not_this_one(self):
        """The whole point of confirming blind: you cannot claim a run you
        already did."""
        assert self._solved([_sub('A', started=NOW - 3600)]) == []
        # within clock slack is fine
        assert self._solved([_sub('A', started=NOW - 30)]) == ['A']

    def test_a_virtual_must_start_inside_the_reveal_window(self):
        """Reading the problems for an hour and starting the clock later is
        the whole exploit; the start deadline is the guard."""
        deadline = virtual.start_deadline(NOW)
        assert self._solved([_sub('A', started=deadline)]) == ['A']
        # Clock slack is tolerated, and a tightening must not eat it.
        assert self._solved([_sub('A', started=deadline + 30)]) == ['A']
        assert self._solved([_sub('A', started=deadline + 61)]) == []
        assert self._solved([_sub('A', started=NOW + 3600)]) == []

    def test_session_is_over_allows_the_slack(self):
        session = _session()
        assert not virtual.session_is_over(session, session.expires_at + 60)
        assert virtual.session_is_over(session, session.expires_at + 61)

    def test_solves_after_the_contest_clock_do_not_count(self):
        assert self._solved([_sub('A', elapsed=-1)]) == []
        assert self._solved([_sub('A', elapsed=DURATION + 1)]) == []
        assert self._solved([_sub('A', elapsed=DURATION)]) == ['A']

    def test_a_full_length_run_from_the_last_legal_start_still_counts(self):
        # The start bound plus the contest clock already imply the wall-clock
        # bound; this pins the legitimate maximum so a tighter check would fail.
        started = virtual.start_deadline(NOW)
        assert self._solved([_sub('A', started=started, elapsed=DURATION)]) == ['A']

    def test_only_accepted(self):
        assert self._solved([_sub('A', verdict='WRONG_ANSWER')]) == []

    def test_each_problem_once_and_never_twice_across_claims(self):
        subs = [_sub('A', elapsed=100), _sub('A', elapsed=200), _sub('B')]
        assert self._solved(subs) == ['A', 'B']
        assert [p.index for p in virtual.virtual_solves(
            subs, _session(), DURATION, credited={'A'})] == ['B']


# --- storage --------------------------------------------------------------

class TestDb:
    def test_one_active_session_per_user(self):
        db = _Db()
        first = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1, 1900)
        assert first is not None
        assert db.start_virtual_session(GUILD, USER, HANDLE, 3, 'R902', NOW, NOW + 1, 1900) is None
        assert db.get_active_virtual_session(GUILD, USER).contest_id == 1
        assert db.virtual_session_contest_ids(GUILD, USER) == {1}

    def test_the_rule_is_global_across_guilds(self):
        """Points are per user, so a second guild sees the first's session."""
        db = _Db()
        db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1, 1900)
        assert db.start_virtual_session(99, USER, HANDLE, 3, 'R902', NOW, NOW + 1, 1900) is None
        assert db.get_active_virtual_session(99, USER).contest_id == 1
        assert db.virtual_session_contest_ids(99, USER) == {1}

    def test_the_session_remembers_handle_and_base_rating(self):
        db = _Db()
        db.start_virtual_session(GUILD, USER, 'Alice', 1, 'R900', NOW, NOW + 1, 2100)
        row = db.get_active_virtual_session(GUILD, USER)
        assert (row.handle, row.base_rating) == ('Alice', 2100)

    def test_credit_writes_a_completed_challenge_once(self):
        db = _Db()
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1, 1900)
        problem = _problem(1, index='C', name='Nice', rating=1900)
        assert db.credit_virtual_solve(sid, USER, problem, 100, 12, NOW + 50) is True
        assert db.credit_virtual_solve(sid, USER, problem, 100, 12, NOW + 50) is False
        assert db.credited_virtual_problems(sid) == {'C'}
        assert db.get_gudgitter_score(USER) == 12
        (entry,) = db.gitlog(str(USER))
        issue, finish, name, contest, index, delta, status = entry
        assert (name, contest, index, delta, finish) == ('Nice', 1, 'C', 100, NOW + 50)
        assert db.get_active_virtual_session(GUILD, USER).points == 12

    def test_credit_refuses_a_finished_session(self):
        db = _Db()
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1, 1900)
        assert db.finish_virtual_session(sid) is True
        assert db.finish_virtual_session(sid) is False
        assert db.credit_virtual_solve(sid, USER, _problem(1), 0, 8, NOW) is False
        assert db.get_active_virtual_session(GUILD, USER) is None

    def test_credit_does_not_touch_the_live_gitgud_challenge(self):
        """A virtual credit must not clear or replace an active challenge."""
        db = _Db()
        db.new_challenge(str(USER), NOW, _problem(9, name='Live'), 0)
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1, 1900)
        db.credit_virtual_solve(sid, USER, _problem(1, index='A'), 0, 8, NOW)
        assert db.check_challenge(str(USER))[2] == 'Live'
        assert db.get_gudgitter_score(USER) == 8


class TestSchemaRepair:
    """A database that ran the first cut of ;virtual has the old table."""

    def _old_shape_db(self):
        from tle.util.db.virtual_db import ACTIVE
        conn = sqlite3.connect(':memory:')
        conn.execute("""CREATE TABLE virtual_session (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
            user_id TEXT NOT NULL, handle TEXT NOT NULL, contest_id INTEGER NOT NULL,
            contest_name TEXT NOT NULL, confirmed_at REAL NOT NULL,
            expires_at REAL NOT NULL, status TEXT NOT NULL,
            points INTEGER NOT NULL DEFAULT 0, credited TEXT NOT NULL DEFAULT '[]')""")
        conn.execute("""CREATE INDEX idx_virtual_session_user
            ON virtual_session (guild_id, user_id, status)""")
        for guild, contest in ((1, 1), (2, 3)):  # legal under the per-guild rule
            conn.execute(
                'INSERT INTO virtual_session (guild_id, user_id, handle, contest_id, '
                'contest_name, confirmed_at, expires_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (str(guild), str(USER), HANDLE, contest, 'R', NOW, NOW + 1, ACTIVE))
        conn.commit()
        return conn

    def test_repair_then_create_yields_the_current_shape(self):
        from tle.util.db.virtual_db import (
            ACTIVE, create_virtual_schema, repair_virtual_schema)
        conn = self._old_shape_db()
        repair_virtual_schema(conn)
        create_virtual_schema(conn)  # would raise without the repair
        columns = {row[1] for row in conn.execute('PRAGMA table_info(virtual_session)')}
        assert 'base_rating' in columns
        active = conn.execute('SELECT contest_id FROM virtual_session WHERE status = ?',
                              (ACTIVE,)).fetchall()
        assert active == [(3,)]  # newest kept
        indexes = {row[1] for row in conn.execute('PRAGMA index_list(virtual_session)')}
        assert 'idx_virtual_session_active' in indexes

    def test_repair_is_harmless_on_a_current_or_missing_table(self):
        from tle.util.db.virtual_db import create_virtual_schema, repair_virtual_schema
        conn = sqlite3.connect(':memory:')
        repair_virtual_schema(conn)          # no table yet
        create_virtual_schema(conn)
        repair_virtual_schema(conn)          # already current
        create_virtual_schema(conn)
        columns = [row[1] for row in conn.execute('PRAGMA table_info(virtual_session)')]
        assert columns.count('base_rating') == 1

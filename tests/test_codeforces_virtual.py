"""`;virtual` — a blind random contest whose solves earn gitgud points."""
import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs import _codeforces_virtual as virtual
from tle.cogs._codeforces_helpers import CodeforcesCogError
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util.db.challenge_db import ChallengeDbMixin
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.virtual_db import VirtualDbMixin

from tests.subfilter_common import _problem

GUILD, USER, HANDLE = 1, 42, 'alice'
NOW = 1_700_000_000
DURATION = 2 * 3600


def _contest(cid, name, duration=DURATION):
    return cf.Contest(id=cid, name=name, startTimeSeconds=1_600_000_000,
                      durationSeconds=duration, phase='FINISHED')


CONTESTS = [
    _contest(1, 'Codeforces Round 900 (Div. 2)'),
    _contest(2, 'Codeforces Round 901 (Div. 1)'),
    _contest(3, 'Codeforces Round 902 (Div. 2)'),
    _contest(4, 'April Fools Day Contest (Div. 2)'),
    _contest(5, 'Codeforces Round 903 (Div. 3)'),
]
BY_ID = {c.id: c for c in CONTESTS}


class _Db(ChallengeDbMixin, VirtualDbMixin):
    def __init__(self, rating=1900):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_challenge_tables()
        self._create_virtual_tables()
        self.rating = rating

    def fetch_cf_user(self, handle):
        return SimpleNamespace(handle=handle, effective_rating=self.rating)


def _session(contest_id=1, confirmed_at=NOW, expires_at=None, points=0, sid=1):
    return SimpleNamespace(
        id=sid, contest_id=contest_id, contest_name=BY_ID[contest_id].name,
        confirmed_at=confirmed_at,
        expires_at=expires_at if expires_at is not None
        else virtual.expiry(confirmed_at, DURATION),
        points=points)


def _sub(index='A', *, contest_id=1, ptype='VIRTUAL', started=NOW + 60,
         elapsed=600, verdict='OK', rating=1700, members=1, name=None):
    party = cf.Party(contestId=contest_id,
                     members=[cf.Member(handle=HANDLE)] * members,
                     participantType=ptype, startTimeSeconds=started)
    return cf.Submission(
        id=hash((index, started)) & 0xffff, contestId=contest_id,
        problem=_problem(contest_id, index=index, name=name or f'P{index}',
                         rating=rating),
        author=party, verdict=verdict,
        creationTimeSeconds=(started or NOW) + elapsed,
        relativeTimeSeconds=elapsed)


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
        assert virtual.expiry(NOW, DURATION) == NOW + 30 * 60 + DURATION

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

    def test_a_virtual_started_after_expiry_does_not_count(self):
        late = virtual.expiry(NOW, DURATION) + 1
        assert self._solved([_sub('A', started=late)]) == []

    def test_solves_after_the_contest_clock_do_not_count(self):
        assert self._solved([_sub('A', elapsed=DURATION + 1)]) == []
        assert self._solved([_sub('A', elapsed=DURATION)]) == ['A']

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
        first = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1)
        assert first is not None
        assert db.start_virtual_session(GUILD, USER, HANDLE, 3, 'R902', NOW, NOW + 1) is None
        assert db.get_active_virtual_session(GUILD, USER).contest_id == 1
        assert db.virtual_session_contest_ids(GUILD, USER) == {1}

    def test_credit_writes_a_completed_challenge_once(self):
        db = _Db()
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1)
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
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1)
        assert db.finish_virtual_session(sid) is True
        assert db.finish_virtual_session(sid) is False
        assert db.credit_virtual_solve(sid, USER, _problem(1), 0, 8, NOW) is False
        assert db.get_active_virtual_session(GUILD, USER) is None

    def test_credit_does_not_touch_the_live_gitgud_challenge(self):
        """A virtual credit must not clear or replace an active challenge."""
        db = _Db()
        db.new_challenge(str(USER), NOW, _problem(9, name='Live'), 0)
        sid = db.start_virtual_session(GUILD, USER, HANDLE, 1, 'R900', NOW, NOW + 1)
        db.credit_virtual_solve(sid, USER, _problem(1, index='A'), 0, 8, NOW)
        assert db.check_challenge(str(USER))[2] == 'Live'
        assert db.get_gudgitter_score(USER) == 8


# --- cog flows ------------------------------------------------------------

class _Cog(virtual.CodeforcesVirtualMixin):
    converter = None


def _ctx(db):
    message = SimpleNamespace(edit=AsyncMock())
    return SimpleNamespace(
        guild=SimpleNamespace(id=GUILD), author=SimpleNamespace(id=USER),
        channel=SimpleNamespace(id=7), send=AsyncMock(return_value=message))


def _interaction(user_id=USER):
    return SimpleNamespace(user=SimpleNamespace(id=user_id),
                           response=SimpleNamespace(
                               edit_message=AsyncMock(), send_message=AsyncMock()))


@pytest.fixture
def env(monkeypatch):
    db = _Db()
    subs = []
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(cf_common, 'cache2', SimpleNamespace(
        contest_cache=SimpleNamespace(
            get_contests_in_phase=lambda phase: list(CONTESTS),
            get_contest=lambda cid: BY_ID[cid])))
    monkeypatch.setattr(cf_common, 'is_contest_writer', lambda cid, h: False)

    async def visited(handles):
        return set()
    monkeypatch.setattr(cf_common, 'get_visited_contests', visited)

    async def resolve(ctx, converter, handles, **kw):
        return [HANDLE]
    monkeypatch.setattr(cf_common, 'resolve_handles', resolve)

    async def status(*, handle):
        return list(subs)
    monkeypatch.setattr(cf, 'user', SimpleNamespace(status=status), raising=False)
    monkeypatch.setattr(virtual.random, 'choice', lambda seq: seq[0])
    clock = SimpleNamespace(now=NOW)
    monkeypatch.setattr(virtual.time, 'time', lambda: clock.now)
    return SimpleNamespace(db=db, subs=subs, clock=clock, cog=_Cog())


def _offer(env):
    ctx = _ctx(env.db)
    asyncio.run(env.cog._virtual_impl(ctx))
    kwargs = ctx.send.call_args.kwargs
    return ctx, kwargs['embed'], kwargs['view']


class TestOffer:
    def test_the_offer_does_not_name_the_contest(self, env):
        ctx, embed, view = _offer(env)
        text = f'{embed.title} {embed.description}'
        assert 'Round 900' not in text
        assert 'Div. 2' in text
        assert view.contest.id == 1
        assert env.db.get_active_virtual_session(GUILD, USER) is None

    def test_confirming_reveals_and_opens_the_session(self, env):
        ctx, _, view = _offer(env)
        interaction = _interaction()
        asyncio.run(view.decide(interaction, True))
        session = env.db.get_active_virtual_session(GUILD, USER)
        assert session.contest_id == 1
        assert session.expires_at == virtual.expiry(NOW, DURATION)
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert 'Round 900' in embed.title
        assert '/contest/1/virtual' in embed.description

    def test_cancelling_reveals_nothing(self, env):
        ctx, _, view = _offer(env)
        interaction = _interaction()
        asyncio.run(view.decide(interaction, False))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert 'Round 900' not in f'{embed.title} {embed.description}'

    def test_someone_else_cannot_press(self, env):
        ctx, _, view = _offer(env)
        assert asyncio.run(view.interaction_check(_interaction(user_id=99))) is False

    def test_deciding_twice_does_nothing(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        asyncio.run(view.decide(_interaction(), True))
        assert len(env.db.virtual_session_contest_ids(GUILD, USER)) == 1

    def test_a_stale_offer_cannot_open_a_second_session(self, env):
        ctx, _, first = _offer(env)
        ctx, _, second = _offer(env)
        asyncio.run(first.decide(_interaction(), True))
        interaction = _interaction()
        asyncio.run(second.decide(interaction, True))
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert embed.title == 'Already running'
        assert len(env.db.virtual_session_contest_ids(GUILD, USER)) == 1

    def test_an_offered_contest_is_not_offered_again(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        env.clock.now = NOW + 10 * 3600  # first session long expired
        ctx, embed, view = _offer(env)
        assert view.contest.id == 3

    def test_nothing_left_is_an_error(self, env, monkeypatch):
        monkeypatch.setattr(cf_common, 'is_contest_writer', lambda cid, h: True)
        with pytest.raises(CodeforcesCogError, match='No untouched'):
            asyncio.run(env.cog._virtual_impl(_ctx(env.db)))


class TestStatusAndClaim:
    def _start(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        return env.db.get_active_virtual_session(GUILD, USER)

    def test_virtual_while_active_shows_status_not_a_new_offer(self, env):
        self._start(env)
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_impl(ctx))
        kwargs = ctx.send.call_args.kwargs
        assert 'view' not in kwargs
        assert 'Round 900' in kwargs['embed'].title

    def test_claim_credits_solves_and_is_idempotent(self, env):
        session = self._start(env)
        env.subs.extend([_sub('A', rating=1700), _sub('C', rating=2200),
                         _sub('D', verdict='WRONG_ANSWER')])
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        # base rating 1900: A is -200 -> 3 points, C is +300 -> 23 points
        assert env.db.get_gudgitter_score(USER) == 26
        assert env.db.credited_virtual_problems(session.id) == {'A', 'C'}
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 26
        assert 'Nothing new' in ctx.send.call_args.kwargs['embed'].description

    def test_claim_after_expiry_finishes_the_session(self, env):
        self._start(env)
        env.subs.append(_sub('B'))
        env.clock.now = virtual.expiry(NOW, DURATION) + 1
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert 'finished' in ctx.send.call_args.kwargs['embed'].title
        assert env.db.get_gudgitter_score(USER) > 0

    def test_a_forgotten_virtual_is_credited_before_the_next_offer(self, env):
        self._start(env)
        env.subs.append(_sub('B', rating=1900))
        env.clock.now = virtual.expiry(NOW, DURATION) + 1
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 8
        first, second = [c.kwargs for c in ctx.send.call_args_list]
        assert 'view' in second and second['view'].contest.id == 3

    def test_the_live_gitgud_problem_is_left_for_gotgud(self, env):
        session = self._start(env)
        env.db.new_challenge(str(USER), NOW, _problem(1, index='A', name='PA'), 0)
        env.subs.append(_sub('A', name='PA'))
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.credited_virtual_problems(session.id) == set()
        assert env.db.check_challenge(str(USER))[2] == 'PA'

    def test_unrated_problems_are_reported_not_paid(self, env):
        self._start(env)
        env.subs.append(_sub('F', rating=None))
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 0
        assert 'unrated' in ctx.send.call_args.kwargs['embed'].description

    def test_claim_without_a_session_is_an_error(self, env):
        with pytest.raises(CodeforcesCogError, match='No virtual'):
            asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))

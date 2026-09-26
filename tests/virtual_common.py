"""Shared fakes and fixtures for the ;virtual tests.

Split out so each test module stays under the project's 500-line limit.
Import ``env`` into a test module to activate the fixture there.
"""
import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs import _codeforces_virtual as virtual
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


def _session(contest_id=1, confirmed_at=NOW, expires_at=None, points=0, sid=1,
             handle=HANDLE, base_rating=1900):
    return SimpleNamespace(
        id=sid, contest_id=contest_id, contest_name=BY_ID[contest_id].name,
        confirmed_at=confirmed_at,
        expires_at=expires_at if expires_at is not None
        else virtual.expiry(confirmed_at, DURATION),
        points=points, handle=handle, base_rating=base_rating)


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


# --- cog flows

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
    monkeypatch.setattr(cf_common, 'is_nonstandard_problem',
                        lambda problem: problem.name.startswith('special'))

    async def visited(handles):
        return set()
    monkeypatch.setattr(cf_common, 'get_visited_contests', visited)

    async def resolve(ctx, converter, handles, **kw):
        return [HANDLE]
    monkeypatch.setattr(cf_common, 'resolve_handles', resolve)

    fetched = []

    async def status(*, handle):
        fetched.append(handle)
        return list(subs)
    monkeypatch.setattr(cf, 'user', SimpleNamespace(status=status), raising=False)
    monkeypatch.setattr(virtual.random, 'choice', lambda seq: seq[0])
    clock = SimpleNamespace(now=NOW)
    monkeypatch.setattr(virtual.time, 'time', lambda: clock.now)
    return SimpleNamespace(db=db, subs=subs, clock=clock, cog=_Cog(), fetched=fetched)


def _offer(env):
    ctx = _ctx(env.db)
    asyncio.run(env.cog._virtual_impl(ctx))
    kwargs = ctx.send.call_args.kwargs
    return ctx, kwargs['embed'], kwargs['view']

"""get_visited_contests — complaint #299.

`;vc nifeshe temporary1 catgirl` recommended Spectral Cup (2222) although two
of them had done it virtually and one had been an official contestant. The
contest was looked up only through the problemset map, which lacked 2222's
problems, so every submission in it mapped to nothing.
"""
import asyncio
from collections import defaultdict
from types import SimpleNamespace

import pytest

from tle.util import cache_system2
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common

from tests.subfilter_common import _sub


class _ContestCache:
    def __init__(self, contests):
        self.by_id = {c.id: c for c in contests}

    def get_contest(self, cid):
        try:
            return self.by_id[cid]
        except KeyError:
            raise cache_system2.ContestNotFound(cid)


@pytest.fixture
def cache(monkeypatch):
    div1 = cf.Contest(id=2, name='Round (Div. 1)', startTimeSeconds=1000)
    div2 = cf.Contest(id=1, name='Round (Div. 2)', startTimeSeconds=1000)
    spectral = cf.Contest(id=2222, name='Spectral::Cup', startTimeSeconds=5000)
    problem_to_contests = defaultdict(list)
    # The map knows the Div. 1/2 pair but has nothing for 2222: its problems
    # were unrated when the problemset was last fetched.
    problem_to_contests[('Shared', 1000)] = [1, 2]
    monkeypatch.setattr(cf_common, 'cache2', SimpleNamespace(
        contest_cache=_ContestCache([div1, div2, spectral]),
        problemset_cache=SimpleNamespace(problem_to_contests=problem_to_contests)))
    return problem_to_contests


def _visited(monkeypatch, subs):
    async def status(*, handle):
        return list(subs)
    monkeypatch.setattr(cf, 'user', SimpleNamespace(status=status), raising=False)
    return asyncio.run(cf_common.get_visited_contests(['x']))


def test_a_contest_missing_from_the_problemset_map_is_still_visited(cache, monkeypatch):
    subs = [_sub(contest_id=2222, name='Seek the Truth', ptype='VIRTUAL')]
    assert 2222 in _visited(monkeypatch, subs)


def test_a_contest_missing_from_the_contest_cache_is_still_visited(cache, monkeypatch):
    subs = [_sub(contest_id=3333, name='Unknown Round Problem')]
    assert 3333 in _visited(monkeypatch, subs)


def test_sibling_contests_are_still_excluded_through_the_map(cache, monkeypatch):
    subs = [_sub(contest_id=1, name='Shared')]
    assert _visited(monkeypatch, subs) == {1, 2}


def test_compilation_errors_do_not_count(cache, monkeypatch):
    subs = [_sub(contest_id=2222, name='A', verdict='COMPILATION_ERROR')]
    assert _visited(monkeypatch, subs) == set()


def test_acmsguru_without_a_contest_is_ignored(cache, monkeypatch):
    problem = cf.Problem(contestId=None, problemsetName='acmsguru', index='99', name='Old',
                         type='PROGRAMMING', points=None, rating=None, tags=[])
    sub = _sub(contest_id=None)._replace(problem=problem)
    assert _visited(monkeypatch, [sub]) == set()

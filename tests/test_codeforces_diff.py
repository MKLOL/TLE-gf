"""`;diff` — problems solved by one handle but not another (complaint #279).

The interesting part is not the subtraction but the two rules around it: a
problem cross-listed in two divisions is one problem, and the filters narrow
only the left side.
"""
import asyncio
from types import SimpleNamespace

import pytest

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.cogs import _codeforces_problems as problems
from tle.cogs._codeforces_helpers import CodeforcesCogError

from tests.subfilter_common import _problem, _sub


def _contest(cid, start, name='Codeforces Round 900 (Div. 2)'):
    return cf.Contest(id=cid, name=name, startTimeSeconds=start)


class _Cache2:
    def __init__(self, contests):
        self.contest_cache = SimpleNamespace(
            contest_by_id={c.id: c for c in contests},
            get_contest=lambda cid: {c.id: c for c in contests}[cid])


# Contests 1 and 2 start at the same second: the Div. 1 / Div. 2 pair whose
# shared problems are the same problem.
CONTESTS = [
    _contest(1, 1_600_000_000, 'Codeforces Round 900 (Div. 2)'),
    _contest(2, 1_600_000_000, 'Codeforces Round 900 (Div. 1)'),
    _contest(3, 1_700_000_000, 'Educational Codeforces Round 160'),
]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(cf_common, 'cache2', _Cache2(CONTESTS))
    monkeypatch.setattr(discord_common, 'cf_color_embed',
                        lambda **kw: kw.get('description'))

    async def _resolve(ctx, converter, handles, **kwargs):
        return list(handles)
    monkeypatch.setattr(cf_common, 'resolve_handles', _resolve)


@pytest.fixture
def pages(monkeypatch):
    captured = []
    monkeypatch.setattr(paginator, 'paginate',
                        lambda bot, channel, pages, **kw: captured.extend(pages))
    return captured


def _status(by_handle, monkeypatch, rated=None, rating_error=None):
    async def _fetch(handle):
        return list(by_handle.get(handle, []))

    async def _rating(handle):
        if rating_error is not None:
            raise rating_error
        return [SimpleNamespace(contestId=cid)
                for cid in (rated or {}).get(handle, [])]
    monkeypatch.setattr(cf, 'user',
                        SimpleNamespace(status=_fetch, rating=_rating),
                        raising=False)


class _Cog(problems.CodeforcesProblemsMixin):
    bot = None
    converter = None


def _run(args, by_handle, monkeypatch, rated=None, rating_error=None):
    _status(by_handle, monkeypatch, rated=rated, rating_error=rating_error)
    ctx = SimpleNamespace(author=SimpleNamespace(id=1),
                          channel=SimpleNamespace(id=2))
    return asyncio.run(_Cog()._diff_impl(ctx, args))


def _names(pages):
    return [line.split(']')[0].lstrip('[')
            for _, description in pages
            for line in description.split('\n')]


class TestProblemIdentity:
    def test_cross_listed_problem_has_one_key(self):
        div2 = _problem(contest_id=1, index='E', name='Mirror')
        div1 = _problem(contest_id=2, index='C', name='Mirror')
        assert problems._problem_key(div2) == problems._problem_key(div1)

    def test_same_name_in_unrelated_contest_is_distinct(self):
        early = _problem(contest_id=1, name='Two Arrays')
        later = _problem(contest_id=3, name='Two Arrays')
        assert problems._problem_key(early) != problems._problem_key(later)

    def test_uncached_contests_do_not_collapse_by_name(self):
        """Gym and acmsguru problems are not in the contest cache.

        Keying them by name alone would let two unrelated problems that share
        a title cancel, hiding a real gap.
        """
        gym_a = _problem(contest_id=100500, index='A', name='Ships')
        gym_b = _problem(contest_id=200700, index='A', name='Ships')
        assert problems._problem_key(gym_a) != problems._problem_key(gym_b)

    def test_the_same_uncached_problem_still_matches_itself(self):
        one = _problem(contest_id=100500, index='C', name='Ships')
        two = _problem(contest_id=100500, index='C', name='Ships')
        assert problems._problem_key(one) == problems._problem_key(two)

    def test_solved_keys_ignore_unsuccessful_attempts(self):
        subs = [_sub(name='Solved', verdict='OK'),
                _sub(name='Failed', verdict='WRONG_ANSWER'),
                _sub(name='Compile', verdict='COMPILATION_ERROR')]
        keys = problems._solved_problem_keys(subs)
        assert keys == {problems._problem_key(_problem(name='Solved'))}


class TestDifference:
    def test_lists_only_what_the_right_handle_lacks(self, pages, monkeypatch):
        _run(('alice', 'bob'), {
            'alice': [_sub(sid=1, name='Shared', created=100),
                      _sub(sid=2, name='Only Alice', created=200)],
            'bob': [_sub(sid=3, name='Shared', created=150)],
        }, monkeypatch)
        assert _names(pages) == ['Only Alice']

    def test_direction_matters(self, pages, monkeypatch):
        by_handle = {
            'alice': [_sub(sid=1, name='Only Alice')],
            'bob': [_sub(sid=2, name='Only Bob')],
        }
        _run(('bob', 'alice'), by_handle, monkeypatch)
        assert _names(pages) == ['Only Bob']

    def test_mirrored_solve_is_not_a_gap(self, pages, monkeypatch):
        """Bob solved the Div. 1 listing of the problem Alice solved in Div. 2."""
        by_handle = {
            'alice': [_sub(sid=1, contest_id=1, index='E', name='Mirror')],
            'bob': [_sub(sid=2, contest_id=2, index='C', name='Mirror')],
        }
        with pytest.raises(CodeforcesCogError, match='has solved nothing'):
            _run(('alice', 'bob'), by_handle, monkeypatch)

    def test_right_handle_counts_solves_outside_the_filter(self, pages, monkeypatch):
        """`+contest` narrows what Alice is shown, never what Bob has solved.

        Bob only ever solved the problem in practice; it is still solved, so
        it must not be reported as something Alice has over him.
        """
        by_handle = {
            'alice': [_sub(sid=1, name='Practiced', ptype='CONTESTANT'),
                      _sub(sid=2, name='Genuine Gap', ptype='CONTESTANT')],
            'bob': [_sub(sid=3, name='Practiced', ptype='PRACTICE')],
        }
        _run(('alice', 'bob', '+contest'), by_handle, monkeypatch)
        assert _names(pages) == ['Genuine Gap']

    def test_same_titled_gym_problems_are_not_cancelled(self, pages, monkeypatch):
        by_handle = {
            'alice': [_sub(sid=1, contest_id=100500, name='Ships')],
            'bob': [_sub(sid=2, contest_id=200700, name='Ships')],
        }
        _run(('alice', 'bob'), by_handle, monkeypatch)
        assert _names(pages) == ['Ships']

    def test_same_titled_gym_solves_survive_left_side_deduplication(self, pages,
                                                                  monkeypatch):
        by_handle = {
            'alice': [_sub(sid=1, contest_id=100500, name='Ships', created=100),
                      _sub(sid=2, contest_id=200700, name='Ships', created=200)],
            'bob': [_sub(sid=3, contest_id=100500, name='Ships')],
        }
        _run(('alice', 'bob'), by_handle, monkeypatch)
        assert _names(pages) == ['Ships']
        assert '/gym/200700/problem/A' in pages[0][1]

    def test_same_titled_gym_solves_are_both_listed(self, pages, monkeypatch):
        _run(('alice', 'bob'), {
            'alice': [_sub(sid=1, contest_id=100500, name='Ships', created=100),
                      _sub(sid=2, contest_id=200700, name='Ships', created=200),
                      _sub(sid=3, contest_id=200700, name='Ships', created=300)],
            'bob': [],
        }, monkeypatch)
        assert _names(pages) == ['Ships', 'Ships']

    def test_unsolved_attempts_by_the_right_handle_are_still_gaps(self, pages,
                                                                 monkeypatch):
        by_handle = {
            'alice': [_sub(sid=1, name='Hard One')],
            'bob': [_sub(sid=2, name='Hard One', verdict='WRONG_ANSWER')],
        }
        _run(('alice', 'bob'), by_handle, monkeypatch)
        assert _names(pages) == ['Hard One']


class TestOrderingAndPaging:
    def test_default_is_newest_first(self, pages, monkeypatch):
        _run(('alice', 'bob'), {
            'alice': [_sub(sid=1, name='Old', created=100, rating=2400),
                      _sub(sid=2, name='New', created=900, rating=800)],
            'bob': [],
        }, monkeypatch)
        assert _names(pages) == ['New', 'Old']

    def test_hardest_sorts_by_rating(self, pages, monkeypatch):
        _run(('alice', 'bob', '+hardest'), {
            'alice': [_sub(sid=1, name='Old', created=100, rating=2400),
                      _sub(sid=2, name='New', created=900, rating=800)],
            'bob': [],
        }, monkeypatch)
        assert _names(pages) == ['Old', 'New']

    def test_pages_hold_ten_problems(self, pages, monkeypatch):
        _run(('alice', 'bob'), {
            'alice': [_sub(sid=i, name=f'P{i}', created=i) for i in range(25)],
            'bob': [],
        }, monkeypatch)
        assert len(pages) == 3
        assert len(_names(pages[:1])) == 10

    def test_a_truncated_list_says_so(self, pages, monkeypatch):
        _run(('alice', 'bob'), {
            'alice': [_sub(sid=i, name=f'P{i}', created=i) for i in range(140)],
            'bob': [],
        }, monkeypatch)
        assert 'showing 100 of 140' in pages[0][0]
        assert len(pages) == 10

    def test_an_untruncated_list_says_nothing(self, pages, monkeypatch):
        _run(('alice', 'bob'), {'alice': [_sub(name='X')], 'bob': []},
             monkeypatch)
        assert 'showing' not in pages[0][0]

    def test_both_handles_are_named_in_the_title(self, pages, monkeypatch):
        _run(('alice', 'bob'), {'alice': [_sub(name='X')], 'bob': []},
             monkeypatch)
        title = pages[0][0]
        assert 'alice' in title and 'bob' in title


class TestArgumentErrors:
    def _expect_error(self, args, monkeypatch, by_handle=None, match=None):
        with pytest.raises(CodeforcesCogError, match=match):
            _run(args, by_handle or {}, monkeypatch)

    def test_one_handle_is_rejected(self, monkeypatch):
        self._expect_error(('alice',), monkeypatch,
                           match='exactly two handles')

    def test_three_handles_are_rejected(self, monkeypatch):
        self._expect_error(('alice', 'bob', 'carol'), monkeypatch,
                           match='exactly two handles')

    def test_no_handles_are_rejected(self, monkeypatch):
        self._expect_error((), monkeypatch,
                           match='exactly two handles')

    def test_a_handle_against_itself_is_rejected(self, monkeypatch):
        self._expect_error(('alice', 'ALICE'), monkeypatch,
                           {'alice': [_sub(name='X')]},
                           match='has solved exactly what')

    def test_empty_difference_reports_instead_of_paging(self, monkeypatch):
        self._expect_error(('alice', 'bob'), monkeypatch,
                           {'alice': [_sub(name='X')], 'bob': [_sub(name='X')]},
                           match='has solved nothing')


class TestRatedFlag:
    """+rated is advertised in the usage string, so it has to do something."""

    def test_only_contests_rated_for_the_left_handle_are_kept(self, pages,
                                                              monkeypatch):
        # filter_subs keys rated contests on the submission author's handle.
        by_handle = {
            'alice': [_sub(sid=1, contest_id=1, name='Rated Round',
                           handle='alice'),
                      _sub(sid=2, contest_id=3, name='Unrated For Alice',
                           handle='alice')],
            'bob': [],
        }
        _run(('alice', 'bob', '+rated'), by_handle, monkeypatch,
             rated={'alice': [1]})
        assert _names(pages) == ['Rated Round']

    def test_unknown_handle_rating_history_is_not_fatal(self, monkeypatch):
        by_handle = {'alice': [_sub(sid=1, contest_id=1, name='X',
                                    handle='alice')], 'bob': []}
        with pytest.raises(CodeforcesCogError):
            _run(('alice', 'bob', '+rated'), by_handle, monkeypatch,
                 rating_error=cf.HandleNotFoundError())

"""Tests for SubFilter (stalk command filtering) in codeforces_common.

Locks in existing behaviour so the +rated feature doesn't break anything.
"""
import types

import pytest

from tle.util.codeforces_common import SubFilter, ParamParseError
from tle.util import codeforces_api as cf

# ---------------------------------------------------------------------------
# Helpers to build fake CF objects
# ---------------------------------------------------------------------------

def _contest(cid, name='Codeforces Round 900 (Div. 2)'):
    return cf.Contest(id=cid, name=name)


def _problem(contest_id=1, index='A', name='Test', rating=1500, tags=None):
    return cf.Problem(
        contestId=contest_id, problemsetName=None, index=index,
        name=name, type='PROGRAMMING', points=None, rating=rating,
        tags=tags or [],
    )


def _party(handle='tourist', ptype='CONTESTANT', contest_id=1):
    return cf.Party(
        contestId=contest_id,
        members=[cf.Member(handle=handle)],
        participantType=ptype,
    )


def _sub(sid=1, contest_id=1, handle='tourist', ptype='CONTESTANT',
         rating=1500, tags=None, verdict='OK', created=1000, name='Test',
         index='A'):
    return cf.Submission(
        id=sid, contestId=contest_id,
        problem=_problem(contest_id, index=index, name=name,
                         rating=rating, tags=tags),
        author=_party(handle, ptype, contest_id),
        programmingLanguage='C++', verdict=verdict,
        creationTimeSeconds=created, relativeTimeSeconds=0,
    )


# ---------------------------------------------------------------------------
# Fake contest cache injected into codeforces_common.cache2
# ---------------------------------------------------------------------------

class _FakeContestCache:
    def __init__(self, contests):
        self.contest_by_id = {c.id: c for c in contests}

    def get_contest(self, cid):
        return self.contest_by_id.get(cid, _contest(cid, 'Unknown'))


class _FakeCache2:
    def __init__(self, contests):
        self.contest_cache = _FakeContestCache(contests)


@pytest.fixture(autouse=True)
def _inject_cache(monkeypatch):
    """Inject a fake cache2 with standard contests into codeforces_common."""
    import tle.util.codeforces_common as cf_common
    contests = [
        _contest(1, 'Codeforces Round 900 (Div. 2)'),
        _contest(2, 'Codeforces Round 901 (Div. 1)'),
        _contest(3, 'Educational Codeforces Round 160'),
        _contest(4, 'Codeforces Round 902 (Div. 1 + Div. 2)'),
        _contest(5, 'Codeforces Global Round 25'),
    ]
    monkeypatch.setattr(cf_common, 'cache2', _FakeCache2(contests))


# ===================================================================
# SubFilter.parse — flag parsing
# ===================================================================

class TestParseFlagsBasic:
    """Verify that parse() sets the right internal state."""

    def test_no_args_defaults(self):
        f = SubFilter(rated=False)
        rest = f.parse([])
        assert rest == []
        # When no type flag given, all types are included
        assert set(f.types) == {'CONTESTANT', 'OUT_OF_COMPETITION', 'VIRTUAL', 'PRACTICE'}

    def test_contest_flag(self):
        f = SubFilter()
        f.parse(['+contest'])
        assert f.types == ['CONTESTANT']

    def test_practice_flag(self):
        f = SubFilter()
        f.parse(['+practice'])
        assert f.types == ['PRACTICE']

    def test_virtual_flag(self):
        f = SubFilter()
        f.parse(['+virtual'])
        assert f.types == ['VIRTUAL']

    def test_outof_flag(self):
        f = SubFilter()
        f.parse(['+outof'])
        assert f.types == ['OUT_OF_COMPETITION']

    def test_multiple_type_flags(self):
        f = SubFilter()
        f.parse(['+contest', '+virtual'])
        assert set(f.types) == {'CONTESTANT', 'VIRTUAL'}

    def test_tag_flags(self):
        f = SubFilter()
        f.parse(['+dp', '+greedy'])
        # Tags go into self.tags, NOT self.types
        assert set(f.tags) == {'dp', 'greedy'}
        # Type defaults remain (no explicit type flag)
        assert len(f.types) == 4

    def test_bantag_flags(self):
        f = SubFilter()
        f.parse(['~dp'])
        assert f.bantags == ['dp']

    def test_contest_marker(self):
        f = SubFilter()
        f.parse(['c+div2'])
        assert f.contests == ['div2']

    def test_index_marker(self):
        f = SubFilter()
        f.parse(['i+A'])
        assert f.indices == ['A']

    def test_rating_range(self):
        f = SubFilter()
        f.parse(['r>=1200', 'r<=2000'])
        assert f.rlo == 1200
        assert f.rhi == 2000

    def test_team_flag(self):
        f = SubFilter()
        f.parse(['+team'])
        assert f.team is True

    def test_unknown_args_returned_as_rest(self):
        f = SubFilter()
        rest = f.parse(['somehandle', 'otherhandle'])
        assert set(rest) == {'somehandle', 'otherhandle'}

    def test_flags_and_handles_mixed(self):
        f = SubFilter()
        rest = f.parse(['+contest', 'myhandle', 'r>=1000'])
        assert rest == ['myhandle']
        assert f.types == ['CONTESTANT']
        assert f.rlo == 1000


# ===================================================================
# SubFilter.filter_subs — submission filtering
# ===================================================================

class TestFilterSubsTypes:
    """Verify type-based filtering (the core of +contest behaviour)."""

    def test_contest_only(self):
        f = SubFilter(rated=False)
        f.parse(['+contest'])
        subs = [
            _sub(1, ptype='CONTESTANT', name='P1'),
            _sub(2, ptype='PRACTICE', name='P2'),
            _sub(3, ptype='VIRTUAL', name='P3'),
            _sub(4, ptype='OUT_OF_COMPETITION', name='P4'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_practice_only(self):
        f = SubFilter(rated=False)
        f.parse(['+practice'])
        subs = [
            _sub(1, ptype='CONTESTANT', name='ProbA'),
            _sub(2, ptype='PRACTICE', name='ProbB'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [2]

    def test_all_types_default(self):
        f = SubFilter(rated=False)
        f.parse([])
        subs = [
            _sub(1, ptype='CONTESTANT', name='P1'),
            _sub(2, ptype='PRACTICE', name='P2'),
            _sub(3, ptype='VIRTUAL', name='P3'),
            _sub(4, ptype='OUT_OF_COMPETITION', name='P4'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 4

    def test_contest_does_not_exclude_edu_contestant(self):
        """+contest includes CONTESTANT on edu rounds — this is the current
        behaviour that +rated is designed to address."""
        f = SubFilter(rated=False)
        f.parse(['+contest'])
        subs = [
            _sub(1, contest_id=3, ptype='CONTESTANT', name='EduProblem'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 1  # +contest does NOT filter edu

    def test_contest_excludes_outof_on_div2(self):
        """A high-rated user participating in div2 is OUT_OF_COMPETITION;
        +contest correctly excludes them."""
        f = SubFilter(rated=False)
        f.parse(['+contest'])
        subs = [
            _sub(1, contest_id=1, ptype='OUT_OF_COMPETITION', name='Div2Problem'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 0


class TestFilterSubsRating:
    """Verify rating-based filtering."""

    def test_rated_default_filters_by_rating(self):
        f = SubFilter(rated=True)
        f.parse(['r>=1000', 'r<=2000'])
        subs = [
            _sub(1, rating=1500, name='InRange'),
            _sub(2, rating=800, name='TooLow'),
            _sub(3, rating=2500, name='TooHigh'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_unrated_mode_ignores_rating(self):
        f = SubFilter(rated=False)
        f.parse([])
        subs = [
            _sub(1, rating=None, name='NoRating'),
            _sub(2, rating=1500, name='HasRating'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 2


class TestFilterSubsTags:
    """Verify tag-based filtering."""

    def test_include_tag(self):
        f = SubFilter(rated=False)
        f.parse(['+dp'])
        subs = [
            _sub(1, tags=['dp', 'greedy'], name='HasDP'),
            _sub(2, tags=['math'], name='NoDP'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_ban_tag(self):
        f = SubFilter(rated=False)
        f.parse(['~dp'])
        subs = [
            _sub(1, tags=['dp', 'greedy'], name='HasDP'),
            _sub(2, tags=['math'], name='NoDP'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [2]


class TestFilterSubsContestMarker:
    """Verify c+marker contest name matching."""

    def test_contest_marker_div2(self):
        f = SubFilter(rated=False)
        f.parse(['c+div2'])
        subs = [
            _sub(1, contest_id=1, name='Div2Prob'),  # contest 1 = "Div. 2"
            _sub(2, contest_id=2, name='Div1Prob'),  # contest 2 = "Div. 1"
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_contest_marker_edu(self):
        f = SubFilter(rated=False)
        f.parse(['c+edu'])
        subs = [
            _sub(1, contest_id=3, name='EduProb'),   # contest 3 = "Educational"
            _sub(2, contest_id=1, name='Div2Prob'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]


class TestFilterSubsDedup:
    """filter_subs deduplicates by (name, contest start time), keeping earliest."""

    def test_duplicate_problem_keeps_first(self):
        f = SubFilter(rated=False)
        f.parse([])
        subs = [
            _sub(1, name='Same', created=100),
            _sub(2, name='Same', created=200),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 1
        assert result[0].id == 1

    def test_different_names_kept(self):
        f = SubFilter(rated=False)
        f.parse([])
        subs = [
            _sub(1, name='ProbA', created=100),
            _sub(2, name='ProbB', created=200),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 2


class TestFilterSubsIndex:
    """Verify i+index filtering."""

    def test_index_filter(self):
        f = SubFilter(rated=False)
        f.parse(['i+B'])
        subs = [
            _sub(1, index='A', name='ProbA'),
            _sub(2, index='B', name='ProbB'),
            _sub(3, index='C', name='ProbC'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [2]


class TestFilterSubsVerdict:
    """Only OK (accepted) submissions are kept."""

    def test_only_accepted(self):
        f = SubFilter(rated=False)
        f.parse([])
        subs = [
            _sub(1, verdict='OK', name='Accepted'),
            _sub(2, verdict='WRONG_ANSWER', name='WA'),
            _sub(3, verdict='TIME_LIMIT_EXCEEDED', name='TLE'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]


# ===================================================================
# +rated flag — only contests that were rated for the user
# ===================================================================

class TestParseRatedFlag:
    """Verify +rated parse behaviour."""

    def test_rated_sets_only_rated(self):
        f = SubFilter()
        f.parse(['+rated'])
        assert f.only_rated is True
        # +rated implies CONTESTANT type
        assert 'CONTESTANT' in f.types

    def test_rated_flag_does_not_affect_contest(self):
        """+rated is separate from +contest — both can be parsed independently."""
        f = SubFilter()
        f.parse(['+contest'])
        assert f.only_rated is False
        assert f.types == ['CONTESTANT']

    def test_rated_and_contest_together(self):
        f = SubFilter()
        f.parse(['+rated', '+contest'])
        assert f.only_rated is True
        # Both add CONTESTANT; set() dedup in parse makes this just one entry
        assert f.types.count('CONTESTANT') >= 1


class TestFilterSubsRated:
    """Verify +rated filtering — the core new feature."""

    def test_rated_excludes_unrated_contest(self):
        """User participated as CONTESTANT in edu round (contest 3) but it
        wasn't rated for them (not in rated_contest_ids_by_handle)."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        # User was rated in contest 1 (div2) but not contest 3 (edu)
        f.rated_contest_ids_by_handle = {'tourist': {1}}
        subs = [
            _sub(1, contest_id=1, handle='tourist', ptype='CONTESTANT', name='Div2Prob'),
            _sub(2, contest_id=3, handle='tourist', ptype='CONTESTANT', name='EduProb'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_rated_includes_rated_edu(self):
        """A sub-2100 user WAS rated in the edu round — should be included."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        f.rated_contest_ids_by_handle = {'lowrated': {3}}  # contest 3 = edu
        subs = [
            _sub(1, contest_id=3, handle='lowrated', ptype='CONTESTANT', name='EduProb'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_rated_excludes_outof_competition(self):
        """+rated implies CONTESTANT type, so OUT_OF_COMPETITION is excluded."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        f.rated_contest_ids_by_handle = {'tourist': {1}}
        subs = [
            _sub(1, contest_id=1, handle='tourist', ptype='OUT_OF_COMPETITION', name='Prob'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 0

    def test_rated_without_handle_data_excludes_all(self):
        """If no rating data was fetched for the handle, everything is excluded."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        f.rated_contest_ids_by_handle = {'otheruser': {1}}
        subs = [
            _sub(1, contest_id=1, handle='tourist', ptype='CONTESTANT', name='Prob'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 0

    def test_rated_case_insensitive_handle(self):
        """Handle lookup should be case-insensitive."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        f.rated_contest_ids_by_handle = {'tourist': {1}}
        subs = [
            _sub(1, contest_id=1, handle='Tourist', ptype='CONTESTANT', name='Prob'),
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1]

    def test_rated_multiple_handles(self):
        """Each handle has its own rated contest set."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        f.rated_contest_ids_by_handle = {
            'user1': {1},     # rated in div2
            'user2': {1, 3},  # rated in both div2 and edu
        }
        subs = [
            _sub(1, contest_id=1, handle='user1', ptype='CONTESTANT', name='P1'),
            _sub(2, contest_id=3, handle='user1', ptype='CONTESTANT', name='P2'),  # unrated for user1
            _sub(3, contest_id=1, handle='user2', ptype='CONTESTANT', name='P3'),
            _sub(4, contest_id=3, handle='user2', ptype='CONTESTANT', name='P4'),  # rated for user2
        ]
        result = f.filter_subs(subs)
        assert [s.id for s in result] == [1, 3, 4]

    def test_without_rated_flag_no_filtering(self):
        """Without +rated, rated_contest_ids_by_handle is ignored even if set."""
        f = SubFilter(rated=False)
        f.parse(['+contest'])
        # Even if someone set this, it should have no effect
        f.rated_contest_ids_by_handle = {'tourist': {1}}
        subs = [
            _sub(1, contest_id=1, handle='tourist', ptype='CONTESTANT', name='P1'),
            _sub(2, contest_id=3, handle='tourist', ptype='CONTESTANT', name='P2'),
        ]
        result = f.filter_subs(subs)
        assert len(result) == 2  # Both included — +contest doesn't check rated

    def test_rated_empty_handle_data_passes_all(self):
        """If only_rated is True but rated_contest_ids_by_handle is empty dict,
        the rated check is skipped (graceful fallback)."""
        f = SubFilter(rated=False)
        f.parse(['+rated'])
        # Don't set rated_contest_ids_by_handle — it stays empty {}
        subs = [
            _sub(1, contest_id=1, handle='tourist', ptype='CONTESTANT', name='P1'),
        ]
        result = f.filter_subs(subs)
        # With empty dict, the guard `self.rated_contest_ids_by_handle` is falsy,
        # so rated_ok2 = True — passes through (the stalk command didn't fetch data yet)
        assert len(result) == 1

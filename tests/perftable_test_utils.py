"""Shared fixtures/builders for the perftable test modules
(``test_perftable.py`` and ``test_perftable_cfvc.py``)."""
from collections import namedtuple

from tle.util.codeforces_api import RatingChange, Contest, Submission


def _make_rc(contest_id, name, handle, rank, time, old, new):
    return RatingChange(contest_id, name, handle, rank, time, old, new)


# Minimal RanklistRow-like namedtuples for tests
_RanklistRow = namedtuple('RanklistRow', 'party rank points penalty problemResults')
_Party = namedtuple('Party', 'contestId members participantType teamId teamName ghost room startTimeSeconds')
_Member = namedtuple('Member', 'handle')


def _make_party(handle, ptype='VIRTUAL'):
    return _Party(contestId=1, members=[_Member(handle)], participantType=ptype,
                  teamId=None, teamName=None, ghost=False, room=None, startTimeSeconds=None)


def _make_ranklist_row(handle, rank, ptype='VIRTUAL'):
    return _RanklistRow(party=_make_party(handle, ptype), rank=rank,
                        points=1000.0, penalty=0, problemResults=[])


def _make_submission(contest_id, handle, ptype='VIRTUAL'):
    return Submission(id=1, contestId=contest_id,
                      problem=None, author=_make_party(handle, ptype),
                      programmingLanguage='C++', verdict='OK',
                      creationTimeSeconds=0, relativeTimeSeconds=0)


def _make_contest(cid, name):
    return Contest(id=cid, name=name, startTimeSeconds=1000,
                   durationSeconds=7200, type='CF', phase='FINISHED', preparedBy=None)

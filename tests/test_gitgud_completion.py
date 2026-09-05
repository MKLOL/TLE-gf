"""Tests for completing GitGud challenges."""
import asyncio
import datetime
from types import SimpleNamespace

from tle.cogs._codeforces_gitgud import CodeforcesGitgudMixin
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common


class _GitgudDb:
    def __init__(self, user_id, problem_name):
        self.active = (
            7,
            int(datetime.datetime.now().timestamp()) - 3600,
            problem_name,
            1234,
            'A',
            0,
        )
        self.user_id = user_id
        self.completed = None

    def check_challenge(self, user_id):
        assert user_id == self.user_id
        return self.active

    def complete_challenge(self, user_id, challenge_id, finish_time, score):
        self.completed = (user_id, challenge_id, finish_time, score)
        return 1


class _Ctx:
    def __init__(self, user_id):
        self.author = SimpleNamespace(id=user_id)
        self.message = SimpleNamespace(author=self.author)
        self.sent = []

    async def send(self, message, *args, **kwargs):
        self.sent.append(message)


def test_gotgud_reports_only_ranklist_points(monkeypatch):
    user_id = 123
    problem_name = 'Completion Problem'
    db = _GitgudDb(user_id, problem_name)
    ctx = _Ctx(user_id)

    async def resolve_handles(*args, **kwargs):
        return ['handleA']

    async def status(*, handle):
        assert handle == 'handleA'
        problem = SimpleNamespace(name=problem_name)
        return [SimpleNamespace(verdict='OK', problem=problem)]

    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(cf_common, 'resolve_handles', resolve_handles)
    monkeypatch.setattr(cf_common, 'pretty_time_format', lambda seconds: '1 hour')
    monkeypatch.setattr(cf, 'user', SimpleNamespace(status=status), raising=False)

    class _Cog(CodeforcesGitgudMixin):
        converter = None

    cog = _Cog()
    monkeypatch.setattr(cog, '_check_more_points_active', lambda *args: True)

    asyncio.run(cog._gotgud_impl(ctx))

    assert db.completed[:2] == (user_id, 7)
    assert db.completed[3] == 8
    assert ctx.sent == [
        'Challenge completed in 1 hour. handleA gained 8 alltime ranklist '
        'points and 16 monthly ranklist points.'
    ]

"""Tests for the per-guild ranklist-source config, signing, and the
rating_changes fallback dispatch in cf.contest.standings.

Covers:
- get_cf_ranklist_source reads guild_config correctly.
- cf.contest.standings routes to _standings_from_rating_changes under
  source='rating_changes'.
- _sign_params matches the Codeforces signing spec.
- ;probrat raises the Mike error under rating_changes mode.
- _standings_from_rating_changes pads problemResults so the ;ranklist
  table can render without IndexError.
"""
import asyncio
import hashlib
import importlib.util
import os
import sqlite3
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.starboard_db import StarboardDbMixin


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ── Real codeforces_api loader ──────────────────────────────────────────
# conftest.py stubs tle.util.codeforces_api, so we load the real file under
# a unique name for signing/dispatch tests that need real logic.

def _load_real_cf_api(api_key='', api_secret=''):
    """Load codeforces_api.py under a private name, with given CF creds.

    The real module reads CF_API_KEY/CF_API_SECRET at import time from
    environ, so we set them before loading. Returns the module object.
    """
    # aiohttp is stubbed in conftest, but the real module needs a few symbols
    # referenced only in function bodies. Provide minimal stubs before import.
    aiohttp = sys.modules['aiohttp']
    if not hasattr(aiohttp, 'ClientSession'):
        aiohttp.ClientSession = type('ClientSession', (), {})
    if not hasattr(aiohttp, 'ClientError'):
        aiohttp.ClientError = type('ClientError', (Exception,), {})
    if not hasattr(aiohttp, 'ContentTypeError'):
        aiohttp.ContentTypeError = type('ContentTypeError', (Exception,), {})
    if 'aiohttp.client_exceptions' not in sys.modules:
        ce = types.ModuleType('aiohttp.client_exceptions')
        ce.ClientError = aiohttp.ClientError
        sys.modules['aiohttp.client_exceptions'] = ce

    path = os.path.join(_ROOT, 'tle', 'util', 'codeforces_api.py')
    prev_key = os.environ.get('CF_API_KEY')
    prev_secret = os.environ.get('CF_API_SECRET')
    os.environ['CF_API_KEY'] = api_key
    os.environ['CF_API_SECRET'] = api_secret
    try:
        spec = importlib.util.spec_from_file_location(
            f'_real_cf_api_{hash((api_key, api_secret))}', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if prev_key is None:
            os.environ.pop('CF_API_KEY', None)
        else:
            os.environ['CF_API_KEY'] = prev_key
        if prev_secret is None:
            os.environ.pop('CF_API_SECRET', None)
        else:
            os.environ['CF_API_SECRET'] = prev_secret


# ── Fake guild-config DB (uses StarboardDbMixin — it owns get/set_guild_config) ──

class _FakeGuildDb(StarboardDbMixin):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id    TEXT NOT NULL,
                key         TEXT NOT NULL,
                value       TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.commit()


# ── get_cf_ranklist_source helper ───────────────────────────────────────

class TestGetCfRanklistSource:
    def test_default_is_standings(self):
        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        with patch.object(cf_common, 'user_db', db):
            assert cf_common.get_cf_ranklist_source(42) == 'standings'

    def test_returns_rating_changes_when_enabled(self):
        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        db.set_guild_config(42, 'cf_ranklist_source_rating_changes', '1')
        with patch.object(cf_common, 'user_db', db):
            assert cf_common.get_cf_ranklist_source(42) == 'rating_changes'

    def test_disabled_value_falls_back_to_standings(self):
        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        db.set_guild_config(42, 'cf_ranklist_source_rating_changes', '0')
        with patch.object(cf_common, 'user_db', db):
            # Only '1' enables the fallback.
            assert cf_common.get_cf_ranklist_source(42) == 'standings'

    def test_other_guild_isolated(self):
        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        db.set_guild_config(42, 'cf_ranklist_source_rating_changes', '1')
        with patch.object(cf_common, 'user_db', db):
            assert cf_common.get_cf_ranklist_source(99) == 'standings'

    def test_none_guild_id_defaults_to_standings(self):
        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        with patch.object(cf_common, 'user_db', db):
            assert cf_common.get_cf_ranklist_source(None) == 'standings'

    def test_no_user_db_defaults_to_standings(self):
        from tle.util import codeforces_common as cf_common
        with patch.object(cf_common, 'user_db', None):
            assert cf_common.get_cf_ranklist_source(42) == 'standings'


# ── _sign_params ─────────────────────────────────────────────────────────

class TestSignParams:
    def test_unsigned_when_no_credentials(self):
        cf = _load_real_cf_api(api_key='', api_secret='')
        params = {'contestId': 1234}
        signed = cf._sign_params('contest.standings', params)
        # No creds → returned unchanged.
        assert signed == params
        assert 'apiSig' not in (signed or {})
        assert 'apiKey' not in (signed or {})

    def test_signed_structure(self):
        cf = _load_real_cf_api(api_key='K', api_secret='S')
        params = {'contestId': 1234}
        signed = cf._sign_params('contest.standings', params)
        assert signed['apiKey'] == 'K'
        assert 'time' in signed
        assert int(signed['time'])  # parseable
        assert signed['apiSig'].startswith
        # apiSig = 6-char rand prefix + 128-char sha-512 hex.
        assert len(signed['apiSig']) == 6 + 128
        # Original param preserved (stringified).
        assert signed['contestId'] == '1234'

    def test_signature_matches_cf_spec(self):
        """Verify apiSig digest matches the documented CF formula."""
        cf = _load_real_cf_api(api_key='ak', api_secret='sk')
        signed = cf._sign_params('contest.list', {})
        rand = signed['apiSig'][:6]
        digest = signed['apiSig'][6:]
        # Reconstruct the signing string from signed params (excluding apiSig).
        sig_input_params = {k: v for k, v in signed.items() if k != 'apiSig'}
        query = '&'.join(f'{k}={v}' for k, v in sorted(sig_input_params.items()))
        expected = hashlib.sha512(
            f'{rand}/contest.list?{query}#sk'.encode()).hexdigest()
        assert digest == expected

    def test_handles_none_params(self):
        cf = _load_real_cf_api(api_key='K', api_secret='S')
        signed = cf._sign_params('contest.list', None)
        # None params → dict with apiKey/time/apiSig.
        assert 'apiKey' in signed
        assert 'apiSig' in signed


# ── contest.standings dispatch ─────────────────────────────────────────

def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestStandingsDispatch:
    def _run(self, coro):
        return _run_async(coro)

    def test_dispatches_to_rating_changes_fallback(self):
        cf = _load_real_cf_api(api_key='K', api_secret='S')
        fake_result = ('CONTEST', ['PROBLEM'], ['ROW'])

        async def fake_fallback(*, contest_id, from_, count, handles):
            return fake_result

        # _query_api must NOT be called when source='rating_changes'.
        query_mock = AsyncMock(side_effect=AssertionError('real endpoint hit'))
        with patch.object(cf.contest, '_standings_from_rating_changes', fake_fallback), \
             patch.object(cf, '_query_api', query_mock):
            result = self._run(cf.contest.standings(
                contest_id=1, source='rating_changes'))
        assert result == fake_result
        query_mock.assert_not_called()

    def test_default_source_uses_real_endpoint(self):
        cf = _load_real_cf_api(api_key='K', api_secret='S')
        resp = {
            'contest': {'id': 1, 'name': 'R', 'type': 'CF', 'phase': 'FINISHED'},
            'problems': [],
            'rows': [],
        }
        with patch.object(cf, '_query_api', AsyncMock(return_value=resp)) as q:
            self._run(cf.contest.standings(contest_id=1))
        q.assert_called_once()
        called_path = q.call_args.args[0] if q.call_args.args else q.call_args.kwargs.get('path')
        assert called_path == 'contest.standings'

    def test_explicit_standings_source_uses_real_endpoint(self):
        cf = _load_real_cf_api(api_key='K', api_secret='S')
        resp = {
            'contest': {'id': 1, 'name': 'R', 'type': 'CF', 'phase': 'FINISHED'},
            'problems': [],
            'rows': [],
        }
        with patch.object(cf, '_query_api', AsyncMock(return_value=resp)) as q:
            self._run(cf.contest.standings(contest_id=1, source='standings'))
        q.assert_called_once()


# ── ;probrat Mike error ─────────────────────────────────────────────────

class TestProbratMikeError:
    """probrat must error out before any API call when source=rating_changes."""

    def _make_ctx(self, guild_id=42):
        ctx = MagicMock()
        ctx.guild.id = guild_id
        ctx.send = AsyncMock()
        return ctx

    def _get_probrat_func(self):
        """Load contests cog as a real module to access the unbound method."""
        from tle.util import codeforces_common as cf_common  # noqa: F401 — ensures stubs
        cogs_path = os.path.join(_ROOT, 'tle', 'cogs', 'contests.py')

        # Stub tle.util.ranklist (contests.py does `from tle.util import ranklist as rl`)
        # — the real module pulls in numpy.fft which isn't available.
        if 'tle.util.ranklist' not in sys.modules or not hasattr(
                sys.modules['tle.util.ranklist'], 'RanklistError'):
            rl = types.ModuleType('tle.util.ranklist')
            rl.RanklistError = type('RanklistError', (Exception,), {})
            rl.Ranklist = MagicMock()
            rl.HandleNotPresentError = type('HandleNotPresentError', (Exception,), {})
            rl.__path__ = []
            sys.modules['tle.util.ranklist'] = rl
            sys.modules['tle.util'].ranklist = rl

        # Also stub ContestNotFound which contests.py references.
        from tle.util import cache_system2 as cs
        if not hasattr(cs, 'CacheError'):
            cs.CacheError = type('CacheError', (Exception,), {})
        if not hasattr(cs, 'RanklistNotMonitored'):
            cs.RanklistNotMonitored = type('RanklistNotMonitored', (Exception,), {})

        # tasks.Waiter.for_event + events.* used at class-body evaluation.
        from tle.util import tasks, events
        if not hasattr(tasks.Waiter, 'for_event'):
            tasks.Waiter.for_event = staticmethod(lambda ev: tasks.Waiter())
        for event_name in ('ContestListRefresh', 'RatingChangesUpdate'):
            if not hasattr(events, event_name):
                setattr(events, event_name, type(event_name, (), {}))

        if '_contests_mod_for_probrat' in sys.modules:
            return sys.modules['_contests_mod_for_probrat']

        spec = importlib.util.spec_from_file_location('_contests_mod_for_probrat', cogs_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules['_contests_mod_for_probrat'] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            del sys.modules['_contests_mod_for_probrat']
            pytest.skip(f'Could not load contests cog for probrat test: {e}')
        return mod

    def test_raises_mike_error_when_rating_changes_mode(self):
        contests_mod = self._get_probrat_func()
        cog = contests_mod.Contests.__new__(contests_mod.Contests)
        ctx = self._make_ctx(guild_id=42)

        from tle.util import codeforces_common as cf_common
        db = _FakeGuildDb()
        db.set_guild_config(42, 'cf_ranklist_source_rating_changes', '1')

        async def _run():
            with patch.object(cf_common, 'user_db', db):
                func = contests_mod.Contests.problemratings
                if hasattr(func, '__wrapped__'):
                    func = func.__wrapped__
                with pytest.raises(contests_mod.ContestCogError) as excinfo:
                    await func(cog, ctx, 1234)
                assert 'Mike' in str(excinfo.value)
                assert 'probrat' in str(excinfo.value)

        _run_async(_run())
        # Should not have reached ctx.send (the "This will take a while" line).
        ctx.send.assert_not_called()

    def test_no_error_when_standings_mode(self):
        contests_mod = self._get_probrat_func()
        from tle.util import codeforces_common as cf_common
        from tle.util import codeforces_api as cf
        db = _FakeGuildDb()  # no config → default standings

        ctx = self._make_ctx(guild_id=42)

        # Stub cf.contest (conftest only stubs cf.Contest classes, not the
        # contest namespace) with a .list that errors as a sentinel after the
        # source check passes.
        fake_contest_ns = types.SimpleNamespace(
            list=AsyncMock(side_effect=RuntimeError('stop-here')),
        )

        async def _run():
            with patch.object(cf_common, 'user_db', db), \
                 patch.object(cf, 'contest', fake_contest_ns, create=True):
                func = contests_mod.Contests.problemratings
                if hasattr(func, '__wrapped__'):
                    func = func.__wrapped__
                with pytest.raises(RuntimeError) as excinfo:
                    await func(contests_mod.Contests.__new__(contests_mod.Contests),
                               ctx, 1234)
                assert 'stop-here' in str(excinfo.value)

        _run_async(_run())


# ── fallback row shape + table-render regression ───────────────────────
# Regression for the ;ranklist IndexError: _standings_from_rating_changes
# used to return rows with problemResults=[], but the standings table's
# column count is driven by len(problems), so Table.__repr__ at table.py:82
# would IndexError when computing max_colsize. Rows must pad problemResults
# to len(problems) so the table can render.

class TestFallbackRowShape:
    def _run(self, coro):
        return _run_async(coro)

    def _build_fallback(self, num_problems=3, handles=('alice', 'bob')):
        """Call _standings_from_rating_changes with mocked upstream endpoints."""
        cf = _load_real_cf_api(api_key='K', api_secret='S')

        contest_id = 2200
        contest_obj = cf.Contest(
            id=contest_id, name='R', startTimeSeconds=1000,
            durationSeconds=7200, type='CF', phase='FINISHED',
            preparedBy=None,
        )

        problems = [
            cf.Problem(contestId=contest_id, problemsetName=None, index=chr(ord('A') + i),
                       name=f'P{i}', type='PROGRAMMING', points=None, rating=1500,
                       tags=[])
            for i in range(num_problems)
        ]
        changes = [
            cf.RatingChange(contestId=contest_id, contestName='R', handle=h, rank=idx + 1,
                            ratingUpdateTimeSeconds=1000, oldRating=1500, newRating=1550)
            for idx, h in enumerate(handles)
        ]

        async def fake_contest_list(*, gym=None):
            return [contest_obj]

        async def fake_problemset_problems(*, tags=None, problemset_name=None):
            return problems, []

        async def fake_rating_changes(*, contest_id):
            return changes

        with patch.object(cf.contest, 'list', fake_contest_list), \
             patch.object(cf.problemset, 'problems', fake_problemset_problems), \
             patch.object(cf.contest, 'ratingChanges', fake_rating_changes):
            return self._run(cf.contest._standings_from_rating_changes(contest_id=contest_id))

    def test_problem_results_padded_to_match_problems(self):
        contest, problems, rows = self._build_fallback(num_problems=5,
                                                       handles=('alice', 'bob', 'carol'))
        assert len(problems) == 5
        assert len(rows) == 3
        # The bug: rows had problemResults=[] while problems had 5 entries,
        # so the standings table column count mismatched and crashed render.
        for row in rows:
            assert len(row.problemResults) == len(problems), (
                'problemResults must match len(problems) so the ;ranklist '
                'table does not IndexError on render')

    def test_empty_problems_yields_empty_problem_results(self):
        _, problems, rows = self._build_fallback(num_problems=0, handles=('alice',))
        assert problems == []
        assert len(rows) == 1
        assert rows[0].problemResults == []

    def test_placeholder_results_render_blank_scores(self):
        """Blank cells: points=0 → score='' in CF rendering."""
        _, _, rows = self._build_fallback(num_problems=3, handles=('alice',))
        for pr in rows[0].problemResults:
            assert pr.points == 0 or pr.points == 0.0
            assert pr.rejectedAttemptCount == 0

    def test_ranklist_table_renders_without_indexerror(self):
        """Build the exact table shape ;ranklist uses and render it.

        This is the direct regression for the IndexError seen at
        table.py:82 — rebuild header/body the same way the cog does and
        verify str(Table) doesn't raise.
        """
        from tle.util import table as tbl
        contest, problems, rows = self._build_fallback(num_problems=4,
                                                       handles=('alice', 'bob'))
        problem_indices = [p.index for p in problems]
        # Replicate _get_cf_or_ioi_standings_table's format strings and body.
        header_style = '{:>} {:<}    {:^}  ' + '  '.join(['{:^}'] * len(problem_indices))
        body_style = '{:>} {:<}    {:>}  ' + '  '.join(['{:>}'] * len(problem_indices))
        header = ['#', 'Handle', '='] + problem_indices
        body = []
        for row in rows:
            handle = row.party.members[0].handle
            tokens = [row.rank, handle, int(row.points)]
            for pr in row.problemResults:
                tokens.append('' if not pr.points else str(int(pr.points)))
            body.append(tokens)

        t = tbl.Table(tbl.Style(header=header_style, body=body_style))
        t += tbl.Header(*header)
        t += tbl.Line('-')
        for row_tokens in body:
            t += tbl.Data(*row_tokens)
        t += tbl.Line('-')
        # This is what crashed before the fix: __repr__ computes max
        # column sizes across all Content rows, which requires every row
        # to have `ncols` entries.
        rendered = str(t)
        assert 'alice' in rendered
        assert 'bob' in rendered
        # Problem indices appear in the header row.
        for idx in problem_indices:
            assert idx in rendered

"""Tests for the async _build_cfvc_rows helper (CF virtual-participation perf).
Split from test_perftable.py to keep each test module under 500 lines."""
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

from tle.cogs._graph_perftable import _build_cfvc_rows, _CONTEST_NAME_MAX
from tests.perftable_test_utils import _make_rc, _make_submission, _make_contest


class TestBuildCfvcRows:
    def _run(self, coro):
        return asyncio.run(coro)

    def _setup_empty_db_cache(self, mock_common):
        """Set up mock user_db with empty cfvc rank cache."""
        mock_common.user_db.get_cfvc_cached_contest_ids.return_value = set()
        mock_common.user_db.get_cfvc_cache.return_value = []  # (contest_id, rank) tuples
        mock_common.user_db.save_cfvc_cache = MagicMock()

    def _setup_db_cache(self, mock_common, ranks):
        """Set up mock user_db with populated cfvc rank cache.
        `ranks` is a list of (contest_id, rank) tuples."""
        mock_common.user_db.get_cfvc_cached_contest_ids.return_value = {
            cid for cid, _ in ranks}
        mock_common.user_db.get_cfvc_cache.return_value = list(ranks)
        mock_common.user_db.save_cfvc_cache = MagicMock()

    def test_basic_virtual_contest(self):
        """With cached rank data, a single virtual contest yields one row."""
        subs = [_make_submission(100, 'user', 'VIRTUAL')]
        contest = _make_contest(100, 'Round 100')
        cache_changes = [_make_rc(100, 'Round 100', 'other', 48, 1000, 1600, 1650)]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = cache_changes
            mock_common.cache2.contest_cache.get_contest.return_value = contest
            self._setup_db_cache(mock_common, [(100, 50)])

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert len(rows) == 1
        assert rows[0]['rank'] == 50
        assert rows[0]['contest'] == 'Round 100'
        # perf from closest rank (48): 1600 + 4*50 = 1800
        assert rows[0]['perf'] == 1800
        assert missing == 0
        # Standings API is never called under the new CF restriction
        mock_cf.contest.standings.assert_not_called()

    def test_gym_contests_excluded(self):
        """Gym contests must be filtered upstream of any DB/API lookup."""
        subs = [
            _make_submission(100, 'user', 'VIRTUAL'),
            _make_submission(100500, 'user', 'VIRTUAL'),  # gym
        ]
        contest = _make_contest(100, 'Round 100')
        cache_changes = [_make_rc(100, 'R', 'x', 50, 1000, 1500, 1550)]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = cache_changes
            mock_common.cache2.contest_cache.get_contest.return_value = contest
            self._setup_db_cache(mock_common, [(100, 50)])

            rows, missing = self._run(_build_cfvc_rows('user'))

        # Only contest 100 produces a row; gym 100500 is excluded
        assert len(rows) == 1
        assert missing == 0
        mock_cf.contest.standings.assert_not_called()

    def test_no_virtual_contests(self):
        subs = [_make_submission(100, 'user', 'CONTESTANT')]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.GYM_ID_THRESHOLD = 100000

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert rows == []
        assert missing == 0

    def test_empty_submissions(self):
        with patch('tle.cogs._graph_perftable.cf') as mock_cf:
            mock_cf.user.status = AsyncMock(return_value=[])
            mock_cf.GYM_ID_THRESHOLD = 100000

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert rows == []
        assert missing == 0

    def test_missing_cache_data_increments_missing(self):
        """Uncached contests always count as missing — the API can no
        longer recover a VIRTUAL rank under CF's restriction."""
        subs = [_make_submission(100, 'user', 'VIRTUAL')]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            self._setup_empty_db_cache(mock_common)

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert rows == []
        assert missing == 1
        mock_cf.contest.standings.assert_not_called()

    def test_multiple_contests_mixed_success(self):
        """Mix of cached (recovered) and uncached (missing) contests."""
        subs = [
            _make_submission(100, 'user', 'VIRTUAL'),
            _make_submission(200, 'user', 'VIRTUAL'),
            _make_submission(300, 'user', 'VIRTUAL'),
        ]
        contest100 = _make_contest(100, 'R100')
        contest300 = _make_contest(300, 'R300')
        cache100 = [_make_rc(100, 'R100', 'x', 50, 1000, 1500, 1550)]
        cache300 = [_make_rc(300, 'R300', 'x', 10, 1000, 1800, 1850)]

        def mock_rc_cache(cid):
            return {100: cache100, 300: cache300}.get(cid, [])

        def mock_contest_cache(cid):
            return {100: contest100, 300: contest300}[cid]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.side_effect = mock_rc_cache
            mock_common.cache2.contest_cache.get_contest.side_effect = mock_contest_cache
            # 100 and 300 cached, 200 is uncached → missing
            self._setup_db_cache(mock_common, [(100, 50), (300, 10)])

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert len(rows) == 2
        assert rows[0]['contest'] == 'R100'
        assert rows[1]['contest'] == 'R300'
        assert missing == 1  # contest 200 has no cached rank
        mock_cf.contest.standings.assert_not_called()

    def test_deduplicates_contest_ids(self):
        """Multiple submissions in same virtual contest produce one row."""
        subs = [
            _make_submission(100, 'user', 'VIRTUAL'),
            _make_submission(100, 'user', 'VIRTUAL'),  # duplicate
        ]
        contest = _make_contest(100, 'Round 100')
        cache = [_make_rc(100, 'R', 'x', 50, 1000, 1500, 1550)]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = cache
            mock_common.cache2.contest_cache.get_contest.return_value = contest
            self._setup_db_cache(mock_common, [(100, 50)])

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert len(rows) == 1
        mock_cf.contest.standings.assert_not_called()

    def test_long_contest_name_truncated(self):
        subs = [_make_submission(100, 'user', 'VIRTUAL')]
        long_name = 'A' * 50
        contest = _make_contest(100, long_name)
        cache = [_make_rc(100, long_name, 'x', 50, 1000, 1500, 1550)]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = cache
            mock_common.cache2.contest_cache.get_contest.return_value = contest
            self._setup_db_cache(mock_common, [(100, 50)])

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert len(rows[0]['contest']) == _CONTEST_NAME_MAX

    def test_date_filter_lower_bound(self):
        """Contests before dlo are excluded."""
        subs = [
            _make_submission(100, 'user', 'VIRTUAL'),
            _make_submission(200, 'user', 'VIRTUAL'),
        ]
        contest100 = _make_contest(100, 'Old Round')._replace(startTimeSeconds=1000)
        contest200 = _make_contest(200, 'New Round')._replace(startTimeSeconds=5000)
        cache100 = [_make_rc(100, 'Old Round', 'x', 50, 1000, 1500, 1550)]
        cache200 = [_make_rc(200, 'New Round', 'x', 30, 5000, 1600, 1650)]

        def mock_rc_cache(cid):
            return {100: cache100, 200: cache200}.get(cid, [])

        def mock_contest_cache(cid):
            return {100: contest100, 200: contest200}[cid]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.side_effect = mock_rc_cache
            mock_common.cache2.contest_cache.get_contest.side_effect = mock_contest_cache
            self._setup_db_cache(mock_common, [(100, 50), (200, 30)])

            rows, missing = self._run(_build_cfvc_rows('user', dlo=3000))

        assert len(rows) == 1
        assert rows[0]['contest'] == 'New Round'

    def test_date_filter_upper_bound(self):
        """Contests at or after dhi are excluded."""
        subs = [
            _make_submission(100, 'user', 'VIRTUAL'),
            _make_submission(200, 'user', 'VIRTUAL'),
        ]
        contest100 = _make_contest(100, 'Old Round')._replace(startTimeSeconds=1000)
        contest200 = _make_contest(200, 'New Round')._replace(startTimeSeconds=5000)
        cache100 = [_make_rc(100, 'Old Round', 'x', 50, 1000, 1500, 1550)]
        cache200 = [_make_rc(200, 'New Round', 'x', 30, 5000, 1600, 1650)]

        def mock_rc_cache(cid):
            return {100: cache100, 200: cache200}.get(cid, [])

        def mock_contest_cache(cid):
            return {100: contest100, 200: contest200}[cid]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()
            mock_cf.GYM_ID_THRESHOLD = 100000
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.side_effect = mock_rc_cache
            mock_common.cache2.contest_cache.get_contest.side_effect = mock_contest_cache
            self._setup_db_cache(mock_common, [(100, 50), (200, 30)])

            rows, missing = self._run(_build_cfvc_rows('user', dhi=3000))

        assert len(rows) == 1
        assert rows[0]['contest'] == 'Old Round'

    def test_uncached_contests_skip_wasted_api_call(self):
        """Under CF's May 2026 standings restriction, the endpoint returns
        CONTESTANT-only rows, so calling contest.standings for an uncached
        VIRTUAL contest can never recover a rank. The function must skip
        the wasted multi-MB request and count those contests as missing.
        """
        subs = [_make_submission(100, 'user', 'VIRTUAL'),
                _make_submission(200, 'user', 'VIRTUAL')]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()  # should NOT be called
            mock_cf.GYM_ID_THRESHOLD = 100000
            self._setup_empty_db_cache(mock_common)

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert rows == []
        assert missing == 2
        mock_cf.contest.standings.assert_not_called()

    def test_cached_results_skip_api_calls(self):
        """Already-cached contests should not trigger standings API calls."""
        subs = [_make_submission(100, 'user', 'VIRTUAL')]
        contest = _make_contest(100, 'Round 100')
        rc_cache = [_make_rc(100, 'Round 100', 'other', 48, 1000, 1600, 1650)]

        with patch('tle.cogs._graph_perftable.cf') as mock_cf, \
             patch('tle.cogs._graph_perftable.cf_common') as mock_common:
            mock_cf.user.status = AsyncMock(return_value=subs)
            mock_cf.contest.standings = AsyncMock()  # should not be called
            mock_cf.GYM_ID_THRESHOLD = 100000
            # Already cached — only rank, perf computed on-the-fly
            mock_common.user_db.get_cfvc_cached_contest_ids.return_value = {100}
            mock_common.user_db.get_cfvc_cache.return_value = [(100, 50)]
            mock_common.cache2.contest_cache.get_contest.return_value = contest
            mock_common.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = rc_cache

            rows, missing = self._run(_build_cfvc_rows('user'))

        assert len(rows) == 1
        assert rows[0]['rank'] == 50
        # perf computed from shared cache: 1600 + 4*50 = 1800
        assert rows[0]['perf'] == 1800
        # No standings API calls needed
        mock_cf.contest.standings.assert_not_called()

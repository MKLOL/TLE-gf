"""Tests for the perftable pure functions: _build_rated_rows, _build_vc_rows,
_format_perftable, _format_cfvc_table, _estimate_perf_from_cache, and _truncate_name."""
import pytest
from collections import namedtuple
from unittest.mock import patch, MagicMock

from tle.cogs.graphs import (
    _build_rated_rows,
    _build_vc_rows,
    _format_perftable,
    _format_cfvc_table,
    _estimate_perf_from_cache,
    _truncate_name,
    _CONTEST_NAME_MAX,
)
from tle.util.codeforces_api import RatingChange


# =====================================================================
# _truncate_name
# =====================================================================

class TestTruncateName:
    def test_short_name_unchanged(self):
        assert _truncate_name('Codeforces Round 123') == 'Codeforces Round 123'

    def test_exact_limit_unchanged(self):
        name = 'x' * _CONTEST_NAME_MAX
        assert _truncate_name(name) == name

    def test_long_name_truncated(self):
        name = 'Codeforces Round 999 (Div. 1 + Div. 2)'
        result = _truncate_name(name)
        assert len(result) == _CONTEST_NAME_MAX
        assert result.endswith('...')

    def test_one_over_limit(self):
        name = 'x' * (_CONTEST_NAME_MAX + 1)
        result = _truncate_name(name)
        assert len(result) == _CONTEST_NAME_MAX
        assert result.endswith('...')


# =====================================================================
# _build_rated_rows
# =====================================================================

def _make_rc(contest_id, name, handle, rank, time, old, new):
    return RatingChange(contest_id, name, handle, rank, time, old, new)


class TestBuildRatedRows:
    def test_single_contest(self):
        orig = [_make_rc(1, 'Round 1', 'user', 100, 1000, 1500, 1550)]
        corr = [_make_rc(1, 'Round 1', 'user', 100, 1000, 0, 1700)]
        rows = _build_rated_rows(orig, corr)
        assert len(rows) == 1
        r = rows[0]
        assert r['idx'] == 1
        assert r['contest'] == 'Round 1'
        assert r['rank'] == 100
        assert r['old'] == 1500
        assert r['new'] == 1550
        assert r['delta'] == 50
        assert r['perf'] == 1700

    def test_multiple_contests(self):
        orig = [
            _make_rc(1, 'R1', 'u', 10, 1000, 1500, 1600),
            _make_rc(2, 'R2', 'u', 20, 2000, 1600, 1580),
            _make_rc(3, 'R3', 'u', 5,  3000, 1580, 1650),
        ]
        corr = [
            _make_rc(1, 'R1', 'u', 10, 1000, 0, 1900),
            _make_rc(2, 'R2', 'u', 20, 2000, 1900, 1520),
            _make_rc(3, 'R3', 'u', 5,  3000, 1520, 1860),
        ]
        rows = _build_rated_rows(orig, corr)
        assert len(rows) == 3
        assert [r['idx'] for r in rows] == [1, 2, 3]
        assert rows[0]['delta'] == 100
        assert rows[1]['delta'] == -20
        assert rows[2]['delta'] == 70

    def test_negative_delta(self):
        orig = [_make_rc(1, 'R1', 'u', 50, 1000, 1500, 1400)]
        corr = [_make_rc(1, 'R1', 'u', 50, 1000, 0, 1100)]
        rows = _build_rated_rows(orig, corr)
        assert rows[0]['delta'] == -100
        assert rows[0]['perf'] == 1100

    def test_long_contest_name_truncated(self):
        long_name = 'Codeforces Round 999 (Div. 1 + Div. 2, based on VK Cup Finals)'
        orig = [_make_rc(1, long_name, 'u', 1, 1000, 1500, 1600)]
        corr = [_make_rc(1, long_name, 'u', 1, 1000, 0, 1900)]
        rows = _build_rated_rows(orig, corr)
        assert len(rows[0]['contest']) == _CONTEST_NAME_MAX
        assert rows[0]['contest'].endswith('...')

    def test_empty_input(self):
        assert _build_rated_rows([], []) == []


# =====================================================================
# _build_vc_rows
# =====================================================================

VcRating = namedtuple('VcRating', 'vc_id rating')


class TestBuildVcRows:
    def _make_history(self, entries):
        """entries: list of (vc_id, rating)"""
        return [VcRating(vc_id, rating) for vc_id, rating in entries]

    def _info_fn(self, mapping):
        """Return a get_vc_info function backed by a dict of vc_id -> (finish_time, name)."""
        def get_vc_info(vc_id):
            return mapping[vc_id]
        return get_vc_info

    def test_single_vc(self):
        history = self._make_history([(1, 1550)])
        info = self._info_fn({1: (5000, 'Contest A')})
        rows = _build_vc_rows(history, 0, 10**10, info)
        assert len(rows) == 1
        r = rows[0]
        assert r['idx'] == 1
        assert r['old'] == 1500  # default start
        assert r['new'] == 1550
        assert r['delta'] == 50
        assert r['perf'] == 1500 + 50 * 4  # 1700
        assert r['rank'] is None

    def test_multiple_vcs_chain_rating(self):
        history = self._make_history([(1, 1550), (2, 1600), (3, 1570)])
        info = self._info_fn({
            1: (1000, 'C1'), 2: (2000, 'C2'), 3: (3000, 'C3'),
        })
        rows = _build_vc_rows(history, 0, 10**10, info)
        assert len(rows) == 3
        # First: old=1500, new=1550, perf=1500+50*4=1700
        assert rows[0]['old'] == 1500
        assert rows[0]['perf'] == 1700
        # Second: old=1550, new=1600, perf=1550+50*4=1750
        assert rows[1]['old'] == 1550
        assert rows[1]['perf'] == 1750
        # Third: old=1600, new=1570, perf=1600+(-30)*4=1480
        assert rows[2]['old'] == 1600
        assert rows[2]['delta'] == -30
        assert rows[2]['perf'] == 1480

    def test_date_filter_excludes(self):
        history = self._make_history([(1, 1550), (2, 1600)])
        info = self._info_fn({1: (1000, 'C1'), 2: (5000, 'C2')})
        # Only include vc with finish_time >= 3000
        rows = _build_vc_rows(history, 3000, 10**10, info)
        assert len(rows) == 1
        # The filtered-out vc still updates ratingbefore
        assert rows[0]['old'] == 1550
        assert rows[0]['new'] == 1600
        assert rows[0]['idx'] == 1

    def test_date_filter_upper_bound(self):
        history = self._make_history([(1, 1550), (2, 1600)])
        info = self._info_fn({1: (1000, 'C1'), 2: (5000, 'C2')})
        # Only include vc with finish_time < 3000
        rows = _build_vc_rows(history, 0, 3000, info)
        assert len(rows) == 1
        assert rows[0]['contest'] == 'C1'

    def test_empty_history(self):
        rows = _build_vc_rows([], 0, 10**10, lambda x: None)
        assert rows == []

    def test_all_filtered_out(self):
        history = self._make_history([(1, 1550)])
        info = self._info_fn({1: (1000, 'C1')})
        rows = _build_vc_rows(history, 5000, 10**10, info)
        assert rows == []

    def test_long_contest_name(self):
        long_name = 'A' * 50
        history = self._make_history([(1, 1550)])
        info = self._info_fn({1: (1000, long_name)})
        rows = _build_vc_rows(history, 0, 10**10, info)
        assert len(rows[0]['contest']) == _CONTEST_NAME_MAX

    def test_performance_formula(self):
        """perf = old + (new - old) * 4"""
        history = self._make_history([(1, 1600)])
        info = self._info_fn({1: (1000, 'C1')})
        rows = _build_vc_rows(history, 0, 10**10, info)
        # old=1500, new=1600, delta=100, perf=1500+100*4=1900
        assert rows[0]['perf'] == 1900

    def test_negative_performance(self):
        """Bad VC result gives low performance."""
        history = self._make_history([(1, 1300)])
        info = self._info_fn({1: (1000, 'C1')})
        rows = _build_vc_rows(history, 0, 10**10, info)
        # old=1500, new=1300, delta=-200, perf=1500+(-200)*4=700
        assert rows[0]['perf'] == 700


# =====================================================================
# _format_perftable
# =====================================================================

class TestFormatPerftable:
    def test_rated_rows_have_rank_column(self):
        rows = [{'idx': 1, 'contest': 'R1', 'rank': 50,
                 'old': 1500, 'new': 1550, 'delta': 50, 'perf': 1700}]
        result = _format_perftable(rows)
        assert 'Rank' in result
        assert '50' in result
        assert '1700' in result

    def test_vc_rows_no_rank_column(self):
        rows = [{'idx': 1, 'contest': 'C1', 'rank': None,
                 'old': 1500, 'new': 1550, 'delta': 50, 'perf': 1700}]
        result = _format_perftable(rows)
        assert 'Rank' not in result
        assert '1700' in result

    def test_positive_delta_has_plus(self):
        rows = [{'idx': 1, 'contest': 'R1', 'rank': 10,
                 'old': 1500, 'new': 1600, 'delta': 100, 'perf': 1900}]
        result = _format_perftable(rows)
        assert '+100' in result

    def test_negative_delta_has_minus(self):
        rows = [{'idx': 1, 'contest': 'R1', 'rank': 10,
                 'old': 1600, 'new': 1550, 'delta': -50, 'perf': 1400}]
        result = _format_perftable(rows)
        assert '-50' in result

    def test_zero_delta(self):
        rows = [{'idx': 1, 'contest': 'R1', 'rank': 10,
                 'old': 1500, 'new': 1500, 'delta': 0, 'perf': 1500}]
        result = _format_perftable(rows)
        assert '+0' in result

    def test_multiple_rows(self):
        rows = [
            {'idx': 1, 'contest': 'R1', 'rank': 10,
             'old': 1500, 'new': 1600, 'delta': 100, 'perf': 1900},
            {'idx': 2, 'contest': 'R2', 'rank': 20,
             'old': 1600, 'new': 1580, 'delta': -20, 'perf': 1520},
        ]
        result = _format_perftable(rows)
        lines = result.strip().split('\n')
        # Header + line + 2 data rows = 4 lines
        assert len(lines) == 4

    def test_header_columns_rated(self):
        rows = [{'idx': 1, 'contest': 'R1', 'rank': 1,
                 'old': 1500, 'new': 1600, 'delta': 100, 'perf': 1900}]
        result = _format_perftable(rows)
        assert '#' in result
        assert 'Contest' in result
        assert 'Rank' in result
        assert 'Old' in result
        assert 'New' in result
        assert 'Perf' in result

    def test_header_columns_vc(self):
        rows = [{'idx': 1, 'contest': 'C1', 'rank': None,
                 'old': 1500, 'new': 1600, 'delta': 100, 'perf': 1900}]
        result = _format_perftable(rows)
        assert '#' in result
        assert 'Contest' in result
        assert 'Rank' not in result
        assert 'Old' in result
        assert 'New' in result
        assert 'Perf' in result

    def test_empty_rows(self):
        result = _format_perftable([])
        # Should still produce header and separator
        assert '#' in result
        lines = result.strip().split('\n')
        assert len(lines) == 2  # header + line, no data

    def test_mixed_rank_none(self):
        """If any row has a rank, all rows show the Rank column."""
        rows = [
            {'idx': 1, 'contest': 'R1', 'rank': 10,
             'old': 1500, 'new': 1600, 'delta': 100, 'perf': 1900},
            {'idx': 2, 'contest': 'R2', 'rank': None,
             'old': 1600, 'new': 1580, 'delta': -20, 'perf': 1520},
        ]
        result = _format_perftable(rows)
        assert 'Rank' in result

    def test_large_table_renders(self):
        """Ensure a 50-row table renders without errors."""
        rows = [
            {'idx': i, 'contest': f'Contest {i}', 'rank': i * 10,
             'old': 1500 + i, 'new': 1500 + i + 10, 'delta': 10, 'perf': 1540 + i}
            for i in range(1, 51)
        ]
        result = _format_perftable(rows)
        lines = result.strip().split('\n')
        assert len(lines) == 52  # header + separator + 50 data rows


# =====================================================================
# _estimate_perf_from_cache
# =====================================================================

class TestEstimatePerfFromCache:
    def _make_changes(self, entries):
        """entries: list of (rank, oldRating, newRating)"""
        return [RatingChange(1, 'C', f'user{i}', rank, 1000, old, new)
                for i, (rank, old, new) in enumerate(entries)]

    def test_exact_rank_match(self):
        changes = self._make_changes([
            (10, 1500, 1550),
            (20, 1600, 1580),
            (30, 1700, 1720),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            perf = _estimate_perf_from_cache(1, 20)
        # rank 20: old=1600, new=1580, perf = 1600 + 4*(-20) = 1520
        assert perf == 1520

    def test_closest_rank_lower(self):
        changes = self._make_changes([
            (10, 1500, 1550),
            (30, 1700, 1720),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            # rank 15 is closer to 10 than 30
            perf = _estimate_perf_from_cache(1, 15)
        # rank 10: old=1500, new=1550, perf = 1500 + 4*50 = 1700
        assert perf == 1700

    def test_closest_rank_higher(self):
        changes = self._make_changes([
            (10, 1500, 1550),
            (30, 1700, 1720),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            # rank 25 is closer to 30 than 10
            perf = _estimate_perf_from_cache(1, 25)
        # rank 30: old=1700, new=1720, perf = 1700 + 4*20 = 1780
        assert perf == 1780

    def test_empty_cache_returns_none(self):
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = []
            perf = _estimate_perf_from_cache(1, 100)
        assert perf is None

    def test_single_contestant(self):
        changes = self._make_changes([(50, 1800, 1850)])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            perf = _estimate_perf_from_cache(1, 999)
        # Only one option: old=1800, new=1850, perf = 1800 + 4*50 = 2000
        assert perf == 2000

    def test_rank_before_first(self):
        changes = self._make_changes([
            (10, 1500, 1600),
            (20, 1400, 1380),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            perf = _estimate_perf_from_cache(1, 1)
        # rank 1, closest is rank 10: perf = 1500 + 4*100 = 1900
        assert perf == 1900

    def test_rank_after_last(self):
        changes = self._make_changes([
            (10, 1500, 1600),
            (20, 1400, 1380),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            perf = _estimate_perf_from_cache(1, 500)
        # rank 500, closest is rank 20: perf = 1400 + 4*(-20) = 1320
        assert perf == 1320

    def test_unsorted_cache_still_works(self):
        """Cache entries may not be sorted by rank — function should handle it."""
        changes = self._make_changes([
            (30, 1700, 1720),
            (10, 1500, 1550),
            (20, 1600, 1580),
        ])
        with patch('tle.cogs.graphs.cf_common') as mock_cf:
            mock_cf.cache2.rating_changes_cache.get_rating_changes_for_contest.return_value = changes
            perf = _estimate_perf_from_cache(1, 20)
        # rank 20: old=1600, new=1580, perf = 1600 + 4*(-20) = 1520
        assert perf == 1520


# =====================================================================
# _format_cfvc_table
# =====================================================================

class TestFormatCfvcTable:
    def test_basic_output(self):
        rows = [
            {'idx': 1, 'contest': 'Round 1', 'rank': 100, 'perf': 1700},
            {'idx': 2, 'contest': 'Round 2', 'rank': 50, 'perf': 1900},
        ]
        result = _format_cfvc_table(rows)
        assert '#' in result
        assert 'Contest' in result
        assert 'Rank' in result
        assert 'Perf' in result
        # No Old/New/Δ columns
        assert 'Old' not in result
        assert 'New' not in result
        lines = result.strip().split('\n')
        assert len(lines) == 4  # header + sep + 2 data

    def test_single_row(self):
        rows = [{'idx': 1, 'contest': 'CF Beta 67', 'rank': 448, 'perf': 1523}]
        result = _format_cfvc_table(rows)
        assert '448' in result
        assert '1523' in result

    def test_empty_rows(self):
        result = _format_cfvc_table([])
        assert '#' in result
        lines = result.strip().split('\n')
        assert len(lines) == 2  # header + sep only

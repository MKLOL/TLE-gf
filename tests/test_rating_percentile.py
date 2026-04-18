"""Tests for the percentile → rating helper in graphs.py."""
import pytest

from tle.cogs.graphs import _rating_at_percentile


class TestRatingAtPercentile:
    def test_empty_returns_none(self):
        assert _rating_at_percentile([], 50) is None

    def test_zero_percentile_returns_min(self):
        assert _rating_at_percentile([100, 200, 300, 400], 0) == 100

    def test_hundred_percentile_returns_max(self):
        # idx clamps to n-1 for p == 100
        assert _rating_at_percentile([100, 200, 300, 400], 100) == 400

    def test_middle_percentile(self):
        # n=4, p=50 → idx = int(0.5 * 4) = 2 → ratings[2] = 300
        assert _rating_at_percentile([100, 200, 300, 400], 50) == 300

    def test_unsorted_input_is_sorted(self):
        assert _rating_at_percentile([400, 100, 300, 200], 50) == 300

    def test_high_percentile_picks_top(self):
        # n=1000 even distribution 0..999, p=99.5 → idx = int(0.995*1000)=995
        ratings = list(range(1000))
        assert _rating_at_percentile(ratings, 99.5) == 995

    def test_low_percentile(self):
        ratings = list(range(1000))
        # p=10 → idx=100 → 100
        assert _rating_at_percentile(ratings, 10) == 100

    def test_single_element(self):
        assert _rating_at_percentile([1234], 0) == 1234
        assert _rating_at_percentile([1234], 50) == 1234
        assert _rating_at_percentile([1234], 100) == 1234

    def test_duplicates(self):
        # With duplicates, the returned rating is just whatever sits at the idx
        ratings = [100, 100, 100, 200, 200]
        # n=5, p=60 → idx = 3 → 200
        assert _rating_at_percentile(ratings, 60) == 200
        # p=20 → idx = 1 → 100
        assert _rating_at_percentile(ratings, 20) == 100

    def test_negative_percentile_raises(self):
        with pytest.raises(ValueError):
            _rating_at_percentile([100, 200], -1)

    def test_over_hundred_raises(self):
        with pytest.raises(ValueError):
            _rating_at_percentile([100, 200], 101)

    def test_consistency_with_centile_formula(self):
        """The centile command computes: for rating r, percentile = 100 * bisect_left(sorted, r) / n.
        Our inverse should round-trip for percentiles that land exactly on indices."""
        import bisect
        ratings = sorted([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000])
        n = len(ratings)
        for i in range(n):
            r = ratings[i]
            p = 100 * bisect.bisect_left(ratings, r) / n
            recovered = _rating_at_percentile(ratings, p)
            # Recovered rating should equal original (for unique values)
            assert recovered == r, f'percentile {p} gave {recovered}, expected {r}'

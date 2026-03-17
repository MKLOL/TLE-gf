"""Formula behavior tests for rpoll."""
from tle.cogs.rpoll import _apply_formula


class TestApplyFormula:
    def test_sum_basic(self):
        assert _apply_formula('sum', [1200, 1800]) == 3000

    def test_sum_empty(self):
        assert _apply_formula('sum', []) == 0

    def test_sum_single(self):
        assert _apply_formula('sum', [1500]) == 1500

    def test_sum_with_zero(self):
        assert _apply_formula('sum', [0, 1500]) == 1500

    def test_exp_empty(self):
        assert _apply_formula('exp', []) == 0

    def test_exp_single_zero_rating(self):
        assert _apply_formula('exp', [0]) == 100

    def test_exp_single_800(self):
        assert _apply_formula('exp', [800]) == 400

    def test_exp_single_2000(self):
        assert _apply_formula('exp', [2000]) == 3200

    def test_exp_multiple(self):
        assert _apply_formula('exp', [800, 2000]) == 3600

    def test_exp_higher_rating_weighs_more(self):
        single_high = _apply_formula('exp', [2400])
        two_mid = _apply_formula('exp', [1200, 1200])
        assert single_high > two_mid

    def test_unknown_formula_falls_back_to_sum(self):
        assert _apply_formula('unknown', [1200, 1800]) == 3000

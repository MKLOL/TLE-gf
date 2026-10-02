"""Column fitting preserves numeric content and bounded text columns."""

import pytest

from tle.cogs._minigame_standings_image import _fit_table_columns


@pytest.mark.parametrize('cols,minimums,flexible_cols,extra_width', [
    # Wider Time, Performance, and delta cells fit by shortening Name/Handle.
    ((54, 280, 190, 90, 80, 100, 66),
     (40, 130, 100, 85, 112, 160, 92), (1, 2), 0),
    # Once both text columns reach their minimum, grow only by the shortfall.
    ((54, 200, 100, 80, 80),
     (40, 190, 95, 120, 125), (1, 2), 70),
    # No flexible column means preserving existing widths and adding deficits.
    ((40, 100, 80), (50, 80, 140), (), 70),
])
def test_column_fitting_preserves_minima_and_only_necessary_width(
        cols, minimums, flexible_cols, extra_width):
    fitted = _fit_table_columns(cols, minimums, flexible_cols)

    assert isinstance(fitted, tuple)
    assert len(fitted) == len(cols)
    assert all(actual >= minimum
               for actual, minimum in zip(fitted, minimums))
    assert all(fitted[index] >= original
               for index, original in enumerate(cols)
               if index not in flexible_cols)
    assert sum(fitted) == sum(cols) + extra_width


def test_sufficient_columns_keep_the_original_layout():
    cols = (54, 246, 174, 140, 164, 82)
    minimums = (28, 100, 90, 115, 150, 72)

    assert _fit_table_columns(cols, minimums, (1,)) == cols


def test_wide_header_fits_even_when_its_values_are_short():
    cols = (54, 300, 280, 90, 80, 56)
    # Performance's measured header plus gutter is wider than its values.
    minimums = (22, 80, 100, 70, 156, 82)

    fitted = _fit_table_columns(cols, minimums, (1, 2))

    assert fitted[4] >= 156
    assert fitted[5] >= 82
    assert fitted[1] >= minimums[1]
    assert fitted[2] >= minimums[2]
    assert sum(fitted) == sum(cols)

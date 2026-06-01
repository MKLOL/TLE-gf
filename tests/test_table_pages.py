import pytest

from tle.util import table


class TestFormatTablePages:
    def test_pages_split_by_embed_budget(self):
        style = table.Style('{:>}  {:<}')
        rows = [(i, f'Player {i:02d}') for i in range(20)]

        pages = table.format_table_pages(
            style, ('#', 'Name'), rows, max_chars=120)

        assert len(pages) > 1
        assert all(len(page) <= 120 for page in pages)
        assert all('#  Name' in page for page in pages)

    def test_flexible_column_truncates_to_fit_row_width(self):
        style = table.Style('{:>}  {:<}  {:>}')
        rows = [(1, 'x' * 200, 'perfect')]

        pages = table.format_table_pages(
            style, ('#', 'Name', 'Result'), rows,
            max_line_width=40, flexible_cols=(1,))

        lines = pages[0].splitlines()[1:-1]
        assert all(table.width(line) <= 40 for line in lines)
        assert '...' in pages[0]

    def test_raises_when_no_column_can_shrink(self):
        style = table.Style('{:>}  {:<}')
        rows = [(1, 'x' * 200)]

        with pytest.raises(table.TableTooWideError):
            table.format_table_pages(
                style, ('#', 'Name'), rows, max_line_width=40)

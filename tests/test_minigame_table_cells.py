from types import SimpleNamespace
from xml.etree import ElementTree

from tle.cogs._minigame_table_cells import (
    _PreserveSuffixText,
    _draw_table_cell,
)


class _Layout:
    def __init__(self):
        self.markup = ''
        self.width = None
        self.drawn = []

    def set_alignment(self, align):
        self.align = align

    def set_width(self, width):
        self.width = width

    def set_markup(self, markup, _length):
        self.markup = markup

    def get_pixel_size(self):
        return len(self.markup) * 10, 18


class _Context:
    def __init__(self):
        self.moves = []

    def rel_move_to(self, x, y):
        self.moves.append((x, y))


class _PangoCairo:
    @staticmethod
    def show_layout(context, layout):
        layout.drawn.append((layout.markup, layout.width))


def test_long_name_ellipsizes_before_preserved_rating_suffix():
    layout = _Layout()
    context = _Context()
    text = _PreserveSuffixText(
        '\N{CHERRIES} stop dragos and adam', ' (1984 E)')

    _draw_table_cell(
        layout, context, SimpleNamespace(SCALE=1), _PangoCairo,
        text, cell_width=230, column_margin=10, align='left')

    assert layout.drawn == [
        ('\N{CHERRIES} stop dragos and adam', 130),
        (' (1984 E)', 90),
    ]
    assert context.moves == [(130, 0), (100, 0)]


def test_short_name_and_rating_suffix_stay_adjacent():
    layout = _Layout()
    context = _Context()
    text = _PreserveSuffixText('Alice', ' (1304 CM)')

    _draw_table_cell(
        layout, context, SimpleNamespace(SCALE=1), _PangoCairo,
        text, cell_width=300, column_margin=10, align='left')

    assert layout.drawn == [('Alice (1304 CM)', 290)]
    assert context.moves == [(300, 0)]


def test_tabular_digits_keep_long_name_suffix_and_escape_markup():
    class _MarkupLayout(_Layout):
        def get_pixel_size(self):
            root = ElementTree.fromstring(f'<root>{self.markup}</root>')
            return len(''.join(root.itertext())) * 10, 18

    layout = _MarkupLayout()
    context = _Context()
    text = _PreserveSuffixText(
        '<Admin> Alice & Bob with a long name', ' (1304 CM)')

    _draw_table_cell(
        layout, context, SimpleNamespace(SCALE=1), _PangoCairo,
        text, cell_width=230, column_margin=10, align='left',
        bold=True, font_features='tnum')

    assert layout.drawn == [
        ('<span font_features="tnum"><b>&lt;Admin&gt; Alice &amp; Bob '
         'with a long name</b></span>', 120),
        ('<span font_features="tnum"><b> (1304 CM)</b></span>', 100),
    ]
    assert context.moves == [(120, 0), (110, 0)]

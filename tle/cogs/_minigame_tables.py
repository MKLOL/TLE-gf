"""Cairo/Pango table-image rendering and row builders for the minigames cog.

Result/rating leaderboard images and the per-puzzle table builders live here.
``minigames.py`` re-exports the public ones so the test suite can import them by
name and monkeypatch ``minigames_module.cairo`` / ``Pango`` / ``PangoCairo`` /
``_get_akari_puzzle_table_image``.  Because the test patches names on the
``minigames`` module, the higher-level builders call the lower-level image
function through ``_mg()`` so the patch takes effect.
"""

import datetime as dt
import html
import io
from collections import namedtuple

import cairo
import discord
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

from tle.util import codeforces_common as cf_common
from tle.util import table
from tle.util.akari_rating import rank_for_rating
from tle.cogs._minigame_common import format_duration
from tle.cogs._minigame_helpers import _mg, _safe_user_name, _safe_cf_handle


_AKARI_IMAGE_MAX_ROWS = 40
_AKARI_IMAGE_WIDTH = 900
_AKARI_IMAGE_MARGIN = 20
_AKARI_IMAGE_ROW_HEIGHT = 36
_AKARI_IMAGE_HEADER_SPACING = 1.25
_AKARI_IMAGE_COLUMN_MARGIN = 10
# Table layouts share the same Cairo renderer.  Akari keeps separate Result
# and Time columns; Queens omits Result because the day leaderboard is ranked
# by time only.  Widths sum to ``_AKARI_IMAGE_WIDTH − 2 × MARGIN`` (860).
_AKARI_RATING_COLS = (54, 300, 260, 150, 96)
_AKARI_PUZZLE_COLS = (54, 300, 260, 150, 96)
_AKARI_PUZZLE_DELTA_COLS = (54, 316, 230, 90, 90, 80)
_QUEENS_RESULTS_COLS = (54, 360, 340, 106)
_QUEENS_RESULTS_DELTA_COLS = (54, 330, 320, 90, 66)

_AKARI_IMAGE_FONTS = [
    'Noto Sans',
    'Noto Sans CJK JP',
    'Noto Sans CJK SC',
    'Noto Sans CJK TC',
    'Noto Sans CJK HK',
    'Noto Sans CJK KR',
    # Keep this in sync with the Cairo/Pango renderers in handles/training.
    # extra/fonts.conf rejects Noto Color Emoji on old Cairo; fonts-color.conf
    # allows it only after startup verifies a compatible Cairo runtime.
    'Noto Color Emoji',
    'Noto Emoji',
]
_DISCORD_GRAY = (.212, .244, .247)
_TABLE_ROW_COLORS = ((0.95, 0.95, 0.95), (0.9, 0.9, 0.9))
_BLACK = (0, 0, 0)
_SMOKE_WHITE = (250, 250, 250)

# Same per-page count as ``;handles updates`` — embed descriptions cap at 4096
# chars so 15 contest lines (~80 chars each) leave plenty of headroom.
_AKARI_HISTORY_PER_PAGE = 15

# Per-puzzle table annotation for one opted-in player: pre-puzzle rating and
# the day's delta (contest + transfer share).  Built from a single full-history
# replay so a stats request only costs one ``compute_ratings`` pass.
_PuzzlePlayerInfo = namedtuple('_PuzzlePlayerInfo', 'pre_rating delta')


def _maybe_parse_puzzle_selector(arg):
    """Resolve a single ``;akari stats`` argument into a puzzle/day selector.

    Returns ``('puzzle', n)``, ``('day', date)``, or ``None`` (the caller then
    treats ``arg`` as a member/filter).

    An explicit ``#N`` or ``p=N`` prefix always means puzzle number ``N``. This
    is the unambiguous way to look up a puzzle whose number collides with a bare
    date format -- e.g. ``#1000`` once daily puzzle numbers reach four digits,
    since a bare ``1000`` parses as the year 1000. Bare numbers keep their
    historical meaning: length 4/6/8 digit strings are dates (year / month-year
    / day-month-year), anything else is a puzzle number.
    """
    if not arg:
        return None
    explicit = None
    if arg.startswith('#'):
        explicit = arg[1:]
    elif arg[:2].lower() == 'p=':
        explicit = arg[2:]
    if explicit is not None:
        return ('puzzle', int(explicit)) if explicit.isdigit() else None
    try:
        day_start = int(cf_common.parse_date(arg))
    except (cf_common.ParamParseError, ValueError, OverflowError):
        if arg.isdigit():
            return ('puzzle', int(arg))
        return None
    day = dt.datetime.fromtimestamp(day_start).date()
    return ('day', day)


def _format_akari_result_status(row):
    """Accuracy cell for the per-puzzle table.

    Uses ``100%`` instead of the word ``perfect`` so the cell stays narrow;
    time lives in its own column next to it.
    """
    pct = 100 if row.is_perfect else int(row.accuracy)
    return f'{pct}%'


def _sort_akari_puzzle_results(rows, *, sort_key_fn=None):
    if sort_key_fn is not None:
        return sorted(rows, key=sort_key_fn)
    return sorted(
        rows,
        key=lambda row: (
            -int(bool(row.is_perfect)),
            -int(getattr(row, 'accuracy', 0)),
            int(getattr(row, 'time_seconds', 0)),
            int(getattr(row, 'message_id', 0)),
        ),
    )


def _akari_puzzle_table_rows(guild, rows, *, puzzle_info=None,
                             registrants=None, identity_fn=None,
                             sort_key_fn=None):
    """Build display rows for a per-puzzle table.

    When ``puzzle_info`` and ``registrants`` are both supplied, each opted-in
    user's name cell gets ``(<pre-rating> <tier>)`` appended and a signed delta
    cell (``+12`` / ``-8``) is included as the 5th column.  Unregistered users
    get the plain name and an empty delta (privacy: we don't surface their
    rating or its change).  Without ``puzzle_info`` the rows are 4-tuples so
    the un-annotated text/image paths stay unchanged.
    """
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    annotated = puzzle_info is not None and registrants is not None
    result = []
    for index, row in enumerate(
            _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn),
            start=1):
        name = _safe_user_name(guild, row.user_id)
        delta_cell = ''
        if (annotated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            r = round(info.pre_rating)
            name = f'{name} ({r} {rank_for_rating(r).title_abbr})'
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            index,
            name,
            identity_fn(guild, row),
            _format_akari_result_status(row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.append(delta_cell)
        result.append(tuple(cells))
    return result


def _format_akari_puzzle_table(guild, rows):
    style = table.Style('{:>}  {:<}  {:<}  {:<}  {:>}')
    t = table.Table(style)
    t += table.Header('#', 'Name', 'Handle', 'Result', 'Time')
    t += table.Line()

    for row in _akari_puzzle_table_rows(guild, rows):
        t += table.Data(*row)
    return str(t)


def _get_akari_puzzle_table_image(table_rows, *, title=None, footer=None,
                                  header=('#', 'Name', 'Handle', 'Result', 'Time'),
                                  cols=_AKARI_PUZZLE_COLS,
                                  right_align_cols=None,
                                  row_colors=None,
                                  cell_colors=None,
                                  width=_AKARI_IMAGE_WIDTH,
                                  filename='akari-results.png'):
    title_height = _AKARI_IMAGE_ROW_HEIGHT if title is not None else 0
    footer_height = _AKARI_IMAGE_ROW_HEIGHT if footer is not None else 0
    height = int(
        (len(table_rows) + _AKARI_IMAGE_HEADER_SPACING) * _AKARI_IMAGE_ROW_HEIGHT
        + title_height + footer_height + 2 * _AKARI_IMAGE_MARGIN
    )

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)
    context.set_source_rgb(*_DISCORD_GRAY)
    context.rectangle(0, 0, width, height)
    context.fill()

    layout = PangoCairo.create_layout(context)
    layout.set_font_description(
        Pango.font_description_from_string(','.join(_AKARI_IMAGE_FONTS) + ' 18'))
    layout.set_ellipsize(Pango.EllipsizeMode.END)

    def draw_bg(y, color):
        context.set_source_rgb(*color)
        context.rectangle(0, y, width, _AKARI_IMAGE_ROW_HEIGHT)
        context.fill()

    def draw_cell(text, cell_width, *, align=Pango.Alignment.LEFT, bold=False):
        text = html.escape(str(text))
        if bold:
            text = f'<b>{text}</b>'
        layout.set_width(max(1, int((cell_width - _AKARI_IMAGE_COLUMN_MARGIN) * Pango.SCALE)))
        layout.set_alignment(align)
        layout.set_markup(text, -1)
        PangoCairo.show_layout(context, layout)
        context.rel_move_to(cell_width, 0)

    def draw_line(text, y, color, *, bold=False):
        context.set_source_rgb(*(component / 255 for component in color))
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        draw_cell(
            text,
            width - 2 * _AKARI_IMAGE_MARGIN,
            bold=bold,
        )

    if right_align_cols is None:
        # Default: rank (#) and the last column (Time / Games) right-align.
        right_set = {0, len(cols) - 1}
    else:
        right_set = set(right_align_cols)

    def draw_row(row, y, color, *, bold=False, per_cell=None):
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        for i, (value, cell_width) in enumerate(zip(row, cols)):
            c = per_cell[i] if per_cell is not None else color
            context.set_source_rgb(*(component / 255 for component in c))
            align = (Pango.Alignment.RIGHT if i in right_set
                     else Pango.Alignment.LEFT)
            draw_cell(value, cell_width, align=align, bold=bold)

    y = _AKARI_IMAGE_MARGIN
    if title is not None:
        draw_line(title, y, _SMOKE_WHITE, bold=True)
        y += _AKARI_IMAGE_ROW_HEIGHT

    draw_row(header, y, _SMOKE_WHITE, bold=True)
    y += int(_AKARI_IMAGE_ROW_HEIGHT * _AKARI_IMAGE_HEADER_SPACING)

    for i, row in enumerate(table_rows):
        draw_bg(y, _TABLE_ROW_COLORS[i % 2])
        # row_colors (when provided) gives the per-row text colour as a 0–255
        # RGB tuple; cell_colors gives per-cell colours and overrides row_colors;
        # otherwise everything stays black like the puzzle tables.
        text_color = row_colors[i] if row_colors is not None else _BLACK
        per_cell = cell_colors[i] if cell_colors is not None else None
        draw_row(row, y, text_color, per_cell=per_cell)
        y += _AKARI_IMAGE_ROW_HEIGHT

    if footer is not None:
        draw_line(footer, y, _SMOKE_WHITE)

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename=filename)


def _get_akari_puzzle_table_image_file(guild, rows, title,
                                       *, puzzle_info=None, registrants=None,
                                       identity_label='Handle',
                                       identity_fn=None,
                                       sort_key_fn=None):
    rows = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    displayed = rows[:_AKARI_IMAGE_MAX_ROWS]
    displayed_rows = _akari_puzzle_table_rows(
        guild, displayed, puzzle_info=puzzle_info, registrants=registrants,
        identity_fn=identity_fn, sort_key_fn=sort_key_fn)
    annotated = puzzle_info is not None and registrants is not None
    row_colors = None
    if annotated:
        # Only opted-in users get a tier colour; the rest stay default-black.
        row_colors = [
            _akari_row_text_color(puzzle_info[row.user_id].pre_rating)
            if row.user_id in registrants and row.user_id in puzzle_info
            else _BLACK
            for row in displayed
        ]
    footer = None
    if len(rows) > len(displayed_rows):
        footer = f'Showing top {len(displayed_rows)} of {len(rows)} results'
    if annotated:
        header = ('#', 'Name', identity_label, 'Result', 'Time', '\N{INCREMENT}')
        cols = _AKARI_PUZZLE_DELTA_COLS
        # Time and Δ both carry numeric content — right-align them so values
        # line up at the column's right edge.
        right_align_cols = (0, 4, 5)
    else:
        header = ('#', 'Name', identity_label, 'Result', 'Time')
        cols = _AKARI_PUZZLE_COLS
        right_align_cols = None  # default — # and Time right
    return _mg()._get_akari_puzzle_table_image(
        displayed_rows, title=title, footer=footer,
        header=header, cols=cols,
        right_align_cols=right_align_cols, row_colors=row_colors)


def _queens_results_table_rows(guild, rows, *, puzzle_info=None,
                               registrants=None, identity_fn=None,
                               name_fn=None, sort_key_fn=None):
    if identity_fn is None:
        identity_fn = lambda _g, row: getattr(row, 'user_id', '-')
    if name_fn is None:
        name_fn = lambda g, row: _safe_user_name(g, row.user_id)
    annotated = puzzle_info is not None and registrants is not None
    result = []
    for index, row in enumerate(
            _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn),
            start=1):
        name = name_fn(guild, row)
        delta_cell = ''
        if (annotated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            r = round(info.pre_rating)
            name = f'{name} ({r} {rank_for_rating(r).title_abbr})'
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            index,
            name,
            identity_fn(guild, row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.append(delta_cell)
        result.append(tuple(cells))
    return result


def _get_queens_results_table_image_file(guild, rows, title,
                                         *, puzzle_info=None, registrants=None,
                                         identity_label='LinkedIn',
                                         identity_fn=None,
                                         name_fn=None,
                                         sort_key_fn=None):
    rows = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    displayed = rows[:_AKARI_IMAGE_MAX_ROWS]
    displayed_rows = _queens_results_table_rows(
        guild, displayed, puzzle_info=puzzle_info, registrants=registrants,
        identity_fn=identity_fn, name_fn=name_fn, sort_key_fn=sort_key_fn)
    annotated = puzzle_info is not None and registrants is not None
    row_colors = None
    if annotated:
        row_colors = [
            _akari_row_text_color(puzzle_info[row.user_id].pre_rating)
            if row.user_id in registrants and row.user_id in puzzle_info
            else _BLACK
            for row in displayed
        ]
    footer = None
    if len(rows) > len(displayed_rows):
        footer = f'Showing top {len(displayed_rows)} of {len(rows)} results'
    if annotated:
        header = ('#', 'Name', identity_label, 'Time', '\N{INCREMENT}')
        cols = _QUEENS_RESULTS_DELTA_COLS
        right_align_cols = (0, 3, 4)
    else:
        header = ('#', 'Name', identity_label, 'Time')
        cols = _QUEENS_RESULTS_COLS
        right_align_cols = None
    return _mg()._get_akari_puzzle_table_image(
        displayed_rows, title=title, footer=footer,
        header=header, cols=cols,
        right_align_cols=right_align_cols, row_colors=row_colors,
        filename='queens-results.png')


def _akari_rating_table_rows(guild, rating_rows, registrants, *,
                             mark_registered=True, identity_fn=None,
                             name_fn=None):
    """Build display rows (#, Name[✓], Handle, Rating · Rank, Games) for the leaderboard.

    ``rating`` is rounded only here for display, and the rank abbreviation
    (N/P/S/E/CM/…) is appended so scanners see the tier without a separate
    column.  When ``mark_registered`` is True, a ``✓`` after the name marks
    users who opted in via ``;mg akari register``; pass False on a registered-only
    view (the marker is redundant when every row is opted in).
    """
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    if name_fn is None:
        name_fn = lambda g, row: _safe_user_name(g, row.user_id)
    rows = []
    for index, row in enumerate(rating_rows, start=1):
        name = name_fn(guild, row)
        if mark_registered and row.user_id in registrants:
            name = f'{name} \N{CHECK MARK}'
        rating = round(row.rating)
        rank = rank_for_rating(rating)
        rows.append((
            index,
            name,
            identity_fn(guild, row),
            f'{rating} · {rank.title_abbr}',
            str(row.games),
        ))
    return rows


# History-line and ban-line formatters moved to _minigame_helpers; re-exported
# here for any caller that still imports them from this module.
from tle.cogs._minigame_helpers import (  # noqa: E402,F401
    _format_akari_history_line, _format_minigame_history_line,
    _format_akari_ban_line,
)


def _akari_row_text_color(rating):
    """Per-row text colour for the rating leaderboard image.

    Uses the rank's ``color_embed`` (the darker integer variant) so the text
    stays legible on the light-gray alternating row backgrounds — the pastel
    ``color_graph`` shades are tuned for plot fills and would wash out here.
    """
    embed = rank_for_rating(round(rating)).color_embed
    return ((embed >> 16) & 0xFF, (embed >> 8) & 0xFF, embed & 0xFF)


def _get_akari_rating_table_image_file(guild, rating_rows, registrants,
                                       *, title='Daily Akari Ratings',
                                       mark_registered=True,
                                       identity_label='Handle',
                                       identity_fn=None,
                                       name_fn=None):
    displayed = rating_rows[:_AKARI_IMAGE_MAX_ROWS]
    table_rows = _akari_rating_table_rows(
        guild, displayed, registrants, mark_registered=mark_registered,
        identity_fn=identity_fn, name_fn=name_fn)
    row_colors = [_akari_row_text_color(row.rating) for row in displayed]
    footer = None
    if len(rating_rows) > len(table_rows):
        footer = f'Showing top {len(table_rows)} of {len(rating_rows)} rated players'
    return _mg()._get_akari_puzzle_table_image(
        table_rows, title=title, footer=footer,
        header=('#', 'Name', identity_label, 'Rating', 'Games'),
        cols=_AKARI_RATING_COLS,
        row_colors=row_colors)

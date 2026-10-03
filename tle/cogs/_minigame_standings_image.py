"""Shared minigame tables with measured columns and protected labels."""

import html
import io
import math

import cairo
import discord
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

from tle.cogs._minigame_table_cells import _draw_table_cell


_MARGIN = 20
_ROW_HEIGHT = 40
_HEADER_HEIGHT = 40
_BACKGROUND = (54, 62, 63)
_HEADER_TEXT = (250, 250, 250)
_SECONDARY_TEXT = (220, 224, 224)
_ROW_BACKGROUNDS = ((245, 245, 245), (235, 235, 235))


def _fit_table_columns(cols, minimums, flexible_cols):
    """Fit metrics first, borrowing text space before widening the image."""
    widths = [max(width, minimum) for width, minimum in zip(cols, minimums)]
    overflow = sum(widths) - sum(cols)
    spare = sum(widths[i] - minimums[i] for i in flexible_cols)
    for i in flexible_cols:
        capacity = widths[i] - minimums[i]
        reduction = (min(capacity, math.ceil(overflow * capacity / spare))
                     if spare else 0)
        widths[i] -= reduction
        overflow -= reduction
        spare -= capacity
    return tuple(widths)


def _get_standings_table_image(
        table_rows, *, title, footer, header, cols, cell_colors, row_colors,
        right_align_cols, column_margins, flexible_cols, fonts, width, filename):
    """Keep numeric fields intact while giving names the remaining space."""
    right_align_cols = ({0, len(cols) - 1} if right_align_cols is None
                        else set(right_align_cols))
    margins = {0: 16, len(cols) - 1: 0, **(column_margins or {})}
    # Measure wrapped headings first so date/filter qualifiers never disappear.
    measure_surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, 1, 1)
    layout = PangoCairo.create_layout(cairo.Context(measure_surface))
    font = Pango.font_description_from_string(','.join(fonts))

    def prepare(text, cell_width, size, weight=400, *, right=False, wrap=False):
        font.set_absolute_size(size * Pango.SCALE)
        layout.set_font_description(font)
        layout.set_width(-1 if cell_width < 0 else int(cell_width * Pango.SCALE))
        layout.set_alignment(
            Pango.Alignment.RIGHT if right else Pango.Alignment.LEFT)
        layout.set_wrap(Pango.WrapMode.WORD_CHAR)
        layout.set_single_paragraph_mode(not wrap)
        layout.set_ellipsize(
            Pango.EllipsizeMode.NONE if wrap else Pango.EllipsizeMode.END)
        # Equal-width digits make Score and delta easier to compare vertically.
        layout.set_markup(
            f'<span font_features="tnum" weight="{weight}">'
            f'{html.escape(str(text))}</span>', -1)
        return layout.get_pixel_extents()[1]

    minimums = []
    for i, label in enumerate(header):
        needed = prepare(label, -1, 20, 600).width
        if i in flexible_cols:
            needed = max(needed, 160 if i == 1 else 128)
            for row in table_rows:
                suffix = getattr(row[i], 'preserved_suffix', None)
                if suffix:
                    needed = max(needed, prepare(suffix, -1, 24).width + 96)
        else:
            for row in table_rows:
                needed = max(needed, prepare(row[i], -1, 24).width)
        minimums.append(needed + 2 + margins.get(i, 24))
    cols = _fit_table_columns(cols, minimums, flexible_cols)
    width = max(width, sum(cols) + 2 * _MARGIN)
    content_width = width - 2 * _MARGIN

    heading_height = (prepare(title, content_width, 24, 700, wrap=True).height
                      if title else 0)
    title_height = 16 + heading_height + 8 if title else 16
    footer_height = (prepare(footer, content_width, 18, wrap=True).height + 8
                     if footer else 0)
    height = (title_height + _HEADER_HEIGHT + len(table_rows) * _ROW_HEIGHT
              + footer_height + _MARGIN)
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)

    def rectangle(y, h, color):
        context.set_source_rgb(*(channel / 255 for channel in color))
        context.rectangle(0, y, width, h)
        context.fill()

    def draw(text, x, y, w, h, color, *, size=24, weight=400,
             right=False, wrap=False):
        logical = prepare(text, w, size, weight, right=right, wrap=wrap)
        context.set_source_rgb(*(channel / 255 for channel in color))
        context.move_to(x, y + (h - logical.height) // 2 - logical.y)
        PangoCairo.update_layout(context, layout)
        PangoCairo.show_layout(context, layout)

    def draw_row(row, y, *, colors, is_header=False):
        x = _MARGIN
        for i, (value, cell_width) in enumerate(zip(row, cols)):
            gap = margins.get(i, 24)
            right = i in right_align_cols
            if is_header:
                draw(value, x, y, cell_width - gap, _HEADER_HEIGHT, colors[i],
                     size=20, weight=600, right=right)
            else:
                logical = prepare(value, cell_width - gap, 24, right=right)
                context.set_source_rgb(
                    *(channel / 255 for channel in colors[i]))
                context.move_to(
                    x, y + (_ROW_HEIGHT - logical.height) // 2 - logical.y)
                PangoCairo.update_layout(context, layout)
                _draw_table_cell(
                    layout, context, Pango, PangoCairo, value, cell_width,
                    gap, Pango.Alignment.RIGHT if right else Pango.Alignment.LEFT,
                    font_features='tnum')
            x += cell_width

    rectangle(0, height, _BACKGROUND)
    if title:
        draw(title, _MARGIN, 16, content_width, heading_height,
             _HEADER_TEXT, size=24, weight=700, wrap=True)
    draw_row(header, title_height, colors=[_HEADER_TEXT] * len(cols),
             is_header=True)
    y = title_height + _HEADER_HEIGHT
    for index, row in enumerate(table_rows):
        rectangle(y, _ROW_HEIGHT, _ROW_BACKGROUNDS[index % 2])
        color = row_colors[index] if row_colors is not None else (0, 0, 0)
        colors = (cell_colors[index] if cell_colors is not None
                  else [color] * len(cols))
        draw_row(row, y, colors=colors)
        y += _ROW_HEIGHT
    if footer:
        draw(footer, _MARGIN, y + 8, content_width, footer_height - 8,
             _SECONDARY_TEXT, size=18, wrap=True)

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename=filename)

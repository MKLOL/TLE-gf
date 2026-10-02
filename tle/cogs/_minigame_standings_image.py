"""Light table card for annotated weekly standings, preserving cell colors."""

import html
import io
import math

import cairo
import discord
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo


_MARGIN = 20
_ROW_HEIGHT = 44
_HEADER_HEIGHT = 44
_TITLE_BG = (37, 43, 47)
_TITLE_TEXT = (248, 250, 252)
_SUBTITLE_TEXT = (185, 197, 204)
_HEADER_BG = (239, 242, 245)
_HEADER_TEXT = (75, 87, 98)
_ROW_BACKGROUNDS = ((255, 255, 255), (246, 248, 250))
_RULE = (214, 221, 227)


def _get_standings_table_image(
        table_rows, *, title, footer, header, cols, cell_colors,
        right_align_cols, column_margins, fonts, width, filename):
    """Render existing row data with a quieter hierarchy and fixed gutters."""
    content_width = width - 2 * _MARGIN
    # Measure wrapped headings first so date/filter qualifiers never disappear.
    measure_surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, 1, 1)
    layout = PangoCairo.create_layout(cairo.Context(measure_surface))
    font = Pango.font_description_from_string(','.join(fonts))

    def prepare(text, cell_width, size, weight=400, *, right=False, wrap=False):
        font.set_absolute_size(size * Pango.SCALE)
        layout.set_font_description(font)
        layout.set_width(int(cell_width * Pango.SCALE))
        layout.set_alignment(
            Pango.Alignment.RIGHT if right else Pango.Alignment.LEFT)
        layout.set_wrap(Pango.WrapMode.WORD_CHAR)
        layout.set_ellipsize(
            Pango.EllipsizeMode.NONE if wrap else Pango.EllipsizeMode.END)
        # Equal-width digits make Score and delta easier to compare vertically.
        layout.set_markup(
            f'<span font_features="tnum" weight="{weight}">'
            f'{html.escape(str(text))}</span>', -1)
        return layout.get_pixel_extents()[1]

    heading, _, subtitle = (title or '').partition(' · ')
    heading_height = (prepare(heading, content_width, 28, 700, wrap=True).height
                      if title else 0)
    subtitle_height = (prepare(subtitle, content_width, 18, wrap=True).height
                       if subtitle else 0)
    title_height = (2 * _MARGIN + heading_height
                    + (8 + subtitle_height if subtitle else 0)) if title else 0
    footer_height = (prepare(footer, content_width, 16, wrap=True).height + 24
                     if footer else 0)
    height = (title_height + _HEADER_HEIGHT + len(table_rows) * _ROW_HEIGHT
              + footer_height + 16)
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)

    # Transparent corners keep the card clean on either Discord theme.
    radius = 12
    for x, y, start in ((width - radius, radius, -math.pi / 2),
                        (width - radius, height - radius, 0),
                        (radius, height - radius, math.pi / 2),
                        (radius, radius, math.pi)):
        context.arc(x, y, radius, start, start + math.pi / 2)
    context.close_path()
    context.clip()

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

    def draw_row(row, y, *, colors, size=24, weight=400):
        x = _MARGIN
        for i, (value, cell_width) in enumerate(zip(row, cols)):
            gap = (column_margins or {}).get(i, 16)
            draw(value, x, y, cell_width - gap, _ROW_HEIGHT, colors[i],
                 size=size, weight=weight, right=i in right_align_cols)
            x += cell_width

    rectangle(0, height, _ROW_BACKGROUNDS[0])
    if title:
        rectangle(0, title_height, _TITLE_BG)
        draw(heading, _MARGIN, _MARGIN, content_width, heading_height,
             _TITLE_TEXT, size=28, weight=700, wrap=True)
        if subtitle:
            draw(subtitle, _MARGIN, _MARGIN + heading_height + 8,
                 content_width, subtitle_height, _SUBTITLE_TEXT, size=18, wrap=True)
    rectangle(title_height, _HEADER_HEIGHT, _HEADER_BG)
    draw_row(header, title_height, colors=[_HEADER_TEXT] * len(cols),
             size=20, weight=600)
    rectangle(title_height + _HEADER_HEIGHT - 1, 1, _RULE)
    y = title_height + _HEADER_HEIGHT
    for index, row in enumerate(table_rows):
        rectangle(y, _ROW_HEIGHT, _ROW_BACKGROUNDS[index % 2])
        draw_row(row, y, colors=cell_colors[index])
        y += _ROW_HEIGHT
    if footer:
        rectangle(y, 1, _RULE)
        draw(footer, _MARGIN, y + 12, content_width, footer_height - 24,
             _HEADER_TEXT, size=16, wrap=True)

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename=filename)

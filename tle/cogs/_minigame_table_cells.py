"""Specialized text cells used by the minigame table-image renderer."""

import html


class _PreserveSuffixText(str):
    """String whose trailing annotation must survive width ellipsizing."""

    def __new__(cls, prefix, suffix):
        value = super().__new__(cls, f'{prefix}{suffix}')
        value.preserved_prefix = str(prefix)
        value.preserved_suffix = str(suffix)
        return value


def _cell_markup(text, bold):
    markup = html.escape(str(text))
    return f'<b>{markup}</b>' if bold else markup


def _draw_table_cell(layout, context, pango, pango_cairo, text, cell_width,
                     column_margin, align, bold=False):
    """Draw one cell, reserving room for a protected trailing annotation."""
    available_width = max(1, cell_width - column_margin)
    suffix = getattr(text, 'preserved_suffix', None)
    layout.set_alignment(align)
    if suffix is not None:
        layout.set_width(-1)
        layout.set_markup(_cell_markup(text, bold), -1)
        full_width, _ = layout.get_pixel_size()
        if full_width > available_width:
            layout.set_markup(_cell_markup(suffix, bold), -1)
            suffix_width, _ = layout.get_pixel_size()
            prefix_width = max(1, available_width - suffix_width)
            layout.set_width(int(prefix_width * pango.SCALE))
            layout.set_markup(
                _cell_markup(text.preserved_prefix, bold), -1)
            pango_cairo.show_layout(context, layout)
            context.rel_move_to(prefix_width, 0)
            layout.set_width(int(suffix_width * pango.SCALE))
            layout.set_markup(_cell_markup(suffix, bold), -1)
            pango_cairo.show_layout(context, layout)
            context.rel_move_to(cell_width - prefix_width, 0)
            return

    layout.set_width(int(available_width * pango.SCALE))
    layout.set_markup(_cell_markup(text, bold), -1)
    pango_cairo.show_layout(context, layout)
    context.rel_move_to(cell_width, 0)

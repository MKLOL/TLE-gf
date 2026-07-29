"""Emoji-aware player-name rendering for minigame dashboards.

Matplotlib's Agg text renderer does not shape multi-codepoint emoji sequences.
Pango already backs the bot's table images and does, so dashboard player names
are rendered to a small transparent PNG with Pango and placed on the axes as a
Matplotlib offset image.  Imports stay lazy so pure model tests do not need the
native Cairo/Pango stack.
"""

import io
import os
import unicodedata

from tle import constants


_FONT_FAMILIES = (
    'Noto Sans',
    'Noto Sans CJK JP',
    'Noto Sans CJK SC',
    'Noto Sans CJK TC',
    'Noto Sans CJK HK',
    'Noto Sans CJK KR',
    'Noto Color Emoji',
    'Noto Emoji',
)
_EMOJI_FORMAT_RANGES = (
    (0xE0020, 0xE007F),  # Emoji tag sequences, including subdivision flags.
)
_EMOJI_FORMAT_CHARS = frozenset(('\u200c', '\u200d'))


def safe_player_name(value):
    """Return a single-line player name without discarding Unicode glyphs.

    Emoji joiners and tag characters must survive for Pango to shape family,
    profession, skin-tone, and flag sequences.  Line separators, bidi controls,
    and other layout-affecting control characters are removed; whitespace is
    collapsed so a Discord display name cannot create an extra dashboard line.
    """
    chars = []
    pending_space = False
    for char in str(value):
        category = unicodedata.category(char)
        if char.isspace() or category in ('Zl', 'Zp'):
            pending_space = bool(chars)
            continue
        if category in ('Cc', 'Cs'):
            continue
        if category == 'Cf' and not _is_emoji_format_char(char):
            continue
        if pending_space:
            chars.append(' ')
            pending_space = False
        chars.append(char)
    return ''.join(chars).strip() or 'Player'


def draw_player_name(ax, value, *, xy=(0, 0), color='#172033',
                     fontsize=24, max_width_px=760, fontweight='normal',
                     transform=None, zorder=5):
    """Draw an emoji-aware, single-line name and return its Matplotlib artist.

    ``max_width_px`` is enforced by Pango's end ellipsis after shaping, so it
    cannot split a grapheme cluster.  If native rendering or image placement
    is unavailable, this falls back to ``Axes.text`` with the complete
    sanitized name and the bundled fonts registered with Matplotlib.
    """
    name = safe_player_name(value)
    transform = transform or ax.transAxes
    try:
        dpi = float(ax.figure.dpi)
        png = _render_name_png(
            name,
            color=color,
            fontsize=fontsize,
            max_width_px=max_width_px,
            fontweight=fontweight,
            dpi=dpi,
        )
        return _place_name_png(
            ax, png, xy=xy, transform=transform, zorder=zorder)
    except Exception:
        return _draw_text_fallback(
            ax,
            name,
            xy=xy,
            color=color,
            fontsize=fontsize,
            fontweight=fontweight,
            transform=transform,
            zorder=zorder,
        )


def _is_emoji_format_char(char):
    if char in _EMOJI_FORMAT_CHARS:
        return True
    codepoint = ord(char)
    return any(start <= codepoint <= end
               for start, end in _EMOJI_FORMAT_RANGES)


def _font_description(Pango, fontsize, fontweight):
    description = Pango.font_description_from_string(
        ','.join(_FONT_FAMILIES))
    description.set_size(max(1, round(float(fontsize) * Pango.SCALE)))
    if str(fontweight).lower() in ('bold', 'heavy', 'semibold', 'demibold'):
        description.set_weight(Pango.Weight.BOLD)
    return description


def _make_layout(context, text, *, fontsize, max_width_px, fontweight, dpi,
                 Pango, PangoCairo):
    layout = PangoCairo.create_layout(context)
    pango_context = layout.get_context()
    PangoCairo.context_set_resolution(pango_context, dpi)
    layout.set_font_description(
        _font_description(Pango, fontsize, fontweight))
    layout.set_single_paragraph_mode(True)
    layout.set_ellipsize(Pango.EllipsizeMode.END)
    layout.set_width(max(1, round(max_width_px * Pango.SCALE)))
    layout.set_text(text, -1)
    PangoCairo.update_layout(context, layout)
    return layout


def _layout_bounds(layout, padding):
    ink, logical = layout.get_pixel_extents()
    left = min(ink.x, logical.x)
    top = min(ink.y, logical.y)
    right = max(ink.x + ink.width, logical.x + logical.width)
    bottom = max(ink.y + ink.height, logical.y + logical.height)
    width = max(1, right - left + 2 * padding)
    height = max(1, bottom - top + 2 * padding)
    return left, top, width, height


def _render_name_png(text, *, color, fontsize, max_width_px, fontweight, dpi):
    import cairo
    import gi

    gi.require_version('Pango', '1.0')
    gi.require_version('PangoCairo', '1.0')
    from gi.repository import Pango, PangoCairo

    probe = cairo.ImageSurface(cairo.FORMAT_ARGB32, 1, 1)
    probe_context = cairo.Context(probe)
    layout = _make_layout(
        probe_context,
        text,
        fontsize=fontsize,
        max_width_px=max_width_px,
        fontweight=fontweight,
        dpi=dpi,
        Pango=Pango,
        PangoCairo=PangoCairo,
    )
    padding = 2
    left, top, width, height = _layout_bounds(layout, padding)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)
    layout = _make_layout(
        context,
        text,
        fontsize=fontsize,
        max_width_px=max_width_px,
        fontweight=fontweight,
        dpi=dpi,
        Pango=Pango,
        PangoCairo=PangoCairo,
    )
    context.translate(padding - left, padding - top)
    context.set_source_rgba(*_rgba(color))
    PangoCairo.show_layout(context, layout)

    output = io.BytesIO()
    surface.write_to_png(output)
    output.seek(0)
    return output


def _rgba(value):
    if isinstance(value, str) and value.startswith('#'):
        raw = value[1:]
        if len(raw) in (3, 4):
            raw = ''.join(char * 2 for char in raw)
        if len(raw) in (6, 8):
            channels = [
                int(raw[index:index + 2], 16) / 255
                for index in range(0, len(raw), 2)
            ]
            if len(channels) == 3:
                channels.append(1.0)
            return tuple(channels)
    channels = tuple(value)
    if len(channels) == 3:
        channels += (1.0,)
    if len(channels) != 4:
        raise ValueError('Expected an RGB/RGBA color')
    if any(channel > 1 for channel in channels):
        channels = tuple(channel / 255 for channel in channels)
    return channels


def _place_name_png(ax, png, *, xy, transform, zorder):
    from matplotlib.image import imread
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage

    image = imread(png, format='png')
    box = OffsetImage(image, zoom=1, dpi_cor=False)
    artist = AnnotationBbox(
        box,
        xy,
        xycoords=transform,
        box_alignment=(0, 0),
        frameon=False,
        pad=0,
        annotation_clip=False,
        zorder=zorder,
    )
    ax.add_artist(artist)
    return artist


def _draw_text_fallback(ax, name, *, xy, color, fontsize, fontweight,
                        transform, zorder):
    families = _matplotlib_font_families()
    return ax.text(
        xy[0],
        xy[1],
        name,
        transform=transform,
        color=color,
        fontsize=fontsize,
        fontweight=fontweight,
        fontfamily=families,
        zorder=zorder,
    )


def _matplotlib_font_families():
    from matplotlib import font_manager

    families = []
    paths = (
        constants.NOTO_SANS_CJK_REGULAR_FONT_PATH,
        constants.NOTO_EMOJI_FONT_PATH,
    )
    for path in paths:
        if not os.path.isfile(path):
            continue
        try:
            font_manager.fontManager.addfont(path)
            family = font_manager.FontProperties(fname=path).get_name()
        except (AttributeError, OSError, RuntimeError, ValueError):
            continue
        if family not in families:
            families.append(family)
    families.append('DejaVu Sans')
    return families

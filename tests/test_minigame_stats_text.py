"""Unicode and renderer-boundary tests for dashboard player names."""

from types import SimpleNamespace

import pytest

from tle.cogs import _minigame_stats_text as stats_text


_ENGLAND_FLAG = (
    '\U0001F3F4'
    '\U000E0067'
    '\U000E0062'
    '\U000E0065'
    '\U000E006E'
    '\U000E0067'
    '\U000E007F'
)


@pytest.mark.parametrize('value', [
    '',
    ' \n\t\r ',
    '\x00\x01\u202e',
    '\ud800',
])
def test_safe_player_name_falls_back_for_blank_or_control_only_input(value):
    assert stats_text.safe_player_name(value) == 'Player'


def test_safe_player_name_collapses_lines_and_removes_layout_controls():
    value = 'Alice\n\tLight\u2028Solver\u202e\x00'

    assert stats_text.safe_player_name(value) == 'Alice Light Solver'


@pytest.mark.parametrize('value', [
    'Róbert 👑🧩',
    '光 💡',
    '👑🧩',
    'Dev 👩🏽\u200d💻',
    'Weather ☀\ufe0f',
])
def test_safe_player_name_preserves_unicode_and_complete_emoji_sequences(value):
    assert stats_text.safe_player_name(value) == value


def test_safe_player_name_preserves_subdivision_flag_tag_sequence():
    value = f'England {_ENGLAND_FLAG}'

    safe = stats_text.safe_player_name(value)

    assert safe == value
    assert [ord(char) for char in safe[-7:]] == [
        0x1F3F4,
        0xE0067,
        0xE0062,
        0xE0065,
        0xE006E,
        0xE0067,
        0xE007F,
    ]


def test_pango_font_description_uses_cjk_and_emoji_fallbacks():
    captured = {}

    class Description:
        def set_size(self, value):
            captured['size'] = value

        def set_weight(self, value):
            captured['weight'] = value

    class Pango:
        SCALE = 1000
        Weight = SimpleNamespace(BOLD='bold')

        @staticmethod
        def font_description_from_string(value):
            captured['families'] = value
            return Description()

    stats_text._font_description(Pango, 24, 'bold')

    families = captured['families'].split(',')
    assert 'Noto Sans CJK JP' in families
    assert families[-2:] == ['Noto Color Emoji', 'Noto Emoji']
    assert captured['size'] == 24000
    assert captured['weight'] == 'bold'


def test_draw_player_name_passes_full_unicode_name_to_png_backend(monkeypatch):
    name = f'光 Dev 👩🏽\u200d💻 {_ENGLAND_FLAG}'
    transform = object()
    artist = object()
    calls = {}

    def render(text, **kwargs):
        calls['render'] = (text, kwargs)
        return 'png'

    def place(ax, png, **kwargs):
        calls['place'] = (ax, png, kwargs)
        return artist

    monkeypatch.setattr(stats_text, '_render_name_png', render)
    monkeypatch.setattr(stats_text, '_place_name_png', place)
    ax = SimpleNamespace(
        figure=SimpleNamespace(dpi=144),
        transAxes=object(),
    )

    result = stats_text.draw_player_name(
        ax,
        name,
        xy=(.1, .2),
        color='#123456',
        fontsize=27,
        max_width_px=640,
        fontweight='bold',
        transform=transform,
        zorder=8,
    )

    assert result is artist
    assert calls['render'] == (name, {
        'color': '#123456',
        'fontsize': 27,
        'max_width_px': 640,
        'fontweight': 'bold',
        'dpi': 144,
    })
    assert calls['place'] == (
        ax,
        'png',
        {'xy': (.1, .2), 'transform': transform, 'zorder': 8},
    )


def test_draw_player_name_fallback_receives_full_unicode_name(monkeypatch):
    name = f'Róbert 👑 Dev 👩🏽\u200d💻 {_ENGLAND_FLAG}'
    transform = object()
    fallback_artist = object()
    captured = {}

    def unavailable(*_args, **_kwargs):
        raise RuntimeError('Pango unavailable')

    def fallback(ax, text, **kwargs):
        captured['fallback'] = (ax, text, kwargs)
        return fallback_artist

    monkeypatch.setattr(stats_text, '_render_name_png', unavailable)
    monkeypatch.setattr(stats_text, '_draw_text_fallback', fallback)
    ax = SimpleNamespace(
        figure=SimpleNamespace(dpi=96),
        transAxes=object(),
    )

    result = stats_text.draw_player_name(
        ax,
        name,
        xy=(.25, .5),
        color='#abcdef',
        fontsize=21,
        fontweight='normal',
        transform=transform,
        zorder=4,
    )

    assert result is fallback_artist
    assert captured['fallback'] == (
        ax,
        name,
        {
            'xy': (.25, .5),
            'color': '#abcdef',
            'fontsize': 21,
            'fontweight': 'normal',
            'transform': transform,
            'zorder': 4,
        },
    )

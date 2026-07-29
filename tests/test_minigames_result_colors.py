"""Per-cell rank colors for Akari and Queens result tables."""

from tle.cogs import minigames as minigames_module

from tests.minigames_test_utils import (
    _FakeDiscordMember,
    _FakeGuild,
    _row,
)


def _capture_renderer(monkeypatch):
    captured = {}

    def render(rows, **kwargs):
        captured.update(rows=rows, **kwargs)
        return object()

    monkeypatch.setattr(
        minigames_module, '_get_akari_puzzle_table_image', render)
    return captured


def _result_fixtures():
    guild = _FakeGuild(1, members=[
        _FakeDiscordMember(10, 'alice', 'Alice'),
        _FakeDiscordMember(20, 'bob', 'Bob'),
    ])
    rows = [
        _row(1, 10, '2026-03-26', True, 60, 100, 445),
        _row(2, 20, '2026-03-26', True, 70, 100, 445),
    ]
    info = {
        '10': minigames_module._PuzzlePlayerInfo(
            pre_rating=1200.0, delta=12.0, performance=1510.0),
        '20': minigames_module._PuzzlePlayerInfo(
            pre_rating=1300.0, delta=0.0, performance=None),
    }
    return guild, rows, info


def test_akari_perf_uses_performance_rank_color(monkeypatch):
    captured = _capture_renderer(monkeypatch)
    guild, rows, info = _result_fixtures()

    minigames_module._get_akari_puzzle_table_image_file(
        guild, rows, 'Akari Results', puzzle_info=info,
        registrants={'10', '20'}, identity_fn=lambda _guild, row: row.user_id)

    blue = minigames_module._akari_row_text_color(1200)
    purple = minigames_module._akari_row_text_color(1300)
    orange = minigames_module._akari_row_text_color(1510)
    assert orange != blue
    assert captured['row_colors'] == [blue, purple]
    assert captured['cell_colors'] == [
        (blue, blue, blue, blue, blue, orange, blue),
        (purple, purple, purple, purple, purple, purple, purple),
    ]


def test_queens_perf_uses_performance_rank_color(monkeypatch):
    captured = _capture_renderer(monkeypatch)
    guild, rows, info = _result_fixtures()

    minigames_module._get_queens_results_table_image_file(
        guild, rows, 'Queens Results', puzzle_info=info,
        registrants={'10', '20'}, identity_fn=lambda _guild, row: row.user_id)

    blue = minigames_module._akari_row_text_color(1200)
    purple = minigames_module._akari_row_text_color(1300)
    orange = minigames_module._akari_row_text_color(1510)
    assert orange != blue
    assert captured['row_colors'] == [blue, purple]
    assert captured['cell_colors'] == [
        (blue, blue, blue, blue, orange, blue),
        (purple, purple, purple, purple, purple, purple),
    ]

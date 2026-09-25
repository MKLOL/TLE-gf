"""Queens displays its own top rank bands; Akari and Tango keep the shared ones."""

from tle.util.akari_ranks import AKARI_RANKS, QUEENS_RANKS, rank_for_rating
from tle.cogs._minigame_helpers import (
    _format_minigame_history_line, game_ranks)
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_tango import TANGO_GAME
from tle.util.akari_rating import HistoryPoint


def test_queens_ranks_pack_top_tiers():
    assert rank_for_rating(1650, QUEENS_RANKS).title_abbr == 'GM'
    assert rank_for_rating(1700, QUEENS_RANKS).title_abbr == 'IGM'
    assert rank_for_rating(1799, QUEENS_RANKS).title_abbr == 'IGM'
    assert rank_for_rating(1800, QUEENS_RANKS).title_abbr == 'LGM'
    assert rank_for_rating(5000, QUEENS_RANKS).title_abbr == 'LGM'
    # Everything below GM is unchanged.
    assert [r for r in QUEENS_RANKS if r.high <= 1600] == \
        [r for r in AKARI_RANKS if r.high <= 1600]
    # Bands stay contiguous.
    for lo, hi in zip(QUEENS_RANKS, QUEENS_RANKS[1:]):
        assert lo.high == hi.low


def test_default_table_is_unchanged():
    assert rank_for_rating(1700).title_abbr == 'GM'
    assert rank_for_rating(1800).title_abbr == 'IGM'
    assert rank_for_rating(2000).title_abbr == 'LGM'


def test_only_queens_overrides_ranks():
    assert game_ranks(QUEENS_GAME) is QUEENS_RANKS
    assert game_ranks(TANGO_GAME) is None
    assert game_ranks(None) is None


def test_history_line_uses_game_ranks():
    point = HistoryPoint(
        puzzle_number=1, puzzle_date='2026-09-01', rating=1750.0,
        delta=10.0, performance=1800.0, is_perfect=True, accuracy=100,
        time_seconds=30, is_decay=False)
    assert '(IGM)' in _format_minigame_history_line(
        point, ranks=QUEENS_RANKS)
    assert '(GM)' in _format_minigame_history_line(point)

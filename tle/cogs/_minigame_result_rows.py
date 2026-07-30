"""Pure row builders for Akari and Queens per-puzzle result tables."""

from collections import namedtuple

from tle.util import table
from tle.util.akari_rating import rank_for_rating
from tle.cogs._minigame_common import format_duration
from tle.cogs._minigame_helpers import _safe_user_name, _safe_cf_handle
from tle.cogs._minigame_table_cells import _PreserveSuffixText


_PuzzlePlayerInfo = namedtuple(
    '_PuzzlePlayerInfo',
    'pre_rating delta performance',
    defaults=(None,),
)


def _format_akari_result_status(row):
    """Compact accuracy cell for the per-puzzle table."""
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


def _akari_result_rank_key(row):
    return (
        -int(bool(row.is_perfect)),
        -int(getattr(row, 'accuracy', 0)),
        int(getattr(row, 'time_seconds', 0)),
    )


def _akari_results_time_sort_key(row):
    """Display Akari results by time, preserving stable tie-breakers."""
    return (
        int(getattr(row, 'time_seconds', 0)),
        -int(bool(getattr(row, 'is_perfect', False))),
        -int(getattr(row, 'accuracy', 0)),
        int(getattr(row, 'message_id', 0)),
    )


def _akari_results_time_rank_key(row):
    return int(getattr(row, 'time_seconds', 0))


def _queens_result_rank_key(row):
    return int(getattr(row, 'time_seconds', 0))


def _queens_result_sort_key(row):
    return (
        int(getattr(row, 'time_seconds', 0)),
        int(getattr(row, 'message_id', 0)),
    )


def _ranked_result_rows(rows, *, sort_key_fn=None, rank_key_fn=None):
    """Return ``(competition_rank, row)`` pairs in display order."""
    ordered = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    previous = object()
    rank = 0
    ranked = []
    for position, row in enumerate(ordered, start=1):
        key = rank_key_fn(row)
        if position == 1 or key != previous:
            rank = position
            previous = key
        ranked.append((rank, row))
    return ranked


def _performance_cell(info):
    return '\N{EM DASH}' if info.performance is None else str(round(info.performance))


def _akari_puzzle_table_rows(guild, rows, *, puzzle_info=None,
                             registrants=None, identity_fn=None,
                             sort_key_fn=None, rank_key_fn=None):
    """Build display rows, including private rating annotations when allowed."""
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    if rank_key_fn is None:
        rank_key_fn = _akari_result_rank_key
    annotated = puzzle_info is not None and registrants is not None
    result = []
    for rank, row in _ranked_result_rows(
            rows, sort_key_fn=sort_key_fn, rank_key_fn=rank_key_fn):
        name = _safe_user_name(guild, row.user_id)
        performance_cell = ''
        delta_cell = ''
        if (annotated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            rating = round(info.pre_rating)
            name = _PreserveSuffixText(
                name, f' ({rating} {rank_for_rating(rating).title_abbr})')
            performance_cell = _performance_cell(info)
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            rank,
            name,
            identity_fn(guild, row),
            _format_akari_result_status(row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.extend((performance_cell, delta_cell))
        result.append(tuple(cells))
    return result


def _format_akari_puzzle_table(guild, rows):
    style = table.Style('{:>}  {:<}  {:<}  {:<}  {:>}')
    result_table = table.Table(style)
    result_table += table.Header('#', 'Name', 'Handle', 'Result', 'Time')
    result_table += table.Line()
    for row in _akari_puzzle_table_rows(guild, rows):
        result_table += table.Data(*row)
    return str(result_table)


def _queens_results_table_rows(guild, rows, *, puzzle_info=None,
                               registrants=None, identity_fn=None,
                               name_fn=None, sort_key_fn=None,
                               rank_key_fn=None, unrated_keys=None):
    if identity_fn is None:
        identity_fn = lambda _g, row: getattr(row, 'user_id', '-')
    if name_fn is None:
        name_fn = lambda g, row: _safe_user_name(g, row.user_id)
    if sort_key_fn is None:
        sort_key_fn = _queens_result_sort_key
    if rank_key_fn is None:
        rank_key_fn = _queens_result_rank_key
    unrated_keys = {
        (str(user_id), int(puzzle_number))
        for user_id, puzzle_number in (unrated_keys or ())
    }
    annotated = puzzle_info is not None and registrants is not None
    ordered = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    rated_ranks = {
        (str(row.user_id), int(row.puzzle_number)): rank
        for rank, row in _ranked_result_rows(
            [
                row for row in ordered
                if (str(row.user_id), int(row.puzzle_number))
                not in unrated_keys
            ],
            sort_key_fn=sort_key_fn,
            rank_key_fn=rank_key_fn,
        )
    }
    result = []
    for row in ordered:
        row_key = (str(row.user_id), int(row.puzzle_number))
        is_unrated = row_key in unrated_keys
        rank = '\N{EM DASH}' if is_unrated else rated_ranks[row_key]
        name = name_fn(guild, row)
        if is_unrated:
            name = _PreserveSuffixText(name, ' (Unrated)')
        performance_cell = ''
        delta_cell = ''
        if (annotated
                and not is_unrated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            rating = round(info.pre_rating)
            name = _PreserveSuffixText(
                name, f' ({rating} {rank_for_rating(rating).title_abbr})')
            performance_cell = _performance_cell(info)
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            rank,
            name,
            identity_fn(guild, row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.extend((performance_cell, delta_cell))
        result.append(tuple(cells))
    return result

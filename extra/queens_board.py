"""Queens puzzle-board reading and play orchestration (Playwright).

Reads the live grid from the page, drives the click loop to place queens,
and waits for completion.  Solver math is imported from ``queens_solver``;
the consent-banner helper is imported from ``queens_linkedin``.
"""
import asyncio
import random
import re
import time

from queens_config import (
    _EMPTY_RE,
    _LEADERBOARD_START_RE,
    _QUEEN_RE,
    _QUEENS_PLAY_URL,
    _READ_GRID_JS,
)
from queens_linkedin import _dismiss_consent_banner
from queens_solver import solve_queens


def _cell_label_selector(row, col):
    """CSS attribute selector for the cell at (row, col), state-agnostic.

    Matches the aria-label *suffix* so it keeps matching after the cell's
    state cycles (Empty → Cross → Queen → Empty).  Works for boards up to
    99×99 because the suffix is anchored at end-of-attribute.
    """
    return f'[aria-label$=", row {row + 1}, column {col + 1}"]'


async def read_queens_grid(page):
    """Read the current puzzle state from the page.

    Returns ``{n, regions: [[id]], queens: [(r,c)], xes: [(r,c)],
    cells: [...]}`` or ``{'error': ...}``.
    """
    raw = await page.evaluate(_READ_GRID_JS)
    if not raw or raw.get('error'):
        return {'error': (raw.get('error') if raw else 'evaluate returned None'),
                'sample': raw.get('sample') if raw else None,
                'n': raw.get('n') if raw else None}
    n = raw['n']
    cells = raw['cells']

    # Color name is the canonical region identity (it's the same string per
    # region per puzzle, surfaced by LinkedIn's a11y layer).  Falls back to
    # the background-color RGB if for some reason the name is missing.
    color_to_id = {}
    regions = [[None] * n for _ in range(n)]
    queens = []
    xes = []
    for c in cells:
        key = c.get('color') or c.get('bg') or ''
        if key not in color_to_id:
            color_to_id[key] = len(color_to_id)
        regions[c['row']][c['col']] = color_to_id[key]
        state = c.get('state', '')
        if _QUEEN_RE.search(state):
            queens.append((c['row'], c['col']))
        elif not _EMPTY_RE.search(state) and state.strip():
            # Anything that's neither queen nor empty is an X-mark of some
            # variety (Cross, X, Marked, ...).
            xes.append((c['row'], c['col']))
    return {
        'n': n,
        'regions': regions,
        'queens': queens,
        'xes': xes,
        'cells': cells,
        'distinct_colors': len(color_to_id),
    }


async def _human_pause(lo, hi):
    """Random sleep in [lo, hi] seconds — used to look human between clicks."""
    await asyncio.sleep(random.uniform(lo, hi))


async def _reset_board_if_dirty(page, grid):
    """Click ``Reset`` if any cell has an existing queen or X.

    A fresh board needs no reset.  A dirty board would otherwise force us to
    reason about the X/Queen/empty cycle per cell, which is fragile.
    """
    if not grid['queens'] and not grid['xes']:
        return False
    try:
        btn = page.get_by_role('button', name=re.compile(r'^reset$', re.I))
        if not await btn.count():
            return False
        if not await btn.first.is_enabled():
            return False
        await btn.first.click(timeout=3000)
        await _human_pause(0.8, 1.6)
        return True
    except Exception:
        return False


async def _place_queens(page, grid, queens, *, slow=True, log=None):
    """Click cells twice each (empty → X → Queen) with human-ish pacing.

    Targets cells by their aria-label suffix (``row N, column M``), which is
    state-agnostic — the same selector still finds the cell after the click
    morphs its label from ``Empty…`` to ``Cross…`` to ``Queen…``.
    """
    if log is None:
        log = lambda *_: None  # noqa: E731

    # Randomize the placement order so the bot doesn't always go top-left to
    # bottom-right.  Humans don't usually solve in strict row order either.
    order = list(queens)
    random.shuffle(order)

    for i, (r, c) in enumerate(order):
        cell = page.locator(_cell_label_selector(r, c)).first
        try:
            await cell.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass
        # First tap → X (Cross), second tap → Queen.
        await cell.click(timeout=5000)
        if slow:
            await _human_pause(0.35, 0.8)
        await cell.click(timeout=5000)
        log(f'  placed queen at row={r+1}, column={c+1}')
        if slow and i < len(order) - 1:
            # Inter-cell delay; occasionally a longer "thinking" pause so
            # the total isn't a perfect uniform cadence.
            if random.random() < 0.25:
                await _human_pause(2.8, 5.5)
            else:
                await _human_pause(1.4, 3.2)


async def _wait_for_completion(page, timeout_seconds=30):
    """Wait for the puzzle to register as solved.

    Returns True if the URL transitions to ``/results/`` or the page text
    starts looking like the post-game state.  Times out (returns False) so
    the caller can decide whether to bail or to try fetching anyway.
    """
    deadline = time.monotonic() + timeout_seconds
    completion_markers = (
        "you're crushing it",
        'great work',
        'congratulations',
        'puzzle complete',
        'you finished',
        'see results',
    )
    while time.monotonic() < deadline:
        try:
            if '/results' in page.url:
                return True
        except Exception:
            pass
        try:
            text = (await page.locator('body').inner_text(timeout=2000)).lower()
            if any(m in text for m in completion_markers):
                return True
        except Exception:
            pass
        await asyncio.sleep(1)
    return False


async def _puzzle_appears_already_played(page):
    """After landing on ``/results/``, return True iff the leaderboard is
    actually present (i.e. today's puzzle was already solved).

    LinkedIn quietly redirects ``/results/`` to ``/games/queens/`` when you
    haven't played today; that's our primary tell.  We also check the body
    text for the friend-list header.
    """
    try:
        if '/results' not in page.url:
            return False
    except Exception:
        return False
    try:
        text = await page.locator('body').inner_text(timeout=3000)
    except Exception:
        return False
    return bool(_LEADERBOARD_START_RE.search(text))


async def play_queens_puzzle(page, *, slow=True, log=None,
                             min_play_seconds=0):
    """Read, solve, and place queens on today's puzzle.

    Assumes ``page`` is (or will be navigated to) the puzzle URL.  Returns a
    diagnostic dict describing what happened.  Never raises for the obvious
    failure modes — surfaces them in the returned dict for the caller.
    """
    if log is None:
        log = lambda *_: None  # noqa: E731

    if '/games/queens' not in page.url or '/results' in page.url:
        try:
            await page.goto(_QUEENS_PLAY_URL, wait_until='domcontentloaded',
                            timeout=60000)
        except Exception as exc:
            return {'ok': False, 'stage': 'navigate', 'error': str(exc)}

    opened_at = time.monotonic()
    await _dismiss_consent_banner(page)
    # Give the grid a moment to render.  We anchor on the aria-label pattern
    # since the React-y class names are unstable.
    try:
        await page.wait_for_selector(
            '[aria-label*="row 1, column 1" i]', timeout=10000)
    except Exception:
        # Selector may differ — read_queens_grid will surface a clean error.
        pass

    grid = await read_queens_grid(page)
    if 'error' in grid:
        return {'ok': False, 'stage': 'read_grid', 'error': grid['error'],
                'sample': grid.get('sample')}

    n = grid['n']
    log(f'read {n}x{n} grid ({grid["distinct_colors"]} distinct region colors)')

    if grid['distinct_colors'] != n:
        # Colour quantization sometimes folds visually-similar regions
        # together (or splits a region across a hover state).  Surface this
        # clearly — solver will likely fail.
        log(f'  WARNING: expected {n} distinct colors, got '
            f'{grid["distinct_colors"]}')

    sol = solve_queens(grid['regions'])
    if sol is None:
        return {'ok': False, 'stage': 'solve', 'error': 'no solution found',
                'regions': grid['regions']}
    log(f'solver placed queens at {sol}')

    # Skip the actual click loop when the board already has the right queens
    # — happens if the puzzle was solved earlier today and we revisited.
    if set(grid['queens']) == set(sol):
        log('board already shows the solution; nothing to click')
        return {'ok': True, 'stage': 'already-solved', 'n': n, 'solution': sol}

    reset = await _reset_board_if_dirty(page, grid)
    if reset:
        log('clicked Reset (board had prior state)')
        # Re-read after reset so cell DOM indices are fresh.
        grid = await read_queens_grid(page)
        if 'error' in grid:
            return {'ok': False, 'stage': 'read_after_reset',
                    'error': grid['error']}

    remaining = max(0, int(min_play_seconds) - (time.monotonic() - opened_at))
    if remaining:
        log(f'waiting {remaining:.0f}s before placing queens')
        await asyncio.sleep(remaining)
    await _place_queens(page, grid, sol, slow=slow, log=log)
    completed = await _wait_for_completion(page, timeout_seconds=20)
    return {'ok': True, 'stage': 'placed', 'n': n, 'solution': sol,
            'completion_detected': completed}

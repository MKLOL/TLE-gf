"""Standalone LinkedIn Queens leaderboard scraper (proof of concept).

This is a *standalone* script — it doesn't touch the bot, doesn't write to the
DB, doesn't depend on discord.py.  It exists to validate the hard part (logging
into LinkedIn + extracting the leaderboard text) in isolation before wiring any
of it into the cog.

It deliberately just extracts the visible text of the leaderboard area and
hands it to the existing ``parse_queens_leaderboard`` from
``tle/cogs/_minigame_queens.py`` — the parser is already designed to chew
through LinkedIn's noisy copy-paste format, so the scraper's only job is "get
the text in DOM order."  If LinkedIn changes its markup, you fix one of the
selectors below, not the parser.

Usage
-----

Install (one-time):

    pip install playwright
    playwright install chromium

Log in interactively (one-time, opens a real browser window):

    python extra/queens_scrape.py login

You'll need to solve any captcha / 2FA / device-confirmation prompts yourself
in that window.  When you're sitting on your LinkedIn home feed, come back to
the terminal and press Enter — the session cookies + localStorage get saved to
``extra/.queens_state.json``.

Fetch and parse today's leaderboard:

    python extra/queens_scrape.py fetch

If the parser comes up empty or you want to see what's actually on the page:

    python extra/queens_scrape.py fetch --debug      # writes screenshot + raw text
    python extra/queens_scrape.py fetch --headed     # show the browser window

Notes
-----

* The first ``fetch`` after a login may need you to have already opened the
  puzzle today — LinkedIn typically only renders the leaderboard once you've
  played (or at least viewed) the day's puzzle.  If the script returns 0
  entries, run with ``--debug`` and check ``queens-debug.txt`` to confirm what
  the page is actually showing.

* The state file is auth material — don't commit it.  Add this to .gitignore
  if you plan to keep it around:

      extra/.queens_state.json
      queens-debug.png
      queens-debug.txt

* LinkedIn's User Agreement forbids automated access.  Use a low frequency
  (once a day, after the puzzle reset) and accept the small risk of the
  account being rate-limited or restricted.
"""

import argparse
import asyncio
import json
import pathlib
import random
import re
import sys
import time

from playwright.async_api import async_playwright

# So we can import the existing parser without installing the bot as a package.
# The parser import is optional — pulling it in transitively imports the cog
# stack (discord.py, aiohttp, lxml, cairo, ...).  In standalone-scraper testing
# you don't need any of that: ``fetch`` will still show the raw extracted text,
# which is the actual signal we care about.  When the bot calls this script in
# JSON mode, it does its own in-process parse on ``raw_text``.
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
try:
    from tle.cogs._minigame_queens import (  # noqa: E402
        normalize_queens_name,
        parse_queens_leaderboard,
    )
except ImportError:
    parse_queens_leaderboard = None

    def normalize_queens_name(name):
        return ' '.join(str(name).strip().casefold().split())


_STATE_PATH = _REPO_ROOT / 'extra' / '.queens_state.json'
# The ``/results/`` page renders the friend leaderboard directly; ``/games/
# queens/`` shows the puzzle UI and only reveals the leaderboard after a
# completed play.
_QUEENS_URL = 'https://www.linkedin.com/games/queens/results/'
_QUEENS_PLAY_URL = 'https://www.linkedin.com/games/queens/'
_INVITATIONS_URL = 'https://www.linkedin.com/mynetwork/invitation-manager/received/'
_LOGIN_URL = 'https://www.linkedin.com/login'

# Tried in order.  The leaderboard sits inside the post-game / results modal
# on LinkedIn and the exact wrapper changes from time to time, so we cast a
# wide net and let the parser pull signal out of noise.  Each selector picks
# the first match.
_LEADERBOARD_SELECTORS = (
    '[data-test-id*="leaderboard" i]',
    '[aria-label*="leaderboard" i]',
    'section:has-text("Leaderboard")',
    'div:has-text("Leaderboard"):has-text(":")',  # has a time-like value
    'main',
)

# Safety cap on "See more" clicks.  Each click loads ~10 more entries (the
# exact page size is LinkedIn's choice) so 50 covers ~500 friends, which is
# more than any realistic GF community size.  Without a cap, a broken
# "is the button gone?" check would spin forever.
_MAX_SEE_MORE_CLICKS = 50

_SEE_FULL_RE = re.compile(r'see\s+full\s+leaderboard', re.IGNORECASE)
_SEE_MORE_RE = re.compile(r'^(see|show)\s+more', re.IGNORECASE)
_YESTERDAY_RE = re.compile(r'^yesterday$', re.IGNORECASE)


async def _prompt(message):
    """Async wrapper around blocking ``input()``."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: input(message))


# ── Pure Queens solver ─────────────────────────────────────────────────────
#
# The board is an N×N grid.  Each cell belongs to one of N "color regions".
# A valid solution places exactly one queen such that every row, every
# column, and every region holds exactly one queen, AND no two queens are
# orthogonally OR diagonally adjacent (no touching, even at a corner).
#
# Since we have exactly one queen per row, two queens can only "touch" when
# they're in adjacent rows; checking the previous row's column against the
# current candidate's column is therefore sufficient.

def solve_queens(regions):
    """Solve a Queens grid.

    ``regions`` is an N×N list-of-lists of region IDs (any hashable values).
    Returns a list of (row, col) tuples — one per row — or ``None`` if the
    grid has no solution (which shouldn't happen for a well-formed LinkedIn
    puzzle but we surface it cleanly anyway).
    """
    n = len(regions)
    if n == 0 or any(len(row) != n for row in regions):
        return None
    queens = []
    cols_used = set()
    regs_used = set()

    def search(row):
        if row == n:
            return True
        for c in range(n):
            if c in cols_used:
                continue
            reg = regions[row][c]
            if reg in regs_used:
                continue
            if row > 0 and abs(queens[row - 1] - c) <= 1:
                # cols_used already excludes same column (diff == 0); this
                # additionally rejects diff == 1, the diagonal-touch case.
                continue
            queens.append(c)
            cols_used.add(c)
            regs_used.add(reg)
            if search(row + 1):
                return True
            queens.pop()
            cols_used.remove(c)
            regs_used.remove(reg)
        return False

    return [(r, c) for r, c in enumerate(queens)] if search(0) else None


def _validate_solution(regions, sol):
    """Confirm ``sol`` actually satisfies the Queens constraints for ``regions``."""
    n = len(regions)
    if sol is None or len(sol) != n:
        return False
    cols = [c for _, c in sol]
    if len(set(cols)) != n:
        return False
    regs = [regions[r][c] for r, c in sol]
    if len(set(regs)) != n:
        return False
    for i in range(1, n):
        if abs(cols[i] - cols[i - 1]) <= 1:
            return False  # diagonal or same-column touch
    return True


def _solver_self_test():
    """Quick sanity check on degenerate inputs.

    The real solver test is the daily integration run — we don't ship a real
    LinkedIn fixture because we'd have to keep it in sync with the puzzle of
    the day.  These cases just confirm the algorithm doesn't crash on
    trivial inputs and surfaces unsolvable boards as ``None``.
    """
    assert solve_queens([[0]]) == [(0, 0)]
    assert solve_queens([]) is None
    # 3×3 with rows-as-regions is unsolvable: the only ways to fit 3 unique
    # columns with each pair differing by >= 2 require columns 0, 2, 4 — but
    # the third doesn't exist on a 3-wide board.
    assert solve_queens([[r] * 3 for r in range(3)]) is None
    # 5×5 by row regions is solvable; verify the structure of the result.
    grid_5 = [[r] * 5 for r in range(5)]
    sol = solve_queens(grid_5)
    assert _validate_solution(grid_5, sol), f'5x5 solve failed: {sol}'
    return True


async def _extract_linkedin_self_name(page):
    """Return the logged-in user's display name, or ``None`` if we can't.

    Navigates to ``/in/me/`` — LinkedIn redirects that to the user's own
    profile URL — and parses ``document.title``, which is reliably formatted
    as ``"<Name> | LinkedIn"`` across UI redesigns.  Best-effort: never
    raises.

    Disturbs the current page (navigates away), so callers that need to
    stay on the games page should run this on a separate ``page`` /
    context, or skip it.
    """
    try:
        await page.goto('https://www.linkedin.com/in/me/',
                        wait_until='domcontentloaded', timeout=30000)
    except Exception:
        return None
    if any(f in page.url for f in _SESSION_EXPIRED_URL_FRAGMENTS):
        return None
    # Let the title settle — LinkedIn often loads with a placeholder title
    # ("LinkedIn") and updates to "<Name> | LinkedIn" once the profile data
    # arrives.  Poll up to ~5s.
    name = None
    for _ in range(10):
        try:
            title = await page.title()
        except Exception:
            title = ''
        if title and '|' in title:
            head = title.split('|', 1)[0].strip()
            if head and head.lower() != 'linkedin':
                name = head
                break
        await asyncio.sleep(0.5)
    return name


async def cmd_whoami(state_path, *, headless):
    """Print the logged-in LinkedIn user's name as JSON (or an error JSON)."""
    if not state_path.exists():
        print(json.dumps({'status': 'session_missing',
                          'error': f'No saved session at {state_path}.'}))
        return 1
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        name = await _extract_linkedin_self_name(page)
        landed_url = page.url
        await browser.close()
    if name is None and any(
            f in landed_url for f in _SESSION_EXPIRED_URL_FRAGMENTS):
        print(json.dumps({'status': 'session_expired',
                          'current_url': landed_url}))
        return 1
    if not name:
        print(json.dumps({'status': 'unknown',
                          'error': 'Could not extract name from /in/me/.'}))
        return 2
    print(json.dumps({'status': 'ok', 'name': name}))
    return 0


async def cmd_dump_grid(state_path, *, headless):
    """Diagnostic: dump DOM cell candidates so we can pick a real selector."""
    if not state_path.exists():
        print(f'No saved session at {state_path}. Run `login` first.',
              file=sys.stderr)
        return 1
    discover_js = r"""
    () => {
      // Look at every element that has both a small square shape and a click
      // handler / role / tabindex.  Bucket by tag + class signature so we can
      // tell which "kind" of element makes up the grid.
      const out = [];
      for (const el of document.querySelectorAll('*')) {
        const rect = el.getBoundingClientRect();
        if (rect.width < 20 || rect.width > 200) continue;
        if (Math.abs(rect.width - rect.height) > 4) continue;
        if (rect.width === 0) continue;
        const cs = getComputedStyle(el);
        if (cs.display === 'none' || cs.visibility === 'hidden') continue;
        out.push({
          tag: el.tagName,
          cls: el.className && el.className.toString
              ? el.className.toString().slice(0, 200)
              : '',
          role: el.getAttribute('role'),
          label: el.getAttribute('aria-label') || '',
          w: Math.round(rect.width),
          h: Math.round(rect.height),
          bg: cs.backgroundColor,
        });
      }
      // Group by (tag, normalised class signature) and report counts.
      const buckets = {};
      for (const e of out) {
        const sig = `${e.tag}|${e.cls.split(/\s+/).filter(c => c).sort().join(' ')}`;
        buckets[sig] = (buckets[sig] || 0) + 1;
      }
      // Return up-to-30 buckets with count >= 4 (a 2x2 grid is the floor),
      // sorted by count desc.
      const result = Object.entries(buckets)
        .filter(([, n]) => n >= 4)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 30)
        .map(([sig, n]) => ({sig, n, sqrt: Math.sqrt(n)}));
      // Also dump a couple of sample cells per top bucket to show their bg.
      const samples = {};
      for (const {sig} of result.slice(0, 8)) {
        const [tag, cls] = sig.split('|');
        const clsSel = cls.split(' ').filter(c => c).map(
            c => '.' + CSS.escape(c)).join('');
        const sel = tag.toLowerCase() + clsSel;
        try {
          const els = Array.from(document.querySelectorAll(sel)).slice(0, 4);
          samples[sig] = els.map(e => ({
            bg: getComputedStyle(e).backgroundColor,
            label: e.getAttribute('aria-label') || '',
            html_preview: (e.innerHTML || '').slice(0, 80),
          }));
        } catch (e) { samples[sig] = [{error: e.message}]; }
      }
      return {buckets: result, samples};
    }
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        await page.goto(_QUEENS_PLAY_URL, wait_until='domcontentloaded',
                        timeout=60000)
        await _dismiss_consent_banner(page)
        await asyncio.sleep(3)
        info = await page.evaluate(discover_js)
        await browser.close()
    print('Square-ish, clickable-ish DOM buckets on /games/queens/:')
    for b in info.get('buckets', []):
        marker = '  (perfect square!)' if b['sqrt'].is_integer() else ''
        print(f'  {b["n"]:4d} × {b["sig"]}{marker}')
    print()
    print('Sample backgrounds per top bucket:')
    for sig, samples in info.get('samples', {}).items():
        print(f'  {sig!r}:')
        for s in samples:
            print(f'    bg={s.get("bg")!r}  label={s.get("label")!r}  '
                  f'html={s.get("html_preview")!r}')
    return 0


async def cmd_solve(state_path, *, headless, debug):
    """Phase A: read the puzzle grid and print the solution, no clicking."""
    if not state_path.exists():
        print(f'No saved session at {state_path}. Run `login` first.',
              file=sys.stderr)
        return 1
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        try:
            await page.goto(_QUEENS_PLAY_URL, wait_until='domcontentloaded',
                            timeout=60000)
        except Exception as exc:
            print(f'Navigation failed: {exc}', file=sys.stderr)
            await browser.close()
            return 1
        await _dismiss_consent_banner(page)
        try:
            await page.wait_for_selector(
                '[aria-label*="row 1, column 1" i]', timeout=10000)
        except Exception:
            pass
        grid = await read_queens_grid(page)
        if debug:
            screenshot = _REPO_ROOT / 'queens-solve-debug.png'
            await page.screenshot(path=str(screenshot), full_page=True)
            print(f'Saved screenshot to {screenshot}')
        await browser.close()

    if 'error' in grid:
        print(f'Grid read failed: {grid["error"]}', file=sys.stderr)
        if grid.get('sample'):
            print(f'Sample cell info: {grid["sample"]}', file=sys.stderr)
        return 1
    n = grid['n']
    print(f'Grid: {n}x{n}  ({grid["distinct_colors"]} distinct region colors)')
    if grid['queens']:
        print(f'Board already has queens at: {grid["queens"]}')
    if grid['xes']:
        print(f'Board already has X marks at: {grid["xes"]}')
    print('Regions (one row per puzzle row):')
    for row in grid['regions']:
        print('  ' + ' '.join(f'{c:2d}' if c is not None else ' ?' for c in row))
    sol = solve_queens(grid['regions'])
    if sol is None:
        print('Solver could not find a solution — region read likely wrong.',
              file=sys.stderr)
        return 2
    print()
    print('Solution (row, column):')
    for r, c in sol:
        print(f'  row {r}, col {c}  (region {grid["regions"][r][c]})')
    return 0


async def cmd_play(state_path, *, headless, slow):
    """Phase B: read, solve, and place queens with human pacing."""
    if not state_path.exists():
        print(f'No saved session at {state_path}. Run `login` first.',
              file=sys.stderr)
        return 1
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        try:
            await page.goto(_QUEENS_PLAY_URL, wait_until='domcontentloaded',
                            timeout=60000)
        except Exception as exc:
            print(f'Navigation failed: {exc}', file=sys.stderr)
            await browser.close()
            return 1
        result = await play_queens_puzzle(page, slow=slow, log=print)
        await browser.close()
    if not result.get('ok'):
        print(f'Play failed at stage={result.get("stage")}: '
              f'{result.get("error")}', file=sys.stderr)
        return 1
    if result['stage'] == 'already-solved':
        print('Board already solved — no clicks performed.')
    else:
        print(f'Placed {len(result["solution"])} queens; '
              f'completion detected: {result["completion_detected"]}')
    return 0


async def cmd_login(state_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(_LOGIN_URL)
        print('A browser window has opened.')
        print('  1. Log into LinkedIn (solve captcha / 2FA / device prompts).')
        print('  2. Wait until you can see your home feed.')
        print('  3. Come back here and press Enter to save the session.')
        await _prompt('> ')
        await context.storage_state(path=str(state_path))
        await browser.close()
    print(f'Saved session to {state_path}')
    print('Do NOT commit this file — it contains your LinkedIn auth cookies.')


async def _dismiss_consent_banner(page):
    """Click any visible cookie/consent banner so it doesn't intercept later clicks.

    LinkedIn shows a fixed-position "We respect your privacy" bar at the top
    of the page on first visit; until you accept or reject, every click into
    the body underneath either misses or hits the banner instead.
    """
    clicked = await page.evaluate(r"""
    () => {
      const actionRe = /^(accept(?: all)?(?: cookies)?|reject(?: all)?(?: cookies)?|got it|ok|agree)$/i;
      const consentRe = /cookie|privacy|consent|we respect your privacy|use cookies|personalized ads/i;
      const invitationRe = /invitation|mutual connection|followers?|ignore/i;
      const visible = (el) => {
        const rect = el.getBoundingClientRect();
        const cs = getComputedStyle(el);
        return rect.width > 0 && rect.height > 0
          && cs.visibility !== 'hidden'
          && cs.display !== 'none';
      };
      const buttons = Array.from(document.querySelectorAll('button,[role="button"]'));
      for (const button of buttons) {
        const label = (
          button.innerText
          || button.getAttribute('aria-label')
          || button.getAttribute('value')
          || ''
        ).replace(/\s+/g, ' ').trim();
        if (!visible(button) || !actionRe.test(label)) continue;
        let context = '';
        let node = button;
        for (let depth = 0; node && node !== document.body && depth < 5; depth += 1) {
          context += ' ' + (node.innerText || '');
          node = node.parentElement;
        }
        if (!consentRe.test(context) || invitationRe.test(context)) continue;
        button.click();
        return true;
      }
      return false;
    }
    """)
    if clicked:
        await page.wait_for_timeout(300)


async def _click_first(page, *candidates, timeout=3000):
    """Try each ``(role, name_regex)`` candidate until one click succeeds.

    Returns True iff a click landed.  We try locator roles in order, then a
    plain text-based fallback — LinkedIn's "See full leaderboard" is rendered
    as a styled button in some experiments and a plain ``<a>`` in others.
    """
    for role, name in candidates:
        try:
            loc = page.get_by_role(role, name=name)
            if not await loc.count():
                continue
            target = loc.first
            if not await target.is_visible():
                continue
            await target.scroll_into_view_if_needed(timeout=1000)
            await target.click(timeout=timeout)
            return True
        except Exception:
            continue
    # Text-fallback: click the first visible node whose text matches.
    for _role, name in candidates:
        try:
            if hasattr(name, 'pattern'):
                loc = page.get_by_text(name).first
            else:
                loc = page.locator(f'text={name}').first
            if not await loc.count():
                continue
            if not await loc.is_visible():
                continue
            await loc.scroll_into_view_if_needed(timeout=1000)
            await loc.click(timeout=timeout)
            return True
        except Exception:
            continue
    return False


# ── Grid reading + play orchestration ──────────────────────────────────────

# LinkedIn's puzzle cells carry aria-labels of the form:
#   "<State> of color <ColorName>, row <N>, column <M>"
# where <State> is "Empty", "Cross", "Queen" (or similar) and <N>/<M> are
# 1-indexed.  This is by far the most stable handle we have — CSS class
# names are obfuscated hashes that change on every build, but the
# accessibility text is part of the product contract.
_READ_GRID_JS = r"""
() => {
  const labelRe = /^\s*(.+?)\s+of\s+color\s+(.+?),\s*row\s+(\d+),\s*column\s+(\d+)\s*$/i;
  const candidates = Array.from(document.querySelectorAll('[aria-label]'));
  const parsed = [];
  for (const el of candidates) {
    const label = el.getAttribute('aria-label') || '';
    const m = label.match(labelRe);
    if (!m) continue;
    // Prefer the outermost element with this label so clicks land on the
    // real interactive target rather than a nested decoration span.  We do
    // this by skipping if any ancestor up to <body> has the same label.
    let p = el.parentElement;
    let skip = false;
    while (p && p !== document.body) {
      if ((p.getAttribute('aria-label') || '') === label) { skip = true; break; }
      p = p.parentElement;
    }
    if (skip) continue;
    const cs = getComputedStyle(el);
    parsed.push({
      state: m[1].trim(),
      color: m[2].trim(),
      row: parseInt(m[3], 10) - 1,
      col: parseInt(m[4], 10) - 1,
      bg: cs.backgroundColor,
      label,
    });
  }
  if (!parsed.length) {
    return {error: 'no cells with row/column aria-labels found'};
  }
  const maxRow = Math.max(...parsed.map(c => c.row));
  const maxCol = Math.max(...parsed.map(c => c.col));
  const n = Math.max(maxRow, maxCol) + 1;
  if (parsed.length !== n * n) {
    return {error: `found ${parsed.length} labelled cells but expected ${n*n}`,
            n, sample: parsed.slice(0, 4)};
  }
  return {n, cells: parsed};
}
"""

# Some LinkedIn experiments label the X-marked state "Cross", "X", "Marked",
# etc.; we just look for anything that isn't "queen" or clearly-empty.
_QUEEN_RE = re.compile(r'queen|crown', re.IGNORECASE)
_EMPTY_RE = re.compile(r'empty|blank|none', re.IGNORECASE)


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


async def _expand_to_full_leaderboard(page):
    """Click "See full leaderboard" if present, then wait for the new page.

    Best-effort — silently no-ops on any failure (the parser is forgiving and
    the /results/ top-3 is still useful output).
    """
    await _dismiss_consent_banner(page)
    clicked = await _click_first(
        page,
        ('link', _SEE_FULL_RE),
        ('button', _SEE_FULL_RE),
    )
    if not clicked:
        return
    # Either the URL changes (full page navigation) or the leaderboard
    # widget swaps content in place.  Wait for whichever happens first.
    try:
        await page.wait_for_url('**/leaderboard/**', timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state('domcontentloaded', timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(
            'text=/\\d{1,2}:\\d{2}/', timeout=10000)
    except Exception:
        pass


async def _switch_results_day(page, day):
    """Switch the results page to a non-default day tab if requested."""
    if day != 'yesterday':
        return False
    await _dismiss_consent_banner(page)
    clicked = await _click_first(
        page,
        ('tab', _YESTERDAY_RE),
        ('button', _YESTERDAY_RE),
        ('link', _YESTERDAY_RE),
        timeout=4000,
    )
    if not clicked:
        clicked = await page.evaluate(r"""
        () => {
          const re = /^yesterday$/i;
          const visible = (el) => {
            const rect = el.getBoundingClientRect();
            const cs = getComputedStyle(el);
            return rect.width > 0 && rect.height > 0
              && cs.visibility !== 'hidden'
              && cs.display !== 'none';
          };
          for (const el of document.querySelectorAll('button,a,[role="tab"],[role="button"]')) {
            const label = (
              el.innerText
              || el.getAttribute('aria-label')
              || ''
            ).replace(/\s+/g, ' ').trim();
            if (visible(el) && re.test(label)) {
              el.click();
              return true;
            }
          }
          return false;
        }
        """)
    if clicked:
        try:
            await page.wait_for_timeout(1200)
        except Exception:
            pass
    return bool(clicked)


async def _click_see_more_until_done(page):
    """Repeatedly click "See more" until it's gone, hidden, or the count stops growing.

    Caps at ``_MAX_SEE_MORE_CLICKS`` so a broken "is it gone yet?" check can't
    spin forever.  Returns the number of clicks actually performed.
    """
    clicks = 0
    last_count = -1
    for _ in range(_MAX_SEE_MORE_CLICKS):
        try:
            btn = page.get_by_role('button', name=_SEE_MORE_RE)
            count = await btn.count()
        except Exception:
            break
        if not count:
            break
        try:
            first = btn.first
            if not await first.is_visible():
                break
            if not await first.is_enabled():
                break
            await first.scroll_into_view_if_needed(timeout=2000)
            await first.click(timeout=3000)
        except Exception:
            break
        clicks += 1
        # Give the new rows a beat to render before checking again.
        try:
            await page.wait_for_timeout(800)
        except Exception:
            pass
        # Detect "the button is still there but nothing's loading anymore"
        # by counting time-shaped lines.  If two consecutive clicks don't
        # grow the count, we're done.
        try:
            now_count = await page.locator(
                'text=/^\\s*\\d{1,2}:\\d{2}\\s*$/').count()
        except Exception:
            now_count = -1
        if now_count == last_count and now_count != -1:
            break
        last_count = now_count
    return clicks


_INVITATION_CANDIDATES_JS = r"""
() => {
  function visible(el) {
    const rect = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    return rect.width > 0 && rect.height > 0
      && cs.visibility !== 'hidden'
      && cs.display !== 'none';
  }

  function cleanName(text) {
    let value = (text || '').replace(/\s+/g, ' ').trim();
    if (!value) return '';
    value = value.replace(/^accept\s+/i, '');
    value = value.replace(/^invitation\s+from\s+/i, '');
    value = value.replace(/[’']s\s+invitation.*$/i, '');
    value = value.replace(/\s+invitation.*$/i, '');
    value = value.replace(/[’']s$/i, '');
    value = value.replace(/^view\s+/i, '');
    value = value.replace(/\s+profile$/i, '');
    value = value.trim();
    if (!value) return '';
    const lower = value.toLowerCase();
    const rejects = [
      'accept', 'ignore', 'message', 'view profile', 'connect',
      'show more', 'see more'
    ];
    if (rejects.includes(lower)) return '';
    if (lower.includes('mutual connection')) return '';
    if (lower.includes('follower')) return '';
    if (lower.includes('invitation')) return '';
    if (value.length > 100) return '';
    return value;
  }

  const buttons = Array.from(document.querySelectorAll('button'));
  const out = [];
  buttons.forEach((button, buttonIndex) => {
    const label = (
      button.innerText || button.getAttribute('aria-label') || ''
    ).trim();
    const aria = (button.getAttribute('aria-label') || '').trim();
    if (!/^accept\b/i.test(label) && !/^accept\b/i.test(aria)) return;
    if (!visible(button)) return;

    let card = button;
    let node = button;
    for (let depth = 0; node && depth < 8; depth += 1, node = node.parentElement) {
      const text = (node.innerText || '').trim();
      if (!text) continue;
      const buttonCount = node.querySelectorAll('button').length;
      if (/accept/i.test(text) && (/ignore/i.test(text) || buttonCount <= 6)) {
        card = node;
        break;
      }
    }
    const cardText = (card.innerText || '').trim();
    const isInvitationAction = /ignore/i.test(cardText)
      || /invitation/i.test(cardText)
      || /invitation/i.test(aria);
    if (!isInvitationAction) return;

    const names = [];
    const push = (value) => {
      const cleaned = cleanName(value);
      if (cleaned && !names.includes(cleaned)) names.push(cleaned);
    };

    push(aria);
    for (const link of card.querySelectorAll('a[href*="/in/"]')) {
      push(link.innerText || link.getAttribute('aria-label') || '');
    }
    for (const el of card.querySelectorAll('[aria-label]')) {
      const value = el.getAttribute('aria-label') || '';
      if (/profile|invitation|accept/i.test(value)) push(value);
    }
    const lines = (card.innerText || '')
      .split(/\n+/)
      .map(line => line.trim())
      .filter(Boolean);
    for (const line of lines.slice(0, 8)) push(line);

    out.push({
      button_index: buttonIndex,
      names,
      text_preview: lines.slice(0, 8).join(' | '),
    });
  });
  return out;
}
"""


async def _load_received_invitations(page, *, click_more=True):
    """Best-effort scroll/load pass for the received invitations page."""
    last_height = -1
    for _ in range(6):
        if click_more:
            clicked = await _click_first(
                page,
                ('button', _SEE_MORE_RE),
                ('link', _SEE_MORE_RE),
                timeout=2000,
            )
            if clicked:
                await page.wait_for_timeout(800)
                continue
        try:
            height = await page.evaluate('document.body.scrollHeight')
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(700)
        except Exception:
            break
        if height == last_height:
            break
        last_height = height


async def _find_matching_invitation(page, remaining):
    """Return ``(candidate, normalized_name)`` for the first exact name match."""
    candidates = await page.evaluate(_INVITATION_CANDIDATES_JS)
    for candidate in candidates:
        candidate_names = candidate.get('names') or []
        normalized_candidates = {
            normalize_queens_name(name)
            for name in candidate_names
            if normalize_queens_name(name)
        }
        for normalized in remaining:
            if normalized in normalized_candidates:
                return candidate, normalized
    return None, None


async def cmd_connect(state_path, names, *, headless, debug, json_out,
                      dry_run=False):
    if not state_path.exists():
        payload = {'status': 'session_missing',
                   'error': f'No saved session at {state_path}.'}
        if json_out:
            print(json.dumps(payload))
        else:
            print(payload['error'], file=sys.stderr)
        return 1
    targets = {}
    for name in names:
        normalized = normalize_queens_name(name)
        if normalized:
            targets[normalized] = name
    if not targets:
        payload = {'status': 'no_targets', 'accepted': [], 'missing': []}
        if json_out:
            print(json.dumps(payload))
        else:
            print('No names supplied.', file=sys.stderr)
        return 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        try:
            await page.goto(_INVITATIONS_URL, wait_until='domcontentloaded',
                            timeout=60000)
        except Exception as exc:
            await browser.close()
            payload = {'status': 'error', 'error': f'Navigation failed: {exc}'}
            if json_out:
                print(json.dumps(payload))
            else:
                print(payload['error'], file=sys.stderr)
            return 1

        if any(f in page.url for f in _SESSION_EXPIRED_URL_FRAGMENTS):
            current = page.url
            await browser.close()
            payload = {'status': 'session_expired', 'current_url': current}
            if json_out:
                print(json.dumps(payload))
            else:
                print(f'Session expired (landed on {current}).', file=sys.stderr)
            return 1

        if not dry_run:
            await _dismiss_consent_banner(page)
        try:
            await page.wait_for_selector('button', timeout=10000)
        except Exception:
            pass
        await _load_received_invitations(page, click_more=not dry_run)

        candidates = await page.evaluate(_INVITATION_CANDIDATES_JS)
        seen_before = []
        for candidate in candidates:
            for name in candidate.get('names') or []:
                if name not in seen_before:
                    seen_before.append(name)
        if dry_run:
            target_names = set(targets)
            matches = []
            for candidate in candidates:
                for name in candidate.get('names') or []:
                    normalized = normalize_queens_name(name)
                    if normalized in target_names and name not in matches:
                        matches.append(name)

            if debug:
                screenshot_path = _REPO_ROOT / 'queens-connect-debug.png'
                text_path = _REPO_ROOT / 'queens-connect-debug.txt'
                await page.screenshot(path=str(screenshot_path), full_page=True)
                try:
                    text = await page.locator('body').inner_text()
                except Exception:
                    text = ''
                text_path.write_text(text or '', encoding='utf-8')
                if not json_out:
                    print(f'Saved screenshot to {screenshot_path}')
                    print(f'Saved page text to    {text_path}')

            await browser.close()
            payload = {
                'status': 'ok',
                'dry_run': True,
                'candidate_count': len(candidates),
                'names': seen_before,
                'matches': matches,
                'accepted': [],
                'accepted_normalized': [],
                'missing': [
                    name for normalized, name in targets.items()
                    if normalized not in {
                        normalize_queens_name(match) for match in matches
                    }
                ],
            }
            if json_out:
                print(json.dumps(payload))
            else:
                print(f'Found {len(seen_before)} received invitation name(s).')
                for name in seen_before:
                    print(f'  {name}')
            return 0

        accepted = []
        failed = []
        remaining = dict(targets)
        for _ in range(len(targets)):
            candidate, normalized = await _find_matching_invitation(page, remaining)
            if candidate is None:
                break
            button_index = candidate['button_index']
            try:
                button = page.locator('button').nth(button_index)
                await button.scroll_into_view_if_needed(timeout=2000)
                await button.click(timeout=5000)
                await page.wait_for_timeout(1200)
            except Exception as exc:
                failed.append({'name': remaining[normalized], 'error': str(exc)})
                remaining.pop(normalized, None)
                continue
            accepted.append(remaining.pop(normalized))

        seen = []
        try:
            for candidate in await page.evaluate(_INVITATION_CANDIDATES_JS):
                for name in candidate.get('names') or []:
                    if name not in seen:
                        seen.append(name)
        except Exception:
            pass

        if debug:
            screenshot_path = _REPO_ROOT / 'queens-connect-debug.png'
            text_path = _REPO_ROOT / 'queens-connect-debug.txt'
            await page.screenshot(path=str(screenshot_path), full_page=True)
            try:
                text = await page.locator('body').inner_text()
            except Exception:
                text = ''
            text_path.write_text(text or '', encoding='utf-8')
            if not json_out:
                print(f'Saved screenshot to {screenshot_path}')
                print(f'Saved page text to    {text_path}')

        await browser.close()

    payload = {
        'status': 'ok',
        'accepted': accepted,
        'accepted_normalized': [
            normalize_queens_name(name) for name in accepted
        ],
        'missing': list(remaining.values()),
        'failed': failed,
        'seen_before': seen_before[:50],
        'seen': seen[:50],
    }
    if json_out:
        print(json.dumps(payload))
    else:
        print(f'Accepted {len(accepted)} connection request(s).')
        for name in accepted:
            print(f'  accepted: {name}')
        for name in remaining.values():
            print(f'  missing:  {name}')
        for item in failed:
            print(f'  failed:   {item["name"]}: {item["error"]}')
    return 0 if accepted or not failed else 2


async def _extract_leaderboard_text(page):
    for selector in _LEADERBOARD_SELECTORS:
        try:
            loc = page.locator(selector).first
            if not await loc.count():
                continue
            text = await loc.inner_text(timeout=2000)
        except Exception:
            continue
        if text and _looks_like_leaderboard(text):
            return selector, _crop_to_leaderboard(text)
    body_text = await page.locator('body').inner_text()
    return None, _crop_to_leaderboard(body_text)


# Match "2 connections played today", "1 connection played yesterday",
# "10 connections played today", etc.  This line marks where the friend
# leaderboard begins; everything above it is the user's own scorecard +
# encouragement banners.
_LEADERBOARD_START_RE = re.compile(
    r'connection(?:s)?\s+played\s+(?:today|yesterday)', re.IGNORECASE)

# URL fragments that mean "LinkedIn bounced us to re-authenticate".  If the
# page lands on one of these after navigating to /games/queens/results/, the
# stored session is dead and the bot needs a fresh ``;queens login``.
_SESSION_EXPIRED_URL_FRAGMENTS = (
    '/login', '/uas/', '/checkpoint/', '/authwall', '/signup',
)
# "See full leaderboard" is the link below the friend list; everything after
# it is the player-stats panel (Plays / Win % / Best score / Max streak), the
# streak badges, and the "play another game" CTAs — none of which the parser
# should see.  Same idea for "See yesterday's results" if LinkedIn ever
# renames it.
_LEADERBOARD_END_PATTERNS = (
    'see full leaderboard',
    "see yesterday's results",
    "see yesterday’s results",
)


def _crop_to_leaderboard(text):
    """Trim ``text`` down to just the lines between the friend leaderboard's
    start banner and its ``See full leaderboard`` footer.

    Falls back to the original ``text`` if either marker is absent — the
    parser is forgiving, and we'd rather feed it everything than feed it
    nothing.
    """
    if not text:
        return text
    lines = text.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if _LEADERBOARD_START_RE.search(line):
            start_idx = i + 1
            break
    if start_idx is None:
        return text
    end_idx = len(lines)
    for i in range(start_idx, len(lines)):
        if any(p in lines[i].lower() for p in _LEADERBOARD_END_PATTERNS):
            end_idx = i
            break
    return '\n'.join(lines[start_idx:end_idx])


def _looks_like_leaderboard(text):
    # Cheap heuristic: a leaderboard chunk has at least one MM:SS-looking
    # line.  Stops us from happily returning the nav bar.
    return bool(re.search(r'\b\d{1,2}:\d{2}\b', text))


async def cmd_fetch(state_path, *, headless, debug, json_out, auto_play, slow,
                    day='today', min_play_seconds=0):
    if not state_path.exists():
        msg = f'No saved session at {state_path}. Run `login` first.'
        if json_out:
            print(json.dumps({'error': msg}))
        else:
            print(msg, file=sys.stderr)
        return 1
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=str(state_path))
        page = await context.new_page()
        try:
            # ``domcontentloaded`` not ``networkidle`` — LinkedIn keeps
            # background scripts running indefinitely so networkidle never
            # fires.  We wait for a time-shaped string below to know the
            # leaderboard widget has rendered.
            await page.goto(_QUEENS_URL, wait_until='domcontentloaded',
                            timeout=60000)
        except Exception as exc:
            if json_out:
                print(json.dumps(
                    {'status': 'error',
                     'error': f'Navigation failed: {exc}'}))
            else:
                print(f'Navigation failed: {exc}', file=sys.stderr)
            await browser.close()
            return 1

        # Session-expired detection: if LinkedIn bounced us to the login
        # page (or any auth-related URL), the cookies are dead.
        if any(f in page.url for f in _SESSION_EXPIRED_URL_FRAGMENTS):
            current = page.url
            await browser.close()
            if json_out:
                print(json.dumps(
                    {'status': 'session_expired', 'current_url': current}))
            else:
                print(f'Session expired (landed on {current}).',
                      file=sys.stderr)
            return 1

        await _dismiss_consent_banner(page)

        # Wait briefly for *either* a leaderboard time-shape *or* the
        # "today's puzzle" CTA so the played-or-not check below has settled
        # content to look at.  Without this, the React leaderboard often
        # hasn't rendered yet and we wrongly assume the puzzle is unplayed.
        try:
            await page.wait_for_selector(
                'text=/\\d{1,2}:\\d{2}/', timeout=5000)
        except Exception:
            pass

        # If the puzzle hasn't been played today, LinkedIn redirects
        # /results/ → /games/queens/ and we land on the puzzle UI with no
        # leaderboard.  When --auto-play is on, solve and place queens here,
        # then navigate back to /results/.
        if auto_play and not await _puzzle_appears_already_played(page):
            log_fn = (lambda *_a: None) if json_out else print
            log_fn('Leaderboard not yet visible — solving today\'s puzzle.')
            play_result = await play_queens_puzzle(
                page, slow=slow, log=log_fn,
                min_play_seconds=min_play_seconds)
            if not play_result.get('ok'):
                msg = (f'Auto-play failed at stage='
                       f'{play_result.get("stage")}: {play_result.get("error")}')
                if json_out:
                    print(json.dumps({'error': msg}))
                else:
                    print(msg, file=sys.stderr)
                await browser.close()
                return 1
            # Re-navigate to /results/ to load the friend leaderboard.
            try:
                await page.goto(_QUEENS_URL, wait_until='domcontentloaded',
                                timeout=60000)
            except Exception as exc:
                if json_out:
                    print(json.dumps(
                        {'error': f'Post-play navigation failed: {exc}'}))
                else:
                    print(f'Post-play navigation failed: {exc}', file=sys.stderr)
                await browser.close()
                return 1

        # Give the leaderboard widget a moment to render.
        try:
            await page.wait_for_selector(
                'text=/\\d{1,2}:\\d{2}/', timeout=10000)
        except Exception:
            pass  # Fall through — debug mode will show what's actually there.

        if day == 'yesterday':
            switched = await _switch_results_day(page, day)
            if not switched:
                try:
                    body_text = await page.locator('body').inner_text(timeout=3000)
                except Exception:
                    body_text = ''
                if not re.search(
                        r'connection(?:s)?\s+played\s+yesterday',
                        body_text, re.IGNORECASE):
                    msg = 'Could not switch to the Yesterday results tab.'
                    if json_out:
                        print(json.dumps({'status': 'error', 'error': msg}))
                    else:
                        print(msg, file=sys.stderr)
                    await browser.close()
                    return 1
            try:
                await page.wait_for_selector(
                    'text=/\\d{1,2}:\\d{2}/', timeout=10000)
            except Exception:
                pass

        # /results/ shows only the top 3 connections.  The "See full
        # leaderboard" link navigates to the full friend ranking at
        # /results/leaderboard/connections/?gameUrn=... (the URN is bound to
        # the logged-in user, so we let LinkedIn build it via the click).
        await _expand_to_full_leaderboard(page)
        await _click_see_more_until_done(page)

        selector, text = await _extract_leaderboard_text(page)

        if debug:
            screenshot_path = _REPO_ROOT / 'queens-debug.png'
            text_path = _REPO_ROOT / 'queens-debug.txt'
            await page.screenshot(path=str(screenshot_path), full_page=True)
            text_path.write_text(text or '', encoding='utf-8')
            if not json_out:
                print(f'Saved screenshot to {screenshot_path}')
                print(f'Saved page text to    {text_path}')
                print(f'Matched selector:     {selector or "<body fallback>"}')
                print()

        await browser.close()

    if not text:
        msg = 'Could not find any leaderboard text on the page.'
        if json_out:
            print(json.dumps({'status': 'error', 'error': msg}))
        else:
            print(msg, file=sys.stderr)
        return 1

    entries = parse_queens_leaderboard(text) if parse_queens_leaderboard else None

    # ``status`` summarises what the bot needs to know:
    #   ok            — leaderboard fetched, at least one entry parsed
    #   not_played    — page is here but no leaderboard rows / time markers
    #                   (the bot account hasn't solved today's puzzle yet)
    #   error         — covered earlier (navigation, no text, etc.)
    has_time = bool(re.search(r'\b\d{1,2}:\d{2}\b', text))
    if entries:
        status = 'ok'
    elif has_time:
        # We saw times but the parser couldn't pull entries — surface as
        # 'ok' anyway so the bot can attempt the import.  Empty result is
        # rare; treat as best-effort.
        status = 'ok'
    else:
        status = 'not_played'

    if json_out:
        # Bot-friendly output.  ``entries`` is informational only — the bot
        # re-parses ``raw_text`` in-process so its parser version is the
        # source of truth.
        payload = {
            'status': status,
            'raw_text': text,
            'matched_selector': selector,
            'day': day,
        }
        if entries is not None:
            payload['entries'] = [
                {
                    'linkedin_name': e.linkedin_name,
                    'time_seconds': e.time_seconds,
                    'no_hints': e.no_hints,
                    'no_mistakes': e.no_mistakes,
                    'is_you': e.is_you,
                }
                for e in entries
            ]
        print(json.dumps(payload))
        return 0 if (entries or not parse_queens_leaderboard) else 2

    if entries is None:
        # Parser unavailable (running outside the bot's dep set).  Show the
        # raw text and let the human eyeball it.
        time_lines = re.findall(r'^\s*\d{1,2}:\d{2}\s*$', text, re.MULTILINE)
        print('(Parser not available — showing raw extracted text.)')
        print(f'Detected {len(time_lines)} time-shaped line(s) in the extracted text.')
        print()
        print('--- raw text ---')
        print(text)
        return 0 if time_lines else 2

    print(f'Parsed {len(entries)} leaderboard entries:')
    for entry in entries:
        badges = []
        if entry.no_hints:
            badges.append('no hints')
        if entry.no_mistakes:
            badges.append('no mistakes')
        badge_str = f' [{", ".join(badges)}]' if badges else ''
        you = ' (You)' if entry.is_you else ''
        mins, secs = divmod(entry.time_seconds, 60)
        print(f'  {entry.linkedin_name}: {mins}:{secs:02d}{badge_str}{you}')
    return 0 if entries else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--state', default=str(_STATE_PATH),
        help=f'Path to the saved session state (default: {_STATE_PATH}).')
    sub = parser.add_subparsers(dest='cmd', required=True)

    sub.add_parser('login', help='Interactive LinkedIn login.')

    fetch_p = sub.add_parser(
        'fetch', help='Fetch and parse the current leaderboard.')
    fetch_p.add_argument(
        '--headed', action='store_true',
        help='Show the browser window (default: headless).')
    fetch_p.add_argument(
        '--debug', action='store_true',
        help='Save a screenshot and the raw page text for inspection.')
    fetch_p.add_argument(
        '--json', dest='json_out', action='store_true',
        help='Emit a machine-readable JSON object on stdout instead of '
             'the human-friendly listing.  Used by the bot.')
    fetch_p.add_argument(
        '--auto-play', dest='auto_play', action='store_true',
        help="If the puzzle hasn't been played today, solve and place "
             'queens automatically before fetching the leaderboard.')
    fetch_p.add_argument(
        '--min-play-seconds', type=int, default=0,
        help='When auto-playing an unplayed puzzle, wait until at least this '
             'many seconds have elapsed on the game page before finishing.')
    fetch_p.add_argument(
        '--no-slow', dest='slow', action='store_false', default=True,
        help='Disable human-pacing delays during auto-play (testing only).')
    fetch_p.add_argument(
        '--day', choices=('today', 'yesterday'), default='today',
        help='Which results tab to fetch (default: today).')

    connect_p = sub.add_parser(
        'connect',
        help='Accept received LinkedIn connection requests for matching names.')
    connect_p.add_argument(
        '--name', dest='names', action='append', default=[],
        help='LinkedIn display name to match exactly. Repeat for multiple names.')
    connect_p.add_argument(
        '--headed', action='store_true',
        help='Show the browser window (default: headless).')
    connect_p.add_argument(
        '--debug', action='store_true',
        help='Save a screenshot and raw page text for inspection.')
    connect_p.add_argument(
        '--json', dest='json_out', action='store_true',
        help='Emit a machine-readable JSON object on stdout. Used by the bot.')
    connect_p.add_argument(
        '--dry-run', action='store_true',
        help='Only report matching received invitations; do not accept any.')

    solve_p = sub.add_parser(
        'solve', help='Read the puzzle grid, print the solution, never click.')
    solve_p.add_argument('--headed', action='store_true')
    solve_p.add_argument(
        '--debug', action='store_true',
        help='Save a screenshot of the puzzle page for inspection.')

    play_p = sub.add_parser(
        'play', help='Solve and place queens on today\'s puzzle (clicks).')
    play_p.add_argument('--headed', action='store_true')
    play_p.add_argument(
        '--no-slow', dest='slow', action='store_false', default=True,
        help='Click as fast as Playwright allows (testing; LinkedIn '
             'will trivially see this as a bot — do not use daily).')

    dump_p = sub.add_parser(
        'dump-grid',
        help='Diagnostic: list square-ish DOM elements so we can find the '
             'right grid-cell selector.')
    dump_p.add_argument('--headed', action='store_true')

    whoami_p = sub.add_parser(
        'whoami',
        help='Print the logged-in LinkedIn user\'s display name as JSON.')
    whoami_p.add_argument('--headed', action='store_true')

    args = parser.parse_args(argv)
    state_path = pathlib.Path(args.state).expanduser().resolve()

    if args.cmd == 'login':
        asyncio.run(cmd_login(state_path))
        return 0
    if args.cmd == 'fetch':
        return asyncio.run(cmd_fetch(
            state_path, headless=not args.headed, debug=args.debug,
            json_out=args.json_out, auto_play=args.auto_play, slow=args.slow,
            day=args.day, min_play_seconds=args.min_play_seconds))
    if args.cmd == 'connect':
        return asyncio.run(cmd_connect(
            state_path, args.names, headless=not args.headed,
            debug=args.debug, json_out=args.json_out, dry_run=args.dry_run))
    if args.cmd == 'solve':
        return asyncio.run(cmd_solve(
            state_path, headless=not args.headed, debug=args.debug))
    if args.cmd == 'play':
        return asyncio.run(cmd_play(
            state_path, headless=not args.headed, slow=args.slow))
    if args.cmd == 'dump-grid':
        return asyncio.run(cmd_dump_grid(
            state_path, headless=not args.headed))
    if args.cmd == 'whoami':
        return asyncio.run(cmd_whoami(
            state_path, headless=not args.headed))
    return 1


if __name__ == '__main__':
    sys.exit(main())

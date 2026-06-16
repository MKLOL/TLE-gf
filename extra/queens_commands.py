"""Subcommand implementations for the standalone Queens scraper.

Holds every ``cmd_*`` entry point except ``cmd_fetch`` (which lives in the
thin ``queens_scrape`` launcher alongside ``main``).  Each command opens its
own Playwright browser, drives helpers from the sibling modules, and prints
human- or JSON-formatted output.
"""
import asyncio
import json
import sys

from playwright.async_api import async_playwright

from queens_board import play_queens_puzzle, read_queens_grid
from queens_config import (
    _INVITATION_CANDIDATES_JS,
    _INVITATIONS_URL,
    _LOGIN_URL,
    _QUEENS_PLAY_URL,
    _REPO_ROOT,
    _SESSION_EXPIRED_URL_FRAGMENTS,
    normalize_queens_name,
)
from queens_linkedin import (
    _dismiss_consent_banner,
    _extract_linkedin_self_name,
    _find_matching_invitation,
    _load_received_invitations,
)
from queens_solver import solve_queens


async def _prompt(message):
    """Async wrapper around blocking ``input()``."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: input(message))


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

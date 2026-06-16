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

The implementation is split across sibling modules to keep each file small:

* ``queens_config``   — constants, regexes, page selectors, parser import.
* ``queens_solver``   — the pure Queens solver (no browser).
* ``queens_linkedin`` — LinkedIn nav, consent, leaderboard + invitation scrape.
* ``queens_board``    — puzzle-grid reading and auto-play orchestration.
* ``queens_commands`` — every subcommand except ``fetch`` (defined here).

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
import os
import pathlib
import re
import sys

# Make sibling helper modules importable whether this file is run as
# ``python extra/queens_scrape.py`` (script dir is auto on sys.path) or
# imported from elsewhere.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from playwright.async_api import async_playwright

from queens_board import (  # noqa: E402
    _puzzle_appears_already_played,
    play_queens_puzzle,
)
from queens_commands import (  # noqa: E402
    _prompt,
    cmd_connect,
    cmd_dump_grid,
    cmd_login,
    cmd_play,
    cmd_solve,
    cmd_whoami,
)
from queens_config import (  # noqa: E402
    _QUEENS_URL,
    _REPO_ROOT,
    _SESSION_EXPIRED_URL_FRAGMENTS,
    _STATE_PATH,
    parse_queens_leaderboard,
)
from queens_linkedin import (  # noqa: E402
    _click_see_more_until_done,
    _dismiss_consent_banner,
    _expand_to_full_leaderboard,
    _extract_leaderboard_text,
    _switch_results_day,
)


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

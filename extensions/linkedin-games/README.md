# TLE Games

Load this directory as an unpacked extension in Chrome or another Chromium
browser. See [GamesAPI.md](../../GamesAPI.md) for setup, tokens, import behavior,
API routes, and adding games.

No build step or third-party JavaScript dependencies are required.

- `manifest.json`: Manifest V3; active-tab reads and optional server access.
- `options.*`, `api.js`: local token setup and authenticated server requests.
- `leaderboard.js`, `extract.js`: isolated, credential-free page extraction.
- `popup.*`, `personal.js`: moderator preview/confirmation and member own-score posting.
- `akari.js`, `background.js`: optional Daily Akari completion capture and automatic posting.

Verification: Python service/token tests plus real HTTP/Discord integration run
with `python3 -m pytest tests/ -q`. Browser extraction and popup checks are in
`tests/games_extension_browser.py`, `tests/games_personal_browser.py`, and
`tests/games_akari_runtime_browser.py` (requires Playwright and its Chromium build).

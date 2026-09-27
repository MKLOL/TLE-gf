# TLE Games

Load this directory as an unpacked extension in Chrome or another Chromium
browser. See [GamesAPI.md](../../GamesAPI.md) for setup, tokens, import behavior,
API routes, and adding games.

No build step or third-party JavaScript dependencies are required.

For Chrome Web Store distribution, see the [draft publishing materials](../../docs/chrome-web-store/README.md).
Run `python3 extra/package_games_extension.py` to create a minimal upload ZIP.

- `manifest.json`: Manifest V3; active-tab reads and optional server access.
- `options.*`, `api.js`: local token setup and authenticated server requests.
- `leaderboard.js`, `extract.js`: isolated, credential-free page extraction.
- `popup.*`, `personal.js`: moderator preview/confirmation and member own-score posting.
- `akari.js`, `linkedin-result.js`, `linkedin-auto.js`: read-only personal completion capture.
- `auto-config.js`, `auto-submit.js`, `background.js`: default-on automatic posting and site permissions.
  Explicit opt-outs remain disabled; moderator bulk imports still need confirmation.

Verification: Python service/token tests plus real HTTP/Discord integration run
with `python3 -m pytest tests/ -q`. Browser extraction and popup checks are in
`tests/games_extension_browser.py`, `tests/games_personal_browser.py`, and
`tests/games_akari_runtime_browser.py`, `tests/games_linkedin_auto_browser.py`, and
`tests/games_linkedin_runtime_browser.py`, and `tests/games_auto_options_browser.py`
(requires Playwright and its Chromium build).
`tests/games_import_preview_browser.py` checks the compact confirmation prompt,
explicit Discord matches, unassigned rows, cancellation, and stale previews.

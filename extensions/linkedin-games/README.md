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
- `popup.*`, `personal.js`: leaderboard reading and member own-score posting.
- `review.*`, `review-window.js`: resizable moderator review window, searchable
  LinkedIn-to-Discord matches, and explicit import confirmation.
- `akari.js`, `linkedin-result.js`, `linkedin-auto.js`: read-only personal completion capture.
- `auto-config.js`, `auto-submit.js`, `background.js`: default-on automatic posting and site permissions.
  Explicit opt-outs remain disabled; moderator bulk imports still need confirmation.

Verification: Python service/token tests plus real HTTP/Discord integration run
with `python3 -m pytest tests/ -q`. Browser extraction and popup checks are in
`tests/games_extension_browser.py`, `tests/games_personal_browser.py`, and
`tests/games_akari_runtime_browser.py`, `tests/games_linkedin_auto_browser.py`, and
`tests/games_linkedin_runtime_browser.py`, and `tests/games_auto_options_browser.py`
(requires Playwright and its Chromium build).
`python3 -m tests.games_import_preview_browser` checks the review window and its
popup handoff, including search/filters, explicit Discord matches, unassigned
rows, cancellation, stale previews, and slow-server feedback. Browser tests use
synthetic identities and mocked submission endpoints; they do not import live results.
`python3 -m tests.games_review_runtime_browser` verifies the same handoff in an
actual Manifest V3 extension with disposable Chromium, a loopback API, and a
synthetic LinkedIn page. It checks window reuse/resizing and confirms that no
result submissions or extra LinkedIn requests occur.

**Read leaderboard** shows connection, reading, and matching progress before
opening the full review window. Closing that window keeps the preview available
through **Review matches…** in the toolbar popup until it expires. Searching or
filtering only changes the display; confirmation explicitly imports all rows.

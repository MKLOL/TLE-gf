# Game scores and leaderboard imports

For moderator imports, the Chrome extension reads a displayed connections
leaderboard, sends it to the bot for a preview, and opens a resizable review
window with the matched people, date, times, and badges. Results are saved only
when the user confirms the import inside that window.
Queens and Tango use the same import/rating code as Discord. Regular members can
post their own LinkedIn scores, and automatic Daily Akari posting includes every
field used by the existing rankings.

## Install and connect

1. Restart the bot with this version. Migrations 1.64.0 and 1.65.0 create games tokens and durable submission
   receipts. Migration 1.66.0 makes existing active games tokens permanent; expired
   or revoked tokens remain disabled, and complaint tokens are unchanged.
   The existing complaint HTTP listener serves the new routes on the same
   host and port; `COMPLAINT_API_ENABLED=0` disables both APIs.
2. In Chrome, visit `chrome://extensions`, enable Developer mode, choose **Load
   unpacked**, and select `extensions/linkedin-games` from this repository.
3. In the Discord server, run `;make-games-token`. The bot sends a token by DM
   that stays valid until you revoke it.
4. Open the extension’s **Settings**, enter the server URL and token, and save.
   The default server URL matches the existing complaint API configuration:
   `http://51.81.82.26:8080`. Use an HTTPS endpoint if configured. HTTP sends the
   token and results without transport encryption.
5. **Automatically import and post my LinkedIn scores** and **Automatically
   import and post my Daily Akari scores** are enabled by default. Save Settings
   and grant the requested site access, then reload any open game tabs once.
   Either option can be turned off immediately without a working server/token;
   existing explicit opt-outs
   remain off after updates. New site permission is requested when you save,
   never silently granted on update.
6. Complete Queens or Tango and leave its completed result visible. The extension
   registers and posts your own result without opening the leaderboard, copying,
   or clicking Import. LinkedIn labels currently need to be in English. Only
   today's puzzle (LinkedIn's Pacific calendar) is submitted automatically.
7. Enable **Pro Mode** in Daily Akari to include accuracy. Completing a daily
   puzzle posts and registers it automatically; archive puzzles are skipped.
   The extension's badge and popup show whether posting succeeded.
8. Manual personal posting is still available: open the Queens/Tango completed
   results page or connections leaderboard and click **Read my LinkedIn score**,
   then **Post my score**.
   Moderators use **Read leaderboard** to open a larger review window for everyone
   on the leaderboard. Connection, reading, and matching stages show immediate
   progress, with elapsed time when the server is slow. The review places LinkedIn
   names and Discord accounts in separate columns and includes search and
   matched/unassigned filters, your pending result, the puzzle date, times, and
   badges. Filters only change the display: confirmation always includes all
   previewed results. Cancelling imports nothing. Bulk imports always require
   confirmation.

The game must be enabled (`;meta config enable queens` / `tango` / `akari`) and
have a configured channel (`;queens here` / `;tango here` / `;akari here`). Personal
LinkedIn posting, including automatic posting, requires registering your name
once using `;queens register NAME` or `;tango register NAME`; both share the same
identity. Bulk leaderboard imports do not require the importer to register.
The leaderboard’s **You** row belongs to the token owner, so use your own token
when viewing your LinkedIn account.

One moderator token covers every enabled LinkedIn game in its Discord server,
including games added later. It is not tied to Queens or Tango. Any server member can create a token and submit only their own scores.
Bulk leaderboard imports require the configured Admin or Moderator role, checked
on every import request. Demotion removes bulk access; departure removes all access. Games tokens grant no complaint API access, and complaint tokens
cannot import games.

## Commands

| Command | Purpose |
| --- | --- |
| `;make-games-token [days]` | DM a personal token; permanent by default, or valid for 1–365 days if specified. |
| `;games-tokens` | List your active token IDs and expiry times in this server; permanent tokens show **Never**. |
| `;revoke-games-token <id>` | Revoke one of your tokens in this server. |

Only SHA-256 digests persist in SQLite. Failed DM delivery revokes the new
credential. The extension stores the token locally in the Chrome profile, never
in synced storage or the LinkedIn page. **Forget token** removes the Chrome copy;
the Discord revoke command invalidates it on the server.

## Personal scores and Akari ranking data

Personal posts go to the configured game channel under the token owner's Discord
identity, with mentions disabled. Channel visibility/posting permissions, game
feature flags, membership, and game bans are enforced. Existing LinkedIn identity
and anonymous display settings still apply. Personal LinkedIn scores are clean,
including when the You row has no badges. No other person's score is sent by the
personal button or automatic posting. Automatic readers do not navigate, click,
change game state, or invoke LinkedIn's sharing/copy features.

Akari sends the puzzle number and date, integer solve time in seconds, rounded
accuracy percentage, and a separate `is_perfect` boolean. Daily Akari reports raw
accuracy as a fraction; the extension uses the site's `Math.round(accuracy * 100)`
for the percentage, but only `accuracy === 1` means perfect. An imperfect result
that rounds to 100% stays imperfect. This preserves the accuracy/time beta rating,
the perfect/accuracy/time default ranking, weekly points, and time-only views.
The server generates a canonical share, validates it with the existing game
parser, saves its raw content under the owner's identity, and recomputes ratings
through the same path as ordinary Discord submissions.

A durable receipt permits one personal post per user/game/puzzle across token
changes, reloads, and bot restarts. Retries return the same receipt; they cannot
replace a different existing score. If a send times out, retry promptly (reload
Akari, or press **Post my score** again). The server recovers by Discord nonce or
reuses the same nonce within two minutes. After an older uncertain send that
cannot be found in the next 100 channel messages, it asks for moderator review
instead of risking duplicate posts. Deleting a post keeps a removed receipt; retries report that removal instead of
claiming successful registration or reposting it.

## Import behavior

- Importer registration is optional. Confirmation saves the completed rows for
  registered players and unassigned names, including the importer’s **You** row.
- Names resolve through the existing shared LinkedIn registration. Anonymous
  links keep their existing public label. Unlinked results are saved for later
  registration. Bans and per-result rating opt-outs continue to apply.
- If the importer is unregistered and the own row displays only **You**, its
  result is kept as pending data belonging to that token owner. Registering
  later claims it under the new LinkedIn name. Different importers’ **You** rows
  remain separate; an unregistered importer does not block everyone else’s rows.
- The explicit **You** row is treated as no hints and no mistakes. Everyone
  else’s actual badges are preserved; missing badges do not imply a clean solve.
- Exact duplicates are skipped. A changed time or badge status replaces only
  that player’s result for that puzzle, just like a manual import.
- The extension reads only loaded, displayed leaderboard rows. Expand the
  leaderboard before reading if LinkedIn has hidden additional results.
- The leaderboard’s puzzle identity determines its date. Older layouts display
  a puzzle number; Today/Yesterday layouts use the URL’s game edition and the
  selected tab. The extension refuses ambiguous or loading views. Current page
  labels must be in English. Direct HTTP clients may supply a date without a
  puzzle number.
- Previews expire after ten minutes or a bot restart. A new preview replaces an
  unconfirmed preview for the same token. Closing the review window retains
  the current preview for that Chrome session; **Review matches…** in the toolbar
  popup reopens it. A reused review window displays the new preview. Cancel
  imports nothing and cannot discard a newer preview opened elsewhere.
- Changing registrations, bans, privacy decisions, or the configured channel
  between preview and confirmation requires a fresh preview.
- Repeating Confirm returns the original receipt for ten minutes. After a
  restart or expiry, make a new preview; duplicate-result detection still applies.

## HTTP contract

Use `Authorization: Bearer <games token>`. IDs are scoped to the token’s guild and
user, never supplied by the client. All responses are JSON and `Cache-Control:
no-store`. The listener retains the complaint API’s 16 KiB body cap.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/v1/games` | Game catalog, calendar anchors, Pacific date, enabled/access flags and connected identity. |
| POST | `/v1/games/results` | Register and post only the token owner’s personal result. |
| POST | `/v1/games/imports/preview` | Parse and resolve a leaderboard without saving results. |
| POST | `/v1/games/imports/{preview_id}/confirm` | Commit the server-held preview; body is `{}`. |

Personal result body (exactly these fields):

```json
{
  "game": "akari",
  "puzzle_date": "2026-09-26",
  "puzzle_number": 629,
  "time_seconds": 39,
  "accuracy": 100,
  "is_perfect": false
}
```

For LinkedIn use `accuracy: 100` and `is_perfect: true`. A result returns
`registered`, `posted`, `duplicate`, `game`, `puzzle_number`, and `message_url`.
The client cannot supply a Discord user, guild, channel, or leaderboard to this
route. Conflicting results return 409 and must be corrected through moderation.

Preview body (exactly these fields):

```json
{
  "game": "tango",
  "puzzle_date": "2026-09-26",
  "puzzle_number": null,
  "leaderboard": "Example Player\n🤓💎 No hints & no mistakes!\n0:10\nYou\n0:12"
}
```

`puzzle_number` may be null when the page does not show it; if supplied, it must
match the date. Dates must fall within the published history. Leaderboards are
bounded to 12,000 characters and 200 rows, with at most one You row. Duplicate
player rows are rejected. The response includes `preview_id`, `expires_at`,
`rows`, `registered`, `unresolved`, `skipped`, and the game/puzzle identity.
Rows include display names, time in seconds, registration/rating state, and
`no_hints` / `no_mistakes`. Confirm returns counts of newly saved registered and
unresolved results, plus `unchanged`.

Errors: 400 invalid input, 401 invalid/expired/revoked token, 403 no game access,
404 unknown/expired/other-token preview, 409 stale preview or unconfigured game
channel, 413 oversized body, 415 wrong content type, 408 body timeout, 503 bot
unavailable or API busy. Preview handles are owner-bound and cannot be used as
credentials. Confirmation cannot replace the preview’s leaderboard or date.

## Adding a game

Add its `GameDef` with a `LinkedInDef` calendar anchor and delegated-admin key to
`Minigames.GAMES`, enable its feature flag, and configure its channel. The API
discovers LinkedIn-capable games from that registry; the extension obtains their
slugs and calendars from `/v1/games`, so neither maintains a separate game list.
Games with the same name/time/badge leaderboard layout need no extractor changes.
Different result formats need a parser/scoring adapter and captured-page tests;
a guesses-based game must not silently be treated as a timed game.

The extension uses [Chrome’s activeTab permission](https://developer.chrome.com/docs/extensions/develop/concepts/activeTab)
for user-initiated page reads and optional access to the configured server for
[extension-origin requests](https://developer.chrome.com/docs/extensions/develop/concepts/network-requests).
The API does not need public CORS access and the LinkedIn page never receives the token.

## Verification scope

The shared leaderboard structure was captured from a live Pinpoint connections
page, including LinkedIn’s same-origin preload frame. Timer/badge element placement
was checked against LinkedIn’s current public renderer. Isolated Chromium tests
exercise that captured structure with timed results and renderer-matching badge
markup, hidden badges, reactions, unplayed rows, review-window confirmation, and actual
Manifest V3 loading. The Python suite covers the real importer and HTTP server.
A completed Queens leaderboard was subsequently captured from Brave on
2026-09-26. Its sanitized regression fixture covers the Today/Yesterday tabs,
URL puzzle identity, timed-row and badge structure, and `-:--` unplayed results.
Player identities and solve times in the fixture are synthetic.
Personal posting tests execute the real readers against Queens/Tango completion
views and require confirmation before submitting.

Daily Akari's public renderer (`main-2BUVX3DN.js`, inspected 2026-09-26) receives
`animTimeBtn`, `timeResult`, and fractional `accuracy` from the game frame. Its
outgoing level message contains `dailyDateKey` and `isArchive`. A fresh isolated
browser verified those messages and the `/akari.html` → `/akari` redirect. Capture
uses only that frame and its parent, checks origin/source, and reads only the
known Pro Mode setting. It never starts/resets a puzzle or presses Share. Browser
fixtures exercise perfect, imperfect, rounded-100%, hours, duplicates, archives,
source spoofing, and non-Pro Mode. A disposable Chromium profile also exercises the actual isolated content script,
service worker, and a loopback HTTP server, including frame reloads. This test
grants host permissions in a temporary extension copy; the native permission
dialog is not automated in the isolated runtime tests.

Automatic LinkedIn capture is additionally tested against renderer-derived
completed-result fixtures: normal, golden-chiclet and beginner variants; failed
and playing screens; averages and other players; ambiguous/missing scores;
hidden views; and game mismatch. A real MV3 test exercises default-on behavior,
SPA navigation from the feed, preload frames, unchanged-DOM deduplication, and
explicit opt-out while a previously injected reader remains active. Native site
permission prompts are not automated. The reader waits for a complete supported
results view instead of guessing a solve from a ticking timer or unsupported UI.

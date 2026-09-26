# Game scores and leaderboard imports

The Chrome extension reads a displayed connections leaderboard, sends it to the
bot for a preview, and shows the matched people, date, times, and badges. Results
are saved only when the user presses **Confirm import** inside the extension.
Queens and Tango use the same import/rating code as Discord. Regular members can
post their own LinkedIn scores, and automatic Daily Akari posting includes every
field used by the existing rankings.

## Install and connect

1. Restart the bot with this version. Migrations 1.64.0 and 1.65.0 create games tokens and durable submission
   receipts. The existing complaint HTTP listener serves the new routes on the same
   host and port; `COMPLAINT_API_ENABLED=0` disables both APIs.
2. In Chrome, visit `chrome://extensions`, enable Developer mode, choose **Load
   unpacked**, and select `extensions/linkedin-games` from this repository.
3. In the Discord server, run `;make-games-token`. The bot sends a token by DM.
4. Open the extension’s **Settings**, enter the server URL and token, and save.
   The default server URL matches the existing complaint API configuration:
   `http://51.81.82.26:8080`. Use an HTTPS endpoint if configured. HTTP sends the
   token and results without transport encryption.
5. For your own Queens/Tango result, open its connections leaderboard and click
   **Read my LinkedIn score**, then **Post my score**. Moderators can also use
   **Read leaderboard** → **Confirm import** to import everyone's loaded results.
6. For Daily Akari, enable **Automatically post completed Daily Akari scores**
   in Settings and allow access to `dailyakari.com`. Enable **Pro Mode** in the
   game's settings, then reload the game once. Completing a daily puzzle posts
   and registers it automatically; archive puzzles are skipped. The extension's
   badge and popup show whether posting succeeded.

The game must be enabled (`;meta config enable queens` / `tango` / `akari`) and
have a configured channel (`;queens here` / `;tango here` / `;akari here`). Register your own LinkedIn
name once using `;queens register NAME` or `;tango register NAME`; both share the
same identity. The leaderboard’s **You** row belongs to the token owner, so use
your own token when viewing your LinkedIn account.

One moderator token covers every enabled LinkedIn game in its Discord server,
including games added later. It is not tied to Queens or Tango. Any server member can create a token and submit only their own scores.
Bulk leaderboard imports require the configured Admin or Moderator role, checked
on every import request. Demotion removes bulk access; departure removes all access. Games tokens grant no complaint API access, and complaint tokens
cannot import games.

## Commands

| Command | Purpose |
| --- | --- |
| `;make-games-token [days]` | DM a token; default 30 days, range 1–365; personal to the issuing member. |
| `;games-tokens` | List your active token IDs and expiry times in this server. |
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
personal button.

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

- Names resolve through the existing shared LinkedIn registration. Anonymous
  links keep their existing public label. Unlinked results are saved for later
  registration. Bans and per-result rating opt-outs continue to apply.
- The explicit **You** row is treated as no hints and no mistakes. Everyone
  else’s actual badges are preserved; missing badges do not imply a clean solve.
- Exact duplicates are skipped. A changed time or badge status replaces only
  that player’s result for that puzzle, just like a manual import.
- The extension reads only loaded, displayed leaderboard rows. Expand the
  leaderboard before reading if LinkedIn has hidden additional results.
- The displayed leaderboard’s puzzle number determines its date. The extension
  refuses to guess when it cannot read that number. Current page labels must be
  in English. Direct HTTP clients may supply a date without a puzzle number.
- Previews expire after ten minutes or a bot restart. A new preview replaces an
  unconfirmed preview for the same token. Closing/reopening the popup retains
  the current preview for that Chrome session. Cancel imports nothing.
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
markup, hidden badges, reactions, unplayed rows, popup confirmation, and actual
Manifest V3 loading. The Python suite covers the real importer and HTTP server.
No played Queens/Tango session was available for a live end-to-end check.

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
dialog is not automated. No personal LinkedIn tab was changed.

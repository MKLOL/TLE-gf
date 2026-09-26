# LinkedIn games imports

The Chrome extension reads a displayed connections leaderboard, sends it to the
bot for a preview, and shows the matched people, date, times, and badges. Results
are saved only when the user presses **Confirm import** inside the extension.
Queens and Tango are supported through the same import/rating code as Discord.

## Install and connect

1. Restart the bot with this version. Migration 1.64.0 creates the games token
   table. The existing complaint HTTP listener serves the new routes on the same
   host and port; `COMPLAINT_API_ENABLED=0` disables both APIs.
2. In Chrome, visit `chrome://extensions`, enable Developer mode, choose **Load
   unpacked**, and select `extensions/linkedin-games` from this repository.
3. In the Discord server, run `;make-games-token`. The bot sends a token by DM.
4. Open the extension’s **Settings**, enter the server URL and token, and save.
   The default server URL matches the existing complaint API configuration:
   `http://51.81.82.26:8080`. Use an HTTPS endpoint if configured. HTTP sends the
   token and results without transport encryption.
5. Open a Queens or Tango connections leaderboard, click the extension, and
   press **Read leaderboard**. Check the preview and press **Confirm import**.

The game must be enabled (`;meta config enable queens` / `tango`) and have a
configured channel (`;queens here` / `;tango here`). Register your own LinkedIn
name once using `;queens register NAME` or `;tango register NAME`; both share the
same identity. The leaderboard’s **You** row belongs to the token owner, so use
your own token when viewing your LinkedIn account.

One moderator token covers every enabled LinkedIn game in its Discord server,
including games added later. It is not tied to Queens or Tango. Creating and
using tokens requires the server’s configured Admin or Moderator role.
Every request rechecks membership and import permissions; demotion or departure
removes access. Games tokens grant no complaint API access, and complaint tokens
cannot import games.

## Commands

| Command | Purpose |
| --- | --- |
| `;make-games-token [days]` | DM a token; default 30 days, range 1–365; Admin/Moderator only. |
| `;games-tokens` | List your active token IDs and expiry times in this server. |
| `;revoke-games-token <id>` | Revoke one of your tokens in this server. |

Only SHA-256 digests persist in SQLite. Failed DM delivery revokes the new
credential. The extension stores the token locally in the Chrome profile, never
in synced storage or the LinkedIn page. **Forget token** removes the Chrome copy;
the Discord revoke command invalidates it on the server.

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
| POST | `/v1/games/imports/preview` | Parse and resolve a leaderboard without saving results. |
| POST | `/v1/games/imports/{preview_id}/confirm` | Commit the server-held preview; body is `{}`. |

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

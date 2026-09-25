# Complaint automation

The bot starts a small HTTP API on `0.0.0.0:8080` after database initialization,
accepting remote connections on port 8080. Every route requires a bearer token.
It supports reading, resolving, and reopening complaints in the token's server.
It does not expose SQL, other tables, shell commands, or GitHub credentials.
Your automation makes/tests/commits the fix in its checkout, pushes the commit,
then calls the resolve endpoint with its GitHub link and a user-facing summary.

## Discord commands

An admin has the configured `Admin` role or Discord's `Administrator` permission.
The hidden commands are:

- `;maketoken [days]`: send a new bearer token by DM. Default: 30 days; range: 1–365.
- `;tokens`: list up to 100 active token IDs, issuers, and expiration times in this server.
- `;revoketoken <id>`: revoke a token in this server immediately.

Only a SHA-256 digest is stored in SQLite; the full token is shown once by DM.
If delivery fails, the token is revoked. Tokens survive bot restarts, expire,
and stop working when their issuer leaves or loses admin access. Every request
checks the issuer's current cached Discord membership/roles (fetching on cache
miss); writes recheck authorization after waiting for the mutation lock.
The API refuses requests while the bot is disconnected.

Complaint commands:

```text
;complain resolve <id> <github_commit_url> <resolution summary>
;complain reopen <id>
;complain list [open|resolved|all]
```

Resolve/reopen require admin access. Resolved complaints leave the default open
list and retain the original report, resolution, resolver, time, and commit URL.
Reopening preserves old resolutions in the audit history. Withdrawal/removal
continues to hide a complaint and stops any pending notification retries.

Resolving replies to the original complaint, mentioning only its author. The
reply includes the summary and commit link. Missing/deleted/inaccessible source
messages fall back to a DM. If delivery fails, resolution still succeeds and
`notification_status` reports `failed`; the bot retries every five minutes, up
to five total attempts, including after restart. Repeating the same resolve
request retries failed delivery manually, even after automatic retries stop.
An already delivered resolution is not sent again on normal retries. Delivery
is at least once across process crashes: a crash after Discord accepts a reply
but before its receipt is saved can cause a duplicate.

## HTTP contract

All routes require `Authorization: Bearer <token>`. Tokens are never accepted
in URLs. Responses are JSON with `Cache-Control: no-store`; error responses
have an `error` string. All reads/writes are scoped to the token's server.
Discord IDs are JSON strings; complaint IDs and pagination cursors are integers.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/v1/complaints?status=open&limit=50&before=123` | List complaints, newest ID first. |
| GET | `/v1/complaints/123` | Complaint details and latest 100 audit events, newest first. |
| POST | `/v1/complaints/123/resolve` | Resolve and attempt author notification. |
| POST | `/v1/complaints/123/reopen` | Reopen; send an empty JSON object `{}`. |

List parameters are optional: `status` is `open` (default), `resolved`, or `all`;
`limit` is 1–100 (default 50). Pass the returned `next_before` as `before` for
the next page, until it is `null`. Removed/withdrawn complaints are excluded.

Resolve body (exactly these fields):

```json
{
  "resolution": "Fixed the timer parsing for short recordings.",
  "commit_url": "https://github.com/MKLOL/TLE-gf/commit/0123456789abcdef0123456789abcdef01234567"
}
```

The summary must contain 1–1500 characters. The URL must be an HTTPS github.com
commit link with a 7–40 digit hexadecimal SHA and at most 300 characters. The
API validates the URL's form; the caller must ensure the commit exists and was
pushed. Both mutation routes return `{"complaint": {...}}`, including `status`,
`resolution`, `commit_url`, `resolved_at`, `resolved_by`, `notification_status`,
`notification_link`, and `notification_attempts`.

An identical resolve is idempotent. A different resolution of an already
resolved complaint returns 409; reopen it first. Repeated reopen is idempotent.
Audit events identify the acting admin and token ID without storing token material.

Error codes: 400 invalid input, 401 invalid/expired/revoked token, 403 issuer no
longer an admin, 404 missing/removed/other-server complaint, 409 resolution
conflict, 413 body over 16 KiB, 415 non-JSON write, 408 body timeout, 503 bot
unavailable or API busy. Check `notification_status` even after HTTP 200.

## Connecting automation

Connect directly to the bot's public IP on port 8080. If an existing environment
file sets `COMPLAINT_API_HOST=127.0.0.1`, change it to `0.0.0.0` and restart the
bot. The host/provider firewall must allow inbound TCP port 8080.

```sh
export COMPLAINT_API_URL="http://51.81.82.26:8080"
```

Set `COMPLAINT_TOKEN` privately in your client environment, then query:

```sh
curl --fail-with-body -H "Authorization: Bearer ${COMPLAINT_TOKEN}" \
  "${COMPLAINT_API_URL}/v1/complaints?status=open"

curl --fail-with-body -X POST \
  -H "Authorization: Bearer ${COMPLAINT_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @resolution.json \
  "${COMPLAINT_API_URL}/v1/complaints/123/resolve"
```

Write the body shown above to `resolution.json`. Keep the token out of source
control and request URLs. HTTP does not encrypt the bearer token or complaint
data in transit. To add transport encryption, terminate HTTPS at a reverse proxy
and point `COMPLAINT_API_URL` at its HTTPS hostname. Set the bot's listening
interface to `127.0.0.1` if only a local proxy should reach the API.

Environment settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `COMPLAINT_API_ENABLED` | `1` | Set `0` to disable HTTP; Discord resolution/retries remain active. |
| `COMPLAINT_API_HOST` | `0.0.0.0` | Listening interface; accepts remote IPv4 connections. |
| `COMPLAINT_API_PORT` | `8080` | TCP port, 1–65535. |

Docker's existing host-network setup uses the same defaults. With bridge
networking, publish the container's API port with `-p 8080:8080`.
`--nodb` disables HTTP and notification
workers. Shutdown closes the HTTP listener and cancels the notification worker.
The server uses aiohttp, already installed as a discord.py dependency.

# TLE Games privacy notice — draft for operator review

Prepared September 26, 2026. Review the operator's contact and retention details,
then remove this draft heading before hosting the notice publicly.

TLE Games connects supported puzzle results to a Discord community running the
TLE-gf bot. Your community's bot operator runs the configured API server and
manages its saved game records. Contact that operator through your Discord
server's administrators or moderators for questions or data-removal requests.

## Information the extension handles

- The server address and personal games token you enter in Settings.
- Your automatic-posting preferences, recent posting status, and pending import
  or personal-score previews, including player names and Discord identifiers
  returned by your community's server.
- Your completed Queens, Tango, or Daily Akari puzzle number, date, and solve
  time. Akari submissions also include accuracy and perfect-solve status.
- If you choose to read a connections leaderboard as a moderator, the visible
  player names, solve times, and displayed no-hints/no-mistakes badges.

The extension examines supported page URLs locally to identify the game and
reads the relevant result elements. It reads Daily Akari completion messages and
Pro Mode settings. It does not send full page URLs, feed posts, a browsing
history, LinkedIn passwords, or LinkedIn session cookies to the bot server.

## How information is used and shared

Your token authenticates requests to the server you configure. Personal results
are credited to the Discord account that created that token. With automatic
posting enabled, completion sends your own score to that server and the bot
posts it to the configured Discord game channel. Viewers of that channel can
see those posts. These results also contribute to the community's game records
and rankings, subject to its rating and privacy settings.

Clicking a moderator leaderboard read button sends the visible results to the
server to match identities and create a preview. Confirmation saves the import;
cancelling does not save those leaderboard results. Unlinked player names may
be stored when a moderator confirms an import so they can be linked later.

The extension includes no advertising, analytics, or sale-of-data functionality.
Result requests go to the configured bot server; that server uses Discord to
post scores. Discord and the game websites have their own privacy policies.

## Storage and retention

Settings and the token are stored locally in the browser profile, not in browser
sync storage. Content scripts cannot read the token. Pending extension previews
use session storage. Server-side unconfirmed import previews normally expire
after ten minutes or a bot restart.

The bot stores a hash of a games token, its owner and server identifiers, and
expiry/revocation information. Game results, player links, rankings, and posting
receipts persist in the bot's database until removed by its operator or relevant
bot workflows. Posted Discord messages remain until deleted. Disabling or
uninstalling the extension does not delete previously saved server records or
Discord messages. Operator backups may retain older records under the
community's backup policy; ask the operator about those practices.

## Your choices

LinkedIn automatic posting is off until you explicitly enable and save it;
upgrades from the old default-on setting require this opt-in again.
You can turn off automatic posting separately for LinkedIn and Akari, decline
site permissions, cancel a moderator import before confirmation, or uninstall
the extension. **Forget token** removes the extension's saved token. In Discord,
use `;games-tokens` and `;revoke-games-token <id>` to revoke server access.
For existing results, identity links, or deletion requests, contact the bot
operator in your Discord community.

## Connection security

The publication build must use an HTTPS server to encrypt tokens and results in
transit. The current development build also accepts plain HTTP, which is not
encrypted; it must be updated before this notice is published for a store release.
Local token storage is protected by the browser profile's access controls rather
than a separate extension-managed encryption layer.

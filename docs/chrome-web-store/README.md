# Publish TLE Games

This folder contains draft listing material. Nothing has been uploaded or
published. The ZIP is suitable for creating a dashboard draft, but **do not
submit it for review yet**: the current default API uses plain HTTP, the privacy
notice needs a public URL, and reviewers need access to a test Discord server.

## Create the draft

1. Run `python3 extra/package_games_extension.py`. It writes
   `dist/tle-games-1.2.1-draft.zip` with `manifest.json` at the ZIP root.
2. Open <https://chrome.google.com/webstore/devconsole>, click **Add new item**
   (or **New item**), and upload that ZIP.
3. Fill in **Store listing** using the description below. Choose English and
   the closest available games/tools category.
4. Upload `extensions/linkedin-games/icons/icon-128.png` as the icon,
   `promo-440x280.png` as the small promotional image, and
   `screenshot-settings-1280x800.png` as the screenshot. The screenshot shows
   the actual options screen in a disposable browser, with no token entered.
5. Complete **Privacy practices** using the declarations below.
6. Under **Distribution**, select **Free** and **Unlisted**. Anyone with the
   eventual store link can install; authentication still controls bot access.
7. Save the draft. Complete the remaining items below before **Submit for review**.
   Select deferred/manual publishing if you want to inspect the approved listing
   before making the install link available.

## Detailed description (paste into Store listing)

Post your daily puzzle scores to your Discord community with TLE Games.

Connect the extension to a Discord server running TLE-gf using a personal games
token from the bot. Your own completed LinkedIn Queens and Tango results and
Daily Akari scores can then be registered and posted automatically. Each game
site has its own automatic-posting switch, enabled by default during setup.

Server moderators can also read a visible LinkedIn connections leaderboard,
review the players, times, and hint/mistake badges, and confirm a bulk import.
Other players' results are saved only after that confirmation. Your own LinkedIn
result is treated as having no hints or mistakes under the community's scoring
rules. Other players keep their displayed badge status.

Requirements:
- Membership in a Discord server running a compatible TLE-gf bot.
- A personal token obtained with ;make-games-token in that server.
- A linked LinkedIn name for personal LinkedIn submissions.
- English LinkedIn game labels and Pro Mode in Daily Akari.
- Site permissions granted during setup; reload already-open game tabs once.

Only Queens, Tango, and Daily Akari are currently supported. Akari archive
puzzles are excluded from automatic posting. The extension reads results; it
does not play puzzles, make moves, or post to LinkedIn.

This is an independent community tool, not an official LinkedIn, Daily Akari,
or Discord product.

## Privacy practices

**Single purpose:** Register daily puzzle results with the user's TLE-gf Discord
community, including automatic personal submissions and confirmed moderator
leaderboard imports.

**activeTab:** Read the current LinkedIn connections leaderboard only when the
user presses a read button in the extension.

**scripting:** Run packaged result readers on supported game pages. Register or
remove the automatic readers according to the user's settings and site grants.
No executable code is downloaded from the server.

**storage:** Keep the server URL, personal games token, automatic-posting
preferences, and latest posting status in local extension storage. Keep pending
previews in session storage. Credentials are restricted to trusted extension
contexts and are not synced to the user's Google account.

**Host access:** Send authenticated result requests to the user-configured
TLE-gf server. The broad optional host patterns allow communities to configure
their own server; only the selected origin is requested on Save. Optional
LinkedIn and Daily Akari access enables automatic completion detection. The
LinkedIn reader handles transitions into games from the feed and the embedded
preload document; it does not extract feed posts. Daily Akari access reads
completion messages and Pro Mode status. Game pages do not receive the bot token.

**Remote code:** No. All JavaScript is bundled. The server returns JSON data.

**Data categories:** Disclose personally identifiable information (player names
and Discord identifiers), authentication information (the games token), and
website content (scores and leaderboard badge data). The extension examines
supported page URLs locally to recognize games; it does not send a browsing
history or complete page URLs to the API. Check the dashboard's current category
definitions before certifying these choices.

The extension has no advertising or analytics SDKs. Its data is used for the
requested game import, posting, and ranking features. The publisher must confirm
that their server operation also matches the draft privacy notice before making
the dashboard certifications.

## Finish before submission

- Configure a valid HTTPS endpoint for the existing API. Update the extension's
  default URL and public setup instructions, reject non-local plain HTTP in the
  release, and regenerate the ZIP and settings screenshot. Do not simply change
  `http` to `https` without configuring and testing TLS.
- Review `privacy.md`, confirm the operator's contact and retention practices,
  and host it at a public, accessible URL. Put that URL in **Privacy practices**.
  This local file is not a public privacy-policy URL.
- Provide review instructions in **Test instructions**, including the configured
  HTTPS server and access to a dedicated test Discord community with games
  enabled. Supply a limited test token in the dashboard's private test field if
  needed; never put a real token in this repository, ZIP, or screenshots.
- Verify the release against the deployed API and a completed Queens/Tango
  result. Existing tests use captured Pinpoint structure and renderer-derived
  timed fixtures; a live Queens/Tango end-to-end check is still outstanding.
- Revoke test access after review. Publish manually after approval if deferred
  publishing was selected, then share the store link with the community.

## Reviewer steps (complete server/access details in the private dashboard)

1. Install the extension and open Settings. Enter the supplied HTTPS server and
   test games token; save and grant the requested site permissions.
2. Complete today's Queens or Tango in English, with the test Discord account's
   LinkedIn name registered. Leave the result visible. Check the extension's
   status and the configured test channel for the personal score post.
3. Complete today's Daily Akari with Pro Mode enabled. Check that time, accuracy,
   and perfect status reach the test server and a score appears in the channel.
4. With a test moderator token, open a connections leaderboard and use **Read
   leaderboard**. Inspect the preview and cancel; then repeat and confirm.
5. Turn off either automatic option in Settings. Further results for that site
   must not be posted automatically.

## Assets and sources

`python3 extra/render_games_store_assets.py` regenerates the icons, promo, and
actual settings screenshot using Playwright Chromium in a temporary profile.
It does not use a personal browser, connect to LinkedIn, or send an API request.

- [Package preparation](https://developer.chrome.com/docs/webstore/prepare)
- [Dashboard upload and review](https://developer.chrome.com/docs/webstore/publish)
- [Image requirements](https://developer.chrome.com/docs/webstore/images)
- [Privacy fields](https://developer.chrome.com/docs/webstore/cws-dashboard-privacy)
- [Secure data handling](https://developer.chrome.com/docs/webstore/program-policies/user-data-faq)
- [Unlisted distribution](https://developer.chrome.com/docs/webstore/cws-dashboard-distribution)

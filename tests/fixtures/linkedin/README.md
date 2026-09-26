# LinkedIn leaderboard captures

`pinpoint-leaderboard.html` was captured from the user's displayed connections
leaderboard on 2026-09-26 with read-only JavaScript, after explicit authorization.
LinkedIn hosts it inside a same-origin `https://www.linkedin.com/preload/` iframe
even though the outer tab URL is `/games/pinpoint/results/leaderboard/connections/`.

Sanitization retained the section/player structure and classes, replaced names
with `Player N`, and removed images, buttons, SVG, profile URLs, IDs, and handlers.
The original guessing scores and the `You` marker remain. The raw capture is not
part of the repository. No LinkedIn interaction or account mutation was performed.

Browser checks use the unmodified captured scores to verify that a guesses-based
game is rejected by the timed importer. Additional edge-case tests explicitly
replace scores with synthetic times and inject badge cases into that captured
structure. Those adaptations test edge cases, not a played Queens/Tango session.

The current first-party game renderer independently confirms this is the shared
timed-game structure. Its `connections-leaderboard-player` component places
`completionBadgeData` in `.pr-connections-leaderboard-player__subtitle` within
the content column: separate spans contain the badge emoji and
`.pr-connections-leaderboard-player__subtitle-copy` text. `isHintFree` and
`isMistakeFree` independently select the two badges. Its reaction controls live
outside that column. Unplayed rows instead carry a `__nudge-button` without a
`__score`; privacy-hidden rows carry `__container-blur`.

Inspected public assets (2026-09-26):
- [Game component and templates](https://static.licdn.com/aero-v1/sc/h/cl0bl4ppyv1aap8xm2zre9l9i)
- [English badge labels](https://static.licdn.com/aero-v1/sc/h/3lq4mcd5krrb4l0yob2yjxi07)

Full bundled code is not copied into this repository. Tests recreate the small
badge structure and use synthetic times with the sanitized captured rows.

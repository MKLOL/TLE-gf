# Complaint triage notes

Working notes for the complaint queue (`;complain list`, `GET /v1/complaints`).
Source snapshot: 51 open complaints, #79 – #308, fetched 2026-09-25.

Only findings verified against the source at the time of writing are recorded
as facts; everything else is marked as needing reporter input.

## Resolved meanings (what a vague complaint actually referred to)

### #255 "useless command help" → `;help mg akari vs`

Confirmed by the reporter. `;mg akari vs` is declared at
`tle/cogs/_mgcmds_akari.py:151` with a `brief` and a `usage` string and **no
docstring**, so discord.py's help renders only:

```
Compare two or more players
;mg akari vs @user1 @user2 [@user3 ...] [filters...] [raw|all]
```

Nothing explains what `[filters...]` accepts (`+beta`, `+time`, `+test`,
`+exclude=`, `+include=`, `+dow=`, `d>=date`, `d<date`) or what `raw` and
`all` do.

This is systemic, not one command. Docstring coverage in the minigame command
modules:

| Module | Commands | With docstrings |
|---|---|---|
| `_mgcmds_akari.py` | 36 | 1 |
| `_mgcmds_akarislash.py` | 22 | 0 |
| `_mgcmds_queens.py` | 38 | 1 |
| `_mgcmds_tango.py` | 37 | 0 |
| `_mgcmds_guessgame.py` | 14 | 2 |
| `_mgcmds_*slash*.py` | 54 | 0 |

Two separate pieces of work:

1. Document the shared filter language once and point every `[filters...]`
   usage string at it (one help topic, e.g. `;help filters`).
2. Give the commands people actually run a real docstring.

Fixing only `vs` answers the complaint as filed but leaves the other ~195.

## Implementation plans

### #274 "make it possible to upsolve virtuals" — feasible, small

**The data is already in hand.** `_upsolve_impl`
(`tle/cogs/_codeforces_gitgud.py:89`) takes its contest set from
`cf.user.rating(handle=...)`, which returns **rated participations only**, so a
contest you did virtually never appears — and neither does an official
participation that was unrated for you.

Codeforces exposes virtual participation through `user.status`: every
submission carries `author.participantType`, one of `CONTESTANT`, `PRACTICE`,
`VIRTUAL`, `MANAGER`, `OUT_OF_COMPETITION` (`tle/util/_cf_api_types.py:157`),
and `author.startTimeSeconds` for the party's own start. `_upsolve_impl`
**already fetches `cf.user.status`** on the next line, so this costs no extra
API call:

```python
submissions = await cf.user.status(handle=handle)
participated = {sub.contestId for sub in submissions
                if sub.contestId is not None
                and sub.author.participantType in
                    ('CONTESTANT', 'VIRTUAL', 'OUT_OF_COMPETITION')}
rated = {change.contestId for change in await cf.user.rating(handle=handle)}
contests = rated | participated
```

Notes on that set:

- **`PRACTICE` must stay out.** Including it would make every contest you ever
  upsolved a single problem in an upsolve target, which is most of the
  problemset.
- `MANAGER` is contest writers; also out.
- Consider filtering `cf_common.is_nonstandard_contest` for consistency with
  `;vc`, and note `problem_cache.problems` is what bounds the result anyway.

**What this does not cover:** a virtual contest you registered for and never
submitted to leaves no trace in `user.status`. There is no API route back —
`contest.standings` has returned CONTESTANT rows only to ordinary callers
since May 2026 (documented in this repo at `tle/util/codeforces_api.py:230`),
so virtual standings cannot fill the gap. Covering zero-submission
participation needs explicit opt-in (`;upsolve add <contestId>`), which is the
medium-sized half of the request.

**Knock-on:** `;upsolve <n>` hands the chosen problem to `_gitgud` with the
usual rating-delta scoring, so virtual-contest problems become claimable
gitgud challenges. Same formula as today, no new exploit, but it overlaps
complaint #277 (gitgud points for all virtual solves) — decide the two
together.

Unrelated observation in the same function: `solved` is keyed on
`sub.problem.name` alone, unlike the repo's usual
`(name, contest start time)` identity (`SubFilter.filter_solved`). It
over-matches distinct problems that share a name.

## Shipped

| # | Complaint | Commit | Resolved |
|---|---|---|---|
| 213 | comma + "and" in Great Day messages | `1014b3cf0c1bc4f79b829929a6e8c18bee9ab23e` | yes, notified |
| 278 | paginate the complaint list | `8689901c2be65516ce382fdb2ef81b05b8dc079b` | yes, notified |
| 279 | problems solved by one account but not another (`;diff`) | `d997b6fc1dd51a213a75dec6913cc271eeb2bf62` | yes, notified |
| 281 | capitalization in `;vs` — `;versus tfg Tfg tFg tfG TFG` compared one person with themselves | handle casings collapse in `resolve_handles` | **not yet** — hold notifications |
| 277 (in spirit) | `;virtual`: blind random contest, solves earn gitgud points | `_codeforces_virtual.py`, migration 1.62.0 | **not yet** |

Also built alongside, not from a complaint: `;complain manage` (remove buttons
next to each complaint), complaint context capture (the five messages before a
report, served on the API detail route), complaint tags (`;complain tag`), and
deferred API notifications — an API resolve now queues its notification until
a restarted bot verifies the commit is running.

**Do not resolve further complaints through the API until the deferral commit
is deployed:** the bot currently running still notifies immediately.

## Close list (already implemented before the complaint was reviewed)

All four resolved through the API on 2026-09-25 with the commits below;
authors were notified.

| # | Complaint | Implemented by |
|---|---|---|
| 89 | self pills persistent in db | `f69bbe6e1eb54e5dfa2ee724a35f7aafbe14fd2b` — sticky narcissus marks in `starboard_narcissus`, survive un-reacting and restart |
| 276 | `;greatday history/stats` should show signup date | `d6e4e31000796df1f420c9a94bcb4dc5ca1d9135` — signup/signout event log; "Last signup" in stats |
| 283 | "wat did you do to ;llm we don't wanna grok" | `4f09daf503c094008b8901d7b0c45db5975455c0` — `parse_provider` defaults to Gemini; Grok needs `+grok`/`@grok` |
| 286 | pillboard image not spoilered | `01150e0633a1b483e96b5d0cf4736bc85a4522ad` — spoiler attachments detected and re-sent as spoilers |

**#286 carries a caveat:** spoiler *attachments* are handled
(`_starboard_render.py:224`). If the original was a spoilered **embed or
link** rather than an attachment, it is still not covered — worth checking the
original message before closing, or closing with a summary that says
attachments specifically.

## Policy items, left open deliberately

### #260 "grok shouldn't insult using rating in every message"

Filed under jokes, **not closed**. Two facts about the current prompt
(`tle/cogs/_llm_context.py:69`) regardless of how the complaint is treated:

- Roasting is still **mandatory**: "Every ordinary low-stakes reply needs at
  least two sharp, specific roasts". Rating jabs are fenced to provoked cases.
- The prompt now instructs the model to use **country and nationality** as
  insult material "quite often" and states "You can use slurs" — which
  contradicts `CLAUDE.md`, whose spec says "Country/nationality is neutral
  context and never an insult target."

The prompt and the documented policy disagree. Whichever is wrong, one of them
should change.

## Needs reporter input before any work

| # | Missing |
|---|---|
| 180 | The picture. Attachments are not in the API payload. |
| 242 | Which role. `on_member_join` already re-runs `_update_ranks`. |
| 253 | Complaint is a bare Discord link. |
| 254 | What "akari level rating" means — show 1–5 metadata / derive empirical difficulty / per-level player ratings are three different features. |
| 259 | Handle + month. MGG shows the rating at the *start* of the selected month (`_handles_gudgitters.py:124`), not current; a stale `rating_changes` row after a CF rollback is the other candidate. |
| 265 | One word: "formatting". |
| 281 | The stored text ends "for context:" with the example missing. |
| 294 | Denominator: boarded messages (small, totals already stored) or all messages sent (needs new counting). |
| 299 | **Root-caused and fixed.** `;vc nifeshe temporary1 catgirl` recommended Spectral Cup (2222) although all three had submissions in it: `get_visited_contests` looked contests up only through the problemset map, and 2222 had no problems in the disk cache (fetch failed inside its two-week window, never retried). Fix: a submission's own contest is always visited; the hourly task now backfills missing problemsets (20/tick) and `;cache problemsets missing` repairs the live cache in one go. Earlier reproduction note kept for the record: approximated against the live CF API (average rating 2409 → Div. 1 markers, newest first; the script skipped the writer exclusion — that map only exists inside the running bot — and checked "has a submission in the contest" rather than the bot's problem-level rule that also excludes a parallel Div. 1/Div. 2 round): top pick is Round 1116 (Div. 1), 2026-08-09, which none of the three has a submission in. Unclear what "fixed" means — need the reporter to say which contest came back and what was wrong with it. |

## Not this repo

#284, #285, #287, #288, #289, #290, #295, #296 are about the GF browser game.
#295 (lobby → "notify discord") and #296 (duel previews) are the only two with
a TLE-side component.

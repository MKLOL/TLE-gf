# Minigames Extraction Assessment

*2026-06-11 — five-reviewer assessment of extracting the minigames feature
(akari/guessgame/queens) into a standalone repository/package importable by
other projects.*

**Verdict: feasible but hard — consensus difficulty ~6–7/10, roughly 8–11
focused days for a full extraction. Recommendation: do NOT extract to a
separate repo now; isolate in-repo first (~4–6 days, ~80% of the value).**

## Scope of the feature

- `tle/cogs/minigames.py` (~6,500 lines, ~35 commands) + `_minigame_akari.py`,
  `_minigame_common.py`, `_minigame_guessgame.py`, `_minigame_queens.py`,
  `_minigame_stats.py`
- `tle/util/minigame_rating.py`, `tle/util/akari_rating.py` (pure math — clean)
- `tle/util/db/minigame_db.py` (`MinigameDbMixin`, 59 query methods, zero tle
  imports — but **no DDL**, see below)
- ~15k LOC total including ~5.7k lines of tests
  (`tests/test_minigames.py`, `test_akari_rating.py`)

## Inbound consumers (who depends ON minigames)

1. **rpoll — the big one.** No imports, but runtime coupling via
   `cf_common.user_db`: the `akari`/`akariexp` poll formulas weight votes by
   `get_akari_rating()` and check `is_akari_opted_out()` / `is_akari_banned()`
   (`tle/cogs/rpoll.py:153-162`). Transitively depends on 4 minigame tables
   (`minigame_rating`, legacy `akari_rating` fallback, `akari_optout`,
   `akari_ban`). Privacy invariant: opted-out users must contribute zero
   weight so poll totals can't reconstruct a hidden rating (tested in
   `tests/test_rpoll_formulas.py:170-244`). The legacy dual-read fallback in
   `get_akari_rating` (`minigame_db.py:865-893`) is load-bearing for vote
   weights.
2. **contests.py imports a private symbol from the cog**:
   `from tle.cogs.minigames import _get_akari_puzzle_table_image`
   (`tle/cogs/contests.py:56`, used for `;probrat`). Fix: hoist the renderer
   into `tle/util/` — it's a generic Cairo table renderer anyway.
3. **meta.py** names the feature flags: `_KNOWN_FEATURES` includes `akari`,
   `guessgame`, `queens` (`tle/cogs/meta.py:24`); `;meta config enable` is the
   write side of flags the minigames read. Minigame error strings literally
   tell users to run a meta-cog command (`minigames.py:1072-1073`).
4. **Test infrastructure**: `tests/conftest.py:507,549-553` loads minigame
   modules for every test session; `tests/rpoll_test_utils.py:93-130,178-181`
   creates minigame tables and binds real `MinigameDbMixin` methods;
   `tests/test_migrations.py:364-655` asserts on minigame tables/upgrades.
5. Confirmed clean: no `bot.get_cog('Minigames')` anywhere, no event coupling
   (`tle/util/events.py`), starboard/migrate/greatday/complain/handles/graphs
   never touch minigame data. "Pillboard" is starboard migration, not a
   minigame.

## Outbound dependencies (what minigames need FROM the host)

- **`cf_common.user_db` module global — ~150 call sites.** The dominant
  coupling, but mostly mechanical to rewire since the method names are
  minigame-owned.
- **Shared host tables**: `user_handle` (CF handle display in leaderboards,
  `minigames.py:292`), `guild_config` (feature flags + queens settings —
  accessors defined in the *starboard* mixin, `starboard_db.py:574-590`),
  shared `kvs` table (`mg_import_reply:*` keys).
- **Raw transaction control on the shared connection**:
  `cf_common.user_db.conn.commit()` / `.rollback()` at
  `minigames.py:3283-3318,4462`. A failed minigame import rolls back *any*
  cog's uncommitted writes — latent bug today, transaction-ownership problem
  for extraction. (`replace_minigame_ratings` has the same hazard via
  `with self.conn:` at `minigame_db.py:585`.)
- **~70 permission sites** using `constants.TLE_ADMIN`/`TLE_MODERATOR`, of
  which ~47 are `@commands.has_any_role(...)` decorators that freeze role
  names at import time — injection requires restructuring to dynamic checks.
- **Vendorable small helpers** (~580 lines total): `discord_common` embed
  helpers (71 sites), `paginator`, `table`, `graph_common`,
  `_migrate_retry`, `cf_common.parse_date`/`ParamParseError`. Vendor, don't
  import — `discord_common` transitively pulls the whole CF stack.
- **Environment**: Cairo/Pango image rendering depends on
  `font_config.configure()` running before any Pango import
  (`tle/__main__.py:13-16`) + downloaded Noto fonts; matplotlib style is set
  by the host.
- `AKARI_*` tuning constants live in `tle/constants.py:34-53` but the rating
  engine already takes them as kwargs — only defaults need moving.

## Schema & migration landmines

- **Minigame DDL does not live in minigame files.** Table creation is in
  `UserDbConn.create_tables()` (`user_db_conn.py:294-487`) and in 11 upgrade
  functions interleaved through the shared chain: 1.14–1.16, 1.24–1.28,
  1.30–1.32 (of 33 total). The version history cannot be sliced contiguously.
- **Prod's `db_version` stamp is currently 1.32.0 — a minigame upgrade.**
  `UpgradeRegistry.run()` raises `RuntimeError` on an unrecognized version
  (`upgrades.py:72-81`). Naively deleting minigame upgrades from the host
  repo bricks the bot on the next restart — and `run.sh` auto-`git pull`s on
  every restart, so a bad cutover self-deploys. A split requires:
  - tombstone no-op entries for 1.14–1.32 in the host registry, kept forever;
  - a separate `minigame_db_version` table for the package
    (`set_version` does DELETE-then-INSERT on a single-row table — two
    registries cannot share `db_version`);
  - stamp-don't-run semantics when the package first opens an existing DB
    (re-running 1.26.0's rename or 1.30.0's `INSERT OR IGNORE` rating copy is
    unsafe).
- Fresh-vs-legacy DB detection probes the **starboard** table
  (`user_db_conn.py:104-106`) — must be rewritten for a standalone DB, not
  copied.
- Discord IDs are TEXT; `minigame_db.py` relies on
  `CAST(message_id AS INTEGER)` ordering for dedup "first wins" semantics
  (`minigame_db.py:90,102,265`) — any storage rewrite must preserve this.

## Operational context (why timing matters)

- Prod `user.db` was lost ~2026-06-01 and restored from a ~1-week-old backup;
  the restore invariant is "deployed registry version ≥ backup's db_version".
  Restructuring the registry during this window is maximally risky.
- 85 minigame commits in the last 10 weeks; 3 schema versions in the last
  month; Queens still actively being built. A repo boundary on the hottest
  code = every change becomes commit-package → pin bump → commit-bot →
  deploy, through the auto-pull restart loop.
- When extraction does happen: **inject the existing `user.db` connection
  rather than moving tables to a new file** — zero data movement, idempotent
  baseline, `tle-backup-service` keeps working.

## Recommended path

**Phase 1 — in-repo isolation (~4–6 days), do this first:**

1. Move everything into a `tle/minigames/` subpackage. Promote
   `MinigameDbMixin` → standalone `MinigameStore(conn)`; replace the ~150
   `cf_common.user_db.*` call sites with `self.store.*`.
2. Add a `MinigamesAPI` read facade (rating / opt-out / ban / results /
   links, returning frozen dataclasses) and rewrite rpoll's three calls
   against it — ideally a single
   `get_effective_akari_rating(guild, user) -> int` that folds in
   opt-out/ban so the privacy invariant lives with the data owner.
3. Hoist the Cairo table renderer out of the cog (fixes contests.py's private
   import). Inject config (role names, `AKARI_*`, handle resolver,
   feature-flag hook) instead of importing `tle.constants`/`cf_common`.

**Phase 2 — actual extraction (~2–3 more days), only when a real second
consumer exists or velocity cools:** `git mv` + `pyproject.toml`
(`discord-minigames`, extras `[images]`/`[plots]` for Cairo/matplotlib),
vendored `UpgradeRegistry` with own version table, explicit
`await discord_minigames.setup(bot, db=..., config=MinigamesConfig(...))`
entry point, TLE-gf pins an exact SHA (the `git pull` loop makes floating
deps a live grenade), file-level `user.db` backup + dry-run on a prod copy
before cutover, auto-restart loop disabled during cutover.

**Independent of all this:** the shared-connection `rollback()` in the import
pipeline (`minigames.py:3303-3318`) can silently discard other cogs'
uncommitted writes — worth fixing regardless.

## Per-reviewer difficulty ratings

| Lens | Rating |
|---|---|
| DB coupling | 6/10 |
| Inbound consumers (rpoll etc.) | 4–5/10 |
| Outbound host-infra deps | 5.5/10 |
| Packaging/API design | 6/10 |
| Tests, migration history, ops risk | 8/10 |

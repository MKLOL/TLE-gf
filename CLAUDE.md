# TLE-gf

TLE-gf is a fork of [TLE](https://github.com/cheran-senthil/TLE), a Discord bot for competitive programming communities. It integrates with Codeforces for problem recommendations, rating tracking, duels, and training. The bot uses discord.py v2, SQLite for persistence, and is structured around cogs (modular command groups).

## File size limit

**Hard rule: every file must be under 500 lines.** This is non-negotiable and applies to ALL files — source, tests, helpers, and scripts. When a file approaches 500 lines, split it before adding more. How to split, by layer:

- **Cogs**: extract cohesive command groups into mixin cogs (the cog class inherits from several `*Mixin` classes) and move pure/module-level helpers into `_`-prefixed sibling modules (e.g. `_minigame_akari.py`, `_starboard_render.py`).
- **DB layer**: extract method groups into `*DbMixin` classes in their own file. `UserDbConn` already composes `MinigameDbMixin`, `StarboardDbMixin`, `MigrationDbMixin` — follow that pattern.
- **Tests**: split by feature area into separate `test_*.py` files; pytest auto-collects them. Shared fixtures/fakes go in an imported helper module (not `conftest.py` unless they're true fixtures).

Splits MUST preserve public behavior: keep cog class names, `setup()`, and command/alias names stable, and re-export any moved symbol that something else imports. Run the test suite after every split.

## What was built

### DB Migration System (`tle/util/db/upgrades.py`, `tle/util/db/user_db_upgrades.py`)

TLE-gf had no schema migration system — every table used `CREATE TABLE IF NOT EXISTS`, so adding columns to existing DBs was silently ignored. We added an `UpgradeRegistry` that tracks a `db_version` table and runs versioned upgrade functions (1.0.0 through 1.4.0). Fresh DBs get stamped at the latest version; existing DBs run pending upgrades.

### Multi-Emoji Starboard (`tle/cogs/starboard.py`)

The original starboard was hardcoded to a single star emoji. We rewrote it to support multiple emojis per guild, each with its own threshold, color, and channel. The schema moved from `starboard`/`starboard_message` to `starboard_config_v1`, `starboard_emoji_v1`, `starboard_message_v1`.

### Starboard Leaderboards

Added `;starboard leaderboard <emoji>` (by message count) and `;starboard star-leaderboard <emoji>` (by total stars). Gated behind a `starboard_leaderboard` guild config flag enabled via `;meta config enable starboard_leaderboard`.

### Background Backfill

A one-time background task runs on startup to populate `author_id` and `star_count` for existing starboard messages by fetching them from Discord. Uses `author_id IS NULL` as a checkpoint — already-processed messages are skipped on restart. Unfetchable messages get an `__UNKNOWN__` sentinel to prevent infinite retries.

### Guild Config System

Key-value config per guild (`guild_config` table). Used for feature gating (e.g., `starboard_leaderboard`). Managed via `;meta config`.

### `;llm` — Gemini with a rotating key pool

`;llm <question>` answers in an embed; sent as a *reply*, it answers about the replied-to message and forwards any image attachments (Gemini is multimodal). Prefixing a model — `;llm 3.5f-h <question>` — pins it and its reasoning tier (`;llm models` lists them; aliases are `<version><f|l>` plus `pro`, tiers `-min/-l/-m/-h/-off`, with the long spellings kept as synonyms). Moderators manage the key pool with `;llm keys <key> ...` / `keylist` / `keyforget` / `keystatus`.

**There is no per-user cap or cooldown**, by request. The shared free-tier allowance is the only limit; calls are counted per user purely so `;llm keystatus` can show where it went.

**Two-stage pipeline** (`_llm_pipeline.py`), adapted from [MKLOL/TLE-gf#10](https://github.com/MKLOL/TLE-gf/pull/10): a cheap routing call classifies the question `direct` / `requires_context` / `requires_reply_chain`, and only then is channel history collected (`_llm_history.py`). Routing is charged to the *cheapest* model in the ladder with a 16-token cap, and any failure there falls back to `direct` — losing the optimisation must never block the answer. It does cost a second request per question; `LLM_CONTEXT_ENABLED=0` turns it off.

Reasoning tiers go in `generationConfig.thinkingConfig`, and the encoding differs by family: 3.x uses `thinkingLevel`, while the 2.5 family expresses "off" as `thinkingBudget: 0`. Because a fallback can cross that boundary, the payload is rebuilt per attempt rather than once per call.

The thing to understand before touching this: **Google's free tier meters quota per project per model, not per key** — *"Rate limits are applied per project, not per API key"* ([docs](https://ai.google.dev/gemini-api/docs/rate-limits)). Extra keys minted inside one project share one allowance. So the unit of quota is a *bucket* — the pair `(key, model)` — and `KeyPool` rotates over buckets, not keys. Each subsequent entry in `LLM_MODELS` is a genuinely separate allowance on the same key, so the ladder multiplies capacity rather than just providing a backup.

Every quota failure is an HTTP 429, but they are not interchangeable and are classified out of the error's `QuotaFailure` details: per-minute cools the bucket ~60s **in memory**; per-day blocks it until Google's reset and is **persisted to `llm_bucket`**, so a restart doesn't rediscover dead buckets by burning a request on each. An unclassifiable 429 escalates to daily after 3 strikes.

Three asymmetries drive the rest of the design, and all three are deliberate:

- **Per-minute wins a classification tie.** Google's prose names both windows ("limit 15 per minute … learn about daily limits"), so structured details are read first and minute beats day within either source. Guessing minute wrongly self-corrects via the strike counter; guessing day wrongly parks a live bucket until midnight Pacific with nothing to undo it.
- **A rejected key is benched, not retired, on the first 4xx.** `PERMISSION_DENIED` covers a revoked key *and* a transient billing blip. First rejection benches every bucket for that key 10 min; a second on a later call retires it and logs at `ERROR`, which the logging cog relays to moderators.
- **Failed calls are billed if they reached Google.** `complete()` reports `stats['attempts']`; one invocation can walk several buckets, so a user cannot drain the shared allowance on calls that happen to fail. Nothing is charged when the pool had nothing to try.

`;llm keys` has a `cog_command_error`: a failed role check fires *before* the command body, so without it a non-moderator's pasted key would never be deleted and `MissingAnyRole` would only be logged. Messages containing a key-shaped token get deleted and flagged; ones that don't (`;llm keys are overrated`) get an explanation.

This uses the **native** Gemini endpoint, not Google's OpenAI-compatibility shim — the shim flattens away the error details the classifier depends on.

## Key files

| File | What it does |
|---|---|
| `tle/util/db/upgrades.py` | Generic `UpgradeRegistry` class |
| `tle/util/db/user_db_upgrades.py` | Upgrade functions 1.0.0 - 1.4.0 |
| `tle/util/db/user_db_conn.py` | All DB methods (starboard, guild config, leaderboards) |
| `tle/cogs/starboard.py` | Starboard cog (reactions, commands, backfill) |
| `tle/cogs/meta.py` | Meta cog (guild config commands) |
| `tle/cogs/llm.py` | `;llm` cog (ask, reply-context, mod-only key management) |
| `tle/util/llm_keypool.py` | `KeyPool` — `(key, model)` bucket rotation and 429 classification |
| `tle/util/gemini_api.py` | Native Gemini REST client + retry-across-buckets loop |
| `tle/util/db/llm_db.py` | `LlmDbMixin` — key storage, bucket state, per-user usage |
| `tle/util/llm_models.py` | Selectable model catalog + reasoning-tier encoding |
| `tle/cogs/_llm_pipeline.py` | Route (classify) → gather history → build prompt |
| `tle/cogs/_llm_history.py` | Channel-history collection and transcript rendering |
| `tle/constants.py` | `_DEFAULT_STAR_COLOR`, `_DEFAULT_STAR`, `TLE_ADMIN` |
| `tests/conftest.py` | Test setup — stubs discord.py, aiohttp, etc. via `sys.modules` |

## Architecture notes

- **SQLite with namedtuple rows**: `user_db_conn.py` uses `namedtuple_factory` as the row factory, so query results use attribute access (`row.guild_id`). Non-identifier column names (like `SELECT 1`) get aliased to `col_0`.
- **Discord IDs are TEXT in SQLite**: Discord IDs are Python ints but stored as TEXT. All DB methods cast with `str()`.
- **Per-guild asyncio.Lock**: Starboard uses one lock per guild to prevent duplicate starboard posts from concurrent reactions.
- **`INSERT OR IGNORE` for messages, `ON CONFLICT DO UPDATE` for emojis**: Messages should never be overwritten; emoji config upserts must preserve `channel_id`.
- **Backfill checkpointing**: `author_id IS NULL` = pending. `__UNKNOWN__` = unfetchable (excluded from leaderboards). Already-set `author_id` = done.

## Running tests

```bash
python3 -m pytest tests/ -v
```

Tests stub out discord.py, aiohttp, and other heavy deps in `conftest.py` so they run against in-memory SQLite without the full bot environment.

## Workflow rules

**Always commit after completing a task.** Every discrete unit of work (feature, bugfix, refactor, test addition) must be committed immediately after tests pass. Do not wait for the user to ask — just commit. If multiple tasks are requested in sequence, commit after each one.

## Commits convention

Use imperative mood, short first line. `Co-Authored-By` trailer when AI-assisted. Do not use `$()` command substitution in commit messages — use a plain string with `-m`.

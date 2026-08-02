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

### `;llm` — Gemini plus Grok

`;llm <question>` (or its `;ai` alias) asks Gemini by default and answers in an embed sent as a no-ping Discord reply to the invoking prompt. Exact leading selectors `+gemini` and `+grok`, plus literal channel triggers `@gemini <question>` and `@grok <question>`, pin the provider while sharing the same reply, context, image, access-control, and cooldown pipeline. Provider selectors come before context controls and Gemini model selectors. If the prompt itself replies to another message, the bot answers about that target and forwards supported image attachments. Prefixing a Gemini model — `;llm 3.5f-h <question>` — pins it and its reasoning tier (`;llm models` lists them; aliases are `<version><f|l>` plus `pro`, tiers `-min/-l/-m/-h/-off`, with long spellings kept as synonyms). Only the global bot owner can manage credentials or inspect provider health with `keys` / `keylist` / `keyforget` / `keystatus` and their Grok equivalents.

`;ai` aliases the entire `;llm` command tree. `;help ai` is a curated one-message overview, while `;help ai <command>` shows only that command's syntax and purpose. Because access-control words are real subcommands, wrapping the whole prompt in straight or smart quotes forces request handling: `;llm "ban lists in graph theory"`. Guild admins/moderators use `ban` / `unban` / `banlist` for guild-scoped request bans and `disable [here]` / `enable [here]` for access. A server-wide enable or disable resets local overrides and affects every channel and thread. `here` targets only the exact current channel or thread, while `here +threads` targets the parent channel plus every child thread and clears exact exceptions in that family. Exact channel and thread overrides remain independent after the family baseline is set. These policies gate every command and literal provider entry point before context reads, provider calls, telemetry, or paid-request reservation; management commands remain available so policy can always be reversed.

Guild admins/moderators can configure persistent shared prompt cooldowns with `;ai cooldown <seconds>` for only the current channel or thread, append `+threads` for one shared timer across the parent channel and all of its threads, or append `+global` for every channel and thread in that server; `0` removes the selected scope and no argument reports all three settings. Exact, family, and server cooldowns stack, are shared across users and providers, and are claimed atomically only after runtime admission. Accepted attempts consume the configured scopes even if a provider later fails, while denied retries do not extend them. `+global` is deliberately server-wide, never bot-wide across unrelated guilds.

Grok is a provider route over xAI's Responses API: `;llm +grok <question>` and a channel message beginning with the literal text `@grok <question>` share the same reply/history/image pipeline. Its system prompt keeps Gemini's broad subject scope and truthfulness rules, adds competitive-programming problem solving, and uses an extremely aggressive roast-comedy voice with uncensored vulgarity. Normal roasts target the message, code, reasoning, or choices; direct personal and rating-based clapbacks are reserved for users who clearly insulted Grok first and must not repeat an angle already visible in the supplied exchange. Country/nationality is neutral context and never an insult target. Ambiguous Grok questions route through xAI, so Grok never depends on Gemini quota or credentials. `XAI_MODELS` is a strongest-to-weakest fallback ladder and defaults to the singular `XAI_MODEL` (`grok-4.5`). Model-not-found/access failures skip the unavailable model rather than trying it against every key.

xAI keys are provider-isolated in `llm_api_key` (migration 1.46.0); never put them in the Gemini `KeyPool`. Prefer `XAI_API_KEY` / comma-separated `XAI_API_KEYS`: environment credentials are process-only and are never copied into SQLite. Owner uploads through `;llm grokkeys` remain a supported fallback and are stored in `user.db`. A valid xAI key does not imply credits or model access; reversible health circuits cool 403 billing/access failures and can be cleared with owner-only `;llm healthreset` after funding or permissions are fixed.

**Gemini has no per-user cap or cooldown.** Grok has a persistent credit guard (migration 1.47.0). The environment default gives regular users 15 accepted invocations in a rolling hour; guild admins/moderators can inspect or override the count and window with `;ai ratelimit [requests] [window]`, disable only the personal guard with `off`, or restore the environment default with `default`. Overrides are stored in `guild_config`, and personal usage is counted independently per `(guild, user)`. Members with the configured Admin or Moderator role bypass only that personal cap. Everyone remains subject to 200 accepted invocations bot-wide per UTC day and the private $0.50 daily spend guard. The event ledger survives restarts. Admins/moderators can use `;ai grokreset` to clear current-UTC-day guard reservations without deleting provider telemetry; rolling-window entries from before midnight remain. Personal denials identify the effective allowance and include a Discord-relative retry timestamp, while shared request and spend exhaustion use identical wording so the dollar threshold stays private. Each accepted invocation consumes one slot even if xAI later fails, but its internal router and answer calls together still count as one bot invocation. Migration 1.48.0 adds prompt-free provider telemetry and reconciled xAI cost reservations; owner-only `keystatus` / `grokstatus` report separate attempts, tokens, latency, health, and spend without storing prompts, answers, or key material. xAI's returned `cost_in_usd_ticks` is authoritative when present; configured token prices provide a conservative fallback.

Grok's answer call uses Grok 4.5 with low reasoning and a separate `XAI_MAX_OUTPUT_TOKENS` cap (default 1536, including hidden reasoning). Its prompt asks for answers under roughly 150 words unless code, a proof, or correctness requires more. Gemini keeps `LLM_MAX_OUTPUT_TOKENS` unchanged.

**Hybrid two-stage context pipeline** (`_llm_pipeline.py`), adapted from [MKLOL/TLE-gf#10](https://github.com/MKLOL/TLE-gf/pull/10): resolved replies become `requires_reply_chain` locally, while unmistakably conversation-dependent requests such as “what did I miss?”, “what happened?”, “summarize this”, and “recap it” become `requires_context` locally. When the current message contains an image, visually referential requests such as “summarize this” remain `direct` so “this” refers to the attachment. Only ambiguous non-replies pay for a provider routing call between `direct` and `requires_context`. Both routers have a short timeout. Gemini routing uses `LLM_MODELS[0]` with a strict structured enum; a Gemini router failure falls back to `requires_context`. Grok routing uses xAI with low reasoning and a 256-token reasoning-inclusive cap; an xAI router failure also falls back to `requires_context`. Malformed classifier output becomes `direct`. Users can lead with `+context`, `+direct`, or `messages=N`; moderators can set `auto`, `explicit`, or `off` per guild/channel through `;llm privacy`. `LLM_CONTEXT_ENABLED=0` disables non-reply routing/history while replies retain their structural context.

History selection is session-aware for non-reply context (`_llm_history.py`): recent transcripts walk backward through the newest usable human messages until reaching either an inactivity gap longer than `LLM_CONTEXT_GAP_SECONDS` or the hard age limit in `LLM_CONTEXT_RECENT_MAX_AGE_SECONDS`. These default to ten minutes and six hours respectively, while the existing message-count limit keeps prompts bounded. This allows a continuously active discussion to span longer than ten minutes without including an older topic separated by a substantial pause. Reply transcripts remain relevance-first: they always retain the focused message, then prefer resolved ancestors, direct replies, and participants before chronological fill. Cross-channel and future ancestors are rejected. JSONL records carry bounded IDs/timestamps/reply links/focus/requester/bot markers, escape prompt-like text, and redact likely credentials. Contextual Grok answer prompts end with bot-authored routing metadata that identifies the requester independently from the focused reply target, so a replied-to author or another transcript participant cannot silently become the answer recipient; display-name collisions are resolved by IDs and explicit role flags. Grok also receives a separate bounded participant block containing only linked users in the request/selected transcript, with cached Codeforces handle, current/max rating, rank title/color, country, and requester/reply-target flags; field values are explicitly data rather than instructions. Direct context-free questions skip the routing envelope. Reply collection remains bounded on both sides by `LLM_CONTEXT_WINDOW_SECONDS` and stops before the invoking command, so quiet channels cannot pull in unrelated messages from days later.

One active request per Discord user and small provider-specific semaphores bound concurrency. A single deadline covers history, attachment reads, routing, fallbacks, and the answer; cancellation records the attempted provider call, cools the active lease, releases permits, reconciles a Grok reservation, and records a deadline outcome.

Context prompts must stay honest about what was gathered. `build_context_prompt` only describes a marked replied-to message when one actually exists. If routing requested history but `gather` returned nothing—because the active session contained no usable messages, the hard age limit was reached, or Read Message History was unavailable—`build_question_prompt(context_requested=True)` says that no transcript was retrieved instead of silently pretending the question was self-contained.

**Transcripts are embed-aware** (`llm_history.message_text`). The cog answers in embeds, so `message.content` is empty for its own output; reply quoting and transcript rendering therefore include embed title, description, fields, and footer. Ordinary history still filters bot output, while the direct reply target remains usable even when the bot wrote it.

For Gemini, supported images on the replied/current messages and selected context window are forwarded in focus-first order under the configured count and byte caps. Grok forwards only images on the replied/current messages to keep paid input bounded.

**`maxOutputTokens` includes reasoning tokens.** This is the trap that broke context entirely once: the router had a 16-token cap, which the model spent thinking, returning a 200 with no text — `extract_text` raised, `classify` caught it, and every question in the server silently routed to `direct`. Any call with a tight budget must also pin thinking low (`llm_models.LEAST` resolves to `off` on 2.5, `minimal` on 3.x). An empty `MAX_TOKENS` response now raises `EmptyOutputBudgetError`, which names the setting instead of reading as a model quirk.

Reasoning tiers go in `generationConfig.thinkingConfig`, and the encoding differs by family: 3.x uses `thinkingLevel`, while the 2.5 family expresses "off" as `thinkingBudget: 0`. Because a fallback can cross that boundary, the payload is rebuilt per attempt rather than once per call.

The thing to understand before touching this: **Google's free tier meters quota per project per model, not per key** — *"Rate limits are applied per project, not per API key"* ([docs](https://ai.google.dev/gemini-api/docs/rate-limits)). Extra keys minted inside one project share one allowance. So the unit of quota is a *bucket* — the pair `(key, model)` — and `KeyPool` rotates over buckets, not keys. Each subsequent entry in `LLM_MODELS` is a genuinely separate allowance on the same key, so the ladder multiplies capacity rather than just providing a backup.

Every quota failure is an HTTP 429, but they are not interchangeable and are classified out of the error's `QuotaFailure` details: per-minute cools the bucket ~60s **in memory**; per-day blocks it until Google's reset and is **persisted to `llm_bucket`**, so a restart doesn't rediscover dead buckets by burning a request on each. An unclassifiable 429 escalates to daily after 3 strikes.

Three asymmetries drive the rest of the design, and all three are deliberate:

- **Per-minute wins a classification tie.** Google's prose names both windows ("limit 15 per minute … learn about daily limits"), so structured details are read first and minute beats day within either source. Guessing minute wrongly self-corrects via the strike counter; guessing day wrongly parks a live bucket until midnight Pacific with nothing to undo it.
- **A rejected key is benched, not retired, on the first 4xx.** `PERMISSION_DENIED` covers a revoked key *and* a transient billing blip. First rejection benches every bucket for that key 10 min; a second on a later call retires it and logs at `ERROR`, which the logging cog relays to moderators.
- **Failed calls are billed if they reached Google.** `complete()` reports `stats['attempts']`; one invocation can walk several buckets, so a user cannot drain the shared allowance on calls that happen to fail. Nothing is charged when the pool had nothing to try.

Key-upload callbacks delete the invoking Discord message before owner authorization, fail closed when owner verification errors, never echo key material, and display SHA-256 fingerprints rather than credential fragments. Channel gating also deletes a blocked key-upload command before rejecting it. Forgetting a key clears active material and buckets, but the bot still tells the owner to revoke it because SQLite pages and retained backups are not a revocation mechanism.

This uses the **native** Gemini endpoint, not Google's OpenAI-compatibility shim — the shim flattens away the error details the classifier depends on.

**The ladder must survive one bad rung.** Both failures that are specific to a model rather than to a key or the quota fall through instead of killing the command, because a ladder whose first entry is wrong is otherwise a total outage:

- A **404** retires that model for the rest of the call and tries the next one. Only a ladder where *every* entry 404s raises `ModelUnavailableError` — that is `LLM_MODELS` being wrong, which no retry can fix. Note the discovery is per-call, not persisted, so a bad entry costs one wasted request per `;llm` until a moderator fixes it; the `WARNING` names the model.
- A **400 naming a tool** drops `tools` and retries the *same* bucket (it is not the bucket's fault, and a one-key pool has nothing else to try). `;llm` sends `url_context` so the model can read a URL in the question, and tool support is per-model — an older rung rejecting it must degrade to an answer without URL reading, not to no answer. `is_tool_unsupported_error` keeps this narrow: any other 400 is still a malformed request that fails fast.

The Gemini system instruction advertises **only** URL reading, never web search. Told it can search, the model narrates searches it never ran — which is exactly the fabrication the "never claim to have read a page" rule exists to prevent. Grok is explicitly told it cannot fetch URLs. If `google_search` is ever added to `tools`, that paragraph has to change with it.

## Key files

| File | What it does |
|---|---|
| `tle/util/db/upgrades.py` | Generic `UpgradeRegistry` class |
| `tle/util/db/user_db_upgrades.py` | Upgrade functions 1.0.0 - 1.4.0 |
| `tle/util/db/user_db_conn.py` | All DB methods (starboard, guild config, leaderboards) |
| `tle/cogs/starboard.py` | Starboard cog (reactions, commands, backfill) |
| `tle/cogs/meta.py` | Meta cog (guild config commands) |
| `tle/cogs/llm.py` | `;llm` cog lifecycle, process-only keys, privacy policy, literal provider listeners |
| `tle/cogs/_llm_ask.py` | Shared guarded Gemini/Grok request flow |
| `tle/cogs/_llm_limits.py` | Persistent per-guild regular-user Grok allowance |
| `tle/cogs/_llm_help.py` | Compact AI overview and focused command help |
| `tle/cogs/_llm_entrypoints.py` | Canonical provider selectors, literal triggers, and usage text |
| `tle/cogs/_llm_runtime.py` | Per-user admission, provider concurrency, end-to-end deadlines |
| `tle/cogs/_llm_status.py` | Owner-only provider telemetry/spend formatting |
| `tle/util/llm_keypool.py` | `KeyPool` — `(key, model)` bucket rotation and 429 classification |
| `tle/util/gemini_api.py` | Native Gemini REST client + retry-across-buckets loop |
| `tle/util/xai_api.py` | xAI Responses client + health-aware model/key fallback |
| `tle/util/db/llm_db.py` | `LlmDbMixin` — key storage, quota ledger, per-user usage |
| `tle/util/db/llm_telemetry_db.py` | Prompt-free provider attempts/tokens/latency/cost telemetry |
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

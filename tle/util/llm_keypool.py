"""Rotating pool of Gemini API keys.

Google's free tier meters requests **per project per model**, not per key —
several keys minted inside one project share one allowance, while the same key
pointed at a different model draws on a separate one. So the unit of quota
here is a *bucket*: the pair ``(key, model)``. A pool of N keys across M
models is N×M buckets, and the pool walks them until one answers.

Every quota failure arrives as HTTP 429, but they are not interchangeable:

* **per-minute (RPM)** — the bucket is fine again in seconds. Cool it briefly,
  in memory only; a restart losing this state costs at most one wasted call.
* **per-day (RPD)** — the bucket is dead until Google's daily reset, hours
  away. This is persisted, so a bot restart does not rediscover it the hard
  way by burning a request on every dead bucket.
* **transient overload** — usually 5xx rather than 429; the bucket is healthy
  and only this attempt failed.

Confusing the first two is the expensive mistake in both directions: treat a
daily as a minute and you re-hammer a dead key all day; treat a minute as a
daily and you throw away hundreds of good requests over one burst.

There is no API to ask Google how much quota a key has left, so all of this is
inferred locally from the errors it returns.
"""
import asyncio
import logging
import re
import time
from collections import namedtuple

logger = logging.getLogger(__name__)

QUOTA_DAY = 'day'
QUOTA_MINUTE = 'minute'
QUOTA_UNKNOWN = 'unknown'

# Free-tier daily quota resets at midnight US/Pacific. A fixed -8h offset is
# used rather than a tz database lookup: during daylight saving the real reset
# is an hour earlier than we compute, so we simply wait a little longer. Erring
# late wastes a few minutes; erring early wastes a request per bucket.
_PACIFIC_UTC_OFFSET = -8 * 3600

_DEFAULT_MINUTE_COOLDOWN = 60.0
_UNKNOWN_COOLDOWN = 90.0
_TRANSIENT_COOLDOWN = 5.0
# An unclassifiable 429 that keeps recurring on the same bucket is treated as a
# daily exhaustion after this many strikes, so we stop probing it all day.
_UNKNOWN_STRIKES_TO_DAILY = 3
# Anything shorter than this in a RetryInfo on a daily violation is ignored —
# a daily quota does not come back in 30 seconds.
_MIN_DAILY_WAIT = 300.0
# A key rejected outright is usually revoked for good, but PERMISSION_DENIED
# also covers transient states (a billing blip, the API briefly not enabled).
# Retiring on the first rejection makes those permanent until someone re-adds
# the key by hand, so a key gets benched and only retired if it fails again on
# a later call. Costs one wasted request against a genuinely dead key.
_INVALID_STRIKES_TO_RETIRE = 2
_INVALID_COOLDOWN = 600.0

Lease = namedtuple('Lease', 'key_id api_key label model')


def next_daily_reset(now):
    """Unix timestamp of the next midnight Pacific strictly after ``now``."""
    local = now + _PACIFIC_UTC_OFFSET
    next_local_midnight = (int(local // 86400) + 1) * 86400
    return next_local_midnight - _PACIFIC_UTC_OFFSET


def parse_retry_delay(value):
    """Parse a protobuf Duration string such as ``'51s'`` or ``'1.5s'``.

    Returns seconds as a float, or None if unparseable.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.fullmatch(r'\s*([0-9]*\.?[0-9]+)s?\s*', str(value))
    return float(match.group(1)) if match else None


def _iter_detail_strings(details):
    """Yield every quota-identifying string found in an error's details."""
    for detail in details or []:
        if not isinstance(detail, dict):
            continue
        for violation in detail.get('violations') or []:
            if not isinstance(violation, dict):
                continue
            for field in ('quotaId', 'quotaMetric', 'subject', 'description'):
                value = violation.get(field)
                if value:
                    yield str(value)


def classify_quota_error(payload):
    """Work out which limit a 429 tripped.

    ``payload`` is the decoded JSON error body from the Gemini REST API.
    Returns ``(scope, retry_after)`` where scope is one of ``QUOTA_DAY``,
    ``QUOTA_MINUTE``, ``QUOTA_UNKNOWN`` and ``retry_after`` is seconds or None.

    Google names its quotas descriptively — e.g.
    ``GenerateRequestsPerDayPerProjectPerModel-FreeTier`` — so the scope is
    read out of the quota id. The human-readable message is consulted only
    when the details block is absent (as it is through the OpenAI
    compatibility layer), because that prose routinely mentions *both*
    windows: "limit 15 per minute ... learn more about daily limits".

    Within either source, per-minute wins a tie. Guessing minute when it was
    really daily is self-correcting — the bucket keeps 429ing and the
    unknown-strike counter escalates it to daily. Guessing daily when it was
    really a minute burst has no such path back: the bucket sits idle until
    midnight Pacific with quota still on it.
    """
    error = (payload or {}).get('error') or {}
    details = error.get('details') or []

    retry_after = None
    for detail in details:
        if isinstance(detail, dict) and 'retryDelay' in detail:
            retry_after = parse_retry_delay(detail.get('retryDelay'))
            break

    structured = list(_iter_detail_strings(details))
    message = [str(error.get('message') or '')]
    for source in (structured, message):
        blob = ' '.join(source).lower().replace('_', '').replace(' ', '')
        if not blob:
            continue
        if 'perminute' in blob or 'permin' in blob:
            return QUOTA_MINUTE, retry_after
        if 'perday' in blob or 'perdate' in blob or 'daily' in blob:
            return QUOTA_DAY, retry_after
    return QUOTA_UNKNOWN, retry_after


class KeyPool:
    """Chooses which ``(key, model)`` bucket to spend, and remembers failures.

    Callers do: ``lease = await pool.acquire()`` then exactly one of
    ``report_success`` / ``report_quota`` / ``report_transient`` /
    ``report_invalid``. ``acquire`` returns None when every bucket is blocked.
    """

    def __init__(self, db, models, now_fn=time.time, ephemeral_keys=None):
        self._db = db
        self._models = [m for m in models if m]
        self._now = now_fn
        self._lock = asyncio.Lock()
        self._keys = []              # list of key rows, in preference order
        self._ephemeral_keys = list(ephemeral_keys or [])
        self._ephemeral_ids = set()
        self._disabled_ephemeral = set()
        self._cooldown = {}          # (key_id, model) -> until   [RPM, memory]
        self._exhausted = {}         # (key_id, model) -> until   [RPD, mirrors DB]
        self._unknown_strikes = {}   # (key_id, model) -> count
        self._invalid_strikes = {}   # key_id -> count of outright rejections
        self._last_used = {}         # key_id -> timestamp
        self._loaded = False

    # ── Loading ─────────────────────────────────────────────────────────

    def reload(self):
        """Re-read keys and persisted daily exhaustion from the database.

        In-memory cooldowns are dropped so a key a moderator has just re-added
        is usable immediately — ``llm_add_key`` clears its persisted buckets
        for the same reason. Strike counters deliberately survive: they are
        what retires a revoked key, and resetting them on every ``;llm keys``
        edit would let a dead key bench-and-return forever.
        """
        self._cooldown.clear()
        ephemeral = []
        seen_ephemeral_ids, seen_ephemeral_material = set(), set()
        for row in self._ephemeral_keys:
            if (row.id in seen_ephemeral_ids
                    or row.api_key in seen_ephemeral_material):
                continue
            ephemeral.append(row)
            seen_ephemeral_ids.add(row.id)
            seen_ephemeral_material.add(row.api_key)
        ephemeral_ids = {row.id for row in ephemeral}
        ephemeral_material = {row.api_key for row in ephemeral}
        persisted = [
            row for row in self._db.llm_get_keys(
                active_only=True, provider='gemini')
            if row.id not in ephemeral_ids and row.api_key not in ephemeral_material
        ]
        self._keys = persisted + ephemeral
        self._ephemeral_ids = {row.id for row in ephemeral}
        for key_id in self._disabled_ephemeral:
            for model in self._models:
                self._cooldown[(key_id, model)] = float('inf')
        now = self._now()
        self._exhausted = {
            (row.key_id, row.model): row.exhausted_until
            for row in self._db.llm_get_buckets(now=now)
        }
        self._loaded = True
        logger.info('LLM key pool loaded: %d key(s), %d model(s), '
                    '%d bucket(s) currently exhausted',
                    len(self._keys), len(self._models), len(self._exhausted))

    def set_ephemeral_keys(self, rows):
        """Merge process-only credentials into the pool.

        Rows need ``id``, ``api_key`` and ``label`` attributes. Callers should
        use stable negative ids so their in-memory health survives reloads and
        cannot collide with SQLite ids. An ephemeral row wins over a duplicate
        persisted value so an authentication failure never deactivates the
        environment-managed credential through the database path.
        """
        self._ephemeral_keys = list(rows or [])
        if self._loaded:
            self.reload()

    def _ensure_loaded(self):
        if not self._loaded:
            self.reload()

    @property
    def db(self):
        """The database this pool was built against.

        Exposed so a caller can notice the connection changed underneath it —
        a pool built during startup could otherwise hold a stale handle for
        the life of the process.
        """
        return self._db

    @property
    def models(self):
        return list(self._models)

    def key_count(self):
        self._ensure_loaded()
        return len(self._keys)

    # ── Availability ────────────────────────────────────────────────────

    def _blocked_until(self, key_id, model):
        """When this bucket frees up, or None if it is available now."""
        now = self._now()
        until = max(self._cooldown.get((key_id, model), 0),
                    self._exhausted.get((key_id, model), 0))
        return until if until > now else None

    async def acquire(self, models=None, exclude=None):
        """Lease an available bucket, or None if every bucket is blocked.

        Models are tried in configured order, so the cheap/high-quota model is
        exhausted across every key before falling back to the next one. Within
        a model, the least recently used key wins, which spreads load evenly
        instead of draining one key at a time.

        ``models`` overrides the configured ladder — used when a caller pins a
        specific model. Bucket state is keyed by ``(key, model)``, so a model
        outside the ladder simply starts with no recorded history, which is
        exactly right.
        """
        exclude = set(exclude or ())
        async with self._lock:
            self._ensure_loaded()
            for model in (models or self._models):
                available = [
                    row for row in self._keys
                    if row.id not in self._disabled_ephemeral
                    and (row.id, model) not in exclude
                    and self._blocked_until(row.id, model) is None
                ]
                if not available:
                    continue
                row = min(available, key=lambda r: self._last_used.get(r.id, 0))
                self._last_used[row.id] = self._now()
                return Lease(key_id=row.id, api_key=row.api_key,
                             label=row.label, model=model)
            return None

    def retry_after_hint(self, models=None):
        """Seconds until the soonest bucket frees up, or None if none exist."""
        self._ensure_loaded()
        now = self._now()
        waits = [until - now
                 for row in self._keys
                 for model in (models or self._models)
                 for until in [self._blocked_until(row.id, model)]
                 if until is not None and until != float('inf')]
        return min(waits) if waits else None

    # ── Outcome reporting ───────────────────────────────────────────────
    # These mutate pool state without holding ``self._lock``. That is safe
    # only because each one is fully synchronous — no ``await`` between the
    # reads and the writes, so no other task can interleave. Keep it that way:
    # if any of these ever needs to await, it must take the lock, and
    # ``acquire`` must not be holding it at the time.

    def report_success(self, lease):
        """Clear any lingering block on a bucket that just answered."""
        bucket = (lease.key_id, lease.model)
        self._cooldown.pop(bucket, None)
        self._unknown_strikes.pop(bucket, None)
        self._invalid_strikes.pop(lease.key_id, None)
        if self._exhausted.pop(bucket, None) is not None:
            self._db.llm_clear_bucket(lease.key_id, lease.model)

    def report_quota(self, lease, scope, retry_after=None, message=None):
        """Record a 429 against a bucket and block it for the right duration."""
        bucket = (lease.key_id, lease.model)
        now = self._now()

        if scope == QUOTA_UNKNOWN:
            strikes = self._unknown_strikes.get(bucket, 0) + 1
            self._unknown_strikes[bucket] = strikes
            if strikes >= _UNKNOWN_STRIKES_TO_DAILY:
                # A bucket that keeps 429ing without ever naming a window is
                # spent for the day in all but the label. Without this it is
                # retried every minute forever and rediscovered on every
                # restart, because only daily exhaustion is persisted.
                logger.warning(
                    'LLM bucket key=%s model=%s: %d unclassified 429s, '
                    'treating as daily exhaustion',
                    lease.key_id, lease.model, strikes)
                scope = QUOTA_DAY
            else:
                delay = retry_after or _UNKNOWN_COOLDOWN
                self._cooldown[bucket] = now + delay
                logger.warning(
                    'LLM bucket key=%s model=%s received an unclassified 429 '
                    '(strike %d of %d); cooling down %.0fs',
                    lease.key_id, lease.model, strikes,
                    _UNKNOWN_STRIKES_TO_DAILY, delay)
                return

        if scope == QUOTA_DAY:
            until = now + retry_after if (retry_after or 0) >= _MIN_DAILY_WAIT \
                else next_daily_reset(now)
            self._exhausted[bucket] = until
            self._db.llm_set_bucket_exhausted(lease.key_id, lease.model, until,
                                              last_error=_truncate(message))
            logger.info('LLM bucket key=%s model=%s daily quota spent, '
                        'blocked for %.0f min', lease.key_id, lease.model,
                        (until - now) / 60)
        else:
            self._cooldown[bucket] = now + (retry_after or _DEFAULT_MINUTE_COOLDOWN)
            self._unknown_strikes.pop(bucket, None)

    def report_transient(self, lease):
        """A 5xx or a dropped connection — the bucket itself is fine."""
        self._cooldown[(lease.key_id, lease.model)] = self._now() + _TRANSIENT_COOLDOWN

    def report_invalid(self, lease, message=None):
        """The key was rejected outright (revoked, malformed, API not enabled).

        A leaked key that Google auto-revoked looks like this — but so does a
        temporary billing or enablement hiccup, so the first rejection only
        benches the key. It is retired on the second, and the retirement is
        logged at ERROR so the logging cog relays it to moderators.

        Returns True if the key was retired, False if it was only benched.
        """
        strikes = self._invalid_strikes.get(lease.key_id, 0) + 1
        self._invalid_strikes[lease.key_id] = strikes

        if strikes < _INVALID_STRIKES_TO_RETIRE:
            until = self._now() + _INVALID_COOLDOWN
            for model in set(self._models + [lease.model]):
                self._cooldown[(lease.key_id, model)] = until
            logger.warning(
                'LLM key id=%s rejected (strike %d/%d), benched for %.0f min: %s',
                lease.key_id, strikes, _INVALID_STRIKES_TO_RETIRE,
                _INVALID_COOLDOWN / 60, _truncate(message))
            return False

        if lease.key_id in self._ephemeral_ids:
            self._disabled_ephemeral.add(lease.key_id)
            for model in set(self._models + [lease.model]):
                self._cooldown[(lease.key_id, model)] = float('inf')
            logger.error(
                'Ephemeral LLM key id=%s disabled for this process after %d '
                'rejections. Fix the environment value and restart, or reset '
                'pool health. Last error: %s',
                lease.key_id, strikes, _truncate(message))
            return False

        logger.error(
            'LLM key id=%s retired after %d rejections — re-add it with '
            '`;llm keys` once fixed. Last error: %s',
            lease.key_id, strikes, _truncate(message))
        self._db.llm_forget_key(lease.key_id, provider='gemini')
        self._keys = [row for row in self._keys if row.id != lease.key_id]
        self._invalid_strikes.pop(lease.key_id, None)
        return True

    def reset_health(self, key_id=None, model=None):
        """Clear in-memory health state, optionally for one key or model.

        Persisted daily quota exhaustion is intentionally left alone: a reset
        should not turn a known-spent Google bucket into another paid probe.
        Returns the number of in-memory state entries removed.
        """
        removed = 0

        def matches(bucket):
            bucket_key, bucket_model = bucket
            return ((key_id is None or bucket_key == key_id)
                    and (model is None or bucket_model == model))

        for mapping in (self._cooldown, self._unknown_strikes):
            for bucket in list(mapping):
                if matches(bucket):
                    mapping.pop(bucket, None)
                    removed += 1
        if model is None:
            for candidate in list(self._invalid_strikes):
                if key_id is None or candidate == key_id:
                    self._invalid_strikes.pop(candidate, None)
                    removed += 1
            for candidate in list(self._disabled_ephemeral):
                if key_id is None or candidate == key_id:
                    self._disabled_ephemeral.discard(candidate)
                    removed += 1
        return removed

    reset = reset_health

    # ── Introspection (for ;llm status) ─────────────────────────────────

    def status(self):
        """Per-bucket state for display. Never includes key material."""
        self._ensure_loaded()
        now = self._now()
        out = []
        for row in self._keys:
            for model in self._models:
                until = self._blocked_until(row.id, model)
                if row.id in self._disabled_ephemeral:
                    state, wait = 'invalid environment key', None
                elif until is None:
                    state, wait = 'ready', None
                elif (row.id, model) in self._exhausted and \
                        self._exhausted[(row.id, model)] > now:
                    state, wait = 'daily quota spent', until - now
                else:
                    state, wait = 'cooling down', until - now
                out.append({'key_id': row.id, 'model': model,
                            'state': state, 'wait': wait})
        return out


def _truncate(text, limit=200):
    if not text:
        return None
    text = str(text)
    return text if len(text) <= limit else text[:limit - 1] + '…'

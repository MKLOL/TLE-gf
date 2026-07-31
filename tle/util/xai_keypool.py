"""In-memory health and ordered model rotation for xAI credentials."""
import logging
import time
from collections import namedtuple

logger = logging.getLogger(__name__)

_MAX_CANDIDATES = 64
_DEFAULT_RATE_COOLDOWN = 60.0
_MAX_RATE_COOLDOWN = 900.0
_TRANSIENT_COOLDOWN = 5.0
_AUTH_COOLDOWN = 600.0
_ACCESS_COOLDOWN = 300.0
_MODEL_ACCESS_COOLDOWN = 1800.0
_MODEL_UNAVAILABLE_COOLDOWN = 1800.0
_INVALID_STRIKES_TO_RETIRE = 2

Lease = namedtuple('XaiLease', 'key_id api_key label model')


def _models(value):
    if isinstance(value, str):
        value = [value]
    return list(dict.fromkeys(item for item in value or [] if item))


def _bounded_wait(value, default, ceiling):
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = default
    return max(1.0, min(value, ceiling))


def _truncate(value, limit=200):
    if not value:
        return None
    value = ' '.join(str(value).split())
    return value if len(value) <= limit else value[:limit - 1] + '…'


class XaiKeyPool:
    """Rotate xAI keys over a strongest-to-weakest model ladder.

    Health is intentionally process-local. Rate limits and provider outages
    are short-lived, while a 403 may be fixed by adding credits or enabling a
    model. Persisting those states would turn a temporary provider condition
    into an outage that survives a restart.
    """

    def __init__(self, db, model, now_fn=time.time, ephemeral_keys=None):
        self.db = db
        self._models = _models(model)
        self._now = now_fn
        self._ephemeral_keys = list(ephemeral_keys or [])
        self._ephemeral_ids = set()
        self._disabled_ephemeral = set()
        self._keys = []
        self._cursor = 0
        self._bucket_cooldown = {}
        self._bucket_reason = {}
        self._key_cooldown = {}
        self._key_reason = {}
        self._model_cooldown = {}
        self._model_reason = {}
        self._invalid_strikes = {}
        self.reload()

    @property
    def model(self):
        """The preferred model, retained for old callers."""
        return self._models[0] if self._models else None

    @property
    def models(self):
        return list(self._models)

    def set_ephemeral_keys(self, rows):
        """Replace process-only key rows and merge them on every reload."""
        self._ephemeral_keys = list(rows or [])
        self.reload()

    def reload(self):
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
            row for row in self.db.llm_get_keys(
                active_only=True, provider='xai')
            if row.id not in ephemeral_ids and row.api_key not in ephemeral_material
        ]
        self._keys = persisted + ephemeral
        self._ephemeral_ids = {row.id for row in ephemeral}
        if self._keys:
            self._cursor %= len(self._keys)
        else:
            self._cursor = 0

    def key_count(self):
        return len(self._keys)

    def candidate_count(self, models=None):
        return len(self._keys) * len(_models(
            self._models if models is None else models))

    def _blocked_until(self, key_id, model):
        now = self._now()
        until = max(self._bucket_cooldown.get((key_id, model), 0),
                    self._key_cooldown.get(key_id, 0),
                    self._model_cooldown.get(model, 0))
        return until if until > now else None

    def _active_reason(self, key_id, model):
        bucket = (key_id, model)
        candidates = (
            (self._bucket_cooldown.get(bucket, 0),
             self._bucket_reason.get(bucket)),
            (self._key_cooldown.get(key_id, 0),
             self._key_reason.get(key_id)),
            (self._model_cooldown.get(model, 0),
             self._model_reason.get(model)),
        )
        active = [(until, reason) for until, reason in candidates
                  if until > self._now()]
        return max(active, default=(None, None), key=lambda item: item[0])

    def is_available(self, lease):
        active_ids = {row.id for row in self._keys}
        return (lease.key_id in active_ids
                and lease.key_id not in self._disabled_ephemeral
                and self._blocked_until(lease.key_id, lease.model) is None)

    def leases(self, max_attempts=None, models=None):
        """Return an ordered rotating snapshot of healthy key/model buckets."""
        if not self._keys:
            return []
        ladder = _models(self._models if models is None else models)
        start = self._cursor % len(self._keys)
        self._cursor = (start + 1) % len(self._keys)
        ordered = self._keys[start:] + self._keys[:start]
        candidates = [
            Lease(row.id, row.api_key, row.label, model)
            for model in ladder for row in ordered
        ]
        limit = (_MAX_CANDIDATES if max_attempts is None else
                 min(max(0, int(max_attempts)), _MAX_CANDIDATES))
        return [lease for lease in candidates if self.is_available(lease)][:limit]

    def retry_after_hint(self, models=None):
        now = self._now()
        ladder = _models(self._models if models is None else models)
        waits = []
        for row in self._keys:
            for model in ladder:
                until = self._blocked_until(row.id, model)
                if until is not None and until != float('inf'):
                    waits.append(max(0.0, until - now))
        return min(waits) if waits else None

    def report_success(self, lease):
        bucket = (lease.key_id, lease.model)
        self._bucket_cooldown.pop(bucket, None)
        self._bucket_reason.pop(bucket, None)
        self._key_cooldown.pop(lease.key_id, None)
        self._key_reason.pop(lease.key_id, None)
        self._model_cooldown.pop(lease.model, None)
        self._model_reason.pop(lease.model, None)
        self._invalid_strikes.pop(lease.key_id, None)

    def report_rate_limit(self, lease, retry_after=None, message=None):
        wait = _bounded_wait(retry_after, _DEFAULT_RATE_COOLDOWN,
                             _MAX_RATE_COOLDOWN)
        bucket = (lease.key_id, lease.model)
        self._bucket_cooldown[bucket] = self._now() + wait
        self._bucket_reason[bucket] = ('rate limited', _truncate(message))
        return wait

    def report_transient(self, lease, message=None):
        bucket = (lease.key_id, lease.model)
        self._bucket_cooldown[bucket] = self._now() + _TRANSIENT_COOLDOWN
        self._bucket_reason[bucket] = ('provider/network cooldown',
                                       _truncate(message))

    def report_invalid(self, lease, message=None):
        """Bench once, then retire persisted keys after a later rejection."""
        strikes = self._invalid_strikes.get(lease.key_id, 0) + 1
        self._invalid_strikes[lease.key_id] = strikes
        if strikes < _INVALID_STRIKES_TO_RETIRE:
            self._key_cooldown[lease.key_id] = self._now() + _AUTH_COOLDOWN
            self._key_reason[lease.key_id] = ('authentication cooldown',
                                              _truncate(message))
            return False

        if lease.key_id in self._ephemeral_ids:
            self._disabled_ephemeral.add(lease.key_id)
            self._key_cooldown[lease.key_id] = float('inf')
            self._key_reason[lease.key_id] = ('invalid environment key',
                                              _truncate(message))
            logger.error('Ephemeral xAI key id=%s disabled for this process',
                         lease.key_id)
            return False

        self.db.llm_forget_key(lease.key_id, provider='xai')
        self._keys = [row for row in self._keys if row.id != lease.key_id]
        self._invalid_strikes.pop(lease.key_id, None)
        logger.error('xAI key id=%s retired after %d authentication failures',
                     lease.key_id, strikes)
        return True

    def report_access(self, lease, message=None, model_specific=False):
        """Open a reversible circuit for a 403 access or billing failure."""
        if model_specific:
            bucket = (lease.key_id, lease.model)
            self._bucket_cooldown[bucket] = (
                self._now() + _MODEL_ACCESS_COOLDOWN)
            self._bucket_reason[bucket] = ('model access cooldown',
                                           _truncate(message))
            return
        self._key_cooldown[lease.key_id] = self._now() + _ACCESS_COOLDOWN
        self._key_reason[lease.key_id] = ('access/billing cooldown',
                                          _truncate(message))

    def report_model_unavailable(self, lease, message=None):
        self._model_cooldown[lease.model] = (
            self._now() + _MODEL_UNAVAILABLE_COOLDOWN)
        self._model_reason[lease.model] = ('model unavailable',
                                           _truncate(message))

    def reset_health(self, key_id=None, model=None):
        """Clear reversible circuits, optionally scoped to one key/model."""
        removed = 0

        def bucket_matches(bucket):
            return ((key_id is None or bucket[0] == key_id)
                    and (model is None or bucket[1] == model))

        for mapping in (self._bucket_cooldown, self._bucket_reason):
            for bucket in list(mapping):
                if bucket_matches(bucket):
                    mapping.pop(bucket, None)
                    removed += 1
        if model is None:
            for mapping in (self._key_cooldown, self._key_reason,
                            self._invalid_strikes):
                for candidate in list(mapping):
                    if key_id is None or candidate == key_id:
                        mapping.pop(candidate, None)
                        removed += 1
            for candidate in list(self._disabled_ephemeral):
                if key_id is None or candidate == key_id:
                    self._disabled_ephemeral.discard(candidate)
                    removed += 1
        if key_id is None:
            for mapping in (self._model_cooldown, self._model_reason):
                for candidate in list(mapping):
                    if model is None or candidate == model:
                        mapping.pop(candidate, None)
                        removed += 1
        return removed

    reset = reset_health

    def status(self):
        """Return health rows without credential material."""
        now = self._now()
        out = []
        for row in self._keys:
            for model in self._models:
                until, reason = self._active_reason(row.id, model)
                if row.id in self._disabled_ephemeral:
                    state, wait = 'invalid environment key', None
                elif until is None:
                    state, wait = 'ready', None
                else:
                    state = reason[0] if reason else 'cooling down'
                    wait = None if until == float('inf') else until - now
                out.append({
                    'key_id': row.id, 'model': model,
                    'state': state, 'wait': wait,
                })
        return out

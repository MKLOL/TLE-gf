"""Bounded round-robin rotation for Gemini API keys.

Each Gemini request receives a :class:`GeminiKeyCycle`.  A key is taken from
the front of the shared queue for an API call and returned to the back after
the call finishes.  The cycle remembers which keys the request has tried, so
it stops after one full circulation when every key returns a quota error.

This module deliberately does not track quotas, cooldowns, or rate limits.
The Gemini API response is the authority on whether a key has quota.
"""

import os
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterable, Set


_ENV_NAME = 'GEMINI_API_KEYS'


@dataclass(frozen=True)
class GeminiApiKey:
    """A selected key whose secret is excluded from ``repr``."""

    index: int
    value: str = field(repr=False)

    @property
    def label(self) -> str:
        return f'key #{self.index + 1}'


class GeminiKeyCycleExhausted(RuntimeError):
    """Raised after one request has tried every configured key."""


class GeminiKeyCycle:
    """One request's bounded circulation through the shared key queue."""

    def __init__(self, pool: 'GeminiKeyPool'):
        self._pool = pool
        self._seen: Set[int] = set()

    def next_key(self) -> GeminiApiKey:
        """Take the next key that this request has not tried."""
        return self._pool._take_key(self._seen)

    def complete_call(self, key: GeminiApiKey) -> None:
        """Return a call's key to the back of the shared queue."""
        self._pool._return_key(key)


class GeminiKeyPool:
    """Thread-safe queue built from the comma-separated environment value."""

    def __init__(self, api_keys: Iterable[str]):
        keys = tuple(key.strip() for key in api_keys if key.strip())
        if not keys:
            raise ValueError('At least one Gemini API key is required.')
        if len(set(keys)) != len(keys):
            raise ValueError('GEMINI_API_KEYS contains a duplicate key.')

        self._queue: Deque[GeminiApiKey] = deque(
            GeminiApiKey(index=index, value=value)
            for index, value in enumerate(keys)
        )
        self._lock = threading.Lock()

    @classmethod
    def from_environment(cls) -> 'GeminiKeyPool':
        """Build a pool from ``GEMINI_API_KEYS``."""
        value = os.environ.get(_ENV_NAME)
        if value is None:
            raise ValueError(f'{_ENV_NAME} is not set.')
        return cls(value.split(','))

    def cycle(self) -> GeminiKeyCycle:
        """Create a single-circulation key cycle for one Gemini request."""
        return GeminiKeyCycle(self)

    def _take_key(self, seen: Set[int]) -> GeminiApiKey:
        with self._lock:
            for _ in range(len(self._queue)):
                key = self._queue.popleft()
                if key.index not in seen:
                    seen.add(key.index)
                    return key
                self._queue.append(key)
        raise GeminiKeyCycleExhausted('Every Gemini API key was tried.')

    def _return_key(self, key: GeminiApiKey) -> None:
        with self._lock:
            self._queue.append(key)

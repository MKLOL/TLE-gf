"""Serialize vote, repair and expiry writes to each poll's Discord messages."""
import asyncio
from weakref import WeakValueDictionary

_locks = WeakValueDictionary()


def poll_lock(poll_id):
    # Holders and waiters retain the lock; idle polls need no permanent entry.
    lock = _locks.get(poll_id)
    if lock is None:
        lock = asyncio.Lock()
        _locks[poll_id] = lock
    return lock

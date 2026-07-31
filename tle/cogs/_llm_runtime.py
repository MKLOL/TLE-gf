"""In-process concurrency and deadline protection for LLM requests."""
import asyncio


class RequestBusyError(Exception):
    """The same Discord user already has a request in flight."""


class ProviderQueueError(Exception):
    """The provider queue did not open within its short wait budget."""


class RequestDeadlineError(Exception):
    """The complete context/router/answer path exceeded its deadline."""


class RequestRuntime:
    """One active request per user plus small per-provider semaphores."""

    def __init__(self, limits, *, queue_timeout, request_timeout):
        self._limits = {name: max(1, int(value))
                        for name, value in limits.items()}
        self._queue_timeout = max(0.01, float(queue_timeout))
        self._request_timeout = max(0.01, float(request_timeout))
        self._active_users = set()
        self._semaphores = {}

    def _semaphore(self, provider):
        loop = asyncio.get_running_loop()
        entry = self._semaphores.get(provider)
        if entry is None or entry[0] is not loop:
            entry = (loop, asyncio.Semaphore(self._limits[provider]))
            self._semaphores[provider] = entry
        return entry[1]

    async def run(self, provider, user_id, operation):
        """Run an async callable inside all admission/deadline guards."""
        user_key = str(user_id)
        if user_key in self._active_users:
            raise RequestBusyError
        self._active_users.add(user_key)
        semaphore = self._semaphore(provider)
        acquired = False
        try:
            try:
                await asyncio.wait_for(
                    semaphore.acquire(), timeout=self._queue_timeout)
                acquired = True
            except TimeoutError as err:
                raise ProviderQueueError from err
            try:
                return await asyncio.wait_for(
                    operation(), timeout=self._request_timeout)
            except TimeoutError as err:
                raise RequestDeadlineError from err
        finally:
            if acquired:
                semaphore.release()
            self._active_users.discard(user_key)

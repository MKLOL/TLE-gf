"""Discover and chronologically merge Discord histories for Great Day."""
from __future__ import annotations

from dataclasses import dataclass
import heapq

import discord

from tle.cogs._greatday_event_parse import message_time


@dataclass(frozen=True)
class HistoryChannelScope:
    channels: tuple
    discovery_failures: int = 0


def _channel_key(channel, fallback):
    channel_id = getattr(channel, 'id', None)
    return f'id:{channel_id}' if channel_id is not None else f'index:{fallback}'


def _add_channel(channels, seen, channel):
    if not callable(getattr(channel, 'history', None)):
        return
    channel_id = getattr(channel, 'id', None)
    identity = ('id', str(channel_id)) if channel_id is not None else ('obj', id(channel))
    if identity not in seen:
        seen.add(identity)
        channels.append(channel)


async def _add_archived_threads(parent, channels, seen, **kwargs):
    archived_threads = getattr(parent, 'archived_threads', None)
    if not callable(archived_threads):
        return 0
    try:
        async for thread in archived_threads(limit=None, **kwargs):
            _add_channel(channels, seen, thread)
    except (discord.Forbidden, discord.NotFound):
        return 1
    return 0


async def discover_guild_history_channels(guild, extra_channels=()):
    """Return every discoverable message channel, including archived threads.

    Public archived threads and private archived threads joined by the bot are
    fetched because Discord does not retain archived threads in the guild
    cache. A failed listing is reported so the final audit cannot claim that
    the reconstructed history is complete.
    """
    channels = []
    seen = set()
    parents = list(getattr(guild, 'channels', None) or ())
    for channel in (*parents, *(getattr(guild, 'threads', None) or ()),
                    *extra_channels):
        _add_channel(channels, seen, channel)

    failures = 0
    for parent in parents:
        failures += await _add_archived_threads(parent, channels, seen)
        if isinstance(parent, discord.TextChannel):
            failures += await _add_archived_threads(
                parent, channels, seen, private=True, joined=True)
    return HistoryChannelScope(tuple(channels), failures)


def _message_heap_key(message, serial):
    try:
        message_id = int(message.id)
    except (TypeError, ValueError):
        message_id = 0
    return message_time(message), message_id, serial


async def merged_channel_history(channels, *, tolerate_unreadable=False,
                                 on_unreadable=None):
    """Yield ``(channel_key, message)`` globally oldest-first.

    Histories are merged lazily, keeping only one pending message per channel.
    This preserves cross-channel membership order without holding an entire
    server history in memory.
    """
    heap = []
    serial = 0

    async def advance(channel_key, iterator):
        nonlocal serial
        try:
            message = await iterator.__anext__()
        except StopAsyncIteration:
            return
        except (discord.Forbidden, discord.NotFound):
            if not tolerate_unreadable:
                raise
            if on_unreadable is not None:
                on_unreadable(channel_key)
            return
        serial += 1
        heapq.heappush(
            heap, (*_message_heap_key(message, serial), channel_key,
                   message, iterator))

    for index, channel in enumerate(channels):
        channel_key = _channel_key(channel, index)
        iterator = channel.history(limit=None, oldest_first=True).__aiter__()
        await advance(channel_key, iterator)

    while heap:
        _, _, _, channel_key, message, iterator = heapq.heappop(heap)
        yield channel_key, message
        await advance(channel_key, iterator)

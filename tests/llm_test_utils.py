"""Shared fakes for the ``;llm`` test modules."""
import asyncio
from datetime import datetime, timedelta, timezone
import sqlite3

import discord

from tle.util.db.llm_db import LlmDbMixin
from tle.util.db.user_db_conn import namedtuple_factory


def run(coro):
    """Drive a coroutine to completion (no pytest-asyncio in this repo)."""
    return asyncio.run(coro)


class FakeLlmDb(LlmDbMixin):
    """In-memory database exposing only the LLM tables and methods."""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_llm_tables()
        self.conn.commit()


class FakeClock:
    """Deterministic replacement for ``time.time``."""

    def __init__(self, now=1_700_000_000.0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        return self.now


def quota_error(quota_id=None, message='Quota exceeded', retry_delay=None):
    """Build a Gemini 429 error body the way the real API shapes it."""
    details = []
    if quota_id is not None:
        details.append({
            '@type': 'type.googleapis.com/google.rpc.QuotaFailure',
            'violations': [{'quotaMetric': 'generativelanguage.googleapis.com/'
                                           'generate_content_free_tier_requests',
                            'quotaId': quota_id}],
        })
    if retry_delay is not None:
        details.append({'@type': 'type.googleapis.com/google.rpc.RetryInfo',
                        'retryDelay': retry_delay})
    return {'error': {'code': 429, 'status': 'RESOURCE_EXHAUSTED',
                      'message': message, 'details': details}}


def text_response(text, finish_reason='STOP'):
    """Build a successful ``generateContent`` response body."""
    return {'candidates': [{'content': {'parts': [{'text': text}]},
                            'finishReason': finish_reason}]}


class FakeAttachment:
    def __init__(self, content_type='image/png', size=1024, data=b'\x89PNG',
                 fail=False):
        self.content_type = content_type
        self.size = size
        self._data = data
        self._fail = fail

    async def read(self):
        if self._fail:
            raise OSError('download failed')
        return self._data


_HISTORY_BASE = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


class HistMessage:
    """A message with the attributes history collection actually reads."""

    def __init__(self, author='someone', content='hi', offset=0,
                 attachments=None, is_bot=False, author_id=1):
        self.content = content
        self.attachments = attachments or []
        self.created_at = _HISTORY_BASE + timedelta(seconds=offset)
        self.author = type('A', (), {'display_name': author, 'bot': is_bot,
                                     'id': author_id})()


class FakeHistoryChannel:
    """Serves a fixed message list through an async ``history()`` iterator."""

    def __init__(self, messages, fail=False):
        self.messages = sorted(messages, key=lambda message: message.created_at)
        self.fail = fail
        self.calls = []

    def history(self, limit=None, before=None, after=None, oldest_first=None):
        self.calls.append({'limit': limit, 'before': before, 'after': after,
                           'oldest_first': oldest_first})
        channel = self

        class _Iter:
            def __aiter__(self):
                if channel.fail:
                    raise RuntimeError('missing Read Message History')
                picked = list(channel.messages)  # ascending by created_at
                if before is not None:
                    anchor = getattr(before, 'created_at', before)
                    picked = [m for m in picked if m.created_at < anchor]
                if after is not None:
                    anchor = getattr(after, 'created_at', after)
                    picked = [m for m in picked if m.created_at > anchor]
                # Mirror discord.py: oldest_first defaults to True when
                # `after` is given, and `limit` applies in traversal order.
                ascending = oldest_first
                if ascending is None:
                    ascending = after is not None
                if not ascending:
                    picked.reverse()
                if limit is not None:
                    picked = picked[:limit]
                self._items = iter(picked)
                return self

            async def __anext__(self):
                try:
                    return next(self._items)
                except StopIteration:
                    raise StopAsyncIteration

        return _Iter()


class FakeGatherCtx:
    def __init__(self, channel, message):
        self.channel = channel
        self.message = message


class FakeMessage(discord.Message):
    """Stands in for a discord Message, including for ``isinstance`` checks."""

    def __init__(self, content='', attachments=None, author_name='someone'):
        self.content = content
        self.attachments = attachments or []
        self.author = type('Author', (), {'display_name': author_name})()
        self.deleted = False
        self.delete_error = None

    async def delete(self):
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted = True

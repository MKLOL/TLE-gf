"""Shared fakes for the ``;llm`` test modules."""
import asyncio
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

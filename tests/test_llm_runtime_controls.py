"""Integration coverage for LLM admission, deadlines, and explicit context."""
import asyncio

import pytest

from tle.cogs import llm as llm_cog
from tle.cogs._llm_runtime import (
    ProviderQueueError, RequestBusyError, RequestDeadlineError, RequestRuntime,
)
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tle.util.llm_keypool import Lease
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx
from tests.llm_test_utils import FakeHistoryChannel, HistMessage


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    return database


def _invoke(cog, ctx, question):
    return run(llm_cog.Llm.llm.__wrapped__(cog, ctx, question=question))


class TestRequestRuntime:
    def test_duplicate_queue_and_deadline_guards_release_cleanly(self):
        async def scenario():
            runtime = RequestRuntime(
                {'gemini': 1}, queue_timeout=.01, request_timeout=.03)
            started, release = asyncio.Event(), asyncio.Event()

            async def held():
                started.set()
                await release.wait()
                return 'first'

            first = asyncio.create_task(runtime.run('gemini', 1, held))
            await started.wait()
            with pytest.raises(RequestBusyError):
                await runtime.run('gemini', 1, lambda: asyncio.sleep(0))
            with pytest.raises(ProviderQueueError):
                await runtime.run('gemini', 2, lambda: asyncio.sleep(0))
            release.set()
            assert await first == 'first'

            with pytest.raises(RequestDeadlineError):
                await runtime.run('gemini', 3, lambda: asyncio.sleep(1))
            return await runtime.run('gemini', 4, lambda: _answer('ready'))

        assert run(scenario()) == 'ready'


class TestDeadlineAccounting:
    @staticmethod
    def _short_runtime(cog):
        cog._runtime = RequestRuntime(
            {'gemini': 1, 'xai': 1},
            queue_timeout=.02, request_timeout=.02)

    def test_gemini_deadline_is_visible_in_telemetry(
            self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        cog = llm_cog.Llm(bot=None)
        self._short_runtime(cog)

        async def slow(pool, prompt, **kwargs):
            kwargs['stats']['attempts'] += 1
            await asyncio.sleep(1)

        monkeypatch.setattr(gemini_api, 'complete', slow)
        ctx = FakeCtx()
        _invoke(cog, ctx, '+direct hello')

        row = db.conn.execute(
            "SELECT outcome, answer_attempts FROM llm_request_usage"
        ).fetchone()
        assert (row.outcome, row.answer_attempts) == ('deadline', 1)
        assert 'timed out' in ctx.text

    def test_xai_deadline_finalizes_guard_and_telemetry(
            self, db, monkeypatch):
        db.llm_add_key('xai-ExampleKeyValue1234567890', provider='xai')
        cog = llm_cog.Llm(bot=None)
        self._short_runtime(cog)

        async def slow(pool, prompt, **kwargs):
            kwargs['stats']['attempts'] += 1
            await asyncio.sleep(1)

        monkeypatch.setattr(xai_api, 'complete', slow)
        ctx = FakeCtx()
        _invoke(cog, ctx, '+grok +direct hello')

        request = db.conn.execute(
            'SELECT outcome FROM llm_xai_request').fetchone()
        usage = db.conn.execute(
            'SELECT outcome, answer_attempts FROM llm_request_usage'
        ).fetchone()
        assert request.outcome == 'timeout'
        assert (usage.outcome, usage.answer_attempts) == ('deadline', 1)
        assert 'timed out' in ctx.text

    def test_finalization_failure_does_not_hide_a_grok_answer(
            self, db, monkeypatch):
        db.llm_add_key('xai-ExampleKeyValue1234567890', provider='xai')
        cog = llm_cog.Llm(bot=None)

        async def answer(pool, prompt, **kwargs):
            return 'still delivered', xai_api.Lease(
                1, 'redacted', 'test', 'grok-test')

        def fail_finalize(*args, **kwargs):
            raise RuntimeError('disk busy')

        monkeypatch.setattr(xai_api, 'complete', answer)
        monkeypatch.setattr(db, 'llm_finalize_xai_request', fail_finalize)
        ctx = FakeCtx()
        _invoke(cog, ctx, '+grok +direct hello')
        assert 'still delivered' in ctx.text


class TestExplicitContextIntegration:
    def test_bare_context_control_uses_default_summary_prompt(
            self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = []

        async def answer(pool, prompt, **kwargs):
            seen.append(prompt)
            return 'summary', Lease(1, 'redacted', 'test', 'model-a')

        monkeypatch.setattr(gemini_api, 'complete', answer)
        channel = FakeHistoryChannel([
            HistMessage(content='first point', offset=0),
            HistMessage(content='second point', offset=10),
        ])
        channel.id = 55
        command = HistMessage(content=';llm +context', offset=20)
        ctx = FakeCtx(message=command, channel=channel)
        _invoke(llm_cog.Llm(bot=None), ctx, '+context')
        assert len(seen) == 1
        assert 'first point' in seen[0] and 'second point' in seen[0]
        assert 'Summarize this conversation.' in seen[0]

    def test_malformed_message_count_never_reaches_provider(
            self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

        async def forbidden(*args, **kwargs):
            raise AssertionError('malformed control reached provider')

        monkeypatch.setattr(gemini_api, 'complete', forbidden)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm(bot=None), ctx, 'messages=many summarize')
        assert 'positive whole number' in ctx.text

    def test_question_credentials_are_redacted_before_provider(
            self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = []
        secret = 'xai-abcdefghijklmnopqrstuv-secret'

        async def answer(pool, prompt, **kwargs):
            seen.append(prompt)
            return 'ok', Lease(1, 'redacted', 'test', 'model-a')

        monkeypatch.setattr(gemini_api, 'complete', answer)
        _invoke(llm_cog.Llm(bot=None), FakeCtx(),
                f'+direct explain API_KEY={secret}')
        assert secret not in seen[0]
        assert '[REDACTED]' in seen[0]


async def _answer(value):
    return value

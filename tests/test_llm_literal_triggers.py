"""Literal ``@gemini`` and ``@grok`` entry-point tests."""

from datetime import timedelta

import pytest

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx
from tests.llm_test_utils import FakeHistoryChannel, HistMessage


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'GEMINI_API_KEYS', '')
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    return database


def _add_xai_key(db):
    db.llm_add_key('xai-ExampleKeyValue1234567890', provider='xai')


def _xai_answers(monkeypatch, answer='Grok answer'):
    seen = []

    async def fake_complete(pool, prompt, **kwargs):
        seen.append({'prompt': prompt, 'kwargs': kwargs})
        return answer, xai_api.Lease(1, 'redacted', 'test', 'grok-live')

    monkeypatch.setattr(xai_api, 'complete', fake_complete)
    return seen


def _gemini_answers(monkeypatch, answer='Gemini answer'):
    seen = []

    async def fake_complete(pool, prompt, **kwargs):
        seen.append(prompt)
        from tle.util.llm_keypool import Lease
        return answer, Lease(1, 'redacted', 'test', 'model-a')

    monkeypatch.setattr(gemini_api, 'complete', fake_complete)
    return seen


class _FakeBot:
    def __init__(self, ctx):
        self.ctx = ctx
        self.calls = 0
        self.user = None

    async def get_context(self, message):
        self.calls += 1
        self.ctx.message = message
        return self.ctx


def _listener_message(content, guild=True, bot=False):
    message = FakeMessage(content=content)
    message.guild = type('G', (), {'id': 100})() if guild else None
    message.author = type('Author', (), {
        'bot': bot, 'id': 1, 'display_name': 'nife'})()
    return message


class TestLiteralTrigger:
    def test_literal_trigger_uses_the_shared_grok_path(self, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        ctx = FakeCtx()
        bot = _FakeBot(ctx)
        cog = llm_cog.Llm(bot)
        message = _listener_message('@grok hello there')
        run(cog.on_message(message))
        assert seen[-1]['prompt'] == 'hello there'
        assert 'Grok answer' in ctx.text
        assert ctx.send_kwargs[0]['reference'] is message
        assert ctx.send_kwargs[0]['mention_author'] is False

    def test_literal_grok_summarize_this_uses_channel_context(
            self, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)

        history = [
            HistMessage(author='alice', content='first discussion point',
                        offset=0),
            HistMessage(author='bob', content='second discussion point',
                        offset=10),
        ]
        channel = FakeHistoryChannel(history)
        ctx = FakeCtx(channel=channel)
        cog = llm_cog.Llm(_FakeBot(ctx))

        message = _listener_message('@grok summarize this')
        message.created_at = history[-1].created_at + timedelta(seconds=10)

        run(cog.on_message(message))

        assert len(seen) == 1
        prompt = seen[0]['prompt']
        assert 'BEGIN TRANSCRIPT' in prompt
        assert 'first discussion point' in prompt
        assert 'second discussion point' in prompt
        assert 'summarize this' in prompt
        assert 'Grok answer' in ctx.text

    def test_literal_gemini_uses_the_shared_path(self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = _gemini_answers(monkeypatch)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        message = _listener_message('@gemini +direct hello there')
        run(cog.on_message(message))
        assert seen[-1] == 'hello there'
        assert 'Gemini answer' in ctx.text
        assert ctx.send_kwargs[0]['reference'] is message
        assert ctx.send_kwargs[0]['mention_author'] is False

    def test_literal_provider_cannot_be_switched_by_prompt_selector(
            self, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        _add_xai_key(db)
        gemini_seen = _gemini_answers(monkeypatch)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message(
            '@gemini +direct +grok stay with Gemini')))
        assert gemini_seen[-1] == '+grok stay with Gemini'

        xai_seen = _xai_answers(monkeypatch)

        async def no_gemini(*args, **kwargs):
            raise AssertionError('Gemini must not handle literal Grok')

        monkeypatch.setattr(gemini_api, 'complete', no_gemini)
        run(cog.on_message(_listener_message(
            '@grok +direct +gemini stay with Grok')))
        assert xai_seen[-1]['prompt'].endswith('+gemini stay with Grok')

    def test_trigger_is_case_insensitive_and_allows_leading_space(
            self, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message('  @GrOk   hi')))
        assert seen[-1]['prompt'] == 'hi'

        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        gemini_seen = _gemini_answers(monkeypatch)
        run(cog.on_message(_listener_message('  @GeMiNi   +direct hi')))
        assert gemini_seen[-1] == 'hi'

    @pytest.mark.parametrize('content,guild,author_bot', [
        ('hey @grok hi', True, False),
        ('@groks hi', True, False),
        ('@grokish hi', True, False),
        ('hey @gemini hi', True, False),
        ('@geminis hi', True, False),
        ('@geminiish hi', True, False),
        ('@grok hi', False, False),
        ('@gemini hi', False, False),
        ('@grok hi', True, True),
        ('@gemini hi', True, True),
    ])
    def test_near_matches_dms_and_bots_are_ignored(
            self, content, guild, author_bot):
        ctx = FakeCtx()
        bot = _FakeBot(ctx)
        cog = llm_cog.Llm(bot)
        run(cog.on_message(_listener_message(
            content, guild=guild, bot=author_bot)))
        assert bot.calls == 0
        assert ctx.sent == []

    @pytest.mark.parametrize('provider,text', (
        ('grok', 'what does this mean?'),
        ('gemini', 'explain this proof'),
    ))
    def test_empty_trigger_can_ask_about_a_reply(
            self, db, monkeypatch, provider, text):
        if provider == 'grok':
            _add_xai_key(db)
            seen = _xai_answers(monkeypatch)
        else:
            db.llm_add_key('AIzaSyExampleKeyValue1234567')
            seen = _gemini_answers(monkeypatch)
        message = _listener_message(f'@{provider}')
        target = FakeMessage(content=text, author_name='alice')
        message.reference = type('Ref', (), {
            'resolved': target, 'message_id': 8})()
        ctx = FakeCtx(message=message)
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(message))
        prompt = seen[-1]['prompt'] if provider == 'grok' else seen[-1]
        assert text in prompt

    @pytest.mark.parametrize('trigger,usage', (
        ('@grok', '@grok <question>'),
        ('@gemini', '@gemini <question>'),
    ))
    def test_empty_trigger_without_reply_shows_provider_usage(
            self, trigger, usage):
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message(trigger)))
        assert usage in ctx.text

    @pytest.mark.parametrize('trigger', ('@grok hi', '@gemini hi'))
    def test_startup_race_is_reported_not_raised(self, monkeypatch, trigger):
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message(trigger)))
        assert 'starting up' in ctx.text

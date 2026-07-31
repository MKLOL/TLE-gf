"""Provider routing, key management, and literal trigger tests for Grok."""
from discord.ext import commands
import pytest

from tle import constants
from tle.cogs import _llm_context as llm_context
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, xai_api
from tests.llm_test_utils import FakeAttachment, FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    return database


@pytest.fixture
def cog():
    return llm_cog.Llm(bot=None)


def _invoke(command, *args, **kwargs):
    return run(command.__wrapped__(*args, **kwargs))


def _add_xai_key(db, value='xai-ExampleKeyValue1234567890'):
    db.llm_add_key(value, provider='xai')


def _xai_answers(monkeypatch, answer='Grok answer', model='grok-live'):
    seen = []

    async def fake_complete(pool, prompt, **kwargs):
        seen.append({'prompt': prompt, 'kwargs': kwargs})
        return answer, xai_api.Lease(1, 'redacted', 'test', model)

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


class TestProviderSelector:
    def test_grok_needs_no_gemini_key(self, cog, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert seen[-1]['prompt'] == 'hello'
        assert 'Grok answer' in ctx.text
        assert cog._get_pool().key_count() == 0

    def test_selector_is_case_insensitive(self, cog, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+GrOk hello')
        assert seen[-1]['prompt'] == 'hello'

    def test_near_match_remains_a_gemini_question(self, cog, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = _gemini_answers(monkeypatch)

        async def no_xai(*args, **kwargs):
            raise AssertionError('xAI must not handle +grokish')

        monkeypatch.setattr(xai_api, 'complete', no_xai)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grokish hello')
        assert seen[-1] == '+grokish hello'
        assert 'Gemini answer' in ctx.text

    def test_bare_selector_without_reply_shows_usage(self, cog, db):
        _add_xai_key(db)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok')
        assert '@grok <question>' in ctx.text

    def test_missing_xai_key_names_both_configuration_paths(self, cog):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert ';llm grokkeys' in ctx.text
        assert 'XAI_API_KEY' in ctx.text


class TestGrokAskFlow:
    @pytest.fixture(autouse=True)
    def _key(self, db):
        _add_xai_key(db)

    def test_router_and_answer_both_use_xai(self, cog, monkeypatch):
        seen = _xai_answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+grok explain BIT')
        assert len(seen) == 2
        assert seen[0]['kwargs']['system_instruction'] == \
            llm_context.CLASSIFIER_INSTRUCTION
        assert seen[0]['kwargs']['reasoning_effort'] == 'none'
        assert seen[1]['kwargs']['system_instruction'] == \
            llm_context.GROK_SYSTEM_INSTRUCTION

    def test_prompt_has_requested_voice_without_losing_base_rules(self):
        prompt = llm_context.GROK_SYSTEM_INSTRUCTION.lower()
        assert 'profanity' in prompt and 'roast' in prompt
        assert 'competitive programmers' in prompt
        assert 'less accurate' in prompt

    def test_answer_footer_uses_actual_grok_model(self, cog, monkeypatch):
        _xai_answers(monkeypatch, model='grok-4.3')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert ctx.sent[-1].footer['text'] == 'grok-4.3'

    def test_reply_text_and_image_follow_the_existing_pipeline(
            self, cog, monkeypatch):
        seen = _xai_answers(monkeypatch)
        target = FakeMessage(
            content='this code is cursed', author_name='tourist',
            attachments=[FakeAttachment('image/png', data=b'PNG')])
        message = FakeMessage()
        message.reference = type('Ref', (), {
            'resolved': target, 'message_id': 5})()
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(message=message),
                question='+grok why?')
        assert 'this code is cursed' in seen[-1]['prompt']
        assert 'why?' in seen[-1]['prompt']
        assert seen[-1]['kwargs']['images'] == [('image/png', b'PNG')]

    def test_unsupported_image_does_not_crowd_out_a_png(
            self, cog, monkeypatch):
        monkeypatch.setattr(constants, 'LLM_MAX_IMAGES', 1)
        seen = _xai_answers(monkeypatch)
        message = FakeMessage(attachments=[
            FakeAttachment('image/webp', data=b'WEBP'),
            FakeAttachment('image/png', data=b'PNG'),
        ])
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(message=message),
                question='+grok inspect')
        assert seen[-1]['kwargs']['images'] == [('image/png', b'PNG')]

    def test_access_denied_explains_team_credits(self, cog, monkeypatch):
        async def denied(pool, prompt, **kwargs):
            stats = kwargs.get('stats')
            if stats is not None:
                stats['attempts'] = 1
            raise xai_api.AccessDeniedError('team blocked')

        monkeypatch.setattr(xai_api, 'complete', denied)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert 'unfunded' in ctx.text
        assert 'xAI Console' in ctx.text

    def test_mixed_key_failures_get_a_neutral_diagnosis(self, cog, monkeypatch):
        async def mixed(pool, prompt, **kwargs):
            stats = kwargs.get('stats')
            if stats is not None:
                stats['attempts'] = 2
            raise xai_api.NoCapacityError('mixed')

        monkeypatch.setattr(xai_api, 'complete', mixed)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert 'grokkeylist' in ctx.text
        assert 'invalid or revoked' not in ctx.text

    def test_success_records_usage(self, cog, monkeypatch, db):
        _xai_answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+grok hello')
        assert db.llm_get_usage(100, 1, llm_cog._today()) == 1


class TestGrokKeys:
    KEY = 'xai-RuntimeUploadKey1234567890'

    def test_upload_deletes_redacts_and_isolates_the_key(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.grokkeys, cog, ctx, self.KEY)
        assert ctx.message.deleted is True
        assert self.KEY not in ctx.text
        assert db.llm_get_keys(provider='gemini') == []
        assert db.llm_get_keys(provider='xai')[0].api_key == self.KEY
        assert cog._get_pool().key_count() == 0
        assert cog._get_xai_pool().key_count() == 1

    def test_list_and_forget_are_provider_scoped(self, cog, db):
        db.llm_add_key('AIzaSyGeminiKeyValue123456789', provider='gemini')
        db.llm_add_key(self.KEY, provider='xai')
        xai_id = db.llm_get_keys(provider='xai')[0].id
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.grokkeylist, cog, ctx)
        assert self.KEY not in ctx.sent[0].description
        assert 'AIzaSy' not in ctx.sent[0].description
        _invoke(llm_cog.Llm.grokkeyforget, cog, FakeCtx(roles=('Moderator',)),
                xai_id)
        assert db.llm_get_keys(provider='xai') == []
        assert len(db.llm_get_keys(provider='gemini')) == 1

    def test_environment_key_is_imported_once(self, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'XAI_API_KEYS', self.KEY)
        assert cog._get_xai_pool().key_count() == 1
        cog._get_xai_pool()
        assert len(db.llm_get_keys(provider='xai')) == 1

    def test_xai_key_pasted_into_gemini_command_is_redirected(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self.KEY)
        assert 'use `;llm grokkeys`' in ctx.text
        assert db.llm_get_keys(provider='gemini') == []

    def test_gemini_key_is_not_stored_as_xai(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.grokkeys, cog, ctx,
                'AIzaSyGeminiKeyValue123456789')
        assert 'not shaped like an xAI key' in ctx.text
        assert db.llm_get_keys(provider='xai') == []

    def test_nonmoderator_paste_is_deleted(self, cog):
        message = FakeMessage(content=f';llm grokkeys {self.KEY}')
        ctx = FakeCtx(message=message)
        error = commands.MissingAnyRole(['Moderator'])
        run(cog.cog_command_error(ctx, error))
        assert message.deleted is True
        assert self.KEY not in ctx.text
        assert 'revoke' in ctx.text


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
        run(cog.on_message(_listener_message('@grok hello there')))
        assert seen[-1]['prompt'] == 'hello there'
        assert 'Grok answer' in ctx.text

    def test_trigger_is_case_insensitive_and_allows_leading_space(
            self, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message('  @GrOk   hi')))
        assert seen[-1]['prompt'] == 'hi'

    @pytest.mark.parametrize('content,guild,author_bot', [
        ('hey @grok hi', True, False),
        ('@groks hi', True, False),
        ('@grokish hi', True, False),
        ('@grok hi', False, False),
        ('@grok hi', True, True),
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

    def test_empty_trigger_can_ask_about_a_reply(self, db, monkeypatch):
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)
        message = _listener_message('@grok')
        target = FakeMessage(content='what does this mean?', author_name='alice')
        message.reference = type('Ref', (), {
            'resolved': target, 'message_id': 8})()
        ctx = FakeCtx(message=message)
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(message))
        assert 'what does this mean?' in seen[-1]['prompt']

    def test_startup_race_is_reported_not_raised(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        ctx = FakeCtx()
        cog = llm_cog.Llm(_FakeBot(ctx))
        run(cog.on_message(_listener_message('@grok hi')))
        assert 'starting up' in ctx.text

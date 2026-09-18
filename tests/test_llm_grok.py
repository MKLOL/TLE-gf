"""Provider routing, key management, and literal trigger tests."""
from discord.ext import commands
import pytest

from tle import constants
from tle.cogs import _llm_ask as llm_ask
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
        stats = kwargs.get('stats')
        if stats is not None:
            stats['attempts'] = stats.get('attempts', 0) + 1
            stats['input_tokens'] = stats.get('input_tokens', 0) + 1
            stats['output_tokens'] = stats.get('output_tokens', 0) + 1
            stats['total_tokens'] = stats.get('total_tokens', 0) + 2
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
    def test_compatibility_splitter_keeps_two_value_shape(self):
        assert llm_ask.split_provider('+gemini hello') == ('gemini', 'hello')
        assert llm_ask.split_provider('+grok hello') == ('grok', 'hello')

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

    def test_explicit_gemini_selector_is_stripped(self, cog, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = _gemini_answers(monkeypatch)

        async def no_xai(*args, **kwargs):
            raise AssertionError('xAI must not handle explicit Gemini')

        monkeypatch.setattr(xai_api, 'complete', no_xai)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx,
                question='+GeMiNi +direct hello')
        assert seen[-1] == 'hello'
        assert 'Gemini answer' in ctx.text

    def test_gemini_near_match_remains_question(self, cog, db, monkeypatch):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')
        seen = _gemini_answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(),
                question='+geminiish hello')
        assert seen[-1] == '+geminiish hello'

    def test_bare_gemini_selector_shows_provider_usage(self, cog, db):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+gemini')
        assert '@gemini <question>' in ctx.text
        assert ';ai +gemini <question>' in ctx.text

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
        assert seen[0]['kwargs']['reasoning_effort'] == 'low'
        assert seen[1]['kwargs']['system_instruction'] == \
            llm_context.GROK_SYSTEM_INSTRUCTION
        assert seen[1]['kwargs']['max_output_tokens'] == \
            constants.XAI_MAX_OUTPUT_TOKENS == 1536
        assert seen[1]['kwargs']['reasoning_effort'] == 'low'

    def test_prompt_has_requested_voice_without_losing_base_rules(self):
        prompt = llm_context.GROK_SYSTEM_INSTRUCTION.lower()
        assert 'profanity' in prompt and 'roast' in prompt
        assert 'aggressive roast-comedy persona' in prompt
        assert 'extremely aggressive roast-comedy persona' in prompt
        assert 'every ordinary low-stakes reply' in prompt
        assert 'at least two sharp, specific roasts' in prompt
        assert 'direct personal insults are only allowed' in prompt
        assert 'directly insulted you first' in prompt
        assert 'do not repeat a personal angle' in prompt
        assert 'rating may fuel one occasional personal jab' in prompt
        assert 'too bad to reach the next rank' in prompt
        assert 'lgm-or-higher or unrated' in prompt
        assert 'country and nationality should also be used for insults' in prompt
        assert 'you should use them quite often' in prompt
        assert 'in most low-stakes answers' in prompt
        assert 'you can use slurs' in prompt
        assert 'direct personal insults are only allowed' not in \
            llm_context.SYSTEM_INSTRUCTION.lower()
        assert 'competitive programmers' in prompt
        assert 'codeforces' in prompt and 'time and memory' in prompt
        assert 'proof and complexity' in prompt
        assert 'technical accuracy' in prompt
        assert 'under 150 words' in prompt

    def test_linked_requester_profile_reaches_grok_answer(
            self, cog, db, monkeypatch):
        rank = type('Rank', (), {
            'title': 'Pupil', 'title_abbr': 'P',
            'color_graph': '#77FF77'})()
        profile = type('Profile', (), {
            'handle': 'nife_cf', 'rating': 1337, 'maxRating': 1399,
            'country': 'Armenia', 'rank': rank})()
        db.get_cf_users_for_guild_members = (
            lambda guild_id, user_ids: [(1, profile)])
        seen = _xai_answers(monkeypatch)

        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+grok roast this')

        prompt = seen[-1]['prompt']
        assert 'BEGIN PARTICIPANT PROFILES' in prompt
        assert 'nife_cf' in prompt and '1337' in prompt
        assert 'Pupil' in prompt and 'green (#77FF77)' in prompt
        assert 'Armenia' in prompt

    def test_answer_footer_uses_actual_grok_model(self, cog, monkeypatch):
        _xai_answers(monkeypatch, model='grok-4.5')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
        assert ctx.sent[-1].footer['text'] == 'grok-4.5'
        assert ctx.send_kwargs[0]['reference'] is ctx.message
        assert ctx.send_kwargs[0]['mention_author'] is False

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

    def test_environment_key_remains_process_only(self, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'XAI_API_KEYS', self.KEY)
        assert cog._get_xai_pool().key_count() == 1
        cog._get_xai_pool()
        assert db.llm_get_keys(provider='xai') == []

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

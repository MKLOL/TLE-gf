"""Tests for the ``;llm`` cog (``tle/cogs/llm.py``).

Covers Gemini's uncapped request path, reply-context handling, and key
management commands — in particular that adding keys deletes the invoking
message and never echoes key material.
"""
from datetime import datetime, timezone

import pytest

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api
from tle.util.llm_keypool import Lease
from tests.llm_test_utils import (FakeAttachment, FakeLlmDb, FakeMessage, run)


class _NullTyping:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeAuthor:
    def __init__(self, user_id=1, roles=(), display_name='nife'):
        self.id = user_id
        self.display_name = display_name
        self.display_avatar = None
        self.roles = [type('R', (), {'name': name})() for name in roles]


class FakeChannel:
    def __init__(self, fetched=None, fail=False):
        self._fetched = fetched
        self._fail = fail

    async def fetch_message(self, message_id):
        if self._fail:
            raise RuntimeError('not found')
        return self._fetched


class FakeCtx:
    def __init__(self, message=None, roles=(), user_id=1, guild_id=100,
                 channel=None):
        self.message = message if message is not None else FakeMessage()
        self.author = FakeAuthor(user_id, roles)
        self.guild = type('G', (), {'id': guild_id})()
        self.channel = channel or FakeChannel()
        self.command = object()
        self.sent = []
        self.send_kwargs = []
        self.helped = False

    async def send(self, embed=None, **kwargs):
        self.sent.append(embed)
        self.send_kwargs.append(kwargs)

    async def send_help(self, command):
        self.helped = True

    def typing(self):
        return _NullTyping()

    @property
    def text(self):
        """Everything this context was sent, flattened for assertions.

        Alert/success helpers are patched to plain strings; real answers come
        back as embeds, so their description is what matters.
        """
        parts = []
        for item in self.sent:
            description = getattr(item, 'description', None)
            parts.append(description if description is not None else item)
        return ' '.join(str(part) for part in parts)


@pytest.fixture(autouse=True)
def db(monkeypatch):
    """Point the cog at a fresh in-memory database, and make embeds inspectable."""
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_neutral',
                        lambda desc, **kw: f'NEUTRAL: {desc}', raising=False)
    return database


@pytest.fixture
def cog():
    return llm_cog.Llm(bot=None)


def _invoke(command, *args, **kwargs):
    """Call a stubbed command object's underlying coroutine."""
    return run(command.__wrapped__(*args, **kwargs))


def _answers(monkeypatch, answer='the answer', model='model-a'):
    """Make gemini_api.complete succeed, recording every prompt it was given.

    The cog makes two calls per question — routing then answering — so
    ``prompts`` keeps both while ``prompt``/``kwargs`` track the last (answer).
    """
    seen = {'prompts': []}

    async def fake_complete(pool, prompt, **kwargs):
        seen['prompts'].append(prompt)
        seen['prompt'] = prompt
        seen['kwargs'] = kwargs
        return answer, Lease(key_id=1, api_key='k', label='l', model=model)

    monkeypatch.setattr(gemini_api, 'complete', fake_complete)
    return seen


def _raises(monkeypatch, error):
    async def fake_complete(pool, prompt, **kwargs):
        raise error

    monkeypatch.setattr(gemini_api, 'complete', fake_complete)


class TestAskWithoutKeys:
    def test_no_question_and_no_reply_shows_help(self, cog):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question=None)
        assert ctx.helped is True

    def test_missing_keys_tells_you_how_to_add_them(self, cog):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hello?')
        assert ';llm keys' in ctx.text


class TestAsk:
    @pytest.fixture(autouse=True)
    def _key(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

    def test_plain_question_is_answered(self, cog, monkeypatch):
        seen = _answers(monkeypatch, 'segment trees are...')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='what is a segment tree?')
        assert seen['prompt'] == 'what is a segment tree?'
        assert seen['kwargs']['tools'] == [{'url_context': {}}]
        assert 'segment trees are...' in ctx.text
        assert ctx.send_kwargs[0] == {
            'reference': ctx.message, 'mention_author': False}

    def test_only_first_page_replies_to_the_prompt(self, cog, monkeypatch):
        _answers(monkeypatch, 'x' * 5000)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='write a lot')

        assert len(ctx.sent) == 2
        assert ctx.send_kwargs[0]['reference'] is ctx.message
        assert ctx.send_kwargs[0]['mention_author'] is False
        assert ctx.send_kwargs[1] == {}

    def test_the_router_is_told_who_asked_and_when(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        message = FakeMessage()
        message.created_at = datetime(2026, 7, 30, 23, 4, tzinfo=timezone.utc)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(message=message),
                question='does their reasoning hold?')
        routing = seen['prompts'][0]
        assert 'author: nife' in routing
        assert '(id 1)' in routing
        assert 'sent_at: 2026-07-30 23:04 UTC' in routing

    def test_missing_message_metadata_does_not_break_routing(self, cog,
                                                             monkeypatch):
        seen = _answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='hi?')
        assert 'sent_at:' not in seen['prompts'][0]
        assert 'the answer' in seen['prompt'] or seen['prompt'] == 'hi?'

    def test_answer_footer_shows_the_model(self, cog, monkeypatch):
        _answers(monkeypatch, 'hi', model='model-b')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        assert ctx.sent[-1].footer['text'] == 'model-b'

    def test_usage_is_recorded_only_on_success(self, cog, monkeypatch, db):
        _raises(monkeypatch, gemini_api.GeminiError('boom'))
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        from tle.cogs.llm import _today
        assert db.llm_get_usage(100, 1, _today()) == 0

    def test_overlong_question_is_rejected(self, cog, monkeypatch):
        _answers(monkeypatch)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx,
                question='x' * (constants.LLM_MAX_PROMPT_CHARS + 1))
        assert 'too long' in ctx.text.lower()

    def test_out_of_quota_reports_when_to_retry(self, cog, monkeypatch):
        _raises(monkeypatch, gemini_api.NoCapacityError('spent', retry_after=7200))
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        assert 'rate-limited' in ctx.text
        assert '2h' in ctx.text

    def test_blocked_prompt_is_reported_verbatim(self, cog, monkeypatch):
        _raises(monkeypatch, gemini_api.BlockedError('blocked by safety filters'))
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        assert 'blocked by safety filters' in ctx.text

    def test_bad_model_config_does_not_leak_internals_to_the_channel(
            self, cog, monkeypatch):
        _raises(monkeypatch, gemini_api.ModelUnavailableError('models/x not found'))
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        assert 'LLM_MODELS' in ctx.text


class TestModelSelection:
    @pytest.fixture(autouse=True)
    def _key(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

    def test_a_leading_model_pins_the_ladder(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(),
                question='3.5f why is this TLE?')
        assert seen['kwargs']['models'] == ['gemini-3.5-flash']
        assert seen['prompt'] == 'why is this TLE?'

    def test_a_reasoning_tier_is_passed_through(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='3.5f-h explain')
        assert seen['kwargs']['tier'] == 'high'

    def test_the_footer_shows_the_tier(self, cog, monkeypatch):
        _answers(monkeypatch, 'hi', model='model-a')
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='3.5f-h explain')
        assert ctx.sent[-1].footer['text'] == 'model-a (high)'

    def test_the_long_spelling_still_works(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(),
                question='3.5-flash-high explain')
        assert seen['kwargs']['models'] == ['gemini-3.5-flash']
        assert seen['kwargs']['tier'] == 'high'

    def test_a_plain_question_pins_nothing(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='why is this TLE?')
        assert seen['kwargs']['models'] is None
        assert seen['kwargs']['tier'] is None

    def test_an_unsupported_tier_is_explained(self, cog, monkeypatch):
        _answers(monkeypatch)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='pro-off explain')
        assert 'does not support' in ctx.text

    def test_a_model_with_no_question_is_rejected(self, cog, monkeypatch):
        _answers(monkeypatch)
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='3.5f')
        assert 'no question followed' in ctx.text


class TestReplyContext:
    @pytest.fixture(autouse=True)
    def _key(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

    def _ctx_replying_to(self, referenced):
        message = FakeMessage()
        message.reference = type('Ref', (), {'resolved': referenced,
                                             'message_id': 5})()
        return FakeCtx(message=message)

    def test_replying_with_no_question_asks_about_the_message(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        target = FakeMessage(content='for (int i = 0; i < n; i++)',
                             author_name='Miguel')
        _invoke(llm_cog.Llm.llm, cog, self._ctx_replying_to(target), question=None)
        assert 'for (int i = 0; i < n; i++)' in seen['prompt']
        assert 'Miguel' in seen['prompt']

    def test_replying_with_a_question_combines_both(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        target = FakeMessage(content='use a BIT')
        _invoke(llm_cog.Llm.llm, cog, self._ctx_replying_to(target),
                question='why?')
        assert 'use a BIT' in seen['prompt']
        assert 'why?' in seen['prompt']

    def test_images_on_the_replied_to_message_are_forwarded(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        target = FakeMessage(content='', attachments=[
            FakeAttachment(content_type='image/png', data=b'PNGDATA')])
        _invoke(llm_cog.Llm.llm, cog, self._ctx_replying_to(target), question='what?')
        assert seen['kwargs']['images'] == [('image/png', b'PNGDATA')]

    def test_unresolved_reference_is_fetched(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        target = FakeMessage(content='fetched body')
        message = FakeMessage()
        message.reference = type('Ref', (), {'resolved': None, 'message_id': 5})()
        ctx = FakeCtx(message=message, channel=FakeChannel(fetched=target))
        _invoke(llm_cog.Llm.llm, cog, ctx, question='what?')
        assert 'fetched body' in seen['prompt']

    def test_deleted_reference_falls_back_to_a_plain_question(self, cog, monkeypatch):
        seen = _answers(monkeypatch)
        message = FakeMessage()
        message.reference = type('Ref', (), {'resolved': None, 'message_id': 5})()
        ctx = FakeCtx(message=message, channel=FakeChannel(fail=True))
        _invoke(llm_cog.Llm.llm, cog, ctx, question='standalone?')
        assert seen['prompt'] == 'standalone?'


class TestNoRateLimits:
    """Gemini deliberately has no bot-side per-user cap or cooldown.

    Provider quota is its only limit; usage is recorded for visibility in
    `;llm keystatus`, never enforced for this provider.
    """

    @pytest.fixture(autouse=True)
    def _key(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

    def test_back_to_back_calls_all_answer(self, cog, monkeypatch):
        _answers(monkeypatch)
        for _ in range(5):
            ctx = FakeCtx()
            _invoke(llm_cog.Llm.llm, cog, ctx, question='again?')
            assert 'the answer' in ctx.text

    def test_a_heavy_user_is_never_cut_off(self, cog, monkeypatch, db):
        _answers(monkeypatch)
        from tle.cogs.llm import _today
        for _ in range(500):
            db.llm_bump_usage(100, 1, _today())
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, ctx, question='hi?')
        assert 'the answer' in ctx.text

    def test_usage_is_still_recorded(self, cog, monkeypatch, db):
        _answers(monkeypatch)
        from tle.cogs.llm import _today
        _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='hi?')
        assert db.llm_get_usage(100, 1, _today()) == 1


class TestKeyCommands:
    _KEY_A = 'AIzaSyRealLookingKeyAAAAAAAA'
    _KEY_B = 'AIzaSyRealLookingKeyBBBBBBBB'

    def test_keys_are_stored_and_the_message_is_deleted(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self._KEY_A, self._KEY_B)
        assert ctx.message.deleted is True
        assert len(db.llm_get_keys()) == 2
        assert '2 key(s) added' in ctx.text

    def test_the_confirmation_never_contains_key_material(self, cog):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self._KEY_A)
        assert self._KEY_A not in ctx.text

    def test_a_failed_delete_warns_to_rotate_the_keys(self, cog):
        message = FakeMessage()
        message.delete_error = RuntimeError('missing permissions')
        ctx = FakeCtx(message=message, roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self._KEY_A)
        assert 'rotate those keys' in ctx.text

    def test_duplicates_are_reported_not_stored_twice(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self._KEY_A)
        second = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, second, self._KEY_A)
        assert 'already stored' in second.text
        assert len(db.llm_get_keys()) == 1

    def test_obvious_non_keys_are_rejected(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, 'oops')
        assert 'rejected as too short' in ctx.text
        assert db.llm_get_keys() == []

    def test_backticks_and_angle_brackets_are_stripped(self, cog, db):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, f'`{self._KEY_A}`')
        assert db.llm_get_keys()[0].api_key == self._KEY_A

    def test_no_arguments_shows_usage(self, cog):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx)
        assert ';llm keys' in ctx.text

    def test_adding_a_key_makes_it_available_immediately(self, cog):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keys, cog, ctx, self._KEY_A)
        assert cog._get_pool().key_count() == 1

    def test_keylist_is_redacted(self, cog, db):
        db.llm_add_key(self._KEY_A)
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keylist, cog, ctx)
        assert self._KEY_A not in str(ctx.sent[0].description)
        assert 'sha256:' in ctx.sent[0].description

    def test_keyforget_removes_a_key_from_the_pool(self, cog, db):
        db.llm_add_key(self._KEY_A)
        key_id = db.llm_get_keys()[0].id
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keyforget, cog, ctx, key_id)
        assert db.llm_get_keys() == []
        assert cog._get_pool().key_count() == 0

    def test_keyforget_on_an_unknown_id_reports_it(self, cog):
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keyforget, cog, ctx, 999)
        assert 'No active key #999' in ctx.text

    def test_keystatus_lists_every_bucket_without_key_material(self, cog, db):
        db.llm_add_key(self._KEY_A)
        ctx = FakeCtx(roles=('Moderator',))
        _invoke(llm_cog.Llm.keystatus, cog, ctx)
        description = ctx.sent[0].description
        assert 'model-a' in description and 'model-b' in description
        assert self._KEY_A not in description


class TestEnvBootstrap:
    def test_env_keys_remain_process_only(self, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'GEMINI_API_KEYS',
                            'AIzaSyEnvKeyOne1234567890,AIzaSyEnvKeyTwo1234567890')
        assert cog._get_pool().key_count() == 2
        cog._get_pool()
        assert db.llm_get_keys() == []

    def test_short_env_entries_are_ignored(self, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'GEMINI_API_KEYS',
                            'AIzaSyEnvKeyOne1234567890,,junk')
        cog._get_pool()
        assert cog._get_pool().key_count() == 1
        assert db.llm_get_keys() == []

    def test_empty_env_is_a_no_op(self, cog, db, monkeypatch):
        monkeypatch.setattr(constants, 'GEMINI_API_KEYS', '')
        cog._get_pool()
        assert db.llm_get_keys() == []

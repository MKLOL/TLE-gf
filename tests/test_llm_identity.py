"""Requester and reply-subject identity anchoring for Grok prompts."""

import json
from types import SimpleNamespace

from tle import constants
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_history as llm_history
from tle.cogs import _llm_identity as llm_identity
from tle.cogs import _llm_pipeline as llm_pipeline
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, xai_api
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx


def _author(user_id, name, *, bot=False):
    return SimpleNamespace(id=user_id, display_name=name, bot=bot)


def _message(message_id, author, content):
    return SimpleNamespace(
        id=message_id, author=author, content=content,
        attachments=[], embeds=[], reference=None, created_at=None)


def _routing_from_prompt(prompt):
    body = prompt.split(
        '--- BEGIN CURRENT REQUEST ROUTING ---\n', 1)[1]
    return json.loads(body.split(
        '\n--- END CURRENT REQUEST ROUTING ---', 1)[0])


def test_routing_separates_requester_from_replied_message_author():
    requester = _author(1, 'Alice')
    target = _message(50, _author(2, 'Bob'), 'this code is broken')
    command = _message(60, requester, '@grok why?')

    routing = json.loads(llm_identity.build_request_routing(
        requester, command, target))

    assert routing == {
        'requester': {
            'discord_user_id': '1', 'display_name': 'Alice',
            'is_bot': False},
        'request_message_id': '60',
        'focused_reply_target': {
            'message_id': '50',
            'author': {
                'discord_user_id': '2', 'display_name': 'Bob',
                'is_bot': False},
            'same_as_requester': False},
    }


def test_duplicate_names_use_requester_flags_in_structured_transcript():
    requester = _author(1, 'same name')
    other = _author(2, 'same name', bot=True)
    transcript = llm_history.format_transcript([
        _message(10, requester, 'my earlier message'),
        _message(11, other, 'somebody else'),
    ], structured=True, requester_id=1)
    records = [json.loads(line) for line in transcript.splitlines()]

    assert records[0]['author'] == records[1]['author'] == 'same name'
    assert records[0]['is_requester'] is True
    assert records[1]['is_requester'] is False
    assert records[0]['author_is_bot'] is False
    assert records[1]['author_is_bot'] is True


def test_routing_anchor_follows_context_and_keeps_focus_as_subject():
    requester = _author(1, 'Alice')
    random_person = _message(10, _author(3, 'Carol'), 'unrelated opinion')
    focus = _message(11, _author(2, 'Bob'), 'the actual subject')
    routing = llm_identity.build_request_routing(requester, None, focus)

    prompt = llm_pipeline.build_prompt(
        'explain this', focus, [random_person, focus],
        routing=routing, requester_id=1)

    assert prompt.index('BEGIN TRANSCRIPT') < prompt.index(
        'BEGIN CURRENT REQUEST ROUTING')
    assert 'focus: true` identifies the message being discussed' in prompt
    assert 'transcript/profile participants are context, not the addressee' \
        in prompt
    assert prompt.endswith(
        'do not silently switch to another participant.')


def test_live_grok_prompt_anchors_unlinked_requester_over_random_profile(
        monkeypatch):
    database = FakeLlmDb()
    database.llm_add_key(
        'xai-ExampleKeyValue1234567890', provider='xai')
    rank = SimpleNamespace(
        title='Grandmaster', title_abbr='GM', color_graph='#FF7777')
    profile = SimpleNamespace(
        handle='random_cf', rating=2500, maxRating=2600,
        country='Nowhere', rank=rank)
    database.get_cf_users_for_guild_members = (
        lambda guild_id, user_ids: [(2, profile)])
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda text: text, raising=False)
    seen = []

    async def answer(pool, prompt, **kwargs):
        seen.append(prompt)
        return 'answer', xai_api.Lease(1, 'redacted', 'test', 'grok-test')

    monkeypatch.setattr(xai_api, 'complete', answer)
    target = FakeMessage(content='random context', author_name='same name')
    target.id = 50
    target.author.id = 2
    target.author.bot = False
    noise = FakeMessage(content='unrelated opinion', author_name='same name')
    noise.id = 40
    noise.author.id = 3
    noise.author.bot = False

    async def prepare_context(*args, **kwargs):
        return llm_context.MODE_REPLY_CHAIN, [noise, target], True

    monkeypatch.setattr(
        llm_cog.llm_ask, '_prepare_context', prepare_context)
    command = FakeMessage(content=';ai +grok +direct explain')
    command.id = 60
    command.reference = SimpleNamespace(resolved=target, message_id=50)
    ctx = FakeCtx(message=command, user_id=1)
    ctx.author.display_name = 'same name'

    run(llm_cog.Llm.llm.__wrapped__(
        llm_cog.Llm(bot=None), ctx,
        question='+grok +direct explain this'))

    prompt = seen[-1]
    routing = _routing_from_prompt(prompt)
    assert routing['requester']['discord_user_id'] == '1'
    assert routing['focused_reply_target']['author'][
        'discord_user_id'] == '2'
    assert routing['focused_reply_target']['same_as_requester'] is False
    assert 'random_cf' in prompt
    assert '"is_requester":false' in prompt
    assert '"is_reply_target":true' in prompt
    assert '"focus":true' in prompt
    transcript = prompt.split(
        '--- BEGIN TRANSCRIPT ---\n', 1)[1].split(
            '\n--- END TRANSCRIPT ---', 1)[0]
    records = {
        record['id']: record
        for record in map(json.loads, transcript.splitlines())}
    assert records['40']['is_requester'] is False
    assert records['40']['focus'] is False
    assert records['50']['is_requester'] is False
    assert records['50']['focus'] is True

"""Mocked conversation and failure cases through the actual Grok command."""
import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from tle import constants
from tle.cogs._llm_runtime import RequestRuntime
from tle.util import xai_budget
from tests.llm_agent_fakes import env, answer, call, payloads, result
from tests.llm_test_utils import FakeAttachment, run


def test_direct_answer_is_one_call_without_history(env):
    assert env.invoke('+grok Explain binary search') == 'Reply'
    assert env.provider.call_count == 1 and not env.search.called
    assert env.ctx.send_kwargs[0] == dict(reference=env.ctx.message, mention_author=False)


@pytest.mark.parametrize('citation', [False, True])
def test_search_read_and_answer_in_own_voice(env, citation):
    target = env.message('Pizza is my favorite.', -30 * 86400)
    link = f'https://discord.com/channels/100/200/{target.id}'
    reply = 'Pizza. Finally, a preference with a proof.' + (f' {link}' if citation else '')
    env.search.side_effect = [env.hits(), env.hits(), env.hits(target)]
    env.provider.side_effect = [call(author='<@2>', query='food'), call(author='2', query='favorite'),
                                call(author='2', query='favorite', after='2026-01-01'),
                                call('read_message', message_ref=str(target.id)), answer(reply)]
    assert env.invoke() == reply
    assert env.provider.call_count == 5 and env.search.call_count == 3
    assert result(env)['messages'][0]['text'] == target.content
    assert result(env)['messages'][0]['link'] == link
    assert payloads(env)[-1]['tool_choice'] == 'none'
    assert all(not p['store'] and not p['parallel_tool_calls'] for p in payloads(env))
    usage = env.db.conn.execute('SELECT * FROM llm_request_usage').fetchall()
    events = env.db.conn.execute('SELECT * FROM llm_xai_request').fetchall()
    assert len(events) == len(usage) == 1 and usage[0].answer_attempts == 5
    assert events[0].actual_microusd == usage[0].cost_microusd > 0


@pytest.mark.parametrize('policy,control,choice', [
    ('off', '', 'none'), ('explicit', '', 'none'), ('auto', '+direct', 'none'),
    ('explicit', '+context', 'required'), ('auto', '+context', 'required')])
def test_privacy_controls(env, policy, control, choice):
    env.db.set_guild_config(100, 'llm_context', policy)
    env.invoke(f'+grok {control} question')
    assert payloads(env)[0]['tool_choice'] == choice and not env.search.called


@pytest.mark.parametrize('restriction', ['off', 'ban', 'disabled', 'credits', 'personal', 'cooldown'])
def test_admission_blocks_before_any_paid_call(env, monkeypatch, restriction):
    question = '+grok +context question'
    if restriction == 'off':
        env.db.set_guild_config(100, 'llm_context', 'off')
    elif restriction == 'ban':
        env.db.llm_ban_user(100, 1)
    elif restriction == 'disabled':
        env.db.set_guild_config(100, 'llm_disabled', '1')
    elif restriction == 'credits':
        monkeypatch.setattr(constants, 'XAI_DAILY_BUDGET_USD', .000001)
    elif restriction == 'personal':
        env.db.set_guild_config(100, 'llm_grok_user_rate', '1:3600')
        env.db.llm_reserve_xai_request(1, guild_id=100, user_limit=1, window_seconds=3600,
                                     daily_limit=200, reserved_microusd=1, daily_budget_microusd=500000)
    else:
        env.db.llm_cooldown_retry = lambda *a, **k: NS(scope='channel', retry_at=2000000000)
    assert env.invoke(question) and not env.provider.called and not env.search.called


@pytest.mark.parametrize('replied', [False, True])
@pytest.mark.parametrize('change', ['permissions', 'privacy', 'feature'])
def test_access_changes_withhold_retrieved_text(env, replied, change):
    target = env.message('Private evidence')
    env.search.return_value = env.hits(target)
    if replied:
        env.ctx.message.reference = NS(resolved=target)
    async def provider(*args, **kwargs):
        if not replied and env.provider.call_count == 1:
            return call()
        if change == 'permissions':
            env.channel.denied.add(1)
        else:
            env.db.set_guild_config(100, 'llm_context' if change == 'privacy' else 'grok_tools',
                                   'off' if change == 'privacy' else '0')
        return answer(target.content)
    env.provider.side_effect = provider
    text = env.invoke()
    if not replied or change == 'permissions':
        assert 'access changed' in text and target.content not in text
    else:
        assert text == target.content  # Explicit reply remains allowed without history forwarding.


@pytest.mark.parametrize('mode', ['disabled', 'image', 'literal', 'reply'])
def test_existing_entrypoints_and_image_path(env, mode):
    if mode == 'disabled':
        env.db.set_guild_config(100, 'grok_tools', '0')
    elif mode == 'image':
        env.ctx.message.attachments = [FakeAttachment()]
    elif mode == 'reply':
        env.ctx.message.reference = NS(resolved=env.message('reply target'))
    if mode == 'literal':
        env.ctx.message.content = '@grok +direct question'
        env.cog.bot.get_context = AsyncMock(return_value=env.ctx)
        run(env.cog.on_message(env.ctx.message))
    else:
        env.invoke('+grok +direct question')
    assert env.ctx.text == 'Reply'
    assert ('tools' not in payloads(env)[0]) == (mode in ('disabled', 'image'))
    if mode == 'reply':
        assert 'reply target' in str(payloads(env))


@pytest.mark.parametrize('broken', ['unknown', 'incomplete', 'batch', 'missing_id', 'last_turn'])
def test_bad_tool_calls_never_execute_unapproved_actions(env, broken):
    response = call('ban_user' if broken == 'unknown' else 'search_messages')
    if broken == 'incomplete':
        response[1]['status'] = 'incomplete'
    elif broken == 'batch':
        response[1]['output'] *= 2
    elif broken == 'missing_id':
        response[1]['output'][0]['call_id'] = None
    env.provider.side_effect = [call(query=str(i)) for i in range(4)] + [response] if broken == 'last_turn' else [response, answer()]
    env.invoke()
    assert env.search.call_count == (4 if broken == 'last_turn' else 0)
    assert env.provider.call_count <= 5


def test_unknown_attempt_cost_is_retained_after_success(env):
    env.db.llm_add_key('xai-FakeFallbackKey123456789', provider='xai')
    paid = answer('Recovered')
    paid[1]['usage']['cost_in_usd_ticks'] = 10000
    env.provider.side_effect = [TimeoutError(), paid]
    assert env.invoke() == 'Recovered'
    event = env.db.conn.execute('SELECT * FROM llm_xai_request').fetchone()
    assert event.actual_microusd > 100 and env.provider.call_count == 2


def test_output_budget_is_shared_even_without_usage(env):
    first = call()
    del first[1]['usage']
    env.provider.side_effect = [first, answer(usage=False)]
    env.invoke()
    assert sum(p['max_output_tokens'] for p in payloads(env)) == 2500
    assert payloads(env)[-1]['tool_choice'] == 'none'


def test_deadline_cancels_request_and_accounts_for_attempt(env):
    async def hang(*a, **k):
        await asyncio.Event().wait()
    env.cog._runtime = RequestRuntime({'xai': 1}, queue_timeout=.02, request_timeout=.02)
    env.provider.side_effect = hang
    assert 'timed out' in env.invoke() and not env.cog._runtime._active_users
    row = env.db.conn.execute('SELECT * FROM llm_xai_request').fetchone()
    assert row.outcome == 'timeout' and row.actual_microusd > 0


@pytest.mark.parametrize('limit', ['input', 'attempts'])
def test_budget_stops_before_the_next_paid_call(env, monkeypatch, limit):
    if limit == 'input':
        monkeypatch.setattr(xai_budget, 'MAX_INPUT_BYTES', 1)
    else:
        monkeypatch.setattr(xai_budget, 'MAX_ATTEMPTS', 1)
        env.provider.side_effect = [call()]
    assert 'lookup limit' in env.invoke()
    assert env.provider.call_count == (0 if limit == 'input' else 1)

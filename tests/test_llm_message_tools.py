"""Evidence quality and read-only boundaries with mocked Discord history."""
import asyncio
import json
from types import ModuleType, SimpleNamespace as NS
from unittest.mock import AsyncMock
import sys

import discord
import pytest

from tle.cogs import _llm_tools as tools
from tle.util import xai_budget
from tests.llm_agent_fakes import env
from tests.llm_test_utils import run


def test_multiple_matches_merge_neighbors_and_expand(env):
    before = env.message('What do you like?', -180)
    first = env.message('Pizza is my favorite. ' * 30, -120)
    second = env.message('I also like pasta.', -60)
    env.message('Future conversation', 10)
    env.search.return_value = env.hits(first, second, total=8)
    result = env.lookup(author='<@2>', after='2026-01-01', before='2030-01-01')
    assert [row['id'] for row in result['messages']] == [str(m.id) for m in (before, first, second)]
    assert sum(row['match'] for row in result['messages']) == 2 and result['has_more']
    assert result['scope']['author_id'] == '2' and result['scope']['before'] == env.ctx.message.created_at.isoformat()
    expanded = env.lookup('read_message', message_ref=str(first.id))
    assert len(next(row for row in expanded['messages'] if row['match'])['text']) > 300
    assert 'Future conversation' not in str(expanded)


@pytest.mark.parametrize('name,args', [
    ('ban_user', {}), ('delete_message', {}), ('search_messages', {'channel': '999'}),
    ('search_messages', {'author': 'unknown'}), ('search_messages', {'query': None}),
    ('search_messages', {'query': 'a' * 201}), ('search_messages', {'query': ''}),
    ('search_messages', {'after': 'broken'}), ('search_messages', {'after': '2000-01-01'}),
    ('search_messages', {'after': '2030-01-01'}), ('read_message', {}),
    ('read_message', {'message_ref': 'https://discord.com/channels/999/999/123'}),
    ('read_message', {'message_ref': '-1'}), ('read_message', {'message_ref': str(2**64)}),
    ('read_message', {'message_ref': '123', 'surrounding': True}), ('search_messages', []),
])
def test_invalid_or_unapproved_calls_never_read(env, name, args):
    result = json.loads(run(env.tools.execute(name, json.dumps(args))))
    assert 'error' in result and not env.search.called and not env.channel.fetch_message.called


@pytest.mark.parametrize('who', [1, 99])
@pytest.mark.parametrize('restriction', ['history', 'thread', 'left', 'privacy'])
def test_both_members_need_access(env, who, restriction):
    if restriction == 'history':
        env.channel.denied.add(who)
    elif restriction == 'thread':
        env.channel.private = True
        env.channel.thread_members.remove(who)
    elif restriction == 'left':
        env.ctx.guild.get_member = lambda mid: None
    else:
        env.db.set_guild_config(100, 'llm_context', 'off')
    assert 'error' in env.lookup() and not env.search.called


@pytest.mark.parametrize('failure', ['indexing', 'network', 'deleted', 'foreign', 'future', 'revoked'])
def test_unavailable_history_is_never_reported_as_a_clean_empty_search(env, failure):
    target = env.message('Private evidence', 10 if failure == 'future' else -60)
    body = env.hits(target)
    if failure == 'indexing':
        body = {'code': 110000}
    elif failure == 'network':
        env.search.side_effect = RuntimeError('private upstream detail')
    elif failure == 'deleted':
        env.channel.fetch_message.side_effect = discord.NotFound('deleted')
    elif failure == 'foreign':
        body['messages'][0][0]['channel_id'] = '999'
    elif failure == 'revoked':
        async def revoke(*a, **k):
            env.channel.denied.add(1)
            return body
        env.search.side_effect = revoke
    env.search.return_value = body
    result = env.lookup()
    assert result.get('partial') or 'error' in result
    assert 'Private evidence' not in str(result) and 'private upstream detail' not in str(result)


def test_empty_search_scope_and_message_limits(env):
    assert env.lookup(author='me')['scope']['author_id'] == '1'
    target = env.message('xai-SecretCredentialValue12345678901234567890 ' + '🍕' * 3000)
    env.search.return_value = env.hits(target)
    env.tools.message_limit = 1
    result = env.lookup()
    assert len(result['messages']) == 1 and result['truncated']
    assert 'SecretCredential' not in str(result) and len(tools.dumps(result)) <= 4000
    assert 'budget' in env.lookup()['error']


def test_lookup_timeout_and_cancellation(env, monkeypatch):
    original = asyncio.wait_for
    async def short_wait(coro, timeout):
        return await original(coro, timeout=.01)
    async def hang(*a, **k):
        await asyncio.Event().wait()
    monkeypatch.setattr(tools.asyncio, 'wait_for', short_wait)
    env.search.side_effect = hang
    assert 'timed out' in env.lookup()['error']
    env.search.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        env.lookup()


def test_native_search_uses_authenticated_get(monkeypatch):
    module = ModuleType('discord.http')
    module.Route = lambda method, path, **kw: NS(method=method, path=path, **kw)
    monkeypatch.setitem(sys.modules, 'discord.http', module)
    request = AsyncMock(return_value={'messages': []})
    assert run(tools.search_discord(NS(http=NS(request=request)), 100, {'channel_id': '200'})) == {'messages': []}
    route = request.call_args.args[0]
    assert route.method == 'GET' and route.path == '/guilds/{guild_id}/messages/search'
    assert route.guild_id == 100 and request.call_args.kwargs['params']['channel_id'] == '200'


@pytest.mark.parametrize('invalid', [-10, float('nan'), float('inf'), True, '10', None])
def test_invalid_usage_cannot_release_limits(invalid):
    stats = {}
    budget = xai_budget.RequestBudget(stats)
    payload = dict(input='🍕' * 8000, max_output_tokens=1000)
    budget.claim(payload)
    budget.observe(dict(input_tokens=invalid, output_tokens=invalid, cost_microusd=invalid))
    assert budget.output_tokens == 1000 and stats['accounted_microusd'] > 0
    with pytest.raises(xai_budget.BudgetExhausted):
        budget.claim(payload)

"""Offline fixtures; use the real command, xAI loop, and SQLite accounting."""
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import discord
import pytest

from tle import constants
from tle.cogs import llm, _llm_tools as tools, _llm_context as context
from tle.util import codeforces_common, discord_common, xai_api
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeAuthor, FakeCtx


def response(item, usage=True):
    body = dict(status='completed', model='grok-test', output=[item])
    if usage:
        body['usage'] = dict(input_tokens=100, output_tokens=30, total_tokens=130)
    return 200, body, {}


def answer(text='Reply', **kwargs):
    return response(dict(type='message', content=[dict(type='output_text', text=text)]), **kwargs)


def call(name='search_messages', **args):
    return response(dict(type='function_call', call_id='call-1', name=name, arguments=json.dumps(args)))


def payloads(env):
    return [entry.args[1] for entry in env.provider.call_args_list]


def result(env):
    return json.loads([i['output'] for i in payloads(env)[-1]['input']
                       if i.get('type') == 'function_call_output'][-1])


@pytest.fixture
def env(monkeypatch):
    db, config = FakeLlmDb(), {(100, 'grok_tools'): '1'}
    db.get_guild_config = lambda guild, key: config.get((guild, key))
    db.set_guild_config = lambda guild, key, value: config.update({(guild, key): value})
    db.llm_add_key('xai-FakeOfflineKey123456789', provider='xai')
    monkeypatch.setattr(codeforces_common, 'user_db', db, raising=False)
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(constants, 'LLM_CONTEXT_ENABLED', True)
    monkeypatch.setattr(discord_common, 'embed_alert', lambda text: text)
    ctx = FakeCtx()
    members = {1: ctx.author, 2: FakeAuthor(2, display_name='X'), 99: FakeAuthor(99)}
    ctx.guild.me, ctx.guild.get_member = members[99], members.get
    channel = ctx.channel = NS(id=200, guild=ctx.guild, denied=set(), private=False, thread_members={1, 99})
    channel.permissions_for = lambda m: NS(view_channel=m.id not in channel.denied,
                                           read_message_history=m.id not in channel.denied, manage_threads=False)
    channel.is_private = lambda: channel.private
    channel.get_member = lambda mid: members.get(mid) if mid in channel.thread_members else None
    channel.fetch_member = AsyncMock(side_effect=discord.NotFound('missing'))
    messages = {}
    async def fetch(mid):
        if mid not in messages:
            raise discord.NotFound('deleted')
        return messages[mid]
    async def history(*, limit, around):
        ordered = sorted(messages.values(), key=lambda m: m.id)
        index = ordered.index(around)
        for message in ordered[max(0, index - limit // 2):index + limit // 2 + 1]:
            yield message
    channel.fetch_message, channel.history = AsyncMock(side_effect=fetch), history
    now = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)
    def message(text, seconds=-60, author=2):
        msg = FakeMessage(content=text)
        msg.created_at = now + timedelta(seconds=seconds)
        msg.id = tools.snowflake(msg.created_at) + 1
        msg.author, msg.guild, msg.channel = members[author], ctx.guild, channel
        messages[msg.id] = msg
        return msg
    ctx.message = message(';ai +grok question', 0, 1)
    cog = llm.Llm(NS(user=members[99]))
    provider = AsyncMock(return_value=answer())
    search = AsyncMock(return_value=dict(messages=[], total_results=0))
    monkeypatch.setattr(xai_api, 'generate_once', provider)
    monkeypatch.setattr(tools, 'search_discord', search)
    def hits(*matched, total=None):
        return dict(total_results=len(matched) if total is None else total,
                    messages=[[dict(id=str(m.id), channel_id=str(m.channel.id), author={'id': str(m.author.id)})]
                              for m in matched])
    def invoke(question='+grok What is <@2>\'s favorite food?'):
        run(llm.Llm.llm.__wrapped__(cog, ctx, question=question))
        return ctx.text
    executor = tools.MessageTools(cog, ctx, db, context.ContextControls(None))
    def lookup(name='search_messages', **args):
        return json.loads(run(executor.execute(name, json.dumps(args))))
    yield NS(db=db, cog=cog, ctx=ctx, channel=channel, members=members, provider=provider,
             search=search, message=message, hits=hits, invoke=invoke, lookup=lookup, tools=executor)
    db.conn.close()

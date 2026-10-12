"""Two bounded Discord reads, restricted to the invoking channel/thread."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import re

import discord

from tle import constants
from tle.cogs import _llm_access as access, _llm_context as context
from tle.cogs._llm_message_text import message_text
from tle.cogs._llm_transcript import redact_secrets


def _tool(name, description, fields, required=()):
    return dict(type='function', name=name, description=description, parameters=dict(
        type='object', properties={key: dict(type='string', description=value)
                                  for key, value in fields.items()},
        required=list(required), additionalProperties=False))


TOOLS = [
    _tool('search_messages',
          'Search this channel/thread: up to 5 matches with one neighbor either side. '
          'Defaults to the last 7 days; omit query for recent messages.',
          dict(query='Search words, up to 200 characters.',
               author='User ID, mention, or me. Do not guess IDs from names.',
               after='Inclusive ISO date/time; UTC by default.',
               before='Exclusive ISO date/time; UTC by default.')),
    _tool('read_message', 'Expand a message in this channel with up to two neighbors either side.',
          dict(message_ref='Discord message ID, taken from a result or message link.'),
          ('message_ref',)),
]


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def identifier(value):
    if not value.isascii() or not value.isdigit() or not 0 < int(value) < 2**64:
        raise ValueError('Use a Discord ID or user mention.')
    return str(int(value))


def date(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return (stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def snowflake(stamp):
    return (int(stamp.timestamp() * 1000) - 1420070400000) << 22


def search_filters(arguments, ctx):
    end = ctx.message.created_at
    before = min(date(arguments['before']), end) if 'before' in arguments else end
    after = date(arguments['after']) if 'after' in arguments else before - timedelta(days=7)
    if after.year < 2015 or after >= before:
        raise ValueError('Use an ordered date range beginning in 2015 or later.')
    params = dict(channel_id=str(ctx.channel.id), limit=5, sort_by='timestamp',
                  sort_order='desc', min_id=str(snowflake(after) - 1),
                  max_id=str(min(snowflake(before), ctx.message.id)))
    if 'query' in arguments:
        params['content'] = arguments['query']
    if 'author' in arguments:
        author = str(ctx.author.id) if arguments['author'] == 'me' else arguments['author']
        params['author_id'] = identifier(re.sub(r'^<@!?(\d+)>$', r'\1', author))
    return params, dict(channel_id=str(ctx.channel.id), after=after.isoformat(),
                        before=before.isoformat(), query=arguments.get('query'),
                        author_id=params.get('author_id'))


def render(messages, matches, *, scope, partial, has_more, limit, count, expanded):
    rows = []
    for message in sorted(messages.values(), key=lambda m: (str(m.id) not in matches, m.id)):
        matched = str(message.id) in matches
        text = redact_secrets(message_text(message))
        cap = min(1800 if expanded else 300, limit // 2) if matched else 120
        rows.append(dict(id=str(message.id), author_id=str(message.author.id),
                         author=redact_secrets(message.author.display_name)[:80],
                         timestamp=message.created_at.isoformat(), match=matched,
                         text=text[:cap], truncated=len(text) > cap,
                         link=f'https://discord.com/channels/{message.guild.id}/{message.channel.id}/{message.id}'))
    result = dict(scope=scope, messages=rows[:count], partial=partial,
                  has_more=has_more, truncated=len(rows) > count or any(r['truncated'] for r in rows))
    while len(dumps(result)) > limit and result['messages']:
        result['messages'].pop()
        result['truncated'] = True
    result['messages'].sort(key=lambda row: int(row['id']))
    return result


class MessageTools:
    def __init__(self, cog, ctx, database, controls, referenced=None):
        self.cog, self.ctx, self.database = cog, ctx, database
        self.controls, self.referenced = controls, referenced
        self.remaining, self.message_limit = 6000, controls.message_limit or 20
        self.seen = set()

    @property
    def allowed(self):
        policy = self.cog._context_policy(self.ctx)
        explicit = self.controls.mode == context.MODE_CONTEXT or self.referenced is not None
        return (self.database.get_guild_config(self.ctx.guild.id, 'grok_tools') == '1'
                and policy != 'off' and self.controls.mode != context.MODE_DIRECT
                and (explicit if policy == 'explicit' else constants.LLM_CONTEXT_ENABLED or explicit))

    async def check(self, *, history=False):
        ctx = self.ctx
        if history and not self.allowed:
            raise ValueError('Message retrieval is disabled here.')
        if ctx.channel.guild.id != ctx.guild.id or (self.referenced and not self.in_scope(self.referenced)):
            raise ValueError('Only the current channel/thread can be read.')
        reason = access._policy_block_reason(self.database, ctx.guild.id, ctx.channel.id,
                                             ctx.author.id, access.family_channel_id(ctx.channel))
        if reason:
            raise ValueError(reason)
        for member in (ctx.guild.get_member(ctx.author.id), ctx.guild.me):
            if member is None:
                raise ValueError('Channel access could not be verified.')
            permissions = ctx.channel.permissions_for(member)
            if not (permissions.view_channel and permissions.read_message_history):
                raise ValueError('Message history is not accessible.')
            if (getattr(ctx.channel, 'is_private', lambda: False)()
                    and not permissions.manage_threads and ctx.channel.get_member(member.id) is None):
                await ctx.channel.fetch_member(member.id)

    def in_scope(self, message):
        return (message.guild.id == self.ctx.guild.id and message.channel.id == self.ctx.channel.id
                and message.id < self.ctx.message.id)

    async def execute(self, name, raw):
        try:
            if not isinstance(raw, str) or len(raw) > 2000:
                raise ValueError
            args = json.loads(raw)
            schema = next(tool['parameters'] for tool in TOOLS if tool['name'] == name)
            if (not isinstance(args, dict) or args.keys() - schema['properties'].keys()
                    or not set(schema['required']) <= args.keys()
                    or any(not isinstance(v, str) or not 0 < len(v.strip()) <= 200 for v in args.values())):
                raise ValueError
            if self.remaining < 1024 or self.message_limit == 0:
                return dumps({'error': 'Lookup budget reached; answer from existing evidence.'})
            result = await asyncio.wait_for(self.retrieve(name, args), timeout=8)
            output = dumps(result)
            if len(output) > self.remaining:
                raise ValueError
            self.remaining -= len(output)
            self.message_limit -= len(result['messages'])
            self.seen.update(row['id'] for row in result['messages'])
            return output
        except asyncio.TimeoutError:
            return dumps({'error': 'Discord lookup timed out; the search is incomplete.'})
        except (ValueError, TypeError, StopIteration):
            return dumps({'error': 'Invalid or inaccessible lookup. Check the tool, IDs, dates, and arguments.'})
        except Exception:
            return dumps({'error': 'Discord lookup unavailable (access, indexing, or deleted history). '
                                   'This is not evidence that no matching messages exist.'})

    async def retrieve(self, name, args):
        await self.check(history=True)
        partial, total = False, 1
        if name == 'search_messages':
            params, scope = search_filters(args, self.ctx)
            body = await search_discord(self.cog.bot, self.ctx.guild.id, params)
            if body.get('code') == 110000:
                raise RuntimeError('Indexing pending')
            ids = []
            for hit in [hit for group in body['messages'] for hit in group]:
                mid = identifier(hit['id'])
                if (str(hit['channel_id']) != str(self.ctx.channel.id)
                        or not int(params['min_id']) < int(mid) < int(params['max_id'])
                        or params.get('author_id', hit['author']['id']) != hit['author']['id']):
                    partial = True
                elif mid not in ids:
                    ids.append(mid)
            ids, total, surrounding = ids[:5], body['total_results'], 1
        else:
            ids, surrounding = [identifier(args['message_ref'])], 2
            scope = dict(channel_id=str(self.ctx.channel.id), message_id=ids[0])
        messages, matches = {}, set()
        for mid in ids:
            if int(mid) >= self.ctx.message.id:
                raise ValueError
            try:
                target = await self.ctx.channel.fetch_message(int(mid))
                if not self.in_scope(target):
                    raise ValueError
                messages[mid] = target
                matches.add(mid)
                nearby = [m async for m in self.ctx.channel.history(limit=2 * surrounding + 1, around=target)]
                nearby = [m for m in nearby if self.in_scope(m)
                          and abs((m.created_at - target.created_at).total_seconds()) <= 600]
                before = sorted((m for m in nearby if m.id < target.id), key=lambda m: m.id)
                after = sorted((m for m in nearby if m.id > target.id), key=lambda m: m.id)
                messages.update((str(m.id), m) for m in before[-surrounding:] + after[:surrounding])
            except (discord.NotFound, discord.HTTPException):
                partial = True
        await self.check(history=True)
        return render(messages, matches, scope=scope, partial=partial, has_more=total > len(matches),
                      limit=min(4000, self.remaining), count=self.message_limit, expanded=surrounding == 2)


async def search_discord(bot, guild_id, params):
    from discord.http import Route
    return await bot.http.request(
        Route('GET', '/guilds/{guild_id}/messages/search', guild_id=guild_id), params=params)

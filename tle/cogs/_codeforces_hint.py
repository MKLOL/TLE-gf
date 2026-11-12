"""LeekCF hints: bounded public API reads and spoiler-safe rendering."""
import asyncio
from collections import OrderedDict
import json
import re
import time
from urllib.parse import urlsplit

import aiohttp
import discord

from tle.cogs._codeforces_helpers import CodeforcesCogError

API = 'https://leekcf-hinter-api.leekcf.workers.dev'
USAGE = ('Use `;hint 1850A [hint number]`, `;hint 1850 A [hint number]`, '
         '`;hint 1850/A [hint number]`, or a Codeforces problem link.')


def parse_hint_args(args):
    args = list(args)
    if not args:
        raise CodeforcesCogError(USAGE)
    problem = args.pop(0).strip('<>')
    if re.fullmatch(r'[0-9]+', problem) and args:
        problem += args.pop(0)
    if '://' in problem:
        url = urlsplit(problem)
        if (url.scheme not in ('http', 'https')
                or url.netloc.lower() not in ('codeforces.com', 'www.codeforces.com')):
            raise CodeforcesCogError(USAGE)
        match = re.fullmatch(
            r'/(?:contest|gym)/([0-9]+)/problem/([a-z][0-9]{0,2})/?'
            r'|/problemset/problem/([0-9]+)/([a-z][0-9]{0,2})/?',
            url.path, re.I)
        if not match:
            raise CodeforcesCogError(USAGE)
        parts = match.groups()
        contest, index = parts[:2] if parts[0] else parts[2:]
    else:
        match = re.fullmatch(r'([0-9]{1,7})/?([a-z][0-9]{0,2})', problem, re.I)
        if not match:
            raise CodeforcesCogError(USAGE)
        contest, index = match.groups()
    if not 1 <= int(contest) <= 9999999 or len(args) > 1:
        raise CodeforcesCogError(USAGE)
    if args and not re.fullmatch(r'[1-5]', args[0]):
        raise CodeforcesCogError('Hint number must be between 1 and 5. ' + USAGE)
    return int(contest), index.upper(), int(args[0]) if args else 1


def parse_hints(data, contest, index):
    try:
        problem = data['problem']
        if problem['contestId'] != contest or problem['index'].upper() != index:
            raise ValueError
        hints = data['hints']
        if not isinstance(hints, list) or not 2 <= len(hints) <= 5:
            raise ValueError
        hints = sorted(hints, key=lambda h: h['level'])
        if [h['level'] for h in hints] != list(range(1, len(hints) + 1)):
            raise ValueError
        contents = tuple(h['content'] for h in hints)
        if any(not isinstance(s, str) or not s.strip() or len(s) > 30000
               for s in contents):
            raise ValueError
        return contents
    except (KeyError, TypeError, ValueError, AttributeError):
        raise CodeforcesCogError('LeekCF returned an invalid hint response. Try again later.') from None


class HintClient:
    def __init__(self):
        self.cache = OrderedDict()
        self.lock = asyncio.Lock()
        self.next_request = 0

    async def get(self, contest, index):
        key = (contest, index)
        cached = self.cache.get(key)
        if cached and cached[0] > time.monotonic():
            self.cache.move_to_end(key)
            return cached[1]
        if self.lock.locked() or time.monotonic() < self.next_request:
            raise CodeforcesCogError('The hint service is busy. Please try again shortly.')
        async with self.lock:
            self.next_request = time.monotonic() + 1.1
            hints = await self._fetch(contest, index)
            self.cache[key] = (time.monotonic() + 300, hints)
            self.cache.move_to_end(key)
            while len(self.cache) > 128:
                self.cache.popitem(last=False)
            return hints

    async def _fetch(self, contest, index):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(
                        f'{API}/v1/problems/{contest}/{index}/hints',
                        headers={'X-Client': 'TLE-gf/1.0'}, allow_redirects=False) as response:
                    if response.status == 404:
                        raise CodeforcesCogError(
                            f'No hints are available for {contest}{index} yet. '
                            'The problem may not be covered, or its contest may still be running.')
                    if response.status == 429:
                        try:
                            delay = max(1, min(300, int(response.headers.get('Retry-After', '60'))))
                        except ValueError:
                            delay = 60
                        self.next_request = time.monotonic() + delay
                        raise CodeforcesCogError(
                            f'LeekCF is rate limited. Try again in {delay} seconds.')
                    if response.status != 200:
                        raise CodeforcesCogError('LeekCF is unavailable. Please try again later.')
                    body = bytearray()
                    async for chunk in response.content.iter_chunked(8192):
                        body.extend(chunk)
                        if len(body) > 256000:
                            raise CodeforcesCogError('LeekCF returned an oversized response.')
                    return parse_hints(json.loads(body), contest, index)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise CodeforcesCogError('Could not reach LeekCF. Please try again later.') from None
        except (ValueError, UnicodeError):
            raise CodeforcesCogError('LeekCF returned an invalid hint response. Try again later.') from None


def spoiler_chunks(content):
    """Escape provider Markdown so code fences/pipes cannot break the spoiler."""
    chunks = []
    current = ''
    for char in content:
        token = '\\' + char if char in '\\`*_{}[]()#+-.!|>~' else char
        # Each physical line has its own spoiler, including long wrapped lines.
        if char == '\n' or len(current) + len(token) > 700:
            if current:
                chunks.append('||' + current + '||')
            current = ''
        if char != '\n':
            current += token
    if current:
        chunks.append('||' + current + '||')
    pages = []
    for chunk in chunks:
        if pages and len(pages[-1]) + len(chunk) + 1 <= 700:
            pages[-1] += '\n' + chunk
        else:
            pages.append(chunk)
    return pages


async def send_hint(ctx, client, args):
    contest, index, number = parse_hint_args(args)
    hints = await client.get(contest, index)
    if number > len(hints):
        raise CodeforcesCogError(
            f'{contest}{index} has {len(hints)} hints. Choose 1–{len(hints)}.')
    label = f'Hint {number}/{len(hints)}'
    if number == len(hints):
        label += ' — full solution'
    link = f'https://codeforces.com/problemset/problem/{contest}/{index}'
    header = f'**{contest}{index} — {label}**\n<{link}>\n'
    footer = ''
    if number < len(hints):
        footer = f'\nNext: `;hint {contest}{index} {number + 1}`'
    parts = spoiler_chunks(hints[number - 1])
    for i, part in enumerate(parts):
        text = (header if i == 0 else f'**{contest}{index} — {label} (continued)**\n') + part
        if i == len(parts) - 1:
            text += footer
        await ctx.send(text, allowed_mentions=discord.AllowedMentions.none(), suppress_embeds=True)

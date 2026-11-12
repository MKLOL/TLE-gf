"""Hint parsing, provider failures, caching, and spoiler isolation."""
import asyncio
import json
from types import SimpleNamespace as NS

import pytest

from tle.cogs import _codeforces_hint as hint
from tle.cogs.codeforces import Codeforces
from tle.cogs._codeforces_helpers import CodeforcesCogError


@pytest.mark.parametrize('args, expected', [
    (('1850A',), (1850, 'A', 1)),
    (('1850', 'a', '2'), (1850, 'A', 2)),
    (('1850/A', '2'), (1850, 'A', 2)),
    (('https://codeforces.com/contest/2284/problem/C1',), (2284, 'C1', 1)),
    (('<https://codeforces.com/problemset/problem/1850/A?locale=en>', '2'), (1850, 'A', 2)),
])
def test_parse(args, expected):
    assert hint.parse_hint_args(args) == expected


@pytest.mark.parametrize('args', [(), ('0A',), ('1850',), ('1850AA',),
    ('1850A', '0'), ('1850A', '-1'), ('1850A', '6'), ('1850A', '2', 'extra'),
    ('https://evil.test/contest/1850/problem/A',),
    ('https://codeforces.com.evil.test/contest/1850/problem/A',),
    ('https://codeforces.com/contest/1850',)])
def test_invalid_args(args):
    with pytest.raises(CodeforcesCogError):
        hint.parse_hint_args(args)


def payload():
    return {'problem': {'contestId': 1850, 'index': 'A'}, 'hints': [
        {'level': 1, 'content': 'Small nudge'},
        {'level': 2, 'content': 'SECRET full solution'}]}


class ContextManager:
    def __init__(self, obj):
        self.obj = obj

    async def __aenter__(self):
        return self.obj

    async def __aexit__(self, *args):
        pass


def fake_http(monkeypatch, status=200, body=None, headers=None, error=None):
    calls = []

    async def chunks(size):
        if error:
            raise error
        raw = json.dumps(payload()).encode() if body is None else body
        for start in range(0, len(raw), 7):
            yield raw[start:start + 7]

    response = NS(status=status, headers=headers or {}, content=NS(iter_chunked=chunks))

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return ContextManager(response)

    monkeypatch.setattr(hint.aiohttp, 'ClientSession',
                        lambda **kw: ContextManager(NS(get=get)))
    return calls


def test_fetch_and_cache(monkeypatch):
    calls = fake_http(monkeypatch)
    client = hint.HintClient()
    async def run():
        assert await client.get(1850, 'A') == ('Small nudge', 'SECRET full solution')
        assert await client.get(1850, 'A') == ('Small nudge', 'SECRET full solution')
        with pytest.raises(CodeforcesCogError, match='busy'):
            await client.get(1851, 'A')
    asyncio.run(run())
    assert len(calls) == 1
    assert calls[0][1]['headers']['X-Client'] == 'TLE-gf/1.0'


@pytest.mark.parametrize('status, expected', [(404, 'No hints'), (429, 'rate limited'),
                                             (500, 'unavailable'), (302, 'unavailable')])
def test_http_errors(monkeypatch, status, expected):
    fake_http(monkeypatch, status=status, headers={'Retry-After': '7'})
    client = hint.HintClient()
    with pytest.raises(CodeforcesCogError, match=expected):
        asyncio.run(client.get(1850, 'A'))
    assert not client.cache


@pytest.mark.parametrize('body', [b'not json', b'{}', b'x' * 256001,
    json.dumps({**payload(), 'hints': [{'level': 1, 'content': ''}]}).encode(),
    json.dumps({**payload(), 'problem': {'contestId': 1, 'index': 'A'}}).encode()])
def test_bad_response(monkeypatch, body):
    fake_http(monkeypatch, body=body)
    with pytest.raises(CodeforcesCogError):
        asyncio.run(hint.HintClient().get(1850, 'A'))


@pytest.mark.parametrize('error', [asyncio.TimeoutError(), hint.aiohttp.ClientError()])
def test_network_errors(monkeypatch, error):
    fake_http(monkeypatch, error=error)
    with pytest.raises(CodeforcesCogError, match='reach LeekCF'):
        asyncio.run(hint.HintClient().get(1850, 'A'))


def test_command_reveals_only_selected_hint(monkeypatch):
    fake_http(monkeypatch)
    mentions = object()
    monkeypatch.setattr(hint.discord, 'AllowedMentions', NS(none=lambda: mentions), raising=False)
    sent = []
    async def send(text, **kwargs):
        sent.append((text, kwargs))
    cog = Codeforces(None)
    ctx = NS(send=send)
    asyncio.run(Codeforces.hint(cog, ctx, '1850A'))
    assert len(sent) == 1
    assert '||Small nudge||' in sent[0][0]
    assert 'SECRET' not in sent[0][0]
    assert 'Next: `;hint 1850A 2`' in sent[0][0]
    assert sent[0][1] == {'allowed_mentions': mentions, 'suppress_embeds': True}
    sent.clear()
    asyncio.run(Codeforces.hint(cog, ctx, '1850A', '2'))
    assert 'full solution' in sent[0][0]
    assert '||SECRET full solution||' in sent[0][0]
    with pytest.raises(CodeforcesCogError, match='has 2 hints'):
        asyncio.run(Codeforces.hint(cog, ctx, '1850A', '3'))


def test_spoilers_survive_provider_markup_and_long_lines():
    text = '```python\n@everyone ||spoiler escape|| \\`\n' + '😀' * 4000
    pages = hint.spoiler_chunks(text)
    assert len(pages) > 1
    for page in pages:
        assert len(page.encode('utf-16-le')) // 2 < 1500
        for line in page.splitlines():
            assert line.startswith('||') and line.endswith('||')
            assert '||' not in line[2:-2]
            assert '```' not in line

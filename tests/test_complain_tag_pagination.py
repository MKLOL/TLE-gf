"""Large valid tag sets must not break complaint list or management embeds."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tle.cogs import complain as complain_cog
from tle.cogs._complaint_manage import render_page
from tle.cogs._complaint_tags import tag_pages


def _tags():
    return [f'tag{i:04d}' + 'x' * 25 for i in range(200)]


def _row(cid):
    return SimpleNamespace(
        id=cid, user_id='1' * 19, created_at=1_700_000_000.0,
        text=f'Report {cid} ' + 'x' * 490,
        message_link='https://discord.com/channels/' + '9' * 60,
        resolved_at=1_700_000_100.0, resolution='y' * 1500,
        commit_url='https://github.com/a/' + 'b' * 230 + '/commit/' + 'a' * 40)


def test_many_tags_preserve_complaint_and_resolution_within_embed_limit():
    row = _row(1)
    page, = complain_cog._complaint_pages([row], {1: _tags()})
    assert len(page) <= complain_cog._EMBED_DESCRIPTION_LIMIT
    assert row.text in page and row.resolution in page
    assert row.commit_url in page
    assert '(+195 more)' in page


def test_manage_keeps_every_button_target_visible_with_many_tags():
    rows = [_row(cid) for cid in range(5)]
    body = render_page(rows, tags_by_id={row.id: _tags() for row in rows})
    assert len(body) <= 3900
    for row in rows:
        assert f'**#{row.id}**' in body
        assert f'Report {row.id}' in body


def test_tag_directory_paginates_without_omitting_any_tag():
    counts = [SimpleNamespace(tag=tag, count=17) for tag in _tags()]
    pages = tag_pages(counts)
    assert len(pages) > 1
    assert all(len(page) <= 3900 for page in pages)
    assert '\n'.join(pages) == '\n'.join(f'`{row.tag}` — 17' for row in counts)


def test_tags_command_uses_paginator(monkeypatch):
    counts = [SimpleNamespace(tag=tag, count=1) for tag in _tags()]
    monkeypatch.setattr(complain_cog.cf_common, 'user_db', SimpleNamespace(
        get_complaint_tag_counts=lambda guild_id: counts))
    calls = []
    monkeypatch.setattr(complain_cog.paginator, 'paginate',
                        lambda *args, **kwargs: calls.append((args, kwargs)))
    ctx = SimpleNamespace(guild=SimpleNamespace(id=1), channel=object(),
                          author=SimpleNamespace(id=42), send=AsyncMock())
    cog = complain_cog.Complain(None)
    asyncio.run(cog.list_tags.callback(cog, ctx))
    args, kwargs = calls[0]
    assert args[1] is ctx.channel
    assert len(args[2]) > 1
    assert all(len(embed.description) <= 3900 for _, embed in args[2])
    assert kwargs['author_id'] == 42
    ctx.send.assert_not_awaited()

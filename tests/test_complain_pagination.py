"""Complaint list paging — complaint #278.

The list used to push every page into the channel at once; it now builds the
same pages and hands them to the shared button paginator.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs import complain as complain_cog
from tle.cogs.complain import (
    _EMBED_DESCRIPTION_LIMIT, _complaint_entry, _complaint_pages)
from tle.util import codeforces_common as cf_common


def _row(cid, text, *, link=None, resolved=False):
    return SimpleNamespace(
        id=cid, user_id='100', text=text, created_at=1_700_000_000.0,
        message_link=link,
        resolved_at=1_700_000_100.0 if resolved else None,
        resolution='Shipped it.' if resolved else None,
        commit_url='https://github.com/MKLOL/TLE-gf/commit/' + 'a' * 40)


class TestComplaintPages:
    def test_short_list_is_one_page(self):
        rows = [_row(i, f'complaint {i}') for i in range(5)]
        pages = _complaint_pages(rows)
        assert len(pages) == 1
        for row in rows:
            assert row.text in pages[0]

    def test_every_page_fits_the_embed_limit(self):
        rows = [_row(i, 'x' * 480) for i in range(40)]
        pages = _complaint_pages(rows)
        assert len(pages) > 1
        assert all(len(page) <= _EMBED_DESCRIPTION_LIMIT for page in pages)

    def test_no_entry_is_split_across_pages(self):
        rows = [_row(i, 'x' * 480) for i in range(40)]
        joined = _complaint_pages(rows)
        for row in rows:
            entry = _complaint_entry(row)
            assert sum(entry in page for page in joined) == 1

    def test_context_link_and_resolution_are_rendered(self):
        link = 'https://discord.com/channels/1/2/3'
        page, = _complaint_pages([_row(7, 'broken', link=link, resolved=True)])
        assert f'[context]({link})' in page
        assert '**Resolved:** Shipped it.' in page

    def test_empty_input_yields_no_pages(self):
        assert _complaint_pages([]) == []

    def test_the_largest_possible_entry_still_fits_one_page(self):
        """The chunker never splits an entry, so a single entry must fit.

        Bounds come from the API contract: 500-char report
        (``_MAX_COMPLAINT_LENGTH``), 1500-char resolution, 300-char commit
        URL. If any of those caps is raised past this headroom, ;complain list
        starts failing with an embed-too-long error inside a background task,
        where nobody sees it — so the invariant is pinned here.
        """
        worst = SimpleNamespace(
            id=9_999_999, user_id='1' * 19, text='x' * 500,
            created_at=1_700_000_000.0,
            message_link='https://discord.com/channels/' + '9' * 60,
            resolved_at=1_700_000_100.0, resolution='y' * 1500,
            commit_url='https://github.com/MKLOL/TLE-gf/commit/' + 'a' * 40)
        entry = _complaint_entry(worst)
        assert len(entry) < _EMBED_DESCRIPTION_LIMIT
        assert len(_complaint_pages([worst])) == 1

    def test_resolved_entries_are_packed_within_the_limit(self):
        rows = [_row(i, 'x' * 500, resolved=True) for i in range(20)]
        pages = _complaint_pages(rows)
        assert len(pages) > 1
        assert all(len(page) <= _EMBED_DESCRIPTION_LIMIT for page in pages)


class _Db:
    def __init__(self, rows):
        self.rows = rows

    def get_complaints(self, guild_id, status, **kwargs):
        return self.rows

    def get_tags_for_complaints(self, ids):
        return {}


def _ctx():
    return SimpleNamespace(
        guild=SimpleNamespace(id=1),
        author=SimpleNamespace(id=42),
        channel=SimpleNamespace(id=2),
        send=AsyncMock())


class TestListCommandPaginates:
    @pytest.fixture
    def calls(self, monkeypatch):
        recorded = []
        monkeypatch.setattr(
            complain_cog.paginator, 'paginate',
            lambda bot, channel, pages, **kw: recorded.append((channel, pages, kw)))
        return recorded

    def _run_list(self, rows, ctx, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', _Db(rows))
        cog = complain_cog.Complain(bot=None)
        asyncio.run(cog.list.callback(cog, ctx, 'all'))

    def test_pages_go_to_the_paginator_not_the_channel(self, calls, monkeypatch):
        ctx = _ctx()
        self._run_list([_row(i, 'x' * 480) for i in range(40)], ctx, monkeypatch)

        assert len(calls) == 1
        channel, pages, kwargs = calls[0]
        assert channel is ctx.channel
        assert len(pages) > 1
        assert ctx.send.await_count == 0
        assert all(content is None for content, _ in pages)

    def test_only_the_requester_can_navigate(self, calls, monkeypatch):
        ctx = _ctx()
        self._run_list([_row(1, 'one')], ctx, monkeypatch)
        _, _, kwargs = calls[0]
        assert kwargs['author_id'] == ctx.author.id
        assert kwargs['set_pagenum_footers'] is True

    def test_empty_list_still_replies_in_channel(self, calls, monkeypatch):
        ctx = _ctx()
        self._run_list([], ctx, monkeypatch)
        assert calls == []
        assert ctx.send.await_count == 1

"""Tests for gg/mgg interactive leaderboard rendering."""
import asyncio
from collections import namedtuple
from types import SimpleNamespace

from tle.cogs import handles as handles_module
from tle.cogs.handles import Handles
from tle.util import codeforces_common as cf_common


class _FakeMember:
    def __init__(self, user_id, display_name):
        self.id = user_id
        self.display_name = display_name
        self.mention = f'<@{user_id}>'


class _FakeGuild:
    def __init__(self, guild_id, members):
        self.id = guild_id
        self._members = {member.id: member for member in members}

    def get_member(self, user_id):
        return self._members.get(user_id)


class _FakeRatingChangesCache:
    def __init__(self, changes_by_handle):
        self._changes_by_handle = changes_by_handle

    def get_rating_changes_for_handle(self, handle):
        return list(self._changes_by_handle.get(handle, []))


class _FakeUserDb:
    def __init__(self, *, gudgitters=None, monthly_entries=None, handles=None, users=None):
        self._gudgitters = gudgitters or []
        self._monthly_entries = monthly_entries or []
        self._handles = handles or {}
        self._users = users or {}

    def get_gudgitters(self):
        return list(self._gudgitters)

    def get_gudgitters_timerange(self, start_time, end_time):
        return list(self._monthly_entries)

    def get_handle(self, user_id, guild_id):
        return self._handles.get(str(user_id))

    def fetch_cf_user(self, handle):
        return self._users.get(handle)


def _make_handles_cog():
    cog = Handles.__new__(Handles)
    cog.bot = object()
    return cog


def test_gudgitters_uses_paginated_embeds_with_personal_rank(monkeypatch):
    User = namedtuple('User', 'rating')
    members = [_FakeMember(i, f'user{i}') for i in range(1, 13)]
    guild = _FakeGuild(1, members)
    ctx = SimpleNamespace(author=members[-1], guild=guild, channel=object())
    user_db = _FakeUserDb(
        gudgitters=[(str(i), 100 - i) for i in range(1, 13)],
        handles={str(i): f'h{i}' for i in range(1, 13)},
        users={f'h{i}': User(1800 + i) for i in range(1, 13)},
    )

    original_user_db = cf_common.user_db
    cf_common.user_db = user_db
    captured = {}

    def fake_paginate(bot, channel, pages, **kwargs):
        captured['pages'] = pages
        captured['kwargs'] = kwargs

    monkeypatch.setattr(handles_module.paginator, 'paginate', fake_paginate)
    try:
        asyncio.run(Handles.gudgitters(_make_handles_cog(), ctx))
    finally:
        cf_common.user_db = original_user_db

    assert len(captured['pages']) == 2
    first_embed = captured['pages'][0][1]
    second_embed = captured['pages'][1][1]
    assert first_embed.title == 'GG Leaderboard'
    assert '**#1** <@1> — **99** pts | `h1` (1801)' in first_embed.description
    assert 'Your rank: **#12** with **88** points' in first_embed.description
    assert '**#12** <@12> — **88** pts | `h12` (1812)' in second_embed.description
    assert captured['kwargs']['author_id'] == ctx.author.id
    assert captured['kwargs']['set_pagenum_footers'] is True


def test_monthlygudgitters_uses_paginator_and_month_start_rating(monkeypatch):
    User = namedtuple('User', 'rating')
    RatingChange = namedtuple('RatingChange', 'ratingUpdateTimeSeconds newRating')
    members = [_FakeMember(1, 'u1'), _FakeMember(2, 'u2')]
    guild = _FakeGuild(1, members)
    ctx = SimpleNamespace(author=members[0], guild=guild, channel=object())
    user_db = _FakeUserDb(
        monthly_entries=[('1', 0, 1741000000), ('2', 0, 1741000000)],
        handles={'1': 'h1', '2': 'h2'},
        users={'h1': User(2000), 'h2': User(2200)},
    )
    cache = _FakeRatingChangesCache({
        'h1': [RatingChange(1740700000, 1700)],
        'h2': [RatingChange(1740700000, 2200)],
    })

    original_user_db = cf_common.user_db
    original_cache2 = cf_common.cache2
    cf_common.user_db = user_db
    cf_common.cache2 = SimpleNamespace(rating_changes_cache=cache)
    captured = {}

    def fake_paginate(bot, channel, pages, **kwargs):
        captured['pages'] = pages

    monkeypatch.setattr(handles_module.paginator, 'paginate', fake_paginate)
    try:
        asyncio.run(Handles.monthlygudgitters(_make_handles_cog(), ctx, 'div2', 'd=032025'))
    finally:
        cf_common.user_db = original_user_db
        cf_common.cache2 = original_cache2

    assert len(captured['pages']) == 1
    embed = captured['pages'][0][1]
    assert embed.title == 'MGG Leaderboard (Mar 2025)'
    assert '**#1** <@1> — **8** pts | `h1` (1700)' in embed.description
    assert '`h2`' not in embed.description


def test_ggimg_sends_top_20_color_image_rankings(monkeypatch):
    User = namedtuple('User', 'rating')
    members = [_FakeMember(i, f'user{i}') for i in range(1, 23)]
    guild = _FakeGuild(1, members)
    sent = {}

    async def fake_send(*, file=None):
        sent['file'] = file

    ctx = SimpleNamespace(
        author=members[0],
        guild=guild,
        channel=object(),
        send=fake_send,
    )
    user_db = _FakeUserDb(
        gudgitters=[(str(i), 200 - i) for i in range(1, 23)],
        handles={str(i): f'h{i}' for i in range(1, 23)},
        users={f'h{i}': User(1500 + i) for i in range(1, 23)},
    )

    original_user_db = cf_common.user_db
    cf_common.user_db = user_db
    captured = {}

    def fake_get_gudgitters_image(rankings):
        captured['rankings'] = rankings
        return 'fake-image'

    monkeypatch.setattr(handles_module, 'get_gudgitters_image', fake_get_gudgitters_image)
    try:
        asyncio.run(Handles.ggimg(_make_handles_cog(), ctx))
    finally:
        cf_common.user_db = original_user_db

    assert sent['file'] == 'fake-image'
    assert len(captured['rankings']) == 20
    assert captured['rankings'][0] == (0, 'user1', 'h1', 1501, 199)
    assert captured['rankings'][-1] == (19, 'user20', 'h20', 1520, 180)


def test_mggimg_sends_monthly_image_rankings(monkeypatch):
    User = namedtuple('User', 'rating')
    RatingChange = namedtuple('RatingChange', 'ratingUpdateTimeSeconds newRating')
    members = [_FakeMember(1, 'u1'), _FakeMember(2, 'u2')]
    guild = _FakeGuild(1, members)
    sent = {}

    async def fake_send(*, file=None):
        sent['file'] = file

    ctx = SimpleNamespace(
        author=members[0],
        guild=guild,
        channel=object(),
        send=fake_send,
    )
    user_db = _FakeUserDb(
        monthly_entries=[('1', 0, 1741000000), ('2', 0, 1741000000)],
        handles={'1': 'h1', '2': 'h2'},
        users={'h1': User(2000), 'h2': User(2200)},
    )
    cache = _FakeRatingChangesCache({
        'h1': [RatingChange(1740700000, 1700)],
        'h2': [RatingChange(1740700000, 2200)],
    })

    original_user_db = cf_common.user_db
    original_cache2 = cf_common.cache2
    cf_common.user_db = user_db
    cf_common.cache2 = SimpleNamespace(rating_changes_cache=cache)
    captured = {}

    def fake_get_gudgitters_image(rankings):
        captured['rankings'] = rankings
        return 'fake-monthly-image'

    monkeypatch.setattr(handles_module, 'get_gudgitters_image', fake_get_gudgitters_image)
    try:
        asyncio.run(Handles.mggimg(_make_handles_cog(), ctx, 'div2', 'd=032025'))
    finally:
        cf_common.user_db = original_user_db
        cf_common.cache2 = original_cache2

    assert sent['file'] == 'fake-monthly-image'
    assert captured['rankings'] == [(0, 'u1', 'h1', 1700, 8)]

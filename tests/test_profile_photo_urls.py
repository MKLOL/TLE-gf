"""Profile photos use the same image proxy as the Codeforces website."""
import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from tests.test_cf_ratelimit import _load_real_cf_api
from tle.cogs.handles import Handles
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util._cf_api_types import User
from tle.util.db.handle_db import HandleDbMixin


_PHOTO_PATH = '4097417/title/eed7b1c30564f6f5.jpg'
_DIRECT_PHOTO = f'https://userpic.codeforces.org/{_PHOTO_PATH}'
_PROXY_PHOTO = f'https://codeforces.com/userpic.codeforces.org/{_PHOTO_PATH}'


def _user(photo):
    return User('reirugan', None, None, None, None, None,
                0, 2100, 2200, 0, 0, 0, photo)


@pytest.mark.parametrize('prefix', ['https://', 'http://', '//'])
def test_direct_userpic_url_uses_website_proxy(prefix):
    user = _user(f'{prefix}userpic.codeforces.org/{_PHOTO_PATH}?v=2#photo')
    fixed = cf_common.fix_urls(user)
    assert fixed == user._replace(titlePhoto=f'{_PROXY_PHOTO}?v=2#photo')
    assert cf_common.fix_urls(fixed) == fixed


@pytest.mark.parametrize('photo', [
    _PROXY_PHOTO,
    'https://cdn.codeforces.com/default.jpg',
    'https://example.com/userpic.codeforces.org/photo.jpg',
    'https://userpic.codeforces.org.example.com/photo.jpg',
])
def test_other_photo_urls_are_unchanged(photo):
    user = _user(photo)
    assert cf_common.fix_urls(user) == user


def test_other_protocol_relative_urls_still_get_https():
    assert cf_common.fix_urls(_user('//cdn.codeforces.com/default.jpg')).titlePhoto == (
        'https://cdn.codeforces.com/default.jpg')


def test_fresh_api_user_info_uses_proxy(monkeypatch):
    api = _load_real_cf_api()
    query = AsyncMock(return_value=[_user(_DIRECT_PHOTO)._asdict()])
    monkeypatch.setattr(api, '_query_api', query)
    users = asyncio.run(api.user.info(handles=['reirugan']))
    assert users[0].titlePhoto == _PROXY_PHOTO
    query.assert_awaited_once_with('user.info', {'handles': 'reirugan'})


@pytest.mark.parametrize('command', ['get', 'rget'])
def test_existing_cached_photo_is_fixed_in_handle_embed(monkeypatch, command):
    # Seed the old URL directly: existing installations must not need a refresh
    # or a database migration to repair their embeds.
    db = HandleDbMixin()
    db.conn = sqlite3.connect(':memory:')
    db._create_handle_tables()
    db.cache_cf_user(_user(_DIRECT_PHOTO))
    db.set_handle(42, 7, 'reirugan')
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(cf, 'User', User)
    monkeypatch.setattr(discord.Embed, 'set_thumbnail',
                        lambda self, *, url: setattr(self, 'thumbnail_url', url),
                        raising=False)
    member = SimpleNamespace(id=42, mention='<@42>')
    guild = SimpleNamespace(id=7, get_member=lambda user_id: member)
    ctx = SimpleNamespace(guild=guild, send=AsyncMock())
    cog = Handles.__new__(Handles)
    try:
        arg = member if command == 'get' else 'reirugan'
        asyncio.run(getattr(Handles, command).callback(cog, ctx, arg))
        embed = ctx.send.call_args.kwargs['embed']
        assert embed.thumbnail_url == _PROXY_PHOTO
        assert 'reirugan' in embed.description
        assert db.conn.execute('SELECT title_photo FROM cf_user_cache').fetchone() == (
            _DIRECT_PHOTO,)
    finally:
        db.conn.close()

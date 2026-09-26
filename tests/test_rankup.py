"""Rank updates after a contest — why a linked member can be left out.

Reported as "flamestorm not in the list, now reirugan too, and his role did
not change". Every path that silently dropped a member is pinned here.
"""
import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from tle.cogs import _handles_rankup as rankup
from tle.cogs._handles_helpers import HandleCogError
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util.cache_system2._common import late_rating_changes


def _member(uid, *roles):
    return SimpleNamespace(id=uid, roles=[SimpleNamespace(name=r) for r in roles],
                           mention=f'<@{uid}>', display_name=f'm{uid}')


def _change(handle, old=1857, new=1914):
    return SimpleNamespace(handle=handle, oldRating=old, newRating=new,
                           ratingUpdateTimeSeconds=1)


class TestMatching:
    def test_casing_differences_still_match(self):
        pairs, skipped = rankup.match_members_to_changes(
            [(_member(1), 'FlameStorm')], {'flamestorm': _change('flamestorm')})
        assert [c.handle for _, c in pairs] == ['flamestorm']
        assert skipped == []

    def test_every_drop_has_a_reason(self):
        changes = {'a': _change('a'), 'c': _change('c')}
        pairs, skipped = rankup.match_members_to_changes(
            [(None, 'a'), (_member(2), 'b'), (_member(3, 'Shadow Realm'), 'c')], changes)
        assert pairs == []
        assert skipped == [('a', rankup.SKIP_NOT_IN_SERVER),
                           ('b', rankup.SKIP_NOT_RATED),
                           ('c', rankup.SKIP_SHADOW_REALM)]


class TestLateChanges:
    def test_new_and_recalculated_are_separated(self):
        saved = [_change('a'), _change('b', 1, 2)]
        fetched = [_change('A'), _change('b', 1, 3), _change('c')]
        new, changed = late_rating_changes(saved, fetched)
        assert [c.handle for c in new] == ['c']
        assert [c.handle for c in changed] == ['b']

    def test_identical_lists_are_quiet(self):
        assert late_rating_changes([_change('a')], [_change('a')]) == ([], [])


class _Cog(rankup.RankUpMixin):
    def __init__(self):
        self.logger = logging.getLogger('test-rankup')
        self.bot = SimpleNamespace(guilds=[])


class TestTolerantUserInfo:
    def _cog(self, monkeypatch, bad):
        calls = []

        async def info(*, handles):
            calls.append(list(handles))
            for handle in handles:
                if handle.lower() in bad:
                    raise cf.HandleNotFoundError(
                        f'handles: User with handle {handle} not found', handle)
            return [SimpleNamespace(handle=h) for h in handles]
        monkeypatch.setattr(cf, 'user', SimpleNamespace(info=info), raising=False)
        return _Cog(), calls

    def test_one_dead_handle_no_longer_fails_everyone(self, monkeypatch):
        cog, calls = self._cog(monkeypatch, {'gone'})
        users, skipped = asyncio.run(cog._fetch_users_tolerant(['alice', 'Gone', 'bob']))
        assert [u.handle for u in users] == ['alice', 'bob']
        assert skipped == ['gone']
        assert calls == [['alice', 'Gone', 'bob'], ['alice', 'bob']]

    def test_an_unparseable_error_is_not_retried_forever(self, monkeypatch):
        async def info(*, handles):
            raise cf.HandleNotFoundError('handles: something odd', 'x')
        monkeypatch.setattr(cf, 'user', SimpleNamespace(info=info), raising=False)
        with pytest.raises(cf.HandleNotFoundError):
            asyncio.run(_Cog()._fetch_users_tolerant(['alice']))


def _guild(members, channel):
    by_id = {m.id: m for m in members}
    return SimpleNamespace(id=1, get_member=by_id.get, roles=[],
                           get_channel=lambda cid: channel)


class TestEventIsolation:
    @pytest.fixture
    def env(self, monkeypatch):
        channel = SimpleNamespace(send=AsyncMock())
        guild = _guild([_member(10), _member(20)], channel)
        db = SimpleNamespace(
            has_auto_role_update_enabled=lambda gid: True,
            get_rankup_channel=lambda gid: 5,
            get_handles_for_guild=lambda gid: [(10, 'alice'), (20, 'reirugan')])
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(cf_common, 'cache2', SimpleNamespace(
            rating_changes_cache=SimpleNamespace(
                get_rating_changes_for_handle=lambda h: [1, 2])))
        monkeypatch.setattr(cf, 'rating2rank', lambda r: SimpleNamespace(title='Expert'),
                            raising=False)
        monkeypatch.setattr(cf, 'PROFILE_BASE_URL', 'https://codeforces.com/profile/',
                            raising=False)
        monkeypatch.setattr(rankup.discord_common, 'set_same_cf_color', lambda embeds: None,
                            raising=False)
        monkeypatch.setattr(rankup.paginator, 'chunkify',
                            lambda seq, n: [seq[i:i + n] for i in range(0, len(seq), n)] or [[]],
                            raising=False)
        cog = _Cog()
        cog.bot.guilds = [guild]
        event = SimpleNamespace(
            contest=SimpleNamespace(id=2267, name='Round 1123', url='u'),
            rating_changes=[_change('alice'), _change('Reirugan')])
        return cog, channel, event

    def test_a_broken_role_sync_no_longer_swallows_the_announcement(self, env, monkeypatch, caplog):
        cog, channel, event = env

        async def boom(guild):
            raise RuntimeError('user.info exploded')
        monkeypatch.setattr(cog, '_update_ranks_all', boom)
        with caplog.at_level(logging.ERROR):
            asyncio.run(cog._on_rating_changes(event))
        assert channel.send.await_count >= 2  # heading + content
        assert any('Role sync failed' in r.message for r in caplog.records)

    def test_both_members_are_listed_whatever_the_casing(self, env, monkeypatch):
        cog, channel, event = env

        async def ok(guild):
            return None
        monkeypatch.setattr(cog, '_update_ranks_all', ok)
        asyncio.run(cog._on_rating_changes(event))
        text = ' '.join(c.kwargs['embed'].description or '' for c in channel.send.call_args_list)
        assert '<@10>' in text and '<@20>' in text


class TestRoleSyncResilience:
    def test_one_forbidden_role_edit_does_not_stop_the_rest(self, monkeypatch, caplog):
        members = [_member(10), _member(20)]
        guild = SimpleNamespace(id=1, get_member={m.id: m for m in members}.get,
                                roles=[SimpleNamespace(name='Expert')])
        monkeypatch.setattr(cf_common, 'user_db',
                            SimpleNamespace(cache_cf_user=lambda user: 1))

        async def info(*, handles):
            return [SimpleNamespace(handle=h, rank=SimpleNamespace(title='Expert'))
                    for h in handles]
        monkeypatch.setattr(cf, 'user', SimpleNamespace(info=info), raising=False)
        updated = []

        async def update(member, role, *, reason):
            if member.id == 10:
                raise discord.HTTPException('hierarchy')
            updated.append(member.id)
        cog = _Cog()
        monkeypatch.setattr(cog, 'update_member_rank_role', update)
        with caplog.at_level(logging.WARNING):
            asyncio.run(cog._update_ranks(guild, [(10, 'alice'), (20, 'reirugan')]))
        assert updated == [20]
        assert any('Could not update rank role' in r.message for r in caplog.records)

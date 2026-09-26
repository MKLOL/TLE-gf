"""The inactive sweep must never trust an incomplete member cache."""
import asyncio
import logging
from types import SimpleNamespace

from tle.cogs import handles as handles_cog
from tle.util import codeforces_common as cf_common


class _Db:
    def __init__(self, active, inactive):
        self.active = dict(active)      # user_id -> handle
        self.inactive = set(inactive)
        self.calls = []

    def get_handles_for_guild(self, guild_id):
        return list(self.active.items())

    def get_inactive_user_ids_for_guild(self, guild_id):
        return sorted(self.inactive)

    def update_status(self, guild_id, ids):
        self.calls.append(('activate', list(ids)))
        for uid in ids:
            self.inactive.discard(uid)
        return len(ids)

    def set_inactive(self, pairs):
        self.calls.append(('deactivate', [uid for _, uid in pairs]))
        return len(pairs)


def _guild(cached_ids, member_count=None, chunked=None):
    members = [SimpleNamespace(id=i) for i in cached_ids]
    guild = SimpleNamespace(id=1, members=members,
                            get_member={m.id: m for m in members}.get,
                            member_count=member_count if member_count is not None else len(members))
    if chunked is not None:
        guild.chunked = chunked
    return guild


def _run(db, guild, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    cog = handles_cog.Handles.__new__(handles_cog.Handles)
    cog.bot = SimpleNamespace(guilds=[guild])
    cog.logger = logging.getLogger('test-sweep')
    asyncio.run(cog._sweep_member_status())
    return db.calls


def test_complete_cache_marks_leavers_and_reactivates_returners(monkeypatch):
    db = _Db(active={10: 'a', 20: 'gone'}, inactive={30})
    calls = _run(db, _guild([10, 30], chunked=True), monkeypatch)
    assert ('activate', [30]) in calls
    assert ('deactivate', [20]) in calls


def test_partially_chunked_guild_is_left_alone(monkeypatch, caplog):
    """A reconnect re-chunks the guild; mid-way, most members look absent."""
    db = _Db(active={10: 'a', 20: 'b', 30: 'c'}, inactive=set())
    with caplog.at_level(logging.WARNING):
        calls = _run(db, _guild([10], chunked=False), monkeypatch)
    assert calls == [('deactivate', [])]
    assert any('member cache' in r.message for r in caplog.records)


def test_count_fallback_when_chunked_is_unknown(monkeypatch):
    db = _Db(active={10: 'a', 20: 'b'}, inactive=set())
    calls = _run(db, _guild([10], member_count=2), monkeypatch)
    assert calls == [('deactivate', [])]


def test_count_fallback_passes_a_full_cache(monkeypatch):
    db = _Db(active={10: 'a', 20: 'b'}, inactive=set())
    calls = _run(db, _guild([10], member_count=1), monkeypatch)
    assert ('deactivate', [20]) in calls

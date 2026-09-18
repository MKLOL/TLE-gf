"""Hardening tests for the proxy-reaction path.

Covers the reviewer-found gaps: legacy abuse rows (a bot starboard post
tracked as an "original") must never let the proxy path redirect the
engine at a board surface; migration 1.53.0 purges such rows; and drift
self-healing must compare Discord counts against the *physical* pool only,
so proxy reactors never trigger a purge resync.
"""
import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

import discord

from tests.starboard_test_utils import FakeUserDb, GUILD_A, STAR
from tle.cogs.starboard import Starboard
from tle.util.db.user_db_upgrades import upgrade_1_53_0

PILL = '\N{PILL}'
STAR_CHANNEL = 888
PILL_CHANNEL = 777
SOURCE_CHANNEL = 999
ORIGINAL_MSG = 5001
STAR_POST = 7777       # legit bot post for ORIGINAL_MSG on the star board
ABUSE_POST = 8888      # bot post created by pre-fix abuse: STAR_POST -> pill board


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


def _setup_boards(db):
    db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
    db.set_starboard_channel(GUILD_A, STAR, str(STAR_CHANNEL))
    db.add_starboard_emoji(GUILD_A, PILL, 2, 0xff0000)
    db.set_starboard_channel(GUILD_A, PILL, str(PILL_CHANNEL))
    db.add_starboard_message_v1(
        ORIGINAL_MSG, STAR_POST, GUILD_A, STAR,
        author_id='42', channel_id=str(SOURCE_CHANNEL))


def _add_abuse_row(db):
    """A pre-exclusion abuse entry: the star post itself starboarded to pill."""
    db.add_starboard_message_v1(
        STAR_POST, ABUSE_POST, GUILD_A, PILL,
        author_id='bot', channel_id=str(STAR_CHANNEL))


class _FakePayload:
    def __init__(self, guild_id, channel_id, message_id, user_id, emoji):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.message_id = message_id
        self.user_id = user_id
        self.emoji = emoji


def _build_cog(monkeypatch, db):
    from tle.util import codeforces_common as cf
    monkeypatch.setattr(cf, 'user_db', db)
    cog = Starboard.__new__(Starboard)
    cog.bot = SimpleNamespace(
        get_channel=lambda _id: SimpleNamespace(nsfw=False))
    cog.locks = {}
    engine_calls = []

    async def fake_check_and_add(*args, **kwargs):
        engine_calls.append((args, kwargs))

    cog.check_and_add_to_starboard = fake_check_and_add
    return cog, engine_calls


class TestLegacyAbuseRowGuard:
    def test_react_on_abuse_post_is_dropped(self, db, monkeypatch):
        """A reaction on the abuse entry's post must not redirect the engine
        at the star post (a board surface)."""
        _setup_boards(db)
        _add_abuse_row(db)
        cog, engine_calls = _build_cog(monkeypatch, db)

        payload = _FakePayload(GUILD_A, PILL_CHANNEL, ABUSE_POST, 'u1', STAR)
        asyncio.run(cog._handle_reaction_add(payload))

        assert engine_calls == [], 'engine must never run against a bot post'
        assert db.get_proxy_reactors(STAR_POST, STAR) == []

    def test_remove_on_abuse_post_deletes_row_without_refresh(self, db, monkeypatch):
        """Stale proxy rows must stay deletable behind the guard — only the
        display engine is kept away from the board surface."""
        _setup_boards(db)
        _add_abuse_row(db)
        db.add_proxy_reactor(STAR_POST, STAR, 'u1', ABUSE_POST)
        cog, _ = _build_cog(monkeypatch, db)
        refreshes = []

        async def fake_refresh(*args, **kwargs):
            refreshes.append(args)

        cog._refresh_starboard_display = fake_refresh
        payload = _FakePayload(GUILD_A, PILL_CHANNEL, ABUSE_POST, 'u1', STAR)
        asyncio.run(cog._handle_reaction_remove(payload))

        assert db.get_proxy_reactors(STAR_POST, STAR) == []
        assert refreshes == [], 'no display refresh against a board surface'

    def test_unreact_still_deletes_after_channel_becomes_board(self, db, monkeypatch):
        """A legit proxy row must not get stuck when an admin later turns the
        message's source channel into a board channel."""
        _setup_boards(db)
        FIRE = '\N{FIRE}'
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', STAR_POST)
        db.add_starboard_emoji(GUILD_A, FIRE, 3, 0x111111)
        db.set_starboard_channel(GUILD_A, FIRE, str(SOURCE_CHANNEL))
        cog, _ = _build_cog(monkeypatch, db)

        async def fake_refresh(*args, **kwargs):
            pass

        cog._refresh_starboard_display = fake_refresh
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, STAR_POST, 'u1', STAR)
        asyncio.run(cog._handle_reaction_remove(payload))

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == []

    def test_channel_guard_catches_untracked_board_original(self, db, monkeypatch):
        """An entry whose original lives in a board channel is refused even
        when that original is not itself a tracked post."""
        _setup_boards(db)
        db.add_starboard_message_v1(
            6001, ABUSE_POST, GUILD_A, PILL,
            author_id='bot', channel_id=str(STAR_CHANNEL))
        cog, engine_calls = _build_cog(monkeypatch, db)

        payload = _FakePayload(GUILD_A, PILL_CHANNEL, ABUSE_POST, 'u1', PILL)
        asyncio.run(cog._handle_reaction_add(payload))

        assert engine_calls == []
        assert db.get_proxy_reactors(6001, PILL) == []

    def test_legit_proxy_still_works(self, db, monkeypatch):
        _setup_boards(db)
        cog, engine_calls = _build_cog(monkeypatch, db)

        payload = _FakePayload(GUILD_A, STAR_CHANNEL, STAR_POST, 'u1', STAR)
        asyncio.run(cog._handle_reaction_add(payload))

        assert len(engine_calls) == 1
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u1']


class TestPurgeMigration:
    def test_purges_abuse_entries_and_their_rows(self, db):
        _setup_boards(db)
        _add_abuse_row(db)
        db.add_reactor(STAR_POST, PILL, 'u1')
        db.record_narcissus_mark(GUILD_A, STAR_POST, PILL, 'bot')

        upgrade_1_53_0(db.conn)

        assert not db.check_exists_starboard_message_v1(STAR_POST, PILL), \
            'the abuse entry must be purged'
        assert db.check_exists_starboard_message_v1(ORIGINAL_MSG, STAR), \
            'the legit entry must survive'
        assert db.get_reactors(STAR_POST, PILL) == []
        assert db.get_narcissus_leaderboard(GUILD_A, PILL) == []

    def test_purge_is_idempotent_and_safe_when_clean(self, db):
        _setup_boards(db)
        upgrade_1_53_0(db.conn)
        upgrade_1_53_0(db.conn)
        assert db.check_exists_starboard_message_v1(ORIGINAL_MSG, STAR)


class TestDriftUsesPhysicalPoolOnly:
    def test_proxy_rows_do_not_trigger_purge_resync(self, db, monkeypatch):
        """Merged count exceeding Discord's visible reactions via proxy rows
        must not be treated as ghost-reactor drift."""
        from tle.util import codeforces_common as cf
        monkeypatch.setattr(cf, 'user_db', db)
        _setup_boards(db)
        for uid in ('u1', 'u2'):
            db.add_reactor(ORIGINAL_MSG, STAR, uid)
        for uid in ('u3', 'u4'):
            db.add_proxy_reactor(ORIGINAL_MSG, STAR, uid, STAR_POST)

        class _Reaction:
            emoji = STAR
            count = 3

            def __str__(self):
                return self.emoji

        reaction = _Reaction()
        message = SimpleNamespace(
            id=ORIGINAL_MSG, content='hi', embeds=[], attachments=[],
            created_at=datetime(2025, 1, 1),
            author=SimpleNamespace(id=42), reference=None,
            reactions=[reaction], type=discord.MessageType.default)
        source_channel = SimpleNamespace(id=SOURCE_CHANNEL, nsfw=False)

        async def fetch_message(_id):
            return message
        source_channel.fetch_message = fetch_message

        cog = Starboard.__new__(Starboard)
        cog.bot = SimpleNamespace(
            get_channel=lambda _id: source_channel,
            get_guild=lambda _id: SimpleNamespace(
                id=GUILD_A, get_channel=lambda _cid: SimpleNamespace(id=888)),
        )
        cog.locks = {}
        resyncs = []

        async def fake_resync(msg, family):
            resyncs.append((msg, family))
            return db.get_merged_reactor_count(msg.id, family)

        cog._resync_reactors = fake_resync
        updates = []

        async def fake_update(*args, **kwargs):
            updates.append(args)
        cog._update_starboard_message = fake_update

        payload = SimpleNamespace(
            guild_id=GUILD_A, channel_id=SOURCE_CHANNEL,
            message_id=ORIGINAL_MSG, user_id='u5', emoji=STAR)
        asyncio.run(cog.check_and_add_to_starboard(
            888, 3, 0xffaa10, STAR, payload, raw_emoji=STAR))

        assert resyncs == [], \
            'proxy reactors must never be mistaken for ghost drift'
        assert updates and updates[0][3] == 5, \
            'displayed count is the union: u1,u2,u5 physical + u3,u4 proxy'

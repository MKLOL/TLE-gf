"""Tests for proxy reactions — reacting on a bot starboard post counts
toward the original message.

A user without access to the source channel can react on the starboard post
itself; the reaction is stored as a proxy reactor for the original message.
Counts are the distinct-user union of physical and proxy reactors, so
reacting on both surfaces counts once.  A pill react on a star-board post
can put the *original* message onto the pill board — never the bot's post.
"""
import asyncio
from datetime import datetime
from types import SimpleNamespace

import pytest

import discord

from tests.starboard_test_utils import FakeUserDb, GUILD_A, GUILD_B, STAR
from tle.cogs.starboard import Starboard

PILL = '\N{PILL}'

STAR_CHANNEL = 888
PILL_CHANNEL = 777
SOURCE_CHANNEL = 999
ORIGINAL_MSG = 5001
SB_POST_MSG = 7777
AUTHOR_ID = 42


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


class _FakeReaction:
    def __init__(self, emoji, count):
        self.emoji = emoji
        self.count = count

    def __str__(self):
        return self.emoji

    async def users(self):
        for i in range(self.count):
            yield SimpleNamespace(id=f'u{i + 1}')


class _FakeMessage:
    def __init__(self, msg_id, reactions):
        self.id = msg_id
        self.content = 'hi'
        self.embeds = []
        self.attachments = []
        self.created_at = datetime(2025, 1, 1)
        self.jump_url = f'https://discord.com/channels/{GUILD_A}/{SOURCE_CHANNEL}/{msg_id}'
        self.author = SimpleNamespace(id=AUTHOR_ID)
        self.reference = None
        self.type = discord.MessageType.default
        self.reactions = reactions


class _FakeSourceChannel:
    def __init__(self, channel_id, message):
        self.id = channel_id
        self.nsfw = False
        self._message = message

    async def fetch_message(self, _id):
        return self._message


class _FakeSBChannel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.sent = []

    async def send(self, content=None, embeds=None, files=None):
        msg = SimpleNamespace(id=80000 + len(self.sent))
        self.sent.append(content)
        return msg


class _FakeGuild:
    id = GUILD_A

    def __init__(self, channels):
        self._channels = channels

    def get_channel(self, channel_id):
        return self._channels.get(channel_id)


class _FakeBot:
    def __init__(self, guild, source_channel):
        self._guild = guild
        self._source_channel = source_channel

    def get_guild(self, _id):
        return self._guild

    def get_channel(self, _id):
        return self._source_channel

    async def fetch_channel(self, _id):
        return self._source_channel


class _FakePayload:
    def __init__(self, guild_id, channel_id, message_id, user_id, emoji):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.message_id = message_id
        self.user_id = user_id
        self.emoji = emoji


def _setup_boards(db):
    db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
    db.set_starboard_channel(GUILD_A, STAR, str(STAR_CHANNEL))
    db.add_starboard_emoji(GUILD_A, PILL, 2, 0xff0000)
    db.set_starboard_channel(GUILD_A, PILL, str(PILL_CHANNEL))
    db.add_starboard_message_v1(
        ORIGINAL_MSG, SB_POST_MSG, GUILD_A, STAR,
        author_id=str(AUTHOR_ID), channel_id=str(SOURCE_CHANNEL))
    for uid in ('u1', 'u2'):
        db.add_reactor(ORIGINAL_MSG, STAR, uid)
    db.update_starboard_star_count(ORIGINAL_MSG, STAR, 2)


def _build_cog(monkeypatch, db, message):
    from tle.util import codeforces_common as cf
    monkeypatch.setattr(cf, 'user_db', db)
    source_channel = _FakeSourceChannel(SOURCE_CHANNEL, message)
    sb_channels = {STAR_CHANNEL: _FakeSBChannel(STAR_CHANNEL),
                   PILL_CHANNEL: _FakeSBChannel(PILL_CHANNEL)}
    guild = _FakeGuild(sb_channels)
    cog = Starboard.__new__(Starboard)
    cog.bot = _FakeBot(guild, source_channel)
    cog.locks = {}
    updates = []

    async def fake_update(*args, **kwargs):
        updates.append((args, kwargs))

    async def fake_build(message, emoji, count, color):
        return (f'{emoji} {count}', [], [])

    cog._update_starboard_message = fake_update
    cog.build_starboard_message = fake_build
    return cog, updates, sb_channels


class TestProxyReactionAdd:
    def test_star_react_on_sb_post_counts_toward_original(self, db, monkeypatch):
        _setup_boards(db)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u3', STAR)

        asyncio.run(cog._handle_reaction_add(payload))

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u3']
        assert sorted(db.get_reactors(ORIGINAL_MSG, STAR)) == ['u1', 'u2'], \
            'proxy reactions must not pollute the physical reactor pool'
        assert len(updates) == 1
        assert updates[0][0][3] == 3, 'display count must include the proxy reactor'
        row = db.get_starboard_message_v1(ORIGINAL_MSG, STAR)
        assert row.star_count == 3
        assert not db.check_exists_starboard_message_v1(SB_POST_MSG, STAR), \
            'the starboard post itself must never become an original'

    def test_react_on_both_surfaces_counts_once(self, db, monkeypatch):
        _setup_boards(db)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        # u1 already reacted on the original; now also reacts on the sb post
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u1', STAR)

        asyncio.run(cog._handle_reaction_add(payload))

        assert len(updates) == 1
        assert updates[0][0][3] == 2, 'same user on both surfaces is one star'

    def test_cross_board_proxy_can_star_the_original(self, db, monkeypatch):
        """Pill reacts on the star-board post star the ORIGINAL message onto
        the pill board once the pill threshold is met."""
        _setup_boards(db)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, sb_channels = _build_cog(monkeypatch, db, message)

        asyncio.run(cog._handle_reaction_add(
            _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u5', PILL)))
        assert not db.check_exists_starboard_message_v1(ORIGINAL_MSG, PILL), \
            'below pill threshold — no post yet'

        asyncio.run(cog._handle_reaction_add(
            _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u6', PILL)))

        assert db.check_exists_starboard_message_v1(ORIGINAL_MSG, PILL)
        row = db.get_starboard_message_v1(ORIGINAL_MSG, PILL)
        assert row.star_count == 2
        assert len(sb_channels[PILL_CHANNEL].sent) == 1, \
            'the original must be posted to the pill board'
        assert not db.check_exists_starboard_message_v1(SB_POST_MSG, PILL), \
            'the bot post itself must never reach the other board'

    def test_guild_mismatch_is_ignored(self, db, monkeypatch):
        _setup_boards(db)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        # Same emoji configured in another guild pointing at the same channel id
        db.add_starboard_emoji(GUILD_B, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_B, STAR, str(STAR_CHANNEL))
        payload = _FakePayload(GUILD_B, STAR_CHANNEL, SB_POST_MSG, 'u3', STAR)

        asyncio.run(cog._handle_reaction_add(payload))

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == []
        assert updates == []


class TestProxyReactionRemove:
    def test_remove_on_sb_post_removes_only_proxy_row(self, db, monkeypatch):
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u3', SB_POST_MSG)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u3', STAR)

        asyncio.run(cog._handle_reaction_remove(payload))

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == []
        assert len(updates) == 1
        assert updates[0][0][3] == 2

    def test_remove_on_sb_post_keeps_physical_reaction(self, db, monkeypatch):
        """u1 reacted on both surfaces; un-reacting on the sb post must keep
        the original-message reaction counting."""
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 2)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u1', STAR)

        asyncio.run(cog._handle_reaction_remove(payload))

        assert 'u1' in db.get_reactors(ORIGINAL_MSG, STAR)
        assert len(updates) == 1
        assert updates[0][0][3] == 2, 'physical reaction still counts'

    def test_remove_on_original_keeps_proxy_reaction(self, db, monkeypatch):
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        message = _FakeMessage(ORIGINAL_MSG, [_FakeReaction(STAR, 1)])
        cog, updates, _ = _build_cog(monkeypatch, db, message)
        payload = _FakePayload(GUILD_A, SOURCE_CHANNEL, ORIGINAL_MSG, 'u1', STAR)

        asyncio.run(cog._handle_reaction_remove(payload))

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u1']
        assert len(updates) == 1
        assert updates[0][0][3] == 2, 'u1 proxy + u2 physical'


class TestProxyDbMethods:
    def test_merged_count_unions_and_dedupes(self, db):
        db.add_reactor(ORIGINAL_MSG, STAR, 'u1')
        db.add_reactor(ORIGINAL_MSG, STAR, 'u2')
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u2', SB_POST_MSG)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u3', SB_POST_MSG)
        assert db.get_merged_reactor_count(ORIGINAL_MSG, [STAR]) == 3
        assert db.get_merged_physical_reactor_count(ORIGINAL_MSG, [STAR]) == 2

    def test_add_proxy_reactor_is_idempotent(self, db):
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u1']

    def test_same_user_on_two_posts_counts_once(self, db):
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', 8888)
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u1']
        assert db.get_merged_reactor_count(ORIGINAL_MSG, [STAR]) == 1

    def test_remove_proxy_reactor_rowcount(self, db):
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        assert db.remove_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG) == 1
        assert db.remove_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG) == 0

    def test_remove_proxy_reactor_is_surface_scoped(self, db):
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', 8888)
        db.remove_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u1'], \
            'the reaction on the other post must keep counting'

    def test_remove_starboard_message_cascades_proxy_rows(self, db):
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u3', SB_POST_MSG)
        db.remove_starboard_message(original_msg_id=ORIGINAL_MSG, emoji=STAR)
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == []

    def test_remove_by_starboard_id_cascades_proxy_rows(self, db):
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u3', SB_POST_MSG)
        db.remove_starboard_message(starboard_msg_id=SB_POST_MSG)
        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == []

    def test_post_deletion_spares_rows_on_other_posts(self, db):
        """M is on both boards; a star react placed on the PILL post must
        survive deletion of the STAR post (its surface still exists)."""
        _setup_boards(db)
        PILL_POST = 7778
        db.add_starboard_message_v1(
            ORIGINAL_MSG, PILL_POST, GUILD_A, PILL,
            author_id=str(AUTHOR_ID), channel_id=str(SOURCE_CHANNEL))
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u4', PILL_POST)
        db.add_proxy_reactor(ORIGINAL_MSG, PILL, 'u5', SB_POST_MSG)

        db.remove_starboard_message(starboard_msg_id=SB_POST_MSG)

        assert db.get_proxy_reactors(ORIGINAL_MSG, STAR) == ['u4'], \
            'reaction physically on the surviving pill post keeps counting'
        assert db.get_proxy_reactors(ORIGINAL_MSG, PILL) == [], \
            'reaction physically on the deleted star post dies with it'

    def test_star_givers_includes_proxy_reactors_once(self, db):
        _setup_boards(db)
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u9', SB_POST_MSG)  # proxy-only
        db.add_proxy_reactor(ORIGINAL_MSG, STAR, 'u1', SB_POST_MSG)  # also physical
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR)
        counts = {r.user_id: r.stars_given for r in rows}
        assert counts['u9'] == 1
        assert counts['u1'] == 1, 'both surfaces on one message count once'

    def test_alias_removal_migrates_proxy_rows(self, db):
        _setup_boards(db)
        GLOW = '\N{GLOWING STAR}'
        db.add_starboard_alias(GUILD_A, GLOW, STAR)
        db.add_proxy_reactor(ORIGINAL_MSG, GLOW, 'u7', SB_POST_MSG)
        db.remove_starboard_alias(GUILD_A, GLOW)
        assert 'u7' in db.get_proxy_reactors(ORIGINAL_MSG, STAR)
        assert db.get_proxy_reactors(ORIGINAL_MSG, GLOW) == []

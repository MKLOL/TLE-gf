"""Tests for excluding starboard surfaces from being starred.

A reaction on the bot's own starboard post, or on any message inside a
configured starboard channel, must never create or advance a starboard
entry for that message — otherwise reacting on a star-board post with the
pill emoji puts the *bot's post* onto the pill board.
"""
import asyncio

import pytest

from tests.starboard_test_utils import FakeUserDb, GUILD_A, GUILD_B, STAR

PILL = '\N{PILL}'

STAR_CHANNEL = 888
PILL_CHANNEL = 777
SOURCE_CHANNEL = 999
ORIGINAL_MSG = 5001
SB_POST_MSG = 7777


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


class _FakePayload:
    def __init__(self, guild_id, channel_id, message_id, user_id, emoji=STAR):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.message_id = message_id
        self.user_id = user_id
        self.emoji = emoji


class _FakeChannel:
    nsfw = False


class _FakeBot:
    def get_channel(self, _id):
        return _FakeChannel()


def _build_cog(monkeypatch, db):
    from tle.util import codeforces_common as cf
    from tle.cogs.starboard import Starboard
    monkeypatch.setattr(cf, 'user_db', db)
    cog = Starboard.__new__(Starboard)
    cog.bot = _FakeBot()
    cog.locks = {}
    calls = []

    async def fake_check_and_add(*args, **kwargs):
        calls.append((args, kwargs))

    cog.check_and_add_to_starboard = fake_check_and_add
    return cog, calls


def _setup_boards(db):
    db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
    db.set_starboard_channel(GUILD_A, STAR, str(STAR_CHANNEL))
    db.add_starboard_emoji(GUILD_A, PILL, 3, 0xff0000)
    db.set_starboard_channel(GUILD_A, PILL, str(PILL_CHANNEL))
    db.add_starboard_message_v1(
        ORIGINAL_MSG, SB_POST_MSG, GUILD_A, STAR,
        author_id='42', channel_id=str(SOURCE_CHANNEL))


class TestReactionAddExclusion:
    def test_reaction_on_starboard_post_targets_the_original(self, db, monkeypatch):
        """A starboard post is never starred itself — the reaction is
        forwarded to the original message (proxy path)."""
        _setup_boards(db)
        cog, calls = _build_cog(monkeypatch, db)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u1', STAR)
        asyncio.run(cog._handle_reaction_add(payload))
        assert len(calls) == 1
        args, kwargs = calls[0]
        assert args[4].message_id == ORIGINAL_MSG, \
            'the engine must run for the original, never the bot post'
        assert kwargs['record_reactor'] is False

    def test_cross_board_reaction_on_starboard_post_targets_the_original(
            self, db, monkeypatch):
        """The exact abuse: pill react on a star-board post must not move the
        bot's post onto the pill board — it counts for the original instead."""
        _setup_boards(db)
        cog, calls = _build_cog(monkeypatch, db)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, SB_POST_MSG, 'u1', PILL)
        asyncio.run(cog._handle_reaction_add(payload))
        assert len(calls) == 1
        args, _ = calls[0]
        assert args[0] == PILL_CHANNEL, 'a pill react targets the pill board'
        assert args[4].message_id == ORIGINAL_MSG
        assert not db.check_exists_starboard_message_v1(SB_POST_MSG, PILL)

    def test_reaction_in_starboard_channel_is_ignored(self, db, monkeypatch):
        """Even an untracked message is excluded when it lives in a board
        channel (covers posts whose DB row was lost or predates tracking)."""
        _setup_boards(db)
        cog, calls = _build_cog(monkeypatch, db)
        payload = _FakePayload(GUILD_A, STAR_CHANNEL, 6001, 'u1', STAR)
        asyncio.run(cog._handle_reaction_add(payload))
        assert calls == []

    def test_reaction_on_normal_message_still_processed(self, db, monkeypatch):
        _setup_boards(db)
        cog, calls = _build_cog(monkeypatch, db)
        payload = _FakePayload(GUILD_A, SOURCE_CHANNEL, ORIGINAL_MSG, 'u1', STAR)
        asyncio.run(cog._handle_reaction_add(payload))
        assert len(calls) == 1, 'ordinary messages must still reach the engine'


class TestExclusionDbMethods:
    def test_lookup_by_starboard_id(self, db):
        _setup_boards(db)
        row = db.get_starboard_message_by_starboard_id(SB_POST_MSG)
        assert row is not None
        assert row.original_msg_id == str(ORIGINAL_MSG)
        assert row.emoji == STAR

    def test_lookup_by_starboard_id_misses_original_ids(self, db):
        _setup_boards(db)
        assert db.get_starboard_message_by_starboard_id(ORIGINAL_MSG) is None

    def test_is_starboard_channel(self, db):
        _setup_boards(db)
        assert db.is_starboard_channel(GUILD_A, STAR_CHANNEL)
        assert db.is_starboard_channel(GUILD_A, PILL_CHANNEL)
        assert not db.is_starboard_channel(GUILD_A, SOURCE_CHANNEL)

    def test_is_starboard_channel_is_guild_scoped(self, db):
        _setup_boards(db)
        assert not db.is_starboard_channel(GUILD_B, STAR_CHANNEL)

    def test_unset_channel_does_not_match(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        assert not db.is_starboard_channel(GUILD_A, STAR_CHANNEL)

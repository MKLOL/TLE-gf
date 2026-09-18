"""Tests for sticky narcissus (self-star) marks.

A self-reaction that is live at any point while the message is on the
starboard records a permanent mark — un-reacting later keeps the mark and
shows up in the leaderboard's ``unreacted`` column instead.  Re-react /
un-react cycles never add a second mark for the same message.
"""
import asyncio
from types import SimpleNamespace

import pytest

from tests.starboard_test_utils import FakeUserDb, GUILD_A, STAR, THUMBS_UP
from tle.util.db.user_db_upgrades import upgrade_1_52_0

MSG = '100'
SB_POST = '200'
AUTHOR = 'user1'


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


def _track_message(db, msg=MSG, sb=SB_POST, author=AUTHOR):
    db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
    db.add_starboard_message_v1(msg, sb, GUILD_A, STAR,
                                author_id=author, channel_id='999')


def _narcissus_row(db, user_id=AUTHOR):
    rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
    for row in rows:
        if row.user_id == user_id:
            return row
    return None


class TestStickyMarks:
    def test_mark_survives_unreact(self, db):
        """The whole point: removing the self-react keeps the count."""
        _track_message(db)
        db.add_reactor(MSG, STAR, AUTHOR)
        db.update_starboard_star_count(MSG, STAR, 1)

        db.remove_reactor(MSG, STAR, AUTHOR)
        db.update_starboard_star_count(MSG, STAR, 0)

        row = _narcissus_row(db)
        assert row is not None
        assert row.self_stars == 1
        assert row.unreacted == 1

    def test_re_react_cycle_does_not_increment(self, db):
        _track_message(db)
        for _ in range(3):
            db.add_reactor(MSG, STAR, AUTHOR)
            db.update_starboard_star_count(MSG, STAR, 1)
            db.remove_reactor(MSG, STAR, AUTHOR)
            db.update_starboard_star_count(MSG, STAR, 0)

        row = _narcissus_row(db)
        assert row.self_stars == 1, 'react/unreact cycles stay one mark'
        assert row.unreacted == 1

    def test_re_react_makes_mark_live_again(self, db):
        _track_message(db)
        db.add_reactor(MSG, STAR, AUTHOR)
        db.update_starboard_star_count(MSG, STAR, 1)
        db.remove_reactor(MSG, STAR, AUTHOR)
        db.add_reactor(MSG, STAR, AUTHOR)

        row = _narcissus_row(db)
        assert row.self_stars == 1
        assert row.unreacted == 0

    def test_proxy_self_react_records_mark(self, db):
        """Self-starring via the starboard post surface counts too."""
        _track_message(db)
        db.add_proxy_reactor(MSG, STAR, AUTHOR, SB_POST)
        db.update_starboard_star_count(MSG, STAR, 1)

        row = _narcissus_row(db)
        assert row is not None and row.self_stars == 1
        assert row.unreacted == 0

        db.remove_proxy_reactor(MSG, STAR, AUTHOR, SB_POST)
        row = _narcissus_row(db)
        assert row.self_stars == 1
        assert row.unreacted == 1

    def test_no_mark_when_react_never_live_while_tracked(self, db):
        """React added and removed before the update hook ever saw it live."""
        _track_message(db)
        db.add_reactor(MSG, STAR, AUTHOR)
        db.remove_reactor(MSG, STAR, AUTHOR)
        db.update_starboard_star_count(MSG, STAR, 0)
        assert _narcissus_row(db) is None

    def test_update_on_untracked_message_is_harmless(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_reactor('555', STAR, AUTHOR)
        db.update_starboard_star_count('555', STAR, 1)
        assert db.get_narcissus_leaderboard(GUILD_A, STAR) == []

    def test_alias_self_react_liveness(self, db):
        """A mark recorded via an alias react goes un-live only when the
        alias react is gone too."""
        _track_message(db)
        db.add_starboard_alias(GUILD_A, THUMBS_UP, STAR)
        db.add_reactor(MSG, THUMBS_UP, AUTHOR)
        db.update_starboard_star_count(MSG, STAR, 1)

        family = db.get_emoji_family(GUILD_A, STAR)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR, emoji_family=family)
        assert rows[0].unreacted == 0

        db.remove_reactor(MSG, THUMBS_UP, AUTHOR)
        rows = db.get_narcissus_leaderboard(GUILD_A, STAR, emoji_family=family)
        assert rows[0].self_stars == 1
        assert rows[0].unreacted == 1

    def test_cascades_remove_marks(self, db):
        _track_message(db)
        db.record_narcissus_mark(GUILD_A, MSG, STAR, AUTHOR)
        db.remove_starboard_message(original_msg_id=MSG, emoji=STAR)
        assert db.get_narcissus_leaderboard(GUILD_A, STAR) == []

    def test_emoji_removal_clears_marks(self, db):
        _track_message(db)
        db.record_narcissus_mark(GUILD_A, MSG, STAR, AUTHOR)
        db.remove_starboard_emoji(GUILD_A, STAR)
        assert db.get_narcissus_leaderboard(GUILD_A, STAR) == []


class TestSeedMigration:
    def test_seed_marks_live_self_reacts(self, db):
        _track_message(db)
        db.add_reactor(MSG, STAR, AUTHOR)          # live self-react
        db.add_reactor(MSG, STAR, 'user2')         # someone else
        upgrade_1_52_0(db.conn)

        row = _narcissus_row(db)
        assert row is not None and row.self_stars == 1
        assert _narcissus_row(db, 'user2') is None

    def test_seed_covers_alias_and_proxy_reacts(self, db):
        _track_message(db)
        db.add_starboard_alias(GUILD_A, THUMBS_UP, STAR)
        db.add_reactor(MSG, THUMBS_UP, AUTHOR)     # alias self-react
        db.add_starboard_message_v1('101', '201', GUILD_A, STAR,
                                    author_id='user2', channel_id='999')
        db.add_proxy_reactor('101', STAR, 'user2', '201')  # proxy self-react
        upgrade_1_52_0(db.conn)

        assert _narcissus_row(db, AUTHOR).self_stars == 1
        assert _narcissus_row(db, 'user2').self_stars == 1

    def test_seed_skips_unknown_author_and_is_idempotent(self, db):
        _track_message(db)
        db.add_starboard_message_v1('102', '202', GUILD_A, STAR,
                                    author_id='__UNKNOWN__')
        db.add_reactor('102', STAR, '__UNKNOWN__')
        db.add_reactor(MSG, STAR, AUTHOR)
        upgrade_1_52_0(db.conn)
        upgrade_1_52_0(db.conn)

        rows = db.get_narcissus_leaderboard(GUILD_A, STAR)
        assert [(r.user_id, r.self_stars) for r in rows] == [(AUTHOR, 1)]


class TestUnreactedDisplay:
    def test_row_suffix(self):
        from tle.cogs.starboard import Starboard
        assert Starboard._row_suffix(
            SimpleNamespace(self_stars=3, unreacted=2)) == ' (2 unreacted)'
        assert Starboard._row_suffix(
            SimpleNamespace(self_stars=3, unreacted=0)) == ''
        assert Starboard._row_suffix(
            SimpleNamespace(message_count=5)) == ''

    def test_personal_rank_line_includes_suffix(self):
        from tle.cogs.starboard import Starboard
        cog = Starboard.__new__(Starboard)
        ctx = SimpleNamespace(author=SimpleNamespace(id=7))
        ranked = [(1, SimpleNamespace(user_id='7', self_stars=3, unreacted=1))]
        line = cog._get_personal_rank_line(ctx, ranked, 'self-stars')
        assert '**3** self-stars (1 unreacted)' in line


class TestLiveReactionRecordsMark:
    """End-to-end: the author self-reacting on their own starboard post
    (proxy surface) records a sticky mark through the reaction handler."""

    def test_author_proxy_self_react_records_mark(self, db, monkeypatch):
        from tle.util import codeforces_common as cf
        from tle.cogs.starboard import Starboard
        monkeypatch.setattr(cf, 'user_db', db)
        _track_message(db)
        db.set_starboard_channel(GUILD_A, STAR, '888')
        for uid in ('u1', 'u2'):
            db.add_reactor(MSG, STAR, uid)

        message = SimpleNamespace(
            id=int(MSG), content='hi', embeds=[], attachments=[],
            author=SimpleNamespace(id=AUTHOR), reference=None,
            reactions=[], type=__import__('discord').MessageType.default)
        source_channel = SimpleNamespace(
            id=999, nsfw=False,
            fetch_message=None)

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

        async def fake_update(*args, **kwargs):
            pass
        cog._update_starboard_message = fake_update

        payload = SimpleNamespace(
            guild_id=GUILD_A, channel_id=888, message_id=int(SB_POST),
            user_id=AUTHOR, emoji=STAR)
        asyncio.run(cog._handle_reaction_add(payload))

        row = _narcissus_row(db)
        assert row is not None
        assert row.self_stars == 1

"""Akari exclude-filter / multi-member / register-target tests."""
import asyncio
import datetime as dt
import json
import sqlite3
import time
from collections import namedtuple
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.db.user_db_upgrades import upgrade_1_14_0, upgrade_1_15_0
from tle.util.db.minigame_db import (
    MinigameDbMixin, merged_minigame_winners, diff_merged_winners,
)
from tle.cogs._minigame_common import (
    compute_vs,
    compute_streak,
    compute_longest_streak,
    compute_top,
    parse_date_args,
    resolve_scoring,
    strip_codeblock,
)
from tle.cogs._minigame_akari import AKARI_GAME, parse_akari_message, puzzle_date_for
from tle.cogs._minigame_guessgame import (
    GUESSGAME_GAME,
    parse_guessgame_message,
    guessgame_score_matchup,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    normalize_queens_name,
    parse_queens_leaderboard,
    parse_queens_message,
    rank_queens_participants,
)
from tle.cogs.minigames import Minigames
from tle.cogs.minigames import (
    MinigameCogError,
    _SlashCtx,
    _akari_puzzle_table_rows,
    _akari_rating_table_rows,
    _format_akari_puzzle_table,
    _get_akari_puzzle_table_image_file,
    _get_akari_puzzle_table_image,
    _maybe_parse_puzzle_selector,
)
from tle.util.minigame_rating import RatingState

from tests.minigames_test_utils import (
    _GAME,
    _queens_number,
    _row,
    db,
    FakeMinigameDb,
    _FakeGuild,
    _FakeChannel,
    _FakeAttachment,
    _FakeAuthor,
    _FakeDiscordMember,
    _FakeMessage,
    _FakeMember,
    _FakeFollowup,
    _FakeResponse,
    _FakeInteraction,
    _FakeGroup,
    _QueensCommandsBase,
)


class TestAkariExcludeFilter:
    """`+exclude=user1,user2,...` reshapes ratings without disturbing the cache."""

    @staticmethod
    def _enable(db, guild=1, channel=10):
        db.set_guild_config(guild, 'akari', '1')
        db.set_minigame_channel(guild, _GAME, channel)

    @staticmethod
    def _akari_msg_n(msg_id, user_id, puzzle, body, guild=1, channel=10):
        puzzle_date = puzzle_date_for(puzzle).isoformat()
        return _FakeMessage(msg_id, guild, channel, user_id,
                            f'Daily Akari {puzzle}\n✅{puzzle_date}✅\n{body}\n'
                            f'https://dailyakari.com/')

    @staticmethod
    def _no_puzzle_filter(monkeypatch):
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: 10 ** 9)

    def test_extract_filters_parses_decay_and_exclude(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+decay', '+exclude=alice,cara', 'remaining'])
        (remaining, include_decay, excluded, included, _inactive,
         _test) = asyncio.run(_go())
        assert include_decay is True
        assert excluded == {'101', '303'}
        assert included == set()
        assert remaining == ['remaining']

    def test_extract_filters_ignores_empty_exclude_entries(self):
        # `+exclude=alice,,,bob` should split cleanly without resolving an
        # empty member name.
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        guild = _FakeGuild(1, members=[alice, bob])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+exclude=alice,,bob,'])
        (_remaining, _include_decay, excluded, _included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == {'101', '202'}

    def test_extract_filters_parses_include(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+include=alice,bob'])
        (_remaining, _include_decay, excluded, included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == set()
        assert included == {'101', '202'}

    def test_extract_filters_include_and_exclude_compose(self):
        # Include narrows the universe; exclude trims from there.
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        guild = _FakeGuild(1, members=[alice, bob, cara])
        ctx = SimpleNamespace(
            guild=guild,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )
        async def _go():
            return await cog._extract_akari_filters(
                ctx, ['+include=alice,bob,cara', '+exclude=cara'])
        (_remaining, _include_decay, excluded, included,
         _inactive, _test) = asyncio.run(_go())
        assert excluded == {'303'}
        assert included == {'101', '202', '303'}

    def test_filtered_rating_rows_keeps_only_included_users(self, db, monkeypatch):
        # The mirror of the exclude case: only the listed users count, every
        # other row is dropped before the replay.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        rows = cog._akari_filtered_rating_rows(
            1, included_ids={'100', '300'})
        assert {r.user_id for r in rows} == {'100', '300'}

    def test_include_and_exclude_compose_in_replay(self):
        # Plain row-filter behaviour: include narrows, exclude trims.
        Row = namedtuple('Row', 'user_id puzzle_number')
        rows = [Row(str(u), 1) for u in (100, 200, 300, 400)]
        filtered = Minigames._filter_akari_rows(
            rows, included_ids={'100', '200', '300'}, excluded_ids={'200'})
        assert {r.user_id for r in filtered} == {'100', '300'}

    def test_filter_row_helper_is_pass_through_with_no_filters(self):
        # Cheap sanity check: when both filter sets are empty, the helper is
        # a no-op (and notably doesn't copy the list either).
        Row = namedtuple('Row', 'user_id puzzle_number')
        rows = [Row('100', 1), Row('200', 1)]
        assert Minigames._filter_akari_rows(rows) is rows

    def test_filtered_rating_rows_drops_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        assert {r.user_id for r in db.get_akari_ratings(1)} == {'100', '200', '300'}
        rows = cog._akari_filtered_rating_rows(1, excluded_ids={'200'})
        assert {r.user_id for r in rows} == {'100', '300'}

    def test_filtered_rating_rows_does_not_touch_cache(self, db, monkeypatch):
        # The whole point of the ``+exclude`` design: the persisted snapshot
        # stays canonical so subsequent un-filtered queries are still fast and
        # consistent.  This pins that invariant.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        before = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        cog._akari_filtered_rating_rows(1, excluded_ids={'200'})
        after = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        assert before == after

    def test_exclude_changes_remaining_players_rating(self, db, monkeypatch):
        # Excluding a player shrinks the contest field; the surviving players'
        # CF deltas change accordingly.  Without this, the feature would be a
        # display-only hide — but it really replays the math.
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f31f Perfect! \U0001f553 2:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 50% \U0001f553 5:00'))
        asyncio.run(_inner())
        baseline = {r.user_id: r.rating for r in db.get_akari_ratings(1)}
        filtered = {r.user_id: r.rating
                    for r in cog._akari_filtered_rating_rows(1, excluded_ids={'200'})}
        assert '200' not in filtered
        # 100 and 300 are both still in, but their ratings differ from the
        # 3-player snapshot because the contest math is now binary.
        assert filtered['100'] != baseline['100']
        assert filtered['300'] != baseline['300']

    def test_akari_user_data_replays_without_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
        asyncio.run(_inner())
        # Without exclude: a 2-player contest, so the played day has a
        # ``performance`` (the field exists).
        _state, history = cog._akari_user_data(1, 100)
        assert len(history) == 1
        assert history[0].performance is not None
        # Excluding 200 leaves 100 alone on the day → solo, no performance.
        _state, history = cog._akari_user_data(1, 100, excluded_ids={'200'})
        assert len(history) == 1
        assert history[0].performance is None

    def test_akari_puzzle_change_info_omits_excluded_users(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._no_puzzle_filter(monkeypatch)
        self._enable(db)
        cog = Minigames(bot=None)

        async def _inner():
            await cog.on_message(self._akari_msg_n(
                1, 100, 500, '\U0001f31f Perfect! \U0001f553 1:00'))
            await cog.on_message(self._akari_msg_n(
                2, 200, 500, '\U0001f3af 50% \U0001f553 5:00'))
            await cog.on_message(self._akari_msg_n(
                3, 300, 500, '\U0001f3af 70% \U0001f553 3:00'))
        asyncio.run(_inner())
        full = cog._akari_puzzle_change_info(1, 500)
        assert set(full) == {'100', '200', '300'}
        partial = cog._akari_puzzle_change_info(1, 500, excluded_ids={'200'})
        assert set(partial) == {'100', '300'}


class TestAkariMultiMember:
    """``;mg akari rating @a @b ...`` and ``performance @a @b ...`` plot many."""

    def _ctx(self, members):
        guild = _FakeGuild(1, members=members)
        return SimpleNamespace(
            guild=guild,
            author=members[0] if members else None,
            bot=SimpleNamespace(get_guild=lambda gid: guild),
        )

    def test_parse_returns_list_of_resolved_members(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        ctx = self._ctx([alice, bob, cara])
        (members, include_decay, excluded, included,
         _inactive, _test) = asyncio.run(
            cog._parse_akari_rating_args(ctx, ['alice', 'bob']))
        assert [m.id for m in members] == [101, 202]
        assert include_decay is False
        assert excluded == set()
        assert included == set()

    def test_parse_with_decay_and_exclude_alongside_members(self):
        cog = Minigames(bot=None)
        alice = _FakeDiscordMember(101, 'alice')
        bob = _FakeDiscordMember(202, 'bob')
        cara = _FakeDiscordMember(303, 'cara')
        ctx = self._ctx([alice, bob, cara])
        (members, include_decay, excluded, included,
         _inactive, _test) = asyncio.run(
            cog._parse_akari_rating_args(
                ctx, ['alice', '+decay', 'bob', '+exclude=cara',
                      '+include=alice,bob,cara']))
        assert [m.id for m in members] == [101, 202]
        assert include_decay is True
        assert excluded == {'303'}
        assert included == {'101', '202', '303'}

    def test_parse_falls_back_to_ctx_author_when_no_member(self):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(999, 'author')
        ctx = self._ctx([author])
        members, _decay, _excl, _incl, _inactive, _test = asyncio.run(
            cog._parse_akari_rating_args(ctx, []))
        assert members == [author]

    def test_parse_member_required_errors_when_empty(self):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(999, 'author')
        ctx = self._ctx([author])
        from tle.cogs.minigames import MinigameCogError
        with pytest.raises(MinigameCogError):
            asyncio.run(cog._parse_akari_rating_args(
                ctx, [], member_required=True))


class TestRegisterTarget:
    """`;mg akari register [@user]` — anyone can self-register; only mods can
    pass a different @user."""

    @staticmethod
    def _ctx(author_id, author_roles):
        roles = [SimpleNamespace(name=r) for r in author_roles]
        return SimpleNamespace(
            author=SimpleNamespace(id=author_id, roles=roles))

    def test_non_mod_can_self_register_without_arg(self):
        ctx = self._ctx(999, author_roles=[])
        target = Minigames._resolve_registrar_target(ctx, member=None)
        assert target is ctx.author

    def test_non_mod_can_pass_self_explicitly(self):
        ctx = self._ctx(999, author_roles=[])
        target = Minigames._resolve_registrar_target(
            ctx, member=SimpleNamespace(id=999))
        # When the explicit member matches the author, we collapse to the
        # author (so message logic sees a "self" registration).
        assert target.id == ctx.author.id

    def test_non_mod_blocked_from_registering_other(self):
        ctx = self._ctx(999, author_roles=['Member'])
        with pytest.raises(Exception, match='Only.*can register'):
            Minigames._resolve_registrar_target(
                ctx, member=SimpleNamespace(id=888))

    def test_admin_can_register_other(self):
        ctx = self._ctx(999, author_roles=['Admin'])
        other = SimpleNamespace(id=888)
        assert Minigames._resolve_registrar_target(ctx, member=other) is other

    def test_moderator_can_register_other(self):
        ctx = self._ctx(999, author_roles=['Moderator'])
        other = SimpleNamespace(id=888)
        assert Minigames._resolve_registrar_target(ctx, member=other) is other

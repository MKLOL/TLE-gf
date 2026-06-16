"""Formatting, slash-context, member converter, mg backcompat, ranking-filter tests."""
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
    _GAME, _queens_number, _row, db, FakeMinigameDb,
    _FakeGuild, _FakeChannel, _FakeAttachment, _FakeAuthor, _FakeDiscordMember,
    _FakeMessage, _FakeMember, _FakeFollowup, _FakeResponse, _FakeInteraction,
    _FakeGroup, _QueensCommandsBase,
)


class TestFormatting:
    def test_format_akari_puzzle_table_orders_best_results_first(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: {
                '10': 'alice_cf',
                '20': 'bob_cf',
                '30': 'cara_cf',
            }.get(str(user_id))
        ))
        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'alice', 'Alice'),
            _FakeDiscordMember(20, 'bob', 'Bob'),
            _FakeDiscordMember(30, 'cara', 'Cara'),
        ])
        table_str = _format_akari_puzzle_table(guild, [
            _row(3, 30, '2026-03-26', False, 50, 97, 445),
            _row(2, 20, '2026-03-26', True, 80, 100, 445),
            _row(1, 10, '2026-03-26', True, 60, 100, 445),
        ])

        assert '#  Name' in table_str
        assert 'Handle' in table_str
        assert '1  Alice  alice_cf  100%    1:00' in table_str
        assert '2  Bob    bob_cf    100%    1:20' in table_str
        assert '3  Cara   cara_cf   97%     0:50' in table_str

    def test_get_akari_puzzle_table_image_returns_png_file(self, monkeypatch):
        class _Surface:
            def __init__(self, *args):
                pass

            def write_to_png(self, fp):
                fp.write(b'png-data')

        class _Context:
            def __init__(self, *args):
                pass

            def set_source_rgb(self, *args):
                pass

            def rectangle(self, *args):
                pass

            def fill(self):
                pass

            def move_to(self, *args):
                pass

            def rel_move_to(self, *args):
                pass

        class _Layout:
            def set_font_description(self, *args):
                pass

            def set_ellipsize(self, *args):
                pass

            def set_width(self, *args):
                pass

            def set_alignment(self, *args):
                pass

            def set_markup(self, *args):
                pass

        monkeypatch.setattr(minigames_module.cairo, 'FORMAT_ARGB32', 0, raising=False)
        monkeypatch.setattr(minigames_module.cairo, 'ImageSurface', _Surface, raising=False)
        monkeypatch.setattr(minigames_module.cairo, 'Context', _Context, raising=False)
        monkeypatch.setattr(minigames_module.Pango, 'SCALE', 1000, raising=False)
        monkeypatch.setattr(
            minigames_module.Pango, 'Alignment',
            SimpleNamespace(LEFT=0, RIGHT=1), raising=False)
        monkeypatch.setattr(
            minigames_module.PangoCairo, 'create_layout',
            lambda _context: _Layout(), raising=False)
        monkeypatch.setattr(
            minigames_module.PangoCairo, 'show_layout',
            lambda *_args, **_kw: None, raising=False)
        monkeypatch.setattr(
            minigames_module.discord, 'File',
            lambda fp, filename: SimpleNamespace(fp=fp, filename=filename), raising=False)
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: {
                '10': 'alice_cf',
                '20': 'emoji_cf',
            }.get(str(user_id))
        ))

        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'alice', 'Alice' * 200),
            _FakeDiscordMember(20, 'emoji', '🧶'),
        ])
        rows = [
            _row(1, 10, '2026-03-26', True, 60, 100, 445),
            _row(2, 20, '2026-03-26', False, 80, 98, 445),
        ]

        discord_file = _get_akari_puzzle_table_image(
            _akari_puzzle_table_rows(guild, rows))

        assert discord_file.filename == 'akari-results.png'
        assert discord_file.fp.getbuffer().nbytes > 0

    def test_akari_puzzle_table_image_file_is_bounded(self, monkeypatch):
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image',
            lambda rows, *, title=None, footer=None, **_: SimpleNamespace(
                rows=rows, title=title, footer=footer, filename='akari-results.png'))
        handle_lookups = []
        monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
            get_handle=lambda user_id, guild_id: handle_lookups.append(user_id) or f'h{user_id}'
        ))

        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(user_id, f'user{user_id}', f'Player {user_id:03d}')
            for user_id in range(1, 46)
        ])
        rows = [
            _row(user_id, user_id, '2026-03-26', False, user_id, 90, 445)
            for user_id in range(1, 46)
        ]

        discord_file = _get_akari_puzzle_table_image_file(
            guild, rows, 'Akari Results')

        assert len(discord_file.rows) == 40
        assert discord_file.filename == 'akari-results.png'
        assert discord_file.title == 'Akari Results'
        assert discord_file.footer == 'Showing top 40 of 45 results'
        assert handle_lookups == [str(user_id) for user_id in range(1, 41)]

    def test_akari_puzzle_selector_sends_image_file_without_embed(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        db.save_minigame_result(
            1, 1, 'akari', 10, 20, 445, '2026-03-26', 100, 60, True, 'raw')

        image_file = SimpleNamespace(filename='akari-results.png')
        monkeypatch.setattr(
            minigames_module, '_get_akari_puzzle_table_image_file',
            lambda guild, rows, title, **_: image_file)

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=_FakeGuild(1, members=[_FakeDiscordMember(20, 'alice', 'Alice')]),
            author=_FakeDiscordMember(20, 'alice', 'Alice'),
            send=send,
        )

        asyncio.run(Minigames(bot=None)._cmd_stats(ctx, AKARI_GAME, '445'))

        assert sent['content'] is None
        assert sent['embed'] is None
        assert sent['kwargs']['file'] is image_file


def test_slash_context_forwards_file_kwarg():
    captured = {}

    class _Followup:
        async def send(self, content=None, *, embed=None, wait=False, **kw):
            captured['content'] = content
            captured['embed'] = embed
            captured['wait'] = wait
            captured['kw'] = kw

    interaction = SimpleNamespace(
        id=999,
        guild=object(),
        user=object(),
        channel_id=123,
        client=object(),
        followup=_Followup(),
    )
    ctx = _SlashCtx(interaction)

    asyncio.run(ctx.send(embed='embed', file='file'))

    assert captured['embed'] == 'embed'
    assert captured['wait'] is True
    assert captured['kw']['file'] == 'file'


class TestCaseInsensitiveMember:
    """CaseInsensitiveMember falls back to case-insensitive name matching."""

    def _make_ctx(self, members):
        class Ctx:
            pass
        ctx = Ctx()
        guild = _FakeGuild(1)
        guild.members = members
        ctx.guild = guild
        ctx.bot = type('Bot', (), {'get_guild': lambda self, gid: guild})()
        return ctx

    def test_exact_case_matches(self):
        m = _FakeMember('Alice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'Alice'))
        assert result is m

    def test_different_case_matches(self):
        m = _FakeMember('Alice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'alice'))
        assert result is m

    def test_display_name_case_insensitive(self):
        m = _FakeMember('alice123', display_name='BigAlice')
        ctx = self._make_ctx([m])
        from tle.cogs.minigames import CaseInsensitiveMember
        result = asyncio.run(CaseInsensitiveMember().convert(ctx, 'bigalice'))
        assert result is m

    def test_no_match_raises(self):
        ctx = self._make_ctx([_FakeMember('Bob')])
        from tle.cogs.minigames import CaseInsensitiveMember
        with pytest.raises(Exception):
            asyncio.run(CaseInsensitiveMember().convert(ctx, 'alice'))


# ── Slash command adapter tests ────────────────────────────────────────

class TestSlashCtx:
    """_SlashCtx adapter maps Interaction to a ctx-like object."""

    def test_maps_guild_author_channel(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction(guild_id=42, user_id=7, channel_id=99)
        ctx = _SlashCtx(inter)
        assert ctx.guild.id == 42
        assert ctx.author.id == 7
        assert ctx.channel.id == 99
        assert ctx.channel.mention == '<#99>'

    def test_send_uses_followup(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction()
        ctx = _SlashCtx(inter)
        asyncio.run(ctx.send('hello', embed='test_embed'))
        assert len(inter.followup.sent) == 1
        assert inter.followup.sent[0]['content'] == 'hello'
        assert inter.followup.sent[0]['embed'] == 'test_embed'

    def test_channel_send_uses_followup(self):
        from tle.cogs.minigames import _FollowupChannel
        inter = _FakeInteraction()
        ch = _FollowupChannel(inter)
        asyncio.run(ch.send('msg', embed='e', view='v'))
        assert len(inter.followup.sent) == 1
        assert inter.followup.sent[0]['embed'] == 'e'
        assert inter.followup.sent[0]['view'] == 'v'

    def test_author_override_for_streak(self):
        from tle.cogs.minigames import _SlashCtx
        inter = _FakeInteraction(user_id=10)
        ctx = _SlashCtx(inter)
        assert ctx.author.id == 10
        other = _FakeAuthor(20)
        ctx.author = other
        assert ctx.author.id == 20

    def test_channel_send_returns_message(self):
        from tle.cogs.minigames import _FollowupChannel
        inter = _FakeInteraction()
        ch = _FollowupChannel(inter)
        msg = asyncio.run(ch.send('hi'))
        assert hasattr(msg, 'id')


class TestAkariMgBackcompat:
    """`;akari …` is the canonical group; `;mg akari …` keeps working via
    a same-object mirror that cog_load installs on the ;mg group."""

    def _run_cog_load(self, cog, mg, akari):
        """Substitute fake groups onto the cog and run cog_load."""
        # The real cog has .minigames / .akari attributes that resolve to
        # the decorator-built groups; the stubbed harness leaves those as
        # opaque objects.  Patch them with our fakes so cog_load's logic
        # actually runs end-to-end.
        cog.minigames = mg
        cog.akari = akari
        asyncio.run(cog.cog_load())

    def test_mg_resolves_akari_to_same_object(self):
        cog = Minigames(bot=None)
        mg = _FakeGroup(name='minigames', aliases=['mg'])
        akari = _FakeGroup(name='akari', aliases=['dailyakari'])
        self._run_cog_load(cog, mg, akari)
        # ;mg akari and ;mg dailyakari both point at the canonical akari group
        assert mg.get_command('akari') is akari
        assert mg.get_command('dailyakari') is akari

    def test_existing_mg_subcommand_not_clobbered(self):
        """If ;mg.akari somehow already exists, leave it alone."""
        cog = Minigames(bot=None)
        mg = _FakeGroup(name='minigames', aliases=['mg'])
        akari = _FakeGroup(name='akari', aliases=['dailyakari'])
        original = _FakeGroup(name='akari')
        mg.add(original)
        self._run_cog_load(cog, mg, akari)
        assert mg.get_command('akari') is original

    def test_cog_load_no_crash_with_stubbed_group(self):
        """The conftest stub leaves the cog's groups without all_commands;
        cog_load must silently no-op rather than crash."""
        cog = Minigames(bot=None)
        asyncio.run(cog.cog_load())  # must not raise


class TestActiveRankingRowsInactiveFlag:
    """`include_inactive=True` should drop the day-cutoff but keep the
    garbage-future-puzzle filter."""

    def _rows(self):
        from tle.cogs._minigame_akari import expected_puzzle_number
        current = expected_puzzle_number(dt.date.today())
        return [
            SimpleNamespace(user_id='today', last_puzzle=current),
            SimpleNamespace(user_id='week', last_puzzle=current - 7),
            SimpleNamespace(user_id='month', last_puzzle=current - 40),       # >30d
            SimpleNamespace(user_id='year', last_puzzle=current - 400),       # >>30d
            SimpleNamespace(user_id='troll', last_puzzle=9223372036854775806),  # garbage
        ]

    def test_default_hides_inactive_and_garbage(self):
        kept = {r.user_id for r in Minigames._active_ranking_rows(self._rows())}
        assert kept == {'today', 'week'}

    def test_include_inactive_keeps_dormant_but_drops_garbage(self):
        kept = {
            r.user_id for r in
            Minigames._active_ranking_rows(self._rows(), include_inactive=True)
        }
        assert kept == {'today', 'week', 'month', 'year'}
        assert 'troll' not in kept


class TestExtractAkariFiltersInactive:
    """`+inactive` should land as a 5th return value, default False."""

    def _ctx_stub(self):
        # _extract_akari_filters only touches ctx for +include / +exclude.
        return SimpleNamespace()

    def _run(self, args):
        cog = Minigames(bot=None)
        return asyncio.run(cog._extract_akari_filters(self._ctx_stub(), args))

    def test_default_false(self):
        (remaining, include_decay, ex, inc, include_inactive,
         test_decay) = self._run(())
        assert include_inactive is False
        assert remaining == []
        assert include_decay is False
        assert ex == set()
        assert inc == set()
        assert test_decay is False

    def test_flag_sets_true(self):
        (remaining, _decay, _ex, _inc, include_inactive,
         _test) = self._run(('+inactive',))
        assert include_inactive is True

    def test_test_flag_sets_test_decay(self):
        (remaining, _decay, _ex, _inc, _inactive,
         test_decay) = self._run(('+test',))
        assert test_decay is True
        assert remaining == []
        assert remaining == []  # the flag is consumed, not passed through

    def test_flag_composes_with_decay(self):
        remaining, decay, _ex, _inc, inactive, _test = self._run(
            ('+inactive', '+decay'))
        assert decay is True
        assert inactive is True
        assert remaining == []

    def test_unknown_flag_passes_through(self):
        (remaining, _decay, _ex, _inc, _inactive,
         _test) = self._run(('+inactive', 'foo'))
        assert remaining == ['foo']

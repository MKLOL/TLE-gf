"""Queens commands: register/set/update flows."""
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
    _FakeGroup, _QueensCommandsBase, _AkariRatingHelpers,
)


class TestQueensCommandsRegister(_QueensCommandsBase):
    def test_stats_and_streak_use_queens_dates(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        rendered = []
        fake_file = SimpleNamespace(filename='queens-stats.png')

        def fake_queens_stats(results, display_name, *, title_suffix=''):
            rendered.append({
                'dates': [
                    minigames_module.normalize_puzzle_date(row.puzzle_date)
                    .isoformat()
                    for row in results
                ],
                'display_name': display_name,
                'title_suffix': title_suffix,
            })
            return fake_file

        monkeypatch.setattr(
            minigames_module, 'plot_queens_stats', fake_queens_stats)

        self._save_queens_result(db, 1, alice.id, '2026-06-08', 5, True, 100)
        self._save_queens_result(db, 2, alice.id, '2026-06-09', 9, False, 0)
        self._save_queens_result(db, 3, alice.id, '2026-06-10', 4, True, 100)
        self._save_queens_result(db, 4, alice.id, '2026-06-11', 6, True, 100)

        asyncio.run(cog._cmd_queens_stats(ctx))
        assert ctx.sent['embed'] is None
        assert ctx.sent['kwargs']['file'] is fake_file
        assert rendered[-1] == {
            'dates': [
                '2026-06-08', '2026-06-09', '2026-06-10', '2026-06-11',
            ],
            'display_name': 'Alice',
            'title_suffix': '',
        }

        asyncio.run(cog._cmd_queens_stats(ctx, 'd>=10062026'))
        assert ctx.sent['kwargs']['file'] is fake_file
        assert rendered[-1]['dates'] == ['2026-06-10', '2026-06-11']

        asyncio.run(cog._cmd_queens_stats(ctx, '+dow=mon,wed'))
        assert ctx.sent['kwargs']['file'] is fake_file
        assert rendered[-1]['dates'] == ['2026-06-08', '2026-06-10']

        asyncio.run(cog._cmd_queens_streak(ctx))
        streak = ctx.sent['embed']
        assert '**2** consecutive clean day(s)' in streak.description
        assert 'Latest result: **2026-06-11**' in streak.description

        asyncio.run(cog._cmd_queens_streak(ctx, '+dow=wed,thu'))
        weekday_streak = ctx.sent['embed']
        assert '**2** consecutive clean day(s)' in weekday_streak.description

    def test_register_self_queues_connection_check(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        cog._set_queens_connection_account(
            100, 'Linked User', 'https://www.linkedin.com/in/linked/')

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Alice', linkedin='LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == 'Alice LinkedIn'
        instruction = cog._queens_connection_instruction(100)
        assert 'https://www.linkedin.com/in/linked/' in instruction
        assert 'Linked User' not in instruction
        assert ctx.sent['embed'] is not None

    def test_register_other_accepts_after_linkedin_match(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+username', linkedin='bob Bob LinkedIn'))

        assert db.get_minigame_player_link(100, 'queens', bob.id) is None
        pending = list(cog._queens_pending_registrations.values())
        assert pending[0].name == 'Bob LinkedIn'

        async def fake_connect(guild_id, names):
            assert str(guild_id) == '100'
            assert names == ['Bob LinkedIn']
            return {
                'status': 'ok',
                'accepted': ['Bob LinkedIn'],
                'accepted_normalized': [normalize_queens_name('Bob LinkedIn')],
            }, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert cog._queens_pending_registrations == {}

    def test_set_registers_other_without_linkedin_match_and_clears_pending(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, alice, bob])
        cog = Minigames(bot=None)

        alice_ctx = self._make_ctx(guild, alice)
        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, alice_ctx, 'Bob', linkedin='LinkedIn'))
        assert cog._queens_pending_registrations[('100', '300')].name == (
            'Bob LinkedIn')

        mod_ctx = self._make_ctx(guild, mod)
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, mod_ctx, 'bob', linkedin='Bob LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert cog._queens_pending_registrations == {}

    def test_set_accepts_anonymous_flag(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, 'bob', linkedin='Bob LinkedIn +anon'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert row.external_url == minigames_module._QUEENS_ANONYMOUS_LINK_MARKER
        assert 'Bob LinkedIn' not in ctx.sent['embed'].description

    def test_set_accepts_prefix_anonymous_flag(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='bob Bob LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', bob.id)
        assert row.external_name == 'Bob LinkedIn'
        assert row.normalized_name == normalize_queens_name('Bob LinkedIn')
        assert row.external_url == minigames_module._QUEENS_ANONYMOUS_LINK_MARKER
        assert 'Bob LinkedIn' not in ctx.sent['embed'].description

    def test_set_does_not_report_claimed_count_on_repeat(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        bob = _FakeDiscordMember(301, 'dontdefense', 'dontdefense')
        guild = _FakeGuild(100, members=[mod, bob])
        ctx = self._make_ctx(guild, mod)
        cog = Minigames(bot=None)
        for offset, seconds in enumerate((4, 5)):
            puzzle_date = dt.date(2026, 6, 8) + dt.timedelta(days=offset)
            db.save_minigame_unresolved_result(
                100, 'queens', normalize_queens_name('Dragos Ristache'),
                'Dragos Ristache', 200, _queens_number(puzzle_date),
                puzzle_date.isoformat(), 100, seconds, True, 'source')

        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='dontdefense Dragos Ristache'))
        assert 'Claimed' not in ctx.sent['embed'].description
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, ctx, '+anon', linkedin='dontdefense Dragos Ristache'))
        assert 'Claimed' not in ctx.sent['embed'].description
        assert 'Dragos Ristache' not in ctx.sent['embed'].description
        assert len(db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Dragos Ristache'))) == 2

    def test_pending_register_expires_after_linkedin_scan_without_match(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_alert',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'Alice', linkedin='LinkedIn'))
        pending = list(cog._queens_pending_registrations.values())

        async def fake_connect(_guild_id, _names):
            return {'status': 'ok', 'accepted': [], 'accepted_normalized': []}, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        assert cog._queens_pending_registrations == {}
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_plain_username_stays_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, 'bob', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == 'bob Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        assert db.get_minigame_player_link(100, 'queens', bob.id) is None

    def test_register_non_username_plus_token_stays_linkedin_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+bob', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == '+bob Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None

    def test_register_plain_mention_stays_linkedin_name(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '<@301>', linkedin='Bob LinkedIn'))

        pending = cog._queens_pending_registrations[('100', '300')]
        assert pending.name == '<@301> Bob LinkedIn'
        assert db.get_minigame_player_link(100, 'queens', alice.id) is None
        assert db.get_minigame_player_link(100, 'queens', bob.id) is None

    def test_slash_register_self_does_not_require_mod_role(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        interaction = SimpleNamespace(
            id=999,
            guild=guild,
            user=alice,
            channel_id=200,
            client=None,
            response=_FakeResponse(),
            followup=_FakeFollowup(),
        )
        cog = Minigames(bot=None)

        asyncio.run(cog.slash_queens_register(
            interaction, 'Alice LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        assert cog._queens_pending_registrations[('100', '300')].name == (
            'Alice LinkedIn')
        assert interaction.response.deferred is True
        assert interaction.followup.sent

    def test_update_sends_slow_notice_when_not_rate_limited(
            self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.kvs_get = lambda _key: None
        kvs_updates = []
        db.kvs_set = lambda key, value: kvs_updates.append((key, value))
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        async def send(content=None, *, embed=None, **kwargs):
            sent.append({'content': content, 'embed': embed, 'kwargs': kwargs})

        ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
        )
        state_path = tmp_path / 'queens_state.json'
        state_path.write_text('{}')
        cog = Minigames(bot=None)
        monkeypatch.setattr(cog, '_queens_state_path', lambda _guild_id: state_path)

        async def fake_scraper(_guild_id, *, auto_play, results_day='today'):
            assert auto_play is False
            assert results_day == 'today'
            return {'status': 'ok', 'raw_text': ''}, None

        imports = []

        async def fake_import(_ctx, payload, *, source_label, results_day='today'):
            imports.append((payload, source_label, results_day))

        monkeypatch.setattr(cog, '_run_queens_scraper', fake_scraper)
        monkeypatch.setattr(cog, '_do_queens_import', fake_import)

        asyncio.run(Minigames.queens_update.__wrapped__(cog, ctx))

        assert sent
        assert len(kvs_updates) == 1
        assert imports == [({'status': 'ok', 'raw_text': ''}, 'Update', 'today')]

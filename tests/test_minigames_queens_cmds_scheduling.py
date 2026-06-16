"""Queens commands: play/update scheduling and anonymous registration."""
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


class TestQueensCommandsScheduling(_QueensCommandsBase):
    def test_queens_play_waits_before_auto_play_and_imports(
            self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
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
        scraper_calls = []

        async def fake_scraper(
                _guild_id, *, auto_play, results_day='today',
                min_play_seconds=0):
            scraper_calls.append({
                'auto_play': auto_play,
                'results_day': results_day,
                'min_play_seconds': min_play_seconds,
            })
            return {'status': 'ok', 'raw_text': ''}, None

        imports = []

        async def fake_import(_ctx, payload, *, source_label, results_day='today'):
            imports.append((payload, source_label, results_day))

        monkeypatch.setattr(cog, '_run_queens_scraper', fake_scraper)
        monkeypatch.setattr(cog, '_do_queens_import', fake_import)

        asyncio.run(Minigames.queens_play.__wrapped__(cog, ctx))

        assert scraper_calls == [{
            'auto_play': True,
            'results_day': 'today',
            'min_play_seconds': minigames_module._QUEENS_AUTO_PLAY_MIN_SECONDS,
        }]
        assert sent
        assert imports == [({'status': 'ok', 'raw_text': ''}, 'Play', 'today')]

    def test_update_yesterday_passes_scraper_day_and_label(
            self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.kvs_get = lambda _key: None
        db.kvs_set = lambda _key, _value: None
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
            assert results_day == 'yesterday'
            return {'status': 'ok', 'raw_text': ''}, None

        imports = []

        async def fake_import(_ctx, payload, *, source_label, results_day='today'):
            imports.append((payload, source_label, results_day))

        monkeypatch.setattr(cog, '_run_queens_scraper', fake_scraper)
        monkeypatch.setattr(cog, '_do_queens_import', fake_import)

        asyncio.run(Minigames.queens_update.__wrapped__(
            cog, ctx, '+yesterday'))

        assert sent
        assert imports == [
            ({'status': 'ok', 'raw_text': ''}, 'Yesterday update', 'yesterday'),
        ]

    def test_update_rate_limit_skips_slow_notice(self, db, monkeypatch, tmp_path):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.kvs_get = lambda _key: str(time.time())
        db.kvs_set = lambda _key, _value: None
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

        with pytest.raises(MinigameCogError, match='rate-limited'):
            asyncio.run(Minigames.queens_update.__wrapped__(cog, ctx))

        assert sent == []

    def test_queens_here_sets_channel(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        channel = _FakeChannel(777)
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=_FakeGuild(100, members=[alice], channels=[channel]),
            author=alice,
            channel=channel,
            send=send,
        )
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_here.__wrapped__(cog, ctx))

        assert db.get_minigame_channel(100, 'queens') == '777'
        assert sent['embed'] is not None

    def test_daily_queens_update_runs_once_after_target_time(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_channel(100, 'queens', 777)
        guild = _FakeGuild(100, channels=[_FakeChannel(777)])
        cog = Minigames(bot=SimpleNamespace(guilds=[guild]))
        calls = []

        async def fake_send(send_guild):
            calls.append(send_guild.id)
            return True

        monkeypatch.setattr(cog, '_send_queens_daily_update', fake_send)
        now = dt.datetime(
            2026, 6, 13, 0, 0, 11,
            tzinfo=minigames_module.ZoneInfo('US/Pacific'))

        asyncio.run(cog._check_queens_daily_update_guild(
            guild, now, '2026-06-13'))
        asyncio.run(cog._check_queens_daily_update_guild(
            guild, now, '2026-06-13'))

        assert calls == [100]
        assert db.kvs_get('queens_daily_update_last:100') == '2026-06-13'

    def test_daily_queens_update_schedules_next_midnight_timer(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_channel(100, 'queens', 777)
        db.kvs_set('queens_daily_update_last:100', '2026-06-13')
        guild = _FakeGuild(100, channels=[_FakeChannel(777)])
        cog = Minigames(bot=SimpleNamespace(guilds=[guild]))
        calls = []

        async def fake_precise(send_guild, delay):
            calls.append((send_guild.id, delay))

        monkeypatch.setattr(cog, '_precise_queens_daily_update', fake_precise)
        now = dt.datetime(
            2026, 6, 13, 23, 59, 50,
            tzinfo=minigames_module.ZoneInfo('US/Pacific'))

        async def run_check():
            await cog._check_queens_daily_update_guild(
                guild, now, '2026-06-13')
            await asyncio.sleep(0)

        asyncio.run(run_check())

        assert calls == [(100, 20.0)]

    def test_daily_queens_update_plays_before_yesterday_update(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_channel(100, 'queens', 777)
        channel = _FakeChannel(777)
        guild = _FakeGuild(100, channels=[channel])

        async def fetch_channel(channel_id):
            return guild.get_channel(channel_id)

        bot = SimpleNamespace(
            guilds=[guild],
            user=SimpleNamespace(id=999),
            get_channel=lambda channel_id: guild.get_channel(channel_id),
            fetch_channel=fetch_channel,
        )
        cog = Minigames(bot=bot)
        calls = []

        async def fake_play(ctx, *, import_results=True, send_notice=True):
            calls.append((
                'play', ctx.channel.id, import_results, send_notice))
            return {'status': 'ok'}

        async def fake_update(ctx, *, results_day='today'):
            calls.append(('update', ctx.channel.id, results_day))

        monkeypatch.setattr(cog, '_cmd_queens_play', fake_play)
        monkeypatch.setattr(cog, '_cmd_queens_update', fake_update)

        result = asyncio.run(cog._send_queens_daily_update(guild))

        assert result is True
        assert calls == [
            ('play', 777, False, False),
            ('update', 777, 'yesterday'),
        ]

    def test_register_anonymous_keeps_linkedin_name_private(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+anon', linkedin='Alice LinkedIn'))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row is None
        pending = list(cog._queens_pending_registrations.values())
        assert pending[0].name == 'Alice LinkedIn'
        assert 'Anonymous' in ctx.sent['embed'].description
        assert 'Alice LinkedIn' not in ctx.sent['embed'].description

        async def fake_connect(_guild_id, _names):
            return {
                'status': 'ok',
                'accepted': ['Alice LinkedIn'],
                'accepted_normalized': [normalize_queens_name('Alice LinkedIn')],
            }, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        row = db.get_minigame_player_link(100, 'queens', alice.id)
        assert row.external_name == 'Alice LinkedIn'
        assert row.normalized_name == normalize_queens_name('Alice LinkedIn')
        assert row.external_url == (
            minigames_module._QUEENS_ANONYMOUS_LINK_MARKER)

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kwargs: pages.extend(page_list))

        asyncio.run(Minigames.queens_links.__wrapped__(cog, ctx))

        assert pages
        assert 'Alice: `Anonymous`' in pages[0][1].description
        assert 'Alice LinkedIn' not in pages[0][1].description

    def test_anonymous_modal_response_shows_private_name_without_context_object(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_neutral',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = []

        class Response:
            async def send_message(self, content=None, *, embed=None,
                                   ephemeral=False, **kwargs):
                sent.append({
                    'content': content,
                    'embed': embed,
                    'ephemeral': ephemeral,
                    'kwargs': kwargs,
                })

        interaction = SimpleNamespace(
            guild=guild,
            user=alice,
            channel_id=200,
            response=Response(),
        )
        cog = Minigames(bot=None)
        modal = minigames_module._QueensAnonymousRegisterModal(cog)
        modal.linkedin_name.value = 'Alice LinkedIn'

        asyncio.run(modal.on_submit(interaction))

        assert len(sent) == 1
        assert sent[0]['content'] is None
        assert sent[0]['ephemeral'] is True
        assert 'Alice LinkedIn' in sent[0]['embed'].description
        assert '_QueensModalCtx object' not in sent[0]['embed'].description
        assert cog._queens_pending_registrations[('100', '300')].anonymous is True

    def test_anonymous_pending_expiry_hides_linkedin_name(
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
        sent = []

        class Channel:
            async def send(self, *, embed=None, **kwargs):
                sent.append(embed)

        class Bot:
            def get_channel(self, channel_id):
                assert channel_id == 200
                return Channel()

        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=Bot())

        asyncio.run(Minigames.queens_register.__wrapped__(
            cog, ctx, '+anon', linkedin='Alice LinkedIn'))
        pending = list(cog._queens_pending_registrations.values())

        async def fake_connect(_guild_id, _names):
            return {'status': 'ok', 'accepted': [], 'accepted_normalized': []}, None

        monkeypatch.setattr(cog, '_run_queens_connect', fake_connect)
        asyncio.run(cog._process_queens_pending_registrations(100, pending))

        assert sent
        assert 'Anonymous' in sent[-1].description
        assert 'Alice LinkedIn' not in sent[-1].description

    def test_anonymous_duplicate_registration_hides_linkedin_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(100, members=[alice, bob])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        db.set_minigame_player_link(
            100, 'queens', bob.id, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, bob.id)

        with pytest.raises(MinigameCogError) as exc_info:
            asyncio.run(Minigames.queens_register.__wrapped__(
                cog, ctx, '+anon', linkedin='Alice LinkedIn'))

        assert 'Anonymous' in str(exc_info.value)
        assert 'Alice LinkedIn' not in str(exc_info.value)

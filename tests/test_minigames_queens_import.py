"""Queens import: preview / resolve / register tests."""
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


class TestQueensImport:
    def test_queens_date_number_mapping_uses_linkedin_anchor(self):
        assert _queens_number('2026-06-08') == 769
        assert _queens_number('2026-06-09') == 770
        assert minigames_module._queens_date_for_puzzle_number(769) == (
            dt.date(2026, 6, 8))
        assert minigames_module._parse_queens_date_or_number('#770') == (
            dt.date(2026, 6, 9))

    def test_importer_must_be_linked(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'ali', 'Ali'),
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
        )

        cog = Minigames(bot=None)
        with pytest.raises(MinigameCogError, match='Register the importer'):
            cog._make_queens_import_preview(ctx, '2026-06-08', content)

    def test_importer_must_be_linked_even_for_unresolved_only_board(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        cog = Minigames(bot=None)
        with pytest.raises(MinigameCogError, match='Register the importer'):
            cog._make_queens_import_preview(ctx, '2026-06-08', (
                'Alice LinkedIn\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:04\n'
            ))

    def test_preview_resolves_linked_names_and_you_then_saves_ratings(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_player_link(
            100, 'queens', 300, 'Ali Farhat',
            normalize_queens_name('Ali Farhat'), None, 1.0, 999)
        db.set_minigame_player_link(
            100, 'queens', 301, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'),
            'https://www.linkedin.com/in/robert/', 1.0, 999)

        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'ali', 'Ali'),
            _FakeDiscordMember(301, 'robert', 'Robert'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'robert', 'Robert'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'Ali Farhat\n'
            'Ali Farhat\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
            'Unknown Person\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:07\n'
        )

        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', content)

        assert preview.puzzle_date == dt.date(2026, 6, 8)
        assert preview.puzzle_number == 769
        assert [entry.user_id for entry in preview.resolved] == ['300', '301']
        assert [entry.linkedin_name for entry in preview.unresolved] == [
            'Unknown Person',
        ]
        assert '2026-06-08' in cog._format_queens_import_preview(ctx, preview)
        assert '#769' in cog._format_queens_import_preview(ctx, preview)
        assert 'Robert Kocharyan' in cog._format_queens_import_preview(ctx, preview)

        saved = cog._save_queens_import(ctx, preview)

        assert saved.resolved == 2
        assert saved.unresolved == 1
        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert sorted((row.user_id, row.time_seconds) for row in rows) == [
            ('300', 4),
            ('301', 6),
        ]
        unresolved = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', _queens_number('2026-06-08'))
        assert [(row.external_name, row.time_seconds) for row in unresolved] == [
            ('Ali Farhat', 4),
            ('Robert Kocharyan', 6),
            ('Unknown Person', 7),
        ]
        assert {row.puzzle_number for row in rows} == {_queens_number('2026-06-08')}
        assert {row.puzzle_date for row in rows} == {'2026-06-08'}
        ratings = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in ratings] == ['300', '301']
        assert ratings[0].rating > ratings[1].rating

        reimport = cog._make_queens_import_preview(ctx, '08/06/2026', (
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:05\n'
        ))
        saved = cog._save_queens_import(ctx, reimport)
        assert saved.resolved == 1
        assert saved.unresolved == 0
        rows = db.get_minigame_results_for_guild(100, 'queens')
        assert sorted((row.user_id, row.time_seconds) for row in rows) == [
            ('300', 4),
            ('301', 5),
        ]
        source_rows = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', _queens_number('2026-06-08'))
        assert [(row.external_name, row.time_seconds) for row in source_rows] == [
            ('Ali Farhat', 4),
            ('Robert Kocharyan', 5),
            ('Unknown Person', 7),
        ]
        ratings = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in ratings] == ['300', '301']
        assert ratings[0].rating > ratings[1].rating

    def test_register_claims_previously_unresolved_import_rows(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        db.set_minigame_player_link(
            100, 'queens', 300, 'Importer Name',
            normalize_queens_name('Importer Name'), None, 1.0, 999)
        importer = _FakeDiscordMember(300, 'importer', 'Importer')
        alice = _FakeDiscordMember(301, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[importer, alice])
        ctx = SimpleNamespace(
            guild=guild,
            author=importer,
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
            send=lambda *args, **kwargs: None,
        )
        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', (
            'Alice LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:04\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
        ))
        saved = cog._save_queens_import(ctx, preview)
        assert saved.resolved == 1
        assert saved.unresolved == 1
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        register_ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        claimed_count = cog._cmd_queens_register_link(
            register_ctx, alice, 'Alice LinkedIn')

        claimed = db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08'))
        assert claimed is not None
        assert claimed.time_seconds == 4
        assert [
            row.time_seconds for row in db.get_minigame_unresolved_results_for_name(
                100, 'queens', normalize_queens_name('Alice LinkedIn'))
        ] == [4]
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == [
            '301', '300',
        ]
        assert claimed_count == 1

    def test_linkedin_source_result_moves_when_name_is_reclaimed(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
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

        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Shared LinkedIn'),
            'Shared LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')

        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        alice_ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        cog._cmd_queens_register_link(alice_ctx, alice, 'Shared LinkedIn')
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is not None

        asyncio.run(cog._cmd_queens_unregister(alice_ctx, alice))
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Shared LinkedIn'))
        assert db.get_minigame_ratings(100, 'queens') == []

        mod_ctx = SimpleNamespace(
            guild=guild,
            author=mod,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        asyncio.run(Minigames.queens_set.__wrapped__(
            cog, mod_ctx, 'bob', linkedin='Shared LinkedIn'))
        moved = db.get_minigame_result_for_user_puzzle(
            100, 'queens', bob.id, _queens_number('2026-06-08'))
        assert moved is not None
        assert moved.time_seconds == 4
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is None
        assert [
            (row.user_id, row.time_seconds)
            for row in db.get_minigame_results_for_guild(100, 'queens')
        ] == [('301', 4)]
        assert [row.user_id for row in db.get_minigame_ratings(100, 'queens')] == [
            '301',
        ]

    def test_register_normalizes_legacy_unresolved_puzzle_number(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        alice = _FakeDiscordMember(301, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        sent = {}

        async def send(content=None, *, embed=None, **kwargs):
            sent['content'] = content
            sent['embed'] = embed
            sent['kwargs'] = kwargs

        ctx = SimpleNamespace(
            guild=guild,
            author=alice,
            channel=_FakeChannel(200),
            send=send,
            sent=sent,
        )
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, dt.date(2026, 6, 8).toordinal(),
            '2026-06-08', 100, 4, True, 'legacy')
        cog = Minigames(bot=None)

        claimed_count = cog._cmd_queens_register_link(
            ctx, alice, 'Alice LinkedIn')

        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, _queens_number('2026-06-08')) is not None
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, dt.date(2026, 6, 8).toordinal()) is None
        assert claimed_count == 1

    def test_you_row_prefers_importer_even_when_name_is_copied(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'), None, 1.0, 999)
        db.set_minigame_player_link(
            100, 'queens', 301, 'Importer Name',
            normalize_queens_name('Importer Name'), None, 1.0, 999)
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'robert', 'Robert'),
            _FakeDiscordMember(301, 'importer', 'Importer'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(301, 'importer', 'Importer'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        content = (
            'Robert Kocharyan\n'
            'Robert Kocharyan\n'
            'You\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:06\n'
        )

        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(ctx, '2026-06-08', content)

        assert [entry.user_id for entry in preview.resolved] == ['301']

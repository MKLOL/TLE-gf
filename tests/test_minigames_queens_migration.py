"""Queens import: legacy-row migration / sync tests."""
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


class TestQueensImportMigration:
    def test_legacy_live_and_imported_rows_migrate_to_linkedin_source_exactly(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 0, 8, False, 'alice raw')
        db.save_imported_minigame_result(
            12, 100, 'queens', 202, 301, _queens_number('2026-06-09'),
            '2026-06-09', 100, 5, True, 'bob raw')
        db.save_imported_minigame_result(
            13, 100, 'akari', 203, 302, 1, '2026-06-10',
            100, 9, True, 'akari raw')

        cog = Minigames(bot=None)
        saved = cog._sync_queens_materialized_results(100)

        source = {
            row.external_name: row
            for row in db.get_minigame_unresolved_results_for_guild(
                100, 'queens')
        }
        assert set(source) == {'Alice LinkedIn', 'Bob LinkedIn'}
        assert source['Alice LinkedIn'].normalized_name == 'alice linkedin'
        assert source['Alice LinkedIn'].channel_id == '201'
        assert source['Alice LinkedIn'].puzzle_number == _queens_number('2026-06-08')
        assert source['Alice LinkedIn'].puzzle_date == '2026-06-08'
        assert source['Alice LinkedIn'].accuracy == 0
        assert source['Alice LinkedIn'].time_seconds == 8
        assert source['Alice LinkedIn'].is_perfect == 0
        assert source['Alice LinkedIn'].raw_content == 'alice raw'
        assert source['Bob LinkedIn'].channel_id == '202'
        assert source['Bob LinkedIn'].puzzle_number == _queens_number('2026-06-09')
        assert source['Bob LinkedIn'].accuracy == 100
        assert source['Bob LinkedIn'].time_seconds == 5
        assert source['Bob LinkedIn'].is_perfect == 1
        assert source['Bob LinkedIn'].raw_content == 'bob raw'

        materialized = {
            row.user_id: row
            for row in db.get_minigame_results_for_guild(100, 'queens')
        }
        assert set(materialized) == {'300', '301'}
        assert materialized['300'].time_seconds == 8
        assert materialized['301'].time_seconds == 5
        assert saved == 2
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'queens'"
        ).fetchone()[0] == 0
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'akari'"
        ).fetchone()[0] == 1

    def test_additive_filter_migrates_legacy_rows_before_checking_new_entries(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 100, 8, True, 'alice legacy raw')
        guild = _FakeGuild(100, members=[
            _FakeDiscordMember(300, 'alice', 'Alice'),
        ])
        ctx = SimpleNamespace(
            guild=guild,
            author=_FakeDiscordMember(999, 'bot', 'Bot'),
            channel=_FakeChannel(200),
            message=SimpleNamespace(id=555),
        )
        cog = Minigames(bot=None)
        preview = cog._make_queens_import_preview(
            ctx, '2026-06-08', (
                'Alice LinkedIn\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:07\n'
                'Unknown Person\n'
                '\U0001f913\U0001f48e No hints & no mistakes!\n'
                '0:05\n'
            ), skip_importer=True)

        new_resolved, new_unresolved = cog._filter_new_queens_entries(
            100, preview)
        preview = preview._replace(
            resolved=new_resolved, unresolved=new_unresolved)
        saved = cog._save_queens_import(ctx, preview, skip_wipe=True)

        assert saved.resolved == 1
        assert saved.unresolved == 1
        alice_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn'))
        assert [row.time_seconds for row in alice_source] == [7]
        alice_result = db.get_minigame_result_for_user_puzzle(
            100, 'queens', 300, _queens_number('2026-06-08'))
        assert alice_result.time_seconds == 7
        unknown_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Unknown Person'))
        assert [row.time_seconds for row in unknown_source] == [5]

    def test_unlinked_legacy_row_migrates_from_unique_raw_leaderboard_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        raw = (
            'Charlie LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Charlie LinkedIn'))
        assert len(source) == 1
        assert source[0].external_name == 'Charlie LinkedIn'
        assert source[0].time_seconds == 9

        db.set_minigame_player_link(
            100, 'queens', 300, 'Charlie LinkedIn',
            normalize_queens_name('Charlie LinkedIn'), None, 1.0, 999)
        saved = cog._sync_queens_materialized_results(100)

        materialized = db.get_minigame_result_for_user_puzzle(
            100, 'queens', 300, _queens_number('2026-06-08'))
        assert saved == 1
        assert materialized is not None
        assert materialized.time_seconds == 9

    def test_unlinked_imported_legacy_row_migrates_from_unique_raw_name(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        raw = (
            'Charlie LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_imported_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.conn.execute(
            "SELECT COUNT(*) FROM minigame_import_result "
            "WHERE guild_id = '100' AND game = 'queens'"
        ).fetchone()[0] == 0
        source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Charlie LinkedIn'))
        assert [row.time_seconds for row in source] == [9]

        db.set_minigame_player_link(
            100, 'queens', 300, 'Charlie LinkedIn',
            normalize_queens_name('Charlie LinkedIn'), None, 1.0, 999)
        saved = cog._sync_queens_materialized_results(100)

        materialized = db.get_minigame_result_for_user_puzzle(
            100, 'queens', 300, _queens_number('2026-06-08'))
        assert saved == 1
        assert materialized is not None
        assert materialized.time_seconds == 9

    def test_legacy_row_prefers_unique_raw_name_over_current_link(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Current LinkedIn',
            normalize_queens_name('Current LinkedIn'), None, 1.0, 999)
        raw = (
            'Original LinkedIn\n'
            '\U0001f913\U0001f48e No hints & no mistakes!\n'
            '0:09\n'
        )
        db.save_minigame_result(
            11, 100, 'queens', 201, 300, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, raw)
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)

        assert saved == 0
        assert db.get_minigame_results_for_guild(100, 'queens') == []
        original_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Original LinkedIn'))
        current_source = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Current LinkedIn'))
        assert [row.time_seconds for row in original_source] == [9]
        assert current_source == []

    def test_unmapped_legacy_row_is_preserved_when_identity_is_unknown(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.save_minigame_result(
            11, 100, 'queens', 201, 400, _queens_number('2026-06-08'),
            '2026-06-08', 100, 9, True, 'not a copied leaderboard')
        cog = Minigames(bot=None)

        saved = cog._sync_queens_materialized_results(100)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        assert saved == 0
        assert db.get_minigame_result(11) is not None
        assert db.get_minigame_unresolved_results_for_guild(100, 'queens') == []
        assert db.get_minigame_ratings(100, 'queens') == []

    def test_sync_does_not_remigrate_current_materialized_rows(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')
        cog = Minigames(bot=None)

        assert cog._sync_queens_materialized_results(100) == 1
        rows_before = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.time_seconds) for row in rows_before] == [
            ('300', 4),
        ]

        assert cog._migrate_legacy_queens_results_to_external(100) == 0
        rows_after = db.get_minigame_results_for_guild(100, 'queens')
        assert [(row.user_id, row.time_seconds) for row in rows_after] == [
            ('300', 4),
        ]

    def test_sync_does_not_rewrite_current_materialized_rows(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_minigame_player_link(
            100, 'queens', 300, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, _queens_number('2026-06-08'),
            '2026-06-08', 100, 4, True, 'source')
        cog = Minigames(bot=None)

        assert cog._sync_queens_materialized_results(100) == 1
        writes = []

        def record_write(*args, **kwargs):
            writes.append(args)
            raise AssertionError('current projection row was rewritten')

        monkeypatch.setattr(db, 'save_minigame_result', record_write)

        assert cog._sync_queens_materialized_results(
            100, migrate_legacy=False) == 0
        assert writes == []

    def test_generic_recompute_writes_queens_snapshot_only(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.replace_minigame_ratings(
            100, 'akari', [RatingState('999', 1500, 1, 1500, 0)], 1.0)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn'),
                (302, 'Cara LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            1, 100, 'queens', 200, 300, _queens_number('2026-06-08'), '2026-06-08',
            0, 8, False, 'fast no badges')
        db.save_minigame_result(
            2, 100, 'queens', 200, 301, _queens_number('2026-06-08'), '2026-06-08',
            100, 10, True, 'slow perfect')
        db.save_minigame_result(
            3, 100, 'queens', 200, 302, _queens_number('2026-06-08'), '2026-06-08',
            0, 10, False, 'slow imperfect')
        db.save_minigame_result(
            4, 100, 'queens', 200, 300, _queens_number('2026-06-09'), '2026-06-09',
            100, 5, True, 'alice solo')

        cog = Minigames(bot=None)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        queens = {
            row.user_id: row
            for row in db.get_minigame_ratings(100, 'queens')
        }
        assert set(queens) == {'300', '301', '302'}
        assert queens['300'].rating > queens['301'].rating
        assert abs(queens['301'].rating - queens['302'].rating) < 1e-9
        assert queens['300'].games == 2
        assert queens['301'].games == 1
        assert queens['302'].games == 1

        akari = db.get_minigame_rating(100, 'akari', 999)
        assert akari.rating == 1500
        assert akari.games == 1

    def test_queens_rating_does_not_decay_absent_players(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        for user_id, name in (
                (300, 'Alice LinkedIn'),
                (301, 'Bob LinkedIn'),
                (302, 'Cara LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, user_id)
        db.save_minigame_result(
            1, 100, 'queens', 200, 300, _queens_number('2026-06-08'), '2026-06-08',
            100, 5, True, 'alice fast')
        db.save_minigame_result(
            2, 100, 'queens', 200, 301, _queens_number('2026-06-08'), '2026-06-08',
            100, 10, True, 'bob slow')

        cog = Minigames(bot=None)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)
        alice_before = db.get_minigame_rating(100, 'queens', 300)

        db.save_minigame_result(
            3, 100, 'queens', 200, 301, _queens_number('2026-06-09'), '2026-06-09',
            100, 5, True, 'bob fast')
        db.save_minigame_result(
            4, 100, 'queens', 200, 302, _queens_number('2026-06-09'), '2026-06-09',
            100, 10, True, 'cara slow')
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        alice_after = db.get_minigame_rating(100, 'queens', 300)
        assert abs(alice_after.rating - alice_before.rating) < 1e-9
        assert alice_after.skip_streak == 1

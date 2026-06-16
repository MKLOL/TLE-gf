"""Minigame DB mixin + rating DB tests."""
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


class TestDbMixin:
    def test_channel_crud(self, db):
        assert db.get_minigame_channel(123, _GAME) is None
        db.set_minigame_channel(123, _GAME, 456)
        assert db.get_minigame_channel(123, _GAME) == '456'
        db.clear_minigame_channel(123, _GAME)
        assert db.get_minigame_channel(123, _GAME) is None

    def test_result_storage(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'raw msg')
        row = db.get_minigame_result(1)
        assert row is not None
        assert row.user_id == '300'
        assert row.puzzle_number == 445
        assert row.is_perfect == 1
        assert row.time_seconds == 89
        assert row.raw_content == 'raw msg'

    def test_results_for_user(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_minigame_result(2, 100, _GAME, 200, 301, 446, '2026-03-27', 90, 99, False, 'c2')
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '1'

    def test_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c')
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '1'

    def test_imported_results_are_included_in_queries(self, db):
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c')
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_first_message_across_live_and_imported_wins(self, db):
        db.save_minigame_result(20, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'c1')
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False, 'c2')
        row = db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert row is not None
        assert row.message_id == '10'
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].message_id == '10'

    def test_delete_result_for_user_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_imported_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 90, True, 'c2')
        rc = db.delete_minigame_result_for_user_puzzle(100, _GAME, 300, 445)
        assert rc == 2
        assert db.get_minigame_result_for_user_puzzle(100, _GAME, 300, 445) is None

    def test_import_only_results_excludes_rows_with_live_counterpart(self, db):
        # puzzle 445: imported AND live for same user -> not orphaned
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'live')
        db.save_imported_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False, 'imp')
        # puzzle 446: imported only -> orphaned
        db.save_imported_minigame_result(3, 100, _GAME, 200, 300, 446, '2026-03-27', 90, 70, False, 'imp')
        # puzzle 447: imported for a different user, no live -> orphaned
        db.save_imported_minigame_result(4, 100, _GAME, 200, 301, 447, '2026-03-28', 100, 80, True, 'imp')

        orphans = db.get_import_only_minigame_results(100, _GAME)
        assert {(r.user_id, r.puzzle_number) for r in orphans} == {
            ('300', 446), ('301', 447)}

    def test_merged_winners_first_attempt_and_live_precedence(self, db):
        # user 300 puzzle 445: live (msg 20) + an EARLIER imported post (msg 10)
        # -> earliest message wins -> imported 96%/50s.
        db.save_minigame_result(20, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'c1')
        db.save_imported_minigame_result(10, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False, 'c2')
        # user 301 puzzle 446: live only.
        db.save_minigame_result(30, 100, _GAME, 200, 301, 446, '2026-03-27', 100, 70, True, 'c3')
        winners = merged_minigame_winners(db.conn, 100, _GAME)
        assert winners[('300', 445)] == (50, 0, 96)   # earlier imported post wins
        assert winners[('301', 446)] == (70, 1, 100)

    def test_diff_merged_winners_classifies_changes(self, db):
        old = {('a', 1): (60, 1, 100), ('b', 2): (90, 0, 95), ('c', 3): (10, 1, 100)}
        new = {('a', 1): (60, 1, 100),          # unchanged
               ('b', 2): (120, 1, 100),         # changed
               ('d', 4): (30, 1, 100)}          # added; ('c',3) removed
        added, removed, changed = diff_merged_winners(old, new)
        assert [k for k, _, _ in added] == [('d', 4)]
        assert [k for k, _, _ in removed] == [('c', 3)]
        assert changed == [(('b', 2), (90, 0, 95), (120, 1, 100))]

    def test_diff_merged_winners_against_separate_snapshot_db(self, db):
        # Snapshot: user 300 has a perfect 1:00 on puzzle 445.
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'c')
        snapshot = merged_minigame_winners(db.conn, 100, _GAME)
        # Later an EARLIER imported post (msg 0) surfaces a worse 2:00 result.
        db.save_imported_minigame_result(0, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 120, True, 'c')
        current = merged_minigame_winners(db.conn, 100, _GAME)
        added, removed, changed = diff_merged_winners(snapshot, current)
        assert not added and not removed
        assert changed == [(('300', 445), (60, 1, 100), (120, 1, 100))]

    def test_import_only_live_match_uses_user_and_puzzle_not_message(self, db):
        # same (user, puzzle) but the live row carries a different message id;
        # the imported row should still be considered "present in live".
        db.save_minigame_result(99, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 60, True, 'live')
        db.save_imported_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-26', 96, 50, False, 'imp')
        assert db.get_import_only_minigame_results(100, _GAME) == []

    def test_delete_results_for_puzzle(self, db):
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_minigame_result(2, 100, _GAME, 200, 301, 445, '2026-03-26', 100, 90, True, 'c2')
        db.save_imported_minigame_result(3, 100, _GAME, 200, 302, 445, '2026-03-26', 100, 91, True, 'c3')
        db.save_minigame_result(4, 100, _GAME, 200, 300, 446, '2026-03-27', 100, 92, True, 'c4')

        assert db.delete_minigame_results_for_puzzle(100, _GAME, 445) == 3

        rows = db.get_minigame_results_for_guild(100, _GAME)
        assert [(row.user_id, row.puzzle_number) for row in rows] == [('300', 446)]

    def test_puzzle_number_filtering(self, db):
        """plo/phi should filter results by puzzle_number at the DB level."""
        db.save_minigame_result(1, 100, _GAME, 200, 300, 440, '2026-03-20', 100, 60, True, 'c')
        db.save_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-25', 100, 70, True, 'c')
        db.save_minigame_result(3, 100, _GAME, 200, 300, 450, '2026-03-30', 100, 80, True, 'c')
        rows = db.get_minigame_results_for_user(100, _GAME, 300, plo=445, phi=450)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_date_filtering(self, db):
        """dlo/dhi should filter results by puzzle_date at the DB level."""
        import time
        db.save_minigame_result(1, 100, _GAME, 200, 300, 440, '2026-03-20', 100, 60, True, 'c')
        db.save_minigame_result(2, 100, _GAME, 200, 300, 445, '2026-03-25', 100, 70, True, 'c')
        db.save_minigame_result(3, 100, _GAME, 200, 300, 450, '2026-03-30', 100, 80, True, 'c')
        dlo = time.mktime(dt.datetime(2026, 3, 24).timetuple())
        dhi = time.mktime(dt.datetime(2026, 3, 26).timetuple())
        rows = db.get_minigame_results_for_user(100, _GAME, 300, dlo=dlo, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_raw_content_updated_on_replace(self, db):
        """INSERT OR REPLACE should update raw_content when re-saving same message_id."""
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'original')
        db.save_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'edited')
        row = db.get_minigame_result(1)
        assert row.raw_content == 'edited'

    def test_raw_message_storage_and_reparse(self, db):
        """Raw messages are stored and can be reparsed into import results."""
        content = (
            'Daily Akari \U0001f60a 445\n'
            '\u27052026-03-26 (Thu)\u2705\n'
            '\U0001f31f Perfect!   \U0001f553 1:29\n'
            'https://dailyakari.com/'
        )
        db.save_raw_message(1, 100, 200, 300, '2026-03-26T12:00:00', content)
        db.save_raw_message(2, 100, 200, 301, '2026-03-26T12:05:00', 'not a game msg')

        raws = db.get_raw_messages_for_guild(100)
        assert len(raws) == 2

        # Simulate reparse: parse raw content and save matches
        from tle.cogs._minigame_akari import parse_akari_message
        from tle.cogs._minigame_common import strip_codeblock
        parsed_count = 0
        for row in raws:
            results = parse_akari_message(strip_codeblock(row.raw_content))
            for r in results:
                db.save_imported_minigame_result(
                    row.message_id, row.guild_id, _GAME, row.channel_id,
                    row.user_id, r.puzzle_number,
                    r.puzzle_date.isoformat(), r.accuracy,
                    r.time_seconds, r.is_perfect, row.raw_content,
                )
                parsed_count += 1

        assert parsed_count == 1
        rows = db.get_minigame_results_for_user(100, _GAME, 300)
        assert len(rows) == 1
        assert rows[0].puzzle_number == 445

    def test_clear_imported_per_channel(self, db):
        """Import clear with channel_id only removes that channel's rows."""
        db.save_imported_minigame_result(1, 100, _GAME, 200, 300, 445, '2026-03-26', 100, 89, True, 'c1')
        db.save_imported_minigame_result(2, 100, _GAME, 201, 301, 446, '2026-03-27', 100, 90, True, 'c2')
        deleted = db.clear_imported_minigame_results(100, _GAME, channel_id=200)
        assert deleted == 1
        # Channel 201's result should survive
        rows = db.get_minigame_results_for_guild(100, _GAME)
        assert len(rows) == 1
        assert rows[0].channel_id == '201'

    def test_minigame_player_link_crud_and_unique_name(self, db):
        db.set_minigame_player_link(
            100, 'queens', 300, 'Robert Kocharyan',
            normalize_queens_name('Robert Kocharyan'),
            'https://www.linkedin.com/in/robert/', 1.0, 999)
        row = db.get_minigame_player_link(100, 'queens', 300)
        assert row.external_name == 'Robert Kocharyan'
        assert row.external_url == 'https://www.linkedin.com/in/robert/'

        by_name = db.get_minigame_player_link_by_name(
            100, 'queens', normalize_queens_name('  robert   kocharyan '))
        assert by_name.user_id == '300'

        with pytest.raises(sqlite3.IntegrityError):
            db.set_minigame_player_link(
                100, 'queens', 301, 'Robert   Kocharyan',
                normalize_queens_name('Robert   Kocharyan'),
                None, 2.0, 999)

    def test_minigame_unresolved_result_crud(self, db):
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Alice LinkedIn'),
            'Alice LinkedIn', 200, 123, '2026-06-08',
            100, 5, True, 'raw')
        db.save_minigame_unresolved_result(
            100, 'queens', normalize_queens_name('Bob LinkedIn'),
            'Bob LinkedIn', 200, 123, '2026-06-08',
            100, 7, True, 'raw')

        by_name = db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn'))
        assert [(row.external_name, row.time_seconds) for row in by_name] == [
            ('Alice LinkedIn', 5),
        ]
        by_puzzle = db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123)
        assert [(row.external_name, row.time_seconds) for row in by_puzzle] == [
            ('Alice LinkedIn', 5),
            ('Bob LinkedIn', 7),
        ]
        assert db.delete_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name('Alice LinkedIn')) == 1
        assert db.delete_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123) == 1
        assert db.get_minigame_unresolved_results_for_puzzle(
            100, 'queens', 123) == []

    def test_minigame_rating_snapshot_is_game_keyed(self, db):
        states = [
            RatingState('300', 1210.5, 2, 1210.5, 5.0),
            RatingState('301', 1190.0, 2, 1200.0, -5.0),
        ]
        db.replace_minigame_ratings(100, 'queens', states, 12.0)
        db.replace_minigame_ratings(100, 'akari', [RatingState('300', 1500, 1, 1500, 0)], 13.0)

        queens = db.get_minigame_ratings(100, 'queens')
        assert [row.user_id for row in queens] == ['300', '301']
        assert db.get_minigame_rating(100, 'queens', 300).rating == 1210.5
        assert db.get_minigame_rating(100, 'akari', 300).rating == 1500

    def test_minigame_ban_roundtrip(self, db):
        assert db.is_minigame_banned(100, 'queens', 300) is False
        assert db.ban_minigame_user(
            100, 'queens', 300, 12.0, 999, 'spam') == 1
        assert db.ban_minigame_user(
            100, 'queens', 300, 13.0, 999, 'again') == 0
        assert db.is_minigame_banned(100, 'queens', 300) is True
        row = db.get_minigame_ban(100, 'queens', 300)
        assert row.reason == 'spam'
        rows = db.get_minigame_bans(100, 'queens')
        assert [r.user_id for r in rows] == ['300']
        assert db.unban_minigame_user(100, 'queens', 300) == 1
        assert db.is_minigame_banned(100, 'queens', 300) is False


class TestRatingDb:
    def test_default_registered_for_anyone_not_opted_out(self, db):
        # Default-opt-in: even a user we've never heard of is "registered" —
        # is_akari_registered is the inverse of is_akari_opted_out.
        assert db.is_akari_registered(1, 999) is True
        # register on a user with no opt-out is a no-op.
        assert db.register_akari_user(1, 999) is False

    def test_unregister_adds_optout_then_register_lifts_it(self, db):
        # First unregister adds the opt-out; second is a no-op.
        assert db.unregister_akari_user(1, 999, 1.0) is True
        assert db.unregister_akari_user(1, 999, 2.0) is False
        assert db.is_akari_registered(1, 999) is False
        assert db.is_akari_opted_out(1, 999) is True
        # register lifts the opt-out.
        assert db.register_akari_user(1, 999) is True
        assert db.is_akari_opted_out(1, 999) is False
        assert db.is_akari_registered(1, 999) is True

    def test_registrants_lists_users_with_results_minus_optouts(self, db):
        # Only users with any result show up; opt-outs are excluded.
        db.save_minigame_result(
            'm1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_minigame_result(
            'm2', 1, 'akari', 10, 888, 2,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999', '888'}
        db.unregister_akari_user(1, 888, 1.0)
        assert db.get_akari_registrants(1) == {'999'}

    def test_registrants_are_guild_scoped(self, db):
        # Results in guild 1 don't surface in guild 2's registrants list.
        db.save_minigame_result(
            'a', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_minigame_result(
            'b', 2, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.unregister_akari_user(1, 999, 2.0)
        assert db.get_akari_registrants(1) == set()
        assert db.get_akari_registrants(2) == {'999'}

    def test_registrants_dedupe_live_and_imported(self, db):
        # The same user appearing in both tables is listed once.
        db.save_minigame_result(
            'l1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        db.save_imported_minigame_result(
            'i1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999'}

    def test_imported_results_make_user_visible(self, db):
        # An imported-only user (no live results) still appears in registrants.
        db.save_imported_minigame_result(
            'i1', 1, 'akari', 10, 999, 1,
            '2026-06-03', 100, 60, True, 'raw')
        assert db.get_akari_registrants(1) == {'999'}

    def test_replace_and_get_ratings_sorted_desc(self, db):
        states = [
            RatingState('a', 1300.5, 4, 1320.0, 5.0),
            RatingState('b', 1100.25, 4, 1200.0, -3.0),
        ]
        assert db.replace_akari_ratings(1, states, 1000.0) == 2
        rows = db.get_akari_ratings(1)
        assert [r.user_id for r in rows] == ['a', 'b']
        assert abs(rows[0].rating - 1300.5) < 1e-9
        b = db.get_akari_rating(1, 'b')
        assert b.games == 4
        assert abs(b.last_delta + 3.0) < 1e-9

    def test_replace_overwrites_not_appends(self, db):
        db.replace_akari_ratings(1, [RatingState('a', 1300.0, 1, 1300.0, 0.0)], 1.0)
        db.replace_akari_ratings(1, [RatingState('a', 1250.0, 2, 1300.0, -50.0)], 2.0)
        rows = db.get_akari_ratings(1)
        assert len(rows) == 1
        assert abs(rows[0].rating - 1250.0) < 1e-9

    def test_replace_is_guild_scoped(self, db):
        db.replace_akari_ratings(1, [RatingState('a', 1300.0, 1, 1300.0, 0.0)], 1.0)
        db.replace_akari_ratings(2, [RatingState('b', 1400.0, 1, 1400.0, 0.0)], 1.0)
        # Rebuilding guild 1 must leave guild 2 untouched.
        db.replace_akari_ratings(1, [RatingState('a', 1290.0, 2, 1300.0, -10.0)], 3.0)
        assert len(db.get_akari_ratings(2)) == 1
        assert db.get_akari_rating(2, 'b').rating == 1400.0

    def test_replace_persists_decay_fields(self, db):
        state = RatingState('a', 1300.0, 4, 1320.0, -2.5, 7, 612)
        db.replace_akari_ratings(1, [state], 1000.0)
        row = db.get_akari_rating(1, 'a')
        assert row.skip_streak == 7
        assert row.last_puzzle == 612

    def test_akari_rating_reads_legacy_snapshot_if_generic_missing(self, db):
        db.conn.execute(
            '''
            INSERT INTO akari_rating
                (guild_id, user_id, rating, games, peak, last_delta,
                 skip_streak, last_puzzle, updated_at)
            VALUES ('1', 'a', 1300.0, 2, 1310.0, 5.0, 1, 445, 1000.0)
            '''
        )
        db.conn.commit()

        rows = db.get_akari_ratings(1)
        assert [row.user_id for row in rows] == ['a']
        assert db.get_akari_rating(1, 'a').rating == 1300.0

    def test_akari_rating_prefers_generic_snapshot_when_present(self, db):
        db.conn.execute(
            '''
            INSERT INTO akari_rating
                (guild_id, user_id, rating, games, peak, last_delta,
                 skip_streak, last_puzzle, updated_at)
            VALUES ('1', 'a', 1300.0, 2, 1310.0, 5.0, 1, 445, 1000.0)
            '''
        )
        db.replace_minigame_ratings(
            1, 'akari',
            [RatingState('a', 1400.0, 3, 1400.0, 10.0, 0, 446)],
            1001.0,
        )

        rows = db.get_akari_ratings(1)
        assert rows[0].rating == 1400.0
        assert db.get_akari_rating(1, 'a').games == 3

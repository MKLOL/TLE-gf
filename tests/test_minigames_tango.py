"""LinkedIn Tango: definition, calendar, parsing, and the shared LinkedIn link.

Tango reuses every piece of the Queens machinery through ``GameDef.linkedin``;
these tests pin the Tango-specific facts (header word, calendar anchor) and
the one new cross-game behavior — a single LinkedIn registration serving both
games — plus the migration that moves existing Queens links into the shared
namespace.
"""

import asyncio
import datetime as dt
import sqlite3
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import meta as meta_module
from tle.cogs import minigames as minigames_module
from tle.cogs import _minigame_linkedin as linkedin_module
from tle.cogs._minigame_linkedin import (
    LinkedInDef, parse_linkedin_leaderboard,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME, normalize_queens_name, parse_queens_leaderboard,
    parse_queens_message,
)
from tle.cogs._minigame_queens_cog import (
    _linkedin_result_message_id, _queens_result_message_id,
)
from tle.cogs._minigame_tango import (
    TANGO_GAME, TANGO_LINKEDIN, parse_tango_message,
)
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common
from tle.util.db._user_db_upgrades_part5 import upgrade_1_57_0
from tle.util.db.user_db_conn import namedtuple_factory

from tests.minigames_test_utils import (
    db,  # noqa: F401 - imported pytest fixture
    _FakeDiscordMember, _FakeGuild, _FakeMessage, _QueensCommandsBase,
)


_SHARE_INLINE = (
    'Tango #697 | 0:11\n'
    'No mistakes & no hints\n'
    'First 5 placements:\n'
    '\U0001f7e8\U0001f7e82️⃣\U0001f7e84️⃣\U0001f7e8\n'
    '\U0001f3c5 I’m on a 46-day win streak!\n'
    'lnkd.in/tango.'
)
_SHARE_EMOJI = 'Tango #695 | 0:17 \U0001f317\nlnkd.in/tango.'
_SHARE_WRAPPED = 'Tango #695\n0:08 \U0001f317\nlnkd.in/tango.'
_LEADERBOARD = (
    'Artsiom Savich\nArtsiom Savich\nYou\n'
    '\U0001f913\U0001f48e No hints & no mistakes!\n\n0:11\n'
    'Justin Goh\nJustin Goh\nJustin Goh\n'
    '\U0001f913\U0001f48e No hints & no mistakes!\n\n0:13\n'
    '4\nHussein Farhat\nHussein Farhat\n'
    '\U0001f913\U0001f48e No hints & no mistakes!\n\n0:43\n'
    '5\nSami Almajali\nSami Almajali\n\U0001f913 No hints!\n\n0:48\n'
    '6\nWilliam Chittick\nWilliam Chittick\n'
    '\U0001f913\U0001f48e No hints & no mistakes!\n\n2:31\n'
)


class TestTangoDefinition:
    def test_is_a_linkedin_game_sharing_the_queens_link_namespace(self):
        assert TANGO_GAME.linkedin_identity
        assert QUEENS_GAME.linkedin_identity
        assert TANGO_GAME.link_key == QUEENS_GAME.link_key == 'linkedin'
        # Results, opt-outs, and bans stay keyed by the game itself.
        assert TANGO_GAME.name == 'tango'
        assert TANGO_GAME.linkedin.admins_key == 'tango_admin_user_ids'
        assert QUEENS_GAME.linkedin.admins_key == 'queens_admin_user_ids'

    def test_rating_config_mirrors_queens(self):
        assert TANGO_GAME.rating is not None
        assert TANGO_GAME.rating.rank_fn is QUEENS_GAME.rating.rank_fn
        assert TANGO_GAME.rating.decay_base == constants.TANGO_DECAY_BASE
        assert TANGO_GAME.rating.decay_max == constants.TANGO_DECAY_MAX
        assert TANGO_GAME.rating.decay_grace == constants.TANGO_DECAY_GRACE
        assert callable(TANGO_GAME.rating.current_puzzle_number_fn)
        assert TANGO_GAME.score_matchup is QUEENS_GAME.score_matchup
        assert TANGO_GAME.is_eligible_winner(None)

    def test_registered_in_cog_and_feature_flags(self):
        assert Minigames.GAMES['tango'] is TANGO_GAME
        assert hasattr(Minigames, 'tango')
        assert hasattr(Minigames, 'tango_slash')
        assert 'tango' in meta_module._KNOWN_FEATURES
        cog = Minigames(bot=None)
        assert [g.name for g in cog._linkedin_games()] == ['queens', 'tango']
        assert cog._linkedin_games_label() == 'LinkedIn Queens/Tango'

    def test_akari_and_guessgame_are_not_linkedin_games(self):
        assert not minigames_module.AKARI_GAME.linkedin_identity
        assert not minigames_module.GUESSGAME_GAME.linkedin_identity
        assert minigames_module.AKARI_GAME.link_key == 'akari'


class TestTangoCalendar:
    def test_anchor_and_inverse(self):
        assert TANGO_LINKEDIN.date_for_number(697) == dt.date(2026, 9, 4)
        assert TANGO_LINKEDIN.number_for_date(dt.date(2026, 9, 4)) == 697
        assert TANGO_LINKEDIN.date_for_number(695) == dt.date(2026, 9, 2)
        assert TANGO_LINKEDIN.number_for_date('2026-09-05') == 698

    def test_no_legacy_ordinal_numbers_unlike_queens(self):
        day = dt.date(2026, 9, 4)
        assert TANGO_LINKEDIN.puzzle_numbers_for_date(day) == [697]
        queens_numbers = QUEENS_GAME.linkedin.puzzle_numbers_for_date(
            dt.date(2026, 6, 8))
        assert queens_numbers == [769, dt.date(2026, 6, 8).toordinal()]

    def test_current_puzzle_number_uses_linkedin_pacific_day(self, monkeypatch):
        monkeypatch.setattr(
            linkedin_module, 'linkedin_current_puzzle_date',
            lambda now=None: dt.date(2026, 9, 6))
        assert TANGO_LINKEDIN.current_puzzle_number() == 699

    def test_projected_message_ids_are_game_scoped(self):
        queens_id = _linkedin_result_message_id(QUEENS_GAME, 1, '2026-09-04', 300)
        tango_id = _linkedin_result_message_id(TANGO_GAME, 1, '2026-09-04', 300)
        assert queens_id != tango_id
        # Queens' key is byte-for-byte what it was before Tango existed.
        assert queens_id == _queens_result_message_id(1, '2026-09-04', 300)

    def test_linkedin_def_defaults(self):
        generic = LinkedInDef(
            anchor_date=dt.date(2026, 1, 1), anchor_number=1,
            admins_key='x_admin_user_ids')
        assert generic.link_namespace == 'linkedin'
        assert generic.puzzle_numbers_for_date(dt.date(2026, 1, 3)) == [3]


class TestTangoParsing:
    @pytest.mark.parametrize('content, number, seconds, day', [
        (_SHARE_INLINE, 697, 11, dt.date(2026, 9, 4)),
        (_SHARE_EMOJI, 695, 17, dt.date(2026, 9, 2)),
        (_SHARE_WRAPPED, 695, 8, dt.date(2026, 9, 2)),
    ])
    def test_share_shapes(self, content, number, seconds, day):
        (result,) = parse_tango_message(content)
        assert result.puzzle_number == number
        assert result.time_seconds == seconds
        assert result.puzzle_date == day
        assert result.is_perfect and result.accuracy == 100

    def test_header_words_do_not_cross_over(self):
        assert parse_tango_message('Queens #774 | 1:26') == []
        assert parse_queens_message('Tango #697 | 0:11') == []
        assert TANGO_GAME.detect.search('Tango #697')
        assert not TANGO_GAME.detect.search('Queens #774')

    def test_leaderboard_parser_is_shared_and_unchanged(self):
        assert parse_queens_leaderboard is parse_linkedin_leaderboard
        entries = parse_linkedin_leaderboard(_LEADERBOARD)
        assert [(e.linkedin_name, e.time_seconds, e.no_hints, e.no_mistakes,
                 e.is_you) for e in entries] == [
            ('Artsiom Savich', 11, True, True, True),
            ('Justin Goh', 13, True, True, False),
            ('Hussein Farhat', 43, True, True, False),
            ('Sami Almajali', 48, True, False, False),
            ('William Chittick', 151, True, True, False),
        ]


class TestSharedLinkedInRegistration(_QueensCommandsBase):
    """One registration resolves stored results in every LinkedIn game."""

    @staticmethod
    def _seed_unresolved(db, game, name, day, number, seconds):
        db.save_minigame_unresolved_result(
            100, game.name, normalize_queens_name(name), name, 200,
            number, day, 100, seconds, True, 'source')

    def test_registering_through_queens_claims_tango_results_too(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        db.set_guild_config(100, 'tango', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        name = 'Artsiom Savich'
        self._seed_unresolved(db, QUEENS_GAME, name, '2026-06-08', 769, 5)
        self._seed_unresolved(db, TANGO_GAME, name, '2026-09-04', 697, 11)
        cog = Minigames(bot=None)

        asyncio.run(cog._cmd_queens_register(ctx, QUEENS_GAME, alice, name))

        link = db.get_minigame_player_link(100, 'linkedin', alice.id)
        assert link.external_name == name
        # Both games project the shared link onto the user.
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, 769) is not None
        assert db.get_minigame_result_for_user_puzzle(
            100, 'tango', alice.id, 697) is not None
        assert db.get_minigame_rating(100, 'queens', alice.id) is not None
        assert db.get_minigame_rating(100, 'tango', alice.id) is not None
        assert 'LinkedIn Queens/Tango' in ctx.sent['embed'].description
        # Registering again through Tango is refused: it is the same link.
        with pytest.raises(minigames_module.MinigameCogError):
            asyncio.run(cog._cmd_queens_register(ctx, TANGO_GAME, alice, name))

    def test_unregistering_through_tango_drops_both_projections(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        db.set_guild_config(100, 'tango', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        name = 'Artsiom Savich'
        self._seed_unresolved(db, QUEENS_GAME, name, '2026-06-08', 769, 5)
        self._seed_unresolved(db, TANGO_GAME, name, '2026-09-04', 697, 11)
        cog = Minigames(bot=None)
        asyncio.run(cog._cmd_queens_register(ctx, TANGO_GAME, alice, name))
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, 769) is not None

        asyncio.run(cog._cmd_queens_unregister(ctx, TANGO_GAME, None))

        assert db.get_minigame_player_link(100, 'linkedin', alice.id) is None
        assert db.get_minigame_result_for_user_puzzle(
            100, 'queens', alice.id, 769) is None
        assert db.get_minigame_result_for_user_puzzle(
            100, 'tango', alice.id, 697) is None
        # Source rows survive under the LinkedIn name for a later re-link.
        assert db.get_minigame_unresolved_results_for_name(
            100, 'tango', normalize_queens_name(name))
        assert db.get_minigame_unresolved_results_for_name(
            100, 'queens', normalize_queens_name(name))

    def test_optout_stays_per_game(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        db.set_guild_config(100, 'queens', '1')
        db.set_guild_config(100, 'tango', '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)
        asyncio.run(cog._cmd_queens_register(
            ctx, QUEENS_GAME, alice, 'Artsiom Savich'))

        asyncio.run(cog._cmd_queens_optout(ctx, TANGO_GAME))

        assert db.is_minigame_opted_out(100, 'tango', alice.id)
        assert not db.is_minigame_opted_out(100, 'queens', alice.id)

    def test_delegated_admins_are_per_game(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = Minigames(bot=None)
        member = _FakeDiscordMember(301, 'mod', 'Mod')
        cog._set_linkedin_admin_ids(100, TANGO_GAME, ['301'])
        assert cog._has_tango_mod_access(100, member)
        assert not cog._has_queens_mod_access(100, member)
        assert db.get_guild_config(100, 'tango_admin_user_ids')
        assert db.get_guild_config(100, 'queens_admin_user_ids') is None


class TestTangoIngest:
    def test_channel_share_is_ingested_under_tango_only(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'tango', '1')
        db.set_minigame_channel(1, 'tango', 10)
        db.set_minigame_player_link(
            1, 'linkedin', 999, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        cog = Minigames(bot=None)

        asyncio.run(cog.on_message(_FakeMessage(5, 1, 10, 999, _SHARE_INLINE)))

        row = db.get_minigame_result_for_user_puzzle(1, 'tango', 999, 697)
        assert row is not None and row.time_seconds == 11
        assert db.get_minigame_result_for_user_puzzle(
            1, 'queens', 999, 697) is None
        assert db.get_minigame_rating(1, 'tango', 999) is not None
        # The live share is canonicalized under the LinkedIn name for Tango.
        sources = db.get_minigame_unresolved_results_for_name(
            1, 'tango', normalize_queens_name('Alice LinkedIn'))
        assert [int(s.puzzle_number) for s in sources] == [697]

    def _shared_channel_cog(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        for game in ('queens', 'tango'):
            db.set_guild_config(1, game, '1')
            db.set_minigame_channel(1, game, 10)
        db.set_minigame_player_link(
            1, 'linkedin', 999, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        return Minigames(bot=None)

    def test_one_channel_routes_each_share_to_its_own_game(
            self, db, monkeypatch):
        cog = self._shared_channel_cog(db, monkeypatch)
        queens_share = 'Queens #857 | 1:26\nlnkd.in/queens.'

        asyncio.run(cog.on_message(_FakeMessage(5, 1, 10, 999, _SHARE_INLINE)))
        asyncio.run(cog.on_message(_FakeMessage(6, 1, 10, 999, queens_share)))

        tango = db.get_minigame_result_for_user_puzzle(1, 'tango', 999, 697)
        queens = db.get_minigame_result_for_user_puzzle(1, 'queens', 999, 857)
        assert tango is not None and tango.time_seconds == 11
        assert queens is not None and queens.time_seconds == 86
        assert db.get_minigame_result_for_user_puzzle(
            1, 'queens', 999, 697) is None
        assert db.get_minigame_result_for_user_puzzle(
            1, 'tango', 999, 857) is None

    def test_one_channel_edit_that_breaks_a_share_cleans_up_under_tango(
            self, db, monkeypatch):
        cog = self._shared_channel_cog(db, monkeypatch)
        asyncio.run(cog.on_message(_FakeMessage(5, 1, 10, 999, _SHARE_INLINE)))
        recomputed = []
        original = cog._recompute_game_ratings
        monkeypatch.setattr(
            cog, '_recompute_game_ratings',
            lambda guild_id, game: (
                recomputed.append(game.name), original(guild_id, game)))

        before = _FakeMessage(5, 1, 10, 999, _SHARE_INLINE)
        after = _FakeMessage(5, 1, 10, 999, 'never mind, deleted my share')
        asyncio.run(cog.on_message_edit(before, after))

        assert recomputed == ['tango']
        assert db.get_minigame_result_for_user_puzzle(
            1, 'tango', 999, 697) is None
        assert db.get_minigame_unresolved_results_for_name(
            1, 'tango', normalize_queens_name('Alice LinkedIn')) == []

    def test_one_channel_plain_chat_falls_back_to_first_game(
            self, db, monkeypatch):
        cog = self._shared_channel_cog(db, monkeypatch)
        message = _FakeMessage(7, 1, 10, 999, 'good morning everyone')
        assert cog._game_for_channel(message) is QUEENS_GAME

    def test_raw_delete_probes_every_linkedin_game(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'tango', '1')
        db.set_minigame_channel(1, 'tango', 10)
        db.set_minigame_player_link(
            1, 'linkedin', 999, 'Alice LinkedIn',
            normalize_queens_name('Alice LinkedIn'), None, 1.0, 999)
        cog = Minigames(bot=None)
        asyncio.run(cog.on_message(_FakeMessage(5, 1, 10, 999, _SHARE_INLINE)))
        recomputed = []
        original = cog._recompute_minigame_ratings
        monkeypatch.setattr(
            cog, '_recompute_minigame_ratings',
            lambda guild_id, game, **kw: (
                recomputed.append(game.name), original(guild_id, game, **kw)))

        asyncio.run(cog.on_raw_message_delete(
            SimpleNamespace(guild_id=1, message_id=5)))

        assert recomputed == ['tango']
        assert db.get_minigame_unresolved_results_for_name(
            1, 'tango', normalize_queens_name('Alice LinkedIn')) == []
        assert db.get_minigame_result_for_user_puzzle(
            1, 'tango', 999, 697) is None


class TestUpgrade157:
    @staticmethod
    def _conn():
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        return conn

    def test_moves_queens_links_into_shared_namespace(self):
        conn = self._conn()
        conn.execute('''
            CREATE TABLE minigame_player_link (
                guild_id TEXT NOT NULL, game TEXT NOT NULL,
                user_id TEXT NOT NULL, external_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL, external_url TEXT,
                linked_at REAL NOT NULL, linked_by TEXT NOT NULL,
                PRIMARY KEY (guild_id, game, user_id),
                UNIQUE (guild_id, game, normalized_name))''')
        conn.execute(
            "INSERT INTO minigame_player_link VALUES "
            "('1', 'queens', '300', 'Alice', 'alice', NULL, 1.0, '300')")
        conn.execute(
            "INSERT INTO minigame_player_link VALUES "
            "('1', 'queens', '301', 'Bob', 'bob', 'tle:queens:anonymous', "
            "1.0, '301')")
        conn.commit()

        upgrade_1_57_0(conn)
        upgrade_1_57_0(conn)  # idempotent

        rows = conn.execute(
            'SELECT game, user_id, external_url FROM minigame_player_link '
            'ORDER BY user_id').fetchall()
        assert [(r.game, r.user_id, r.external_url) for r in rows] == [
            ('linkedin', '300', None),
            ('linkedin', '301', 'tle:queens:anonymous'),
        ]

    def test_tolerates_missing_table(self):
        conn = self._conn()
        upgrade_1_57_0(conn)  # a DB that predates the minigame tables
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'minigame_player_link'"
        ).fetchone() is None

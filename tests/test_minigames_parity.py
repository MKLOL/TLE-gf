"""Akari/Queens parity: forward-only queens ingest bans, shared date formats,
and the queens export / import-orphans wiring."""
import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle import constants
from tle.cogs import minigames as minigames_module
from tle.util import codeforces_common as cf_common
from tle.util.cf_format import ParamParseError, parse_date
from tle.cogs._minigame_common import parse_date_args
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs.minigames import (
    Minigames, MinigameCogError, _maybe_parse_puzzle_selector,
)

from tests.minigames_test_utils import (
    db, _FakeGuild, _FakeDiscordMember, _FakeMessage, _QueensCommandsBase,
)


class TestQueensIngestBanGate(_QueensCommandsBase):
    """Queens channel shares from banned users are blocked like Akari's."""

    @staticmethod
    def _enable_queens(db, guild=1, channel=10):
        db.set_guild_config(guild, 'queens', '1')
        db.set_minigame_channel(guild, 'queens', channel)

    @staticmethod
    def _queens_share(msg_id, user_id, guild=1, channel=10):
        return _FakeMessage(
            msg_id, guild, channel, user_id,
            'Queens #774 | 1:26\nlnkd.in/queens.')

    def test_banned_user_share_is_dropped_with_notice(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable_queens(db)
        db.ban_minigame_user(1, 'queens', 300, 1.0, 999, 'smurfing')
        cog = Minigames(bot=None)

        message = self._queens_share(1, 300)
        asyncio.run(cog.on_message(message))

        assert db.get_minigame_result(1) is None
        assert message.replies  # ban notice sent
        assert cog._is_ingest_banned(1, 300, QUEENS_GAME) is True

    def test_banned_user_chat_stays_silent(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable_queens(db)
        db.ban_minigame_user(1, 'queens', 300, 1.0, 999, None)
        cog = Minigames(bot=None)

        message = _FakeMessage(1, 1, 10, 300, 'gg everyone, nice puzzles')
        asyncio.run(cog.on_message(message))

        assert message.replies == []
        assert db.get_minigame_result(1) is None

    def test_unbanned_user_share_still_saves(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        self._enable_queens(db)
        cog = Minigames(bot=None)

        message = self._queens_share(1, 300)
        asyncio.run(cog.on_message(message))

        row = db.get_minigame_result(1)
        assert row is not None and row.puzzle_number == 774
        assert message.replies == []


class TestSharedDateFormats:
    """Both games accept ISO, slashed, day-first, and ddmmyyyy dates."""

    def test_parse_date_accepts_all_forms(self):
        expected = parse_date('01062026')
        assert parse_date('2026-06-01') == expected
        assert parse_date('2026/06/01') == expected
        assert parse_date('01-06-2026') == expected
        assert parse_date('01/06/2026') == expected
        with pytest.raises(ParamParseError):
            parse_date('junk-value')

    def test_akari_selector_accepts_iso_dates(self):
        assert _maybe_parse_puzzle_selector('2026-03-27') == (
            'day', dt.date(2026, 3, 27))
        assert _maybe_parse_puzzle_selector('27-03-2026') == (
            'day', dt.date(2026, 3, 27))
        assert _maybe_parse_puzzle_selector('27032026') == (
            'day', dt.date(2026, 3, 27))
        assert _maybe_parse_puzzle_selector('#450') == ('puzzle', 450)

    def test_date_filter_args_accept_iso(self):
        dlo, dhi, _plo, _phi = parse_date_args(
            ['d>=2026-06-01', 'd<2026-06-08'])
        assert dlo == parse_date('01062026')
        assert dhi == parse_date('08062026')

    def test_akari_date_or_number_matches_queens_formats(self):
        parse = Minigames._parse_akari_date_or_number
        assert parse('2026-03-27') == dt.date(2026, 3, 27)
        assert parse('27032026') == dt.date(2026, 3, 27)
        assert parse('#446') == dt.date(2026, 3, 27)
        with pytest.raises(MinigameCogError, match='Could not parse'):
            parse('nonsense')

    def test_queens_date_parser_accepts_ddmmyyyy(self):
        assert minigames_module._parse_queens_date('08062026') == (
            dt.date(2026, 6, 8))
        assert minigames_module._parse_queens_date('2026-06-08') == (
            dt.date(2026, 6, 8))


class TestQueensExportOrphansWiring(_QueensCommandsBase):
    def test_import_orphans_reports_clean_state(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(
            minigames_module.discord_common, 'embed_success',
            lambda desc: SimpleNamespace(description=desc))
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(Minigames.queens_import_orphans.__wrapped__(cog, ctx))
        assert 'No import-only' in ctx.sent['embed'].description

    def test_import_orphans_lists_orphaned_imported_rows(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(100, members=[alice])
        ctx = self._make_ctx(guild, alice)
        cog = Minigames(bot=object())

        db.save_imported_minigame_result(
            5, 100, 'queens', 200, alice.id, 769, '2026-06-08',
            100, 42, True, 'imported')

        pages = []
        monkeypatch.setattr(
            minigames_module.paginator, 'paginate',
            lambda _bot, _channel, page_list, **_kw: pages.extend(page_list))
        asyncio.run(Minigames.queens_import_orphans.__wrapped__(cog, ctx))
        assert pages
        assert '#769' in pages[0][1].description

    def test_export_command_is_wired_for_queens(self):
        # The export/diff bodies are game-generic; the wiring just has to
        # pass QUEENS_GAME through.
        assert Minigames.queens_export.__wrapped__ is not None
        assert Minigames.queens_diff.__wrapped__ is not None
        assert AKARI_GAME.name != QUEENS_GAME.name  # sanity


def _save_akari_result(db, message_id, user_id, puzzle_number, *,
                       time_seconds=90, is_perfect=True, accuracy=100):
    from tle.cogs._minigame_akari import puzzle_date_for
    db.save_minigame_result(
        message_id, 1, 'akari', 10, user_id, puzzle_number,
        puzzle_date_for(puzzle_number).isoformat(), accuracy, time_seconds,
        is_perfect, 'raw')


class TestAkariBanVisibility(_QueensCommandsBase):
    """Akari bans hide at display time (queens-style) — unban restores."""

    def _setup(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        monkeypatch.setattr(minigames_module, 'expected_puzzle_number',
                            lambda _date: 448)
        for name in ('embed_success', 'embed_neutral', 'embed_alert'):
            monkeypatch.setattr(
                minigames_module.discord_common, name,
                lambda desc: SimpleNamespace(description=desc))
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        mod = _FakeDiscordMember(
            999, 'mod', 'Mod',
            roles=[SimpleNamespace(name=constants.TLE_MODERATOR)])
        guild = _FakeGuild(1, members=[alice, bob, mod])
        _save_akari_result(db, 1, alice.id, 446)
        _save_akari_result(db, 2, bob.id, 446, time_seconds=100)
        cog = Minigames(bot=None)
        cog._recompute_akari_ratings(1)
        return cog, guild, alice, bob, mod

    def test_ban_hides_without_optout_and_unban_restores(
            self, db, monkeypatch):
        cog, guild, alice, bob, mod = self._setup(db, monkeypatch)
        ctx = self._make_ctx(guild, mod)

        asyncio.run(Minigames.akari_ban.__wrapped__(cog, ctx, bob))

        # No opt-out row is written; hiding is display-time only.
        assert db.is_akari_opted_out(1, bob.id) is False
        assert db.is_akari_banned(1, bob.id) is True
        # Existing results stay rated.
        assert {row.user_id for row in db.get_akari_ratings(1)} == {
            '300', '301'}

        captured = []
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file',
            lambda guild, rating_rows, registrants, **kwargs: captured.append(
                [row.user_id for row in rating_rows]) or object())
        asyncio.run(cog._cmd_akari_ratings(ctx))
        assert captured[-1] == ['300']
        asyncio.run(cog._cmd_akari_ratings_debug(ctx))
        assert set(captured[-1]) == {'300', '301'}

        # Public graph views refuse banned players.
        with pytest.raises(MinigameCogError, match='banned'):
            asyncio.run(cog._cmd_akari_rating(ctx, [bob]))
        with pytest.raises(MinigameCogError, match='banned'):
            asyncio.run(cog._cmd_akari_history(ctx, bob))

        # Unban restores visibility immediately — no re-register needed.
        asyncio.run(Minigames.akari_unban.__wrapped__(cog, ctx, bob))
        asyncio.run(cog._cmd_akari_ratings(ctx))
        assert set(captured[-1]) == {'300', '301'}

    def test_self_optout_survives_ban_unban_roundtrip(self, db, monkeypatch):
        cog, guild, alice, bob, mod = self._setup(db, monkeypatch)
        ctx = self._make_ctx(guild, mod)
        db.unregister_akari_user(1, bob.id, 1.0)  # bob opted out himself

        asyncio.run(Minigames.akari_ban.__wrapped__(cog, ctx, bob))
        asyncio.run(Minigames.akari_unban.__wrapped__(cog, ctx, bob))

        # His own opt-out is untouched: still hidden until *he* re-registers.
        assert db.is_akari_opted_out(1, bob.id) is True


class TestGamesCountsContestedDaysOnly:
    """A solo day is not a game — for either minigame."""

    def test_akari_games_column_ignores_solo_days(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, 'akari', '1')
        _save_akari_result(db, 1, 300, 446)
        _save_akari_result(db, 2, 301, 446, time_seconds=100)
        _save_akari_result(db, 3, 300, 448)  # solo day — not counted
        cog = Minigames(bot=None)
        cog._recompute_akari_ratings(1)

        by_user = {row.user_id: row for row in db.get_akari_ratings(1)}
        assert by_user['300'].games == 1
        assert by_user['301'].games == 1

    def test_queens_games_column_ignores_solo_days(self, db, monkeypatch):
        from tle.cogs._minigame_queens import normalize_queens_name
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(100, 'queens', '1')
        for user_id, name in ((300, 'Alice LinkedIn'), (301, 'Bob LinkedIn')):
            db.set_minigame_player_link(
                100, 'queens', user_id, name, normalize_queens_name(name),
                None, 1.0, 300)
        db.save_minigame_result(
            1, 100, 'queens', 200, 300, 769, '2026-06-08', 100, 5, True, 'x')
        db.save_minigame_result(
            2, 100, 'queens', 200, 301, 769, '2026-06-08', 100, 6, True, 'x')
        db.save_minigame_result(
            3, 100, 'queens', 200, 300, 770, '2026-06-09', 100, 4, True, 'x')
        cog = Minigames(bot=None)
        cog._recompute_minigame_ratings(100, QUEENS_GAME)

        by_user = {row.user_id: row
                   for row in db.get_minigame_ratings(100, 'queens')}
        assert by_user['300'].games == 1  # solo 06-09 not counted
        assert by_user['301'].games == 1


class TestChannelClearStandardization(_QueensCommandsBase):
    def _ctx(self, db, monkeypatch, flag):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(1, flag, '1')
        for name in ('embed_success', 'embed_neutral'):
            monkeypatch.setattr(
                minigames_module.discord_common, name,
                lambda desc: SimpleNamespace(description=desc))
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(1, members=[alice])
        return self._make_ctx(guild, alice)

    def test_queens_clear_unsets_channel_and_guards_args(
            self, db, monkeypatch):
        ctx = self._ctx(db, monkeypatch, 'queens')
        db.set_minigame_channel(1, 'queens', 200)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='queens delete'):
            asyncio.run(Minigames.queens_channel_clear.__wrapped__(
                cog, ctx, '2026-06-08'))
        assert db.get_minigame_channel(1, 'queens') == '200'

        asyncio.run(Minigames.queens_channel_clear.__wrapped__(cog, ctx))
        assert db.get_minigame_channel(1, 'queens') is None

    def test_akari_clear_guards_args(self, db, monkeypatch):
        ctx = self._ctx(db, monkeypatch, 'akari')
        db.set_minigame_channel(1, 'akari', 10)
        cog = Minigames(bot=None)

        with pytest.raises(MinigameCogError, match='akari delete'):
            asyncio.run(Minigames.akari_clear.__wrapped__(cog, ctx, '446'))
        assert db.get_minigame_channel(1, 'akari') == '10'

        asyncio.run(Minigames.akari_clear.__wrapped__(cog, ctx))
        assert db.get_minigame_channel(1, 'akari') is None


class TestSlashGroupChildLimit:
    """Discord hard-caps an app-command group at 25 direct children.

    conftest stubs discord.py, so the real ``Group.add_command`` ValueError
    never fires in tests — guard at the source level instead.
    """

    @staticmethod
    def _direct_children(source, group_attr):
        # Count decorator uses plus nested groups declared with this parent.
        commands = source.count(f'@{group_attr}.command(')
        subgroups = source.count(f'parent={group_attr})')
        return commands + subgroups

    @pytest.mark.parametrize('module_path,group_attr', [
        ('tle/cogs/_mgcmds_queensslash.py', 'queens_slash'),
        ('tle/cogs/_mgcmds_akarislash.py', 'akari_slash'),
    ])
    def test_group_stays_under_discord_limit(self, module_path, group_attr):
        import pathlib
        root = pathlib.Path(__file__).resolve().parent.parent
        source = (root / module_path).read_text(encoding='utf-8')
        assert self._direct_children(source, group_attr) <= 25, (
            f'{group_attr} exceeds the 25-subcommand Discord limit; '
            'nest related commands in a child Group')


class TestExportGuildScoping:
    """Export snapshots contain only the requesting guild's rows."""

    def test_export_excludes_other_guilds(self, db, monkeypatch, tmp_path):
        import sqlite3
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'TEMP_DIR', str(tmp_path), raising=False)
        db.save_minigame_result(
            1, 1, 'queens', 10, 300, 769, '2026-06-08', 100, 42, True, 'raw')
        db.save_minigame_result(
            2, 2, 'queens', 20, 400, 769, '2026-06-08', 100, 43, True, 'raw')

        exported = []

        async def _send(content=None, file=None, **kw):
            # Read the snapshot inside send — the command deletes it after.
            snap = sqlite3.connect(file.fp)
            try:
                exported.extend(snap.execute(
                    'SELECT guild_id FROM minigame_result').fetchall())
            finally:
                snap.close()

        cog = Minigames(bot=None)
        ctx = SimpleNamespace(
            guild=SimpleNamespace(id=1),
            message=SimpleNamespace(id=99),
            send=_send,
        )
        asyncio.run(cog._cmd_akari_export(ctx, QUEENS_GAME))
        assert exported == [('1',)]

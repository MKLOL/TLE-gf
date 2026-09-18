"""Queens weekly-rating command integration tests."""

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as minigames_module
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
    QUEENS_WEEKDAY_DIFFICULTIES,
    normalize_queens_name,
    queens_weekly_difficulty_map,
)
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import (
    _FakeChannel,
    _FakeDiscordMember,
    _FakeGuild,
    _FakeInteraction,
    _QueensCommandsBase,
    db,
)


_GUILD_ID = 100


def _logical_today(monkeypatch, value):
    """Patch every supported lookup site for Queens' Pacific puzzle date."""
    from tle.cogs import _mgimpl_queenscmd as queens_cmd_impl
    from tle.cogs import _minigame_queens_cog as queens_helpers

    monkeypatch.setattr(
        queens_helpers, '_queens_current_puzzle_date', lambda _now=None: value)
    monkeypatch.setattr(
        queens_cmd_impl, '_queens_current_puzzle_date',
        lambda _now=None: value, raising=False)
    monkeypatch.setattr(
        minigames_module, '_queens_current_puzzle_date',
        lambda _now=None: value)


def _register(db, member, linkedin_name, *, anonymous=False):
    marker = (
        minigames_module._QUEENS_ANONYMOUS_LINK_MARKER
        if anonymous else None
    )
    db.set_minigame_player_link(
        _GUILD_ID,
        QUEENS_GAME.link_key,
        member.id,
        linkedin_name,
        normalize_queens_name(linkedin_name),
        marker,
        1.0,
        member.id,
    )


def _capture_ctx(guild, author):
    sent = []

    async def send(content=None, *, embed=None, **kwargs):
        sent.append({'content': content, 'embed': embed, **kwargs})

    return (
        SimpleNamespace(
            guild=guild,
            author=author,
            channel=_FakeChannel(200),
            send=send,
        ),
        sent,
    )


def _install_render_spies(monkeypatch):
    captured = {'ratings': [], 'scores': []}

    def ratings_renderer(guild, rows, registrants, **kwargs):
        record = {
            'guild': guild,
            'rows': list(rows),
            'registrants': set(registrants),
            **kwargs,
        }
        captured['ratings'].append(record)
        return ('ratings', len(captured['ratings']))

    def scores_renderer(guild, standings, *, title, **kwargs):
        record = {
            'guild': guild,
            'standings': list(standings),
            'title': title,
            **kwargs,
        }
        captured['scores'].append(record)
        return ('scores', len(captured['scores']))

    monkeypatch.setattr(
        minigames_module,
        '_get_akari_rating_table_image_file',
        ratings_renderer,
    )
    monkeypatch.setattr(
        minigames_module,
        '_get_akari_weekly_table_image_file',
        scores_renderer,
    )
    return captured


class TestQueensWeeklyRatings(_QueensCommandsBase):
    def test_weekday_difficulties_follow_linkedin_weekly_ramp(self):
        monday = dt.date(2030, 1, 7)
        rows = [
            SimpleNamespace(
                puzzle_number=100 + offset,
                puzzle_date=monday + dt.timedelta(days=offset),
            )
            for offset in (0, 3, 6)
        ]

        difficulties = queens_weekly_difficulty_map(rows)

        assert QUEENS_WEEKDAY_DIFFICULTIES == (1, 1, 2, 2, 3, 3, 4)
        assert difficulties == {
            100: 1,  # Monday: Easy
            101: 1,  # Tuesday: Easy
            102: 2,  # Wednesday: Medium
            103: 2,  # Thursday: Medium
            104: 3,  # Friday: Hard
            105: 3,  # Saturday: Hard
            106: 4,  # Sunday: Very Hard
        }

    def test_weekly_preview_values_sunday_more_than_monday(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        _register(db, alice, 'Alice LinkedIn')
        _register(db, bob, 'Bob LinkedIn')
        sunday = dt.date(2030, 1, 13)
        monday = sunday - dt.timedelta(days=6)
        _logical_today(monkeypatch, sunday)

        self._save_queens_result(
            db, 1, alice.id, monday.isoformat(), 60)
        self._save_queens_result(
            db, 2, bob.id, sunday.isoformat(), 60)

        _ratings, standings = Minigames(
            bot=None)._queens_weekly_preview(_GUILD_ID, QUEENS_GAME)

        assert [row.user_id for row in standings] == [
            str(bob.id), str(alice.id),
        ]
        assert standings[0].score > standings[1].score

    def test_weekly_sends_ratings_and_pacific_current_week_scores(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(_GUILD_ID, QUEENS_GAME.feature_flag, '1')

        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(_GUILD_ID, members=[alice, bob])
        _register(db, alice, 'Private Alice', anonymous=True)
        _register(db, bob, 'Bob LinkedIn')

        # A fixed logical date proves the preview uses LinkedIn's puzzle date
        # source rather than the host machine's local ``date.today()``.
        today = dt.date(2030, 1, 9)  # Wednesday
        _logical_today(monkeypatch, today)
        current_monday = today - dt.timedelta(days=today.weekday())
        previous_monday = current_monday - dt.timedelta(days=7)

        # Alice wins the completed week; Bob leads the provisional current one.
        self._save_queens_result(
            db, 1, alice.id, previous_monday.isoformat(), 60)
        self._save_queens_result(
            db, 2, bob.id, previous_monday.isoformat(), 120)
        self._save_queens_result(
            db, 3, alice.id, current_monday.isoformat(), 120)
        self._save_queens_result(
            db, 4, bob.id, current_monday.isoformat(), 60)

        captured = _install_render_spies(monkeypatch)
        ctx, sent = _capture_ctx(guild, alice)
        cog = Minigames(bot=None)

        asyncio.run(cog._cmd_queens_ratings(ctx, QUEENS_GAME, weekly=True))

        assert [item['file'][0] for item in sent] == ['ratings', 'scores']
        rating_call = captured['ratings'][0]
        assert 'weekly preview' in rating_call['title'].lower()
        assert rating_call['games_label'] == 'Weeks'
        assert rating_call['identity_label'] == 'LinkedIn'
        assert rating_call['rows'][0].user_id == str(alice.id)
        assert rating_call['rows'][0].rating > rating_call['rows'][1].rating
        assert rating_call['identity_fn'](
            guild, rating_call['rows'][0]) == 'Anonymous'

        score_call = captured['scores'][0]
        assert score_call['standings'][0].user_id == str(bob.id)
        assert score_call['standings'][0].week_start == current_monday
        assert score_call['identity_label'] == 'LinkedIn'
        assert score_call['identity_fn'](
            guild, score_call['standings'][1]) == 'Anonymous'
        assert callable(score_call['name_fn'])
        assert score_call['filename'] == 'queens-weekly-scores.png'

    def test_public_weekly_hides_banned_player_but_debug_includes_them(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(_GUILD_ID, QUEENS_GAME.feature_flag, '1')

        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        guild = _FakeGuild(_GUILD_ID, members=[alice, bob])
        _register(db, alice, 'Alice LinkedIn')
        _register(db, bob, 'Bob LinkedIn')
        db.ban_minigame_user(
            _GUILD_ID, QUEENS_GAME.name, bob.id, 1.0, alice.id, 'test')

        today = dt.date(2030, 1, 9)
        _logical_today(monkeypatch, today)
        monday = today - dt.timedelta(days=today.weekday())
        previous = monday - dt.timedelta(days=7)
        for offset, day in enumerate((previous, monday)):
            self._save_queens_result(
                db, 10 + offset * 2, alice.id, day.isoformat(), 120)
            self._save_queens_result(
                db, 11 + offset * 2, bob.id, day.isoformat(), 60)

        captured = _install_render_spies(monkeypatch)
        cog = Minigames(bot=None)
        public_ctx, _sent = _capture_ctx(guild, alice)
        debug_ctx, _debug_sent = _capture_ctx(guild, alice)

        asyncio.run(cog._cmd_queens_ratings(
            public_ctx, QUEENS_GAME, weekly=True, show_all=False))
        asyncio.run(cog._cmd_queens_ratings(
            debug_ctx, QUEENS_GAME, weekly=True, show_all=True))

        assert [row.user_id for row in captured['ratings'][0]['rows']] == [
            str(alice.id),
        ]
        assert [row.user_id
                for row in captured['scores'][0]['standings']] == [
            str(alice.id),
        ]
        assert {row.user_id for row in captured['ratings'][1]['rows']} == {
            str(alice.id), str(bob.id),
        }
        assert {row.user_id
                for row in captured['scores'][1]['standings']} == {
            str(alice.id), str(bob.id),
        }

    def test_weekly_and_beta_are_mutually_exclusive(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(_GUILD_ID, QUEENS_GAME.feature_flag, '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        guild = _FakeGuild(_GUILD_ID, members=[alice])
        ctx, _sent = _capture_ctx(guild, alice)
        cog = Minigames(bot=None)

        with pytest.raises(
                MinigameCogError,
                match=r'(?i)(weekly.*beta|beta.*weekly)'):
            asyncio.run(cog._cmd_queens_ratings(
                ctx, QUEENS_GAME, weekly=True, improved=True))

    def test_weekly_uses_queens_time_scoring_not_ingestion_badges(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(_GUILD_ID, QUEENS_GAME.feature_flag, '1')
        alice = _FakeDiscordMember(300, 'alice', 'Alice')
        bob = _FakeDiscordMember(301, 'bob', 'Bob')
        _register(db, alice, 'Alice LinkedIn')
        _register(db, bob, 'Bob LinkedIn')

        today = dt.date(2030, 1, 9)
        _logical_today(monkeypatch, today)
        previous = (
            today - dt.timedelta(days=today.weekday() + 7)
        )
        # Alice's faster row mimics a leaderboard import that retained a
        # mistake badge. Bob's slower row mimics a direct share, which cannot
        # retain that metadata. Weekly Queens must compare their times only.
        self._save_queens_result(
            db, 50, alice.id, previous.isoformat(), 30,
            is_perfect=False, accuracy=0)
        self._save_queens_result(
            db, 51, bob.id, previous.isoformat(), 60,
            is_perfect=True, accuracy=100)

        ratings, _standings = Minigames(
            bot=None)._queens_weekly_preview(_GUILD_ID, QUEENS_GAME)

        assert [row.user_id for row in ratings] == [
            str(alice.id), str(bob.id),
        ]
        assert ratings[0].rating > ratings[1].rating

    def test_prefix_and_slash_route_the_weekly_flag(self, monkeypatch):
        cog = Minigames(bot=None)
        author = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = SimpleNamespace(
            guild=SimpleNamespace(id=_GUILD_ID),
            author=author,
        )
        calls = []

        monkeypatch.setattr(
            cog, '_require_enabled', lambda *_args, **_kwargs: None)

        async def capture(_ctx, _game, **kwargs):
            calls.append(kwargs)

        monkeypatch.setattr(cog, '_cmd_queens_ratings', capture)
        asyncio.run(Minigames.queens_ratings.__wrapped__(
            cog, ctx, '+weekly'))

        interaction = _FakeInteraction(
            guild_id=_GUILD_ID, user_id=author.id)
        asyncio.run(cog.slash_queens_ratings(
            interaction, weekly=True))

        assert calls[0]['weekly'] is True
        assert calls[1]['weekly'] is True
        assert interaction.response.deferred is True

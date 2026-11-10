"""Hypothetical linked-result replay must never write official state."""
import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs import minigames as module
from tle.cogs.minigames import Minigames, MinigameCogError
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs._minigame_linkedin_all import all_linked_results
from tle.util import codeforces_common as cf_common
from tle.util.minigame_rating import compute_ratings
from tests.minigames_test_utils import (
    db, _FakeGuild, _FakeDiscordMember, _QueensCommandsBase,
)


@pytest.mark.parametrize('game', [QUEENS_GAME, TANGO_GAME])
class TestAllRatings(_QueensCommandsBase):
    def seed(self, db, monkeypatch, game):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = Minigames(bot=None)
        db.set_guild_config(100, game.name, '1')
        for uid in (300, 301, 302):
            db.set_minigame_player_link(
                100, 'linkedin', uid, str(uid), str(uid), None, 1.0, uid)
        db.optout_minigame_user(100, game.name, 301, 1.0)
        expected = []
        for day in range(3):
            date = dt.date(2026, 9, 4) + dt.timedelta(days=day)
            number = game.linkedin.number_for_date(date)
            for uid, seconds in [(300, 20 + day), (301, 5 + day),
                                 (302, 30 - day), (999, 1)]:
                db.save_minigame_unresolved_result(
                    100, game.name, str(uid), str(uid), 200, number,
                    date.isoformat(), 100, seconds, 1, 'raw',
                    is_rated=uid in (300, 999),
                    rating_override=False if uid == 302 else None)
                if uid != 999:
                    expected.append(SimpleNamespace(
                        user_id=str(uid), puzzle_number=number,
                        puzzle_date=date.isoformat(), accuracy=100,
                        time_seconds=seconds, is_perfect=True))
        cog._recompute_minigame_ratings(100, game)
        member = _FakeDiscordMember(300, 'alice', 'Alice')
        ctx = self._make_ctx(_FakeGuild(100, members=[member]), member)
        return cog, ctx, expected

    def test_full_replay_commands_and_no_writes(self, db, monkeypatch, game):
        cog, ctx, expected = self.seed(db, monkeypatch, game)
        before = list(db.conn.iterdump())
        oracle = compute_ratings(expected, **cog._rating_compute_kwargs(game))
        rows = cog._minigame_rating_rows(100, game, all_results=True)
        assert {r.user_id: r.rating for r in rows} == {
            uid: state.rating for uid, state in oracle.items()}
        assert all(row.games == 3 for row in rows)
        captured = {}
        def ratings(_guild, rows, _linked, **kwargs):
            captured.update(rows=rows, **kwargs)
            return object()
        monkeypatch.setattr(module, '_get_akari_rating_table_image_file', ratings)
        asyncio.run(getattr(Minigames, game.name + '_ratings').__wrapped__(
            cog, ctx, '+all'))
        assert {r.user_id for r in captured['rows']} == {'300', '301', '302'}
        assert 'hypothetical' in captured['title']
        def results(_guild, rows, title, **kwargs):
            captured.clear()
            captured.update(rows=rows, title=title, **kwargs)
            return object()
        monkeypatch.setattr(module, '_get_queens_results_table_image_file', results)
        asyncio.run(getattr(Minigames, game.name + '_results').__wrapped__(
            cog, ctx, '2026-09-06', '+all'))
        assert {r.user_id for r in captured['rows']} == {'300', '301', '302'}
        assert not captured['unrated_keys']
        histories = {}
        compute_ratings(expected, histories=histories, **cog._rating_compute_kwargs(game))
        for uid in ('300', '301', '302'):
            point = next(p for p in histories[uid] if p.puzzle_number == expected[-1].puzzle_number)
            assert captured['puzzle_info'][uid].delta == point.delta
            assert captured['puzzle_info'][uid].pre_rating == point.rating - point.delta
        assert list(db.conn.iterdump()) == before

    def test_filters_and_weekly_error(self, db, monkeypatch, game):
        cog, ctx, _ = self.seed(db, monkeypatch, game)
        rows = cog._filtered_minigame_result_rows(
            100, game, all_results=True, excluded_ids={'301'})
        assert {r.user_id for r in rows} == {'300', '302'}
        for command in ('results', 'ratings'):
            with pytest.raises(MinigameCogError, match='weekly'):
                asyncio.run(getattr(Minigames, game.name + '_' + command).__wrapped__(
                    cog, ctx, '+all', '+weekly'))

    def test_sources_override_stale_projections(self, db, monkeypatch, game):
        cog, _, _ = self.seed(db, monkeypatch, game)
        date = dt.date(2026, 9, 4)
        number = game.linkedin.number_for_date(date)
        db.save_minigame_result(123, 100, game.name, 200, 301, number,
                               date.isoformat(), 100, 99, True, 'stale')
        rows = all_linked_results(100, game)
        matches = [r for r in rows if r.user_id == '301' and r.puzzle_number == number]
        assert len(matches) == 1 and matches[0].time_seconds == 5

    def test_slash_options_forward(self, db, monkeypatch, game):
        cog, ctx, _ = self.seed(db, monkeypatch, game)
        calls = []
        async def capture(*args, **kwargs):
            calls.append((args, kwargs))
        interaction = SimpleNamespace(
            id=999, guild=ctx.guild, user=ctx.author, channel_id=200,
            client=None, response=SimpleNamespace(defer=capture))
        monkeypatch.setattr(cog, '_cmd_queens_results', capture)
        monkeypatch.setattr(cog, '_cmd_queens_ratings', capture)
        for command in ('results', 'ratings'):
            asyncio.run(getattr(cog, 'slash_' + game.name + '_' + command)(
                interaction, all_results=True))
        assert '+all' in calls[1][0][2]
        assert calls[3][1]['all_results'] is True

    def test_beta_replay_is_read_only(self, db, monkeypatch, game):
        cog, _, _ = self.seed(db, monkeypatch, game)
        before = list(db.conn.iterdump())
        rows = cog._minigame_rating_rows(100, game, improved=True, all_results=True)
        assert {r.user_id for r in rows} == {'300', '301', '302'}
        assert list(db.conn.iterdump()) == before

    @pytest.mark.parametrize('command', ['rating', 'performance'])
    @pytest.mark.parametrize('named', [False, True])
    @pytest.mark.parametrize('beta', [False, True])
    def test_personal_graph_all_results(self, db, monkeypatch, game, named, beta, command):
        cog, ctx, expected = self.seed(db, monkeypatch, game)
        ctx.author.name = 'nifeshe'
        before = list(db.conn.iterdump())
        captured = {}
        def plot(series, **kwargs):
            captured['series'] = series
            return SimpleNamespace(filename='plot.png')
        monkeypatch.setattr(module, 'plot_akari_' + command, plot)
        args = (['nifeshe'] if named else []) + ['+all']
        if beta:
            args.append('+beta')
        asyncio.run(getattr(Minigames, game.name + '_' + command).__wrapped__(
            cog, ctx, *args))
        histories = {}
        cog._minigame_rating_engine(game, beta)(
            expected, histories=histories,
            **cog._minigame_compute_kwargs(game, improved=beta))
        assert captured['series'][0][0] == histories['300']
        assert 'hypothetical' in ctx.sent['embed'].title
        assert list(db.conn.iterdump()) == before

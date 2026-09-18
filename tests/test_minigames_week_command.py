"""`;akari week` / `;queens week` command wiring for the weekly recap."""

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs import _mgimpl_weekly as weekly
from tle.cogs._mgimpl_weekly import _parse_week_anchor
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common


_MONDAY = dt.date(2026, 8, 3)


def _row(day, user_id, seconds, message_id):
    return SimpleNamespace(
        message_id=str(message_id), user_id=str(user_id),
        puzzle_number=day.toordinal(), puzzle_date=day.isoformat(),
        time_seconds=seconds, is_perfect=True, accuracy=100)


def _rows():
    rows, message_id = [], 0
    for offset in range(5):
        day = _MONDAY + dt.timedelta(days=offset)
        for user_id, seconds in (('10', 60 + offset), ('20', 90 + offset)):
            message_id += 1
            rows.append(_row(day, user_id, seconds, message_id))
    return rows


def _cog(monkeypatch, rows, *, today=_MONDAY):
    cog = Minigames(bot=None)
    monkeypatch.setattr(cog, '_require_enabled', lambda *a, **k: None)
    monkeypatch.setattr(cog, '_sync_minigame_results_for_read',
                        lambda *a, **k: None)
    monkeypatch.setattr(cog, '_filter_minigame_banned_rows',
                        lambda gid, game, rows: rows)
    monkeypatch.setattr(cog, '_filter_queens_registered_result_rows',
                        lambda gid, game, rows, **kw: rows)
    monkeypatch.setattr(cog, '_minigame_public_user_name',
                        lambda guild, game, uid: f'u{uid}')

    async def _difficulties(_numbers):
        return {}

    monkeypatch.setattr(cog, '_akari_difficulty_map', _difficulties)
    monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
        get_minigame_results_for_guild=lambda *a, **k: list(rows)))
    # Rebind only this module's ``dt`` name — patching ``weekly.dt.date``
    # would replace the real ``datetime.date`` class for the whole session.
    monkeypatch.setattr(weekly, 'dt', SimpleNamespace(
        date=SimpleNamespace(today=lambda: today,
                             fromisoformat=dt.date.fromisoformat),
        timedelta=dt.timedelta))
    monkeypatch.setattr(weekly, '_queens_current_puzzle_date', lambda: today)

    captured = {}
    monkeypatch.setattr(weekly, 'plot_weekly_recap',
                        lambda recap, name_fn, palette=None: captured.update(
                            recap=recap, palette=palette,
                            names=[name_fn(uid) for uid in ('10', '20')]) or
                        'FILE')
    return cog, captured


def _ctx(author_id=10):
    sent = {}

    async def send(**kwargs):
        sent.update(kwargs)

    return SimpleNamespace(
        guild=SimpleNamespace(id=111), channel=object(),
        author=SimpleNamespace(id=author_id), send=send), sent


class TestWeekAnchorParsing:
    def test_no_argument_anchors_today(self):
        assert _parse_week_anchor((), _MONDAY) == _MONDAY

    def test_last_anchors_the_previous_week(self):
        anchor = _parse_week_anchor(('last',), _MONDAY + dt.timedelta(days=3))
        assert anchor == _MONDAY - dt.timedelta(days=1)

    def test_explicit_date_is_used_verbatim(self):
        assert _parse_week_anchor(('2026-07-15',), _MONDAY) == dt.date(2026, 7, 15)

    def test_garbage_is_rejected(self):
        with pytest.raises(MinigameCogError, match='not a date'):
            _parse_week_anchor(('yesterday',), _MONDAY)

    def test_extra_arguments_are_rejected(self):
        with pytest.raises(MinigameCogError, match='Usage'):
            _parse_week_anchor(('2026-07-15', 'last'), _MONDAY)


class TestWeekCommand:
    def test_akari_week_sends_a_rendered_recap(self, monkeypatch):
        cog, captured = _cog(monkeypatch, _rows())
        ctx, sent = _ctx()
        asyncio.run(cog._cmd_week(ctx, AKARI_GAME))

        assert sent['file'] == 'FILE'
        recap = captured['recap']
        assert recap.week_start == _MONDAY
        assert recap.week_end == _MONDAY + dt.timedelta(days=6)
        assert recap.in_progress is True
        assert recap.player_count == 2
        # 10 is faster every day, so the board is decided outright.
        assert recap.leaders[0] == ('10', 5, 0)
        assert captured['names'] == ['u10', 'u20']

    def test_viewer_gets_their_own_personal_section(self, monkeypatch):
        cog, captured = _cog(monkeypatch, _rows())
        ctx, _sent = _ctx(author_id=20)
        asyncio.run(cog._cmd_week(ctx, AKARI_GAME))
        assert captured['recap'].personal.user_id == '20'
        assert captured['recap'].personal.solo_wins == 0

    def test_last_selects_the_previous_week(self, monkeypatch):
        rows = _rows()
        cog, captured = _cog(monkeypatch, rows,
                             today=_MONDAY + dt.timedelta(days=8))
        ctx, _sent = _ctx()
        asyncio.run(cog._cmd_week(ctx, AKARI_GAME, 'last'))
        assert captured['recap'].week_start == _MONDAY
        assert captured['recap'].in_progress is False

    def test_queens_uses_its_own_palette(self, monkeypatch):
        cog, captured = _cog(monkeypatch, _rows())
        ctx, _sent = _ctx()
        asyncio.run(cog._cmd_week(ctx, QUEENS_GAME))
        assert captured['palette'] is weekly.QUEENS_PALETTE

    def test_a_future_week_is_refused(self, monkeypatch):
        cog, _captured = _cog(monkeypatch, _rows())
        ctx, _sent = _ctx()
        with pytest.raises(MinigameCogError, match='future'):
            asyncio.run(cog._cmd_week(ctx, AKARI_GAME, '2027-01-04'))

    def test_an_empty_week_is_reported_not_rendered(self, monkeypatch):
        cog, _captured = _cog(monkeypatch, _rows(),
                              today=_MONDAY + dt.timedelta(days=30))
        ctx, _sent = _ctx()
        with pytest.raises(MinigameCogError, match='results for the week'):
            asyncio.run(cog._cmd_week(ctx, AKARI_GAME))

    def test_no_results_at_all_is_reported(self, monkeypatch):
        cog, _captured = _cog(monkeypatch, [])
        ctx, _sent = _ctx()
        with pytest.raises(MinigameCogError, match='have been posted yet'):
            asyncio.run(cog._cmd_week(ctx, AKARI_GAME))

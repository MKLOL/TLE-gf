"""Current weekly performance and first-days filtering."""
import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from tests.test_akari_weekly import _row
from tests.minigames_test_utils import db
from tle.util.akari_weekly import (
    annotate_weekly_standings, parse_weekly_days, score_week,
)
from tle.util.akari_rating import RatingState, _pow10, event_performance, compute_round
from tle.cogs._minigame_tables import _akari_weekly_table_rows


def test_performance_uses_completed_weekly_field_and_ties():
    monday = dt.date(2026, 6, 15)
    standings = score_week([_row('10', 526, monday),
                            _row('20', 526, monday)])
    ratings = [RatingState('10', 1600, 2, 1600, 0, 0, 525),
               RatingState('20', 1200, 2, 1200, 0, 0, 525)]
    result = annotate_weekly_standings(standings, ratings)
    expected = event_performance([_pow10(1600), _pow10(1200)], 1)
    assert result[0].performance == result[1].performance == expected
    assert [r.rating for r in result] == [1600, 1200]
    table = _akari_weekly_table_rows(
        None, result, name_fn=lambda g, r: r.user_id,
        identity_fn=lambda g, r: '-')
    assert len(table[0]) == 6
    assert table[0][0] == table[1][0] == 1
    assert table[0][2].startswith('1600')
    assert table[0][4] == table[1][4] == str(round(expected))
    expected_deltas = compute_round({'10': 1600, '20': 1200},
                                    {'10': 1, '20': 1}, damping=0.75)
    assert result[0].delta == expected_deltas['10']
    assert table[0][5] == f"{round(expected_deltas['10']):+d}"


def test_new_player_baseline_and_solo_performance():
    result = annotate_weekly_standings(
        score_week([_row('10', 526, dt.date(2026, 6, 15))]), [])
    assert result[0].rating == 1200
    assert result[0].performance is None
    assert result[0].delta is None


@pytest.mark.parametrize('args,current', [
    (('+days=0',), True), (('+days=8',), True),
    (('+days=no',), True), (('+days=2', '+days=3'), True),
    (('+days=2',), False),
])
def test_invalid_days(args, current):
    with pytest.raises(ValueError):
        parse_weekly_days(args, current=current)


def test_parse_days_preserves_other_filters():
    assert parse_weekly_days(('+days=3', '+dow=mon'), current=True) == (
        ('+dow=mon',), 3)


def test_first_days_filters_current_results_only(db, monkeypatch):
    from tle.cogs.minigames import Minigames
    from tle.util import codeforces_common as cf_common
    monkeypatch.setattr(cf_common, 'user_db', db)
    monday = dt.date(2026, 6, 15)
    rows = [_row('10', 519, monday - dt.timedelta(days=7)),
            _row('20', 519, monday - dt.timedelta(days=7), seconds=120),
            _row('10', 526, monday),
            _row('20', 528, monday + dt.timedelta(days=2))]
    monkeypatch.setattr(db, 'get_minigame_results_for_guild', lambda *a: rows)
    cog = Minigames(bot=None)
    async def difficulties(numbers):
        return {}
    monkeypatch.setattr(cog, '_akari_difficulty_map', difficulties)
    async def run():
        full = await cog._akari_weekly_preview(1, as_of_date=monday)
        filtered = await cog._akari_weekly_preview(
            1, as_of_date=monday, first_days=2)
        assert filtered[0] == full[0]
        assert [s.user_id for s in filtered[1]] == ['10']
        assert len(full[1]) == 2
    asyncio.run(run())


def test_command_forwards_day_limit(db, monkeypatch):
    from tle.cogs.minigames import Minigames
    from tests.minigames_test_utils import _FakeGuild, _FakeDiscordMember
    cog = Minigames(bot=None)
    ctx = SimpleNamespace(guild=_FakeGuild(1), author=_FakeDiscordMember(10, 'Alice'))
    captured = []
    async def ratings(ctx, **kwargs):
        captured.append(kwargs)
    monkeypatch.setattr(cog, '_cmd_akari_ratings', ratings)
    callback = getattr(cog.akari_ratings, 'callback', cog.akari_ratings)
    if hasattr(cog.akari_ratings, 'callback'):
        asyncio.run(callback(cog, ctx, '+current', '+days=3'))
    else:
        asyncio.run(callback(ctx, '+current', '+days=3'))
    assert captured[0]['first_days'] == 3
    assert captured[0]['current'] is True


def test_current_renderer_uses_rating_and_performance_colors(monkeypatch):
    from tle.cogs import minigames
    from tle.cogs._minigame_tables import (
        _get_akari_weekly_table_image_file, _akari_row_text_color,
    )
    standings = annotate_weekly_standings(score_week([
        _row('10', 526, dt.date(2026, 6, 15)),
        _row('20', 526, dt.date(2026, 6, 15), seconds=120),
    ]), [])
    monkeypatch.setattr(minigames, '_get_akari_puzzle_table_image',
                        lambda rows, **kwargs: (rows, kwargs))
    rows, options = _get_akari_weekly_table_image_file(
        None, standings, title='Current', name_fn=lambda g, r: r.user_id,
        identity_fn=lambda g, r: '-')
    assert options['header'] == ('#', 'Name', 'Rating', 'Score', 'Performance', 'Δ')
    assert options['center_header_cols'] == ()
    assert options['right_align_cols'] == (0, 3, 5)
    assert sum(options['cols']) == 860
    assert len(options['cell_colors'][0]) == len(rows[0]) == 6
    assert options['cell_colors'][0][2] == _akari_row_text_color(1200)
    assert options['cell_colors'][0][4] == _akari_row_text_color(
        standings[0].performance)

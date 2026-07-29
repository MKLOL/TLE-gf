"""Pure-model tests for the Queens seven-day stats dashboard."""

import datetime as dt
from types import SimpleNamespace

import pytest

from tle.cogs._queens_stats_dashboard import (
    _BG,
    _PANEL,
    _chart_time_cap,
    _queens_dashboard_data,
    _safe_player_name,
    _week_status,
)


def test_dashboard_uses_a_light_canvas_and_white_panels():
    def channels(color):
        return tuple(
            int(color[index:index + 2], 16) for index in (1, 3, 5))

    assert min(channels(_BG)) >= 240
    assert channels(_PANEL) == (255, 255, 255)


def _row(day, seconds=30, *, perfect=True, accuracy=100, message_id=1):
    if isinstance(day, str):
        day = dt.date.fromisoformat(day)
    return SimpleNamespace(
        message_id=str(message_id),
        user_id='10',
        puzzle_number=day.toordinal(),
        puzzle_date=day.isoformat(),
        time_seconds=seconds,
        is_perfect=perfect,
        accuracy=accuracy,
    )


def test_one_result_uses_explicit_current_week_and_day_states():
    monday = dt.date(2030, 1, 7)
    wednesday = monday + dt.timedelta(days=2)
    result = _row(monday, 42)

    data = _queens_dashboard_data(
        [result], as_of_date=wednesday)

    assert data['total'] == 1
    assert data['best_time'] == 42
    assert data['median_time'] == 42
    assert data['recent_median'] == 42
    assert data['week_start'] == monday
    assert data['view_date'] == wednesday
    assert data['week_rows'][monday] is result
    assert _week_status(result, monday, data['view_date'])[0] == 'PLAYED'
    assert _week_status(
        None, monday + dt.timedelta(days=1), data['view_date'])[0] == 'MISSED'
    assert _week_status(None, wednesday, data['view_date'])[0] == 'OPEN'
    assert _week_status(
        None, monday + dt.timedelta(days=3),
        data['view_date'])[0] == 'UP NEXT'


def test_weekday_filter_keeps_seven_slots():
    monday = dt.date(2030, 1, 7)
    wednesday = monday + dt.timedelta(days=2)

    data = _queens_dashboard_data(
        [_row(monday), _row(wednesday, message_id=2)],
        weekdays={0, 2},
        as_of_date=wednesday,
    )

    assert [day.weekday() for day in data['week_days']] == list(range(7))
    assert data['week_rows'][monday] is not None
    assert data['week_rows'][monday + dt.timedelta(days=1)] is None
    assert data['week_rows'][wednesday] is not None


def test_duplicate_day_keeps_fastest_result_once():
    day = dt.date(2030, 1, 7)
    slow = _row(day, 70, message_id=1)
    fast = _row(day, 25, message_id=2)

    data = _queens_dashboard_data(
        [slow, fast], as_of_date=day)

    assert data['results'] == [fast]
    assert data['total'] == 1
    assert data['times'] == [25]
    assert data['week_rows'][day] is fast


def test_dashboard_completion_status_ignores_result_badges():
    day = dt.date(2030, 1, 7)
    result = _row(day, perfect=False, accuracy=100)

    data = _queens_dashboard_data(
        [result], as_of_date=day)

    assert _week_status(result, day, day)[0] == 'PLAYED'
    assert data['weekday_stats'][0] == {'count': 1, 'median': 30}
    assert not {'clean', 'clean_rate', 'no_mistakes'} & data.keys()


def test_weekday_stats_only_use_time_and_count():
    monday = dt.date(2030, 1, 7)
    perfect = _queens_dashboard_data(
        [_row(monday, perfect=True)], as_of_date=monday)
    imperfect = _queens_dashboard_data(
        [_row(monday, perfect=False, accuracy=0)], as_of_date=monday)

    assert perfect['weekday_stats'] == imperfect['weekday_stats']
    assert set(perfect['weekday_stats'][0]) == {'count', 'median'}


def test_as_of_date_controls_logical_week_not_host_clock():
    logical_today = dt.date(2042, 12, 17)  # Wednesday
    monday = logical_today - dt.timedelta(days=2)

    data = _queens_dashboard_data(
        [_row(monday)], as_of_date=logical_today)

    assert data['week_start'] == dt.date(2042, 12, 15)
    assert data['view_date'] == logical_today
    assert logical_today in data['week_days']


def test_old_history_anchors_to_latest_populated_week():
    logical_today = dt.date(2042, 12, 17)
    historical_day = dt.date(2027, 5, 13)  # Thursday

    data = _queens_dashboard_data(
        [_row(historical_day)], as_of_date=logical_today)

    assert data['view_date'] == historical_day
    assert data['week_start'] == dt.date(2027, 5, 10)
    assert data['week_rows'][historical_day] is not None
    assert _week_status(
        None, historical_day + dt.timedelta(days=1),
        data['view_date'])[0] == 'UP NEXT'


def test_chart_cap_preserves_normal_values_and_clips_extreme_outlier():
    values = [6, 20, 40, 180, 250, 3827]

    cap, clipped = _chart_time_cap(values)

    assert clipped is True
    assert max(values[:-1]) <= cap < values[-1]

    normal_cap, normal_clipped = _chart_time_cap([10, 20, 30, 40])
    assert normal_cap == 40
    assert normal_clipped is False


@pytest.mark.parametrize('value', ['👑🧩', '♛\ufe0f\u200d👑'])
def test_safe_player_name_preserves_emoji_only_names(value):
    assert _safe_player_name(value) == value


def test_safe_player_name_preserves_long_cjk_and_emoji_without_truncating():
    raw = '非常に長い女王プレイヤー名' * 5 + ' 👑🧩🌍 ♛'

    assert _safe_player_name(raw) == raw


def test_public_stats_module_reexports_the_dashboard_renderer():
    from tle.cogs import _minigame_stats
    from tle.cogs import _queens_stats_dashboard

    assert (
        _minigame_stats.plot_queens_stats
        is _queens_stats_dashboard.plot_queens_stats
    )

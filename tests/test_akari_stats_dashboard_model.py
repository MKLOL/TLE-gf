"""Pure-model tests for the Daily Akari seven-day stats dashboard."""

import datetime as dt
import inspect
from types import SimpleNamespace

import pytest

from tle.cogs._akari_stats_dashboard import (
    _AMBER,
    _BG,
    _PANEL,
    _akari_dashboard_data,
    _chart_time_cap,
    _duration_or_dash,
    _safe_player_name,
    _week_status,
)


def _row(day, seconds=60, *, perfect=True, accuracy=100, message_id=1,
         puzzle_number=None):
    if isinstance(day, str):
        day = dt.date.fromisoformat(day)
    return SimpleNamespace(
        message_id=str(message_id),
        user_id='10',
        puzzle_number=puzzle_number or day.toordinal(),
        puzzle_date=day.isoformat(),
        time_seconds=seconds,
        is_perfect=perfect,
        accuracy=accuracy,
    )


def test_dashboard_uses_a_light_canvas_and_white_panels():
    def channels(color):
        return tuple(
            int(color[index:index + 2], 16) for index in (1, 3, 5))

    assert min(channels(_BG)) >= 240
    assert channels(_PANEL) == (255, 255, 255)


def test_one_clean_result_populates_kpis_and_current_week_states():
    monday = dt.date(2030, 1, 7)
    wednesday = monday + dt.timedelta(days=2)
    result = _row(monday, 42)

    data = _akari_dashboard_data([result], as_of_date=wednesday)

    assert data['total'] == 1
    assert data['clean'] == 1
    assert data['clean_rate'] == 100
    assert data['best_clean_time'] == 42
    assert data['median_clean_time'] == 42
    assert data['current_streak'] == 1
    assert data['longest_streak'] == 1
    assert data['week_start'] == monday
    assert data['week_rows'][monday] is result
    assert _week_status(result, monday, data['view_date'])[0] == 'CLEAN'
    assert _week_status(
        None, monday + dt.timedelta(days=1), data['view_date'])[0] == 'MISSED'
    assert _week_status(None, wednesday, data['view_date'])[0] == 'OPEN'
    assert _week_status(
        None, monday + dt.timedelta(days=3),
        data['view_date'])[0] == 'UP NEXT'


def test_accuracy_100_is_not_clean_without_is_perfect():
    day = dt.date(2030, 1, 7)
    result = _row(day, 22, perfect=False, accuracy=100)

    data = _akari_dashboard_data([result], as_of_date=day)
    status, color = _week_status(result, day, day)

    assert data['clean'] == 0
    assert data['clean_rate'] == 0
    assert data['clean_results'] == []
    assert data['clean_times'] == []
    assert data['best_clean_time'] is None
    assert data['median_clean_time'] is None
    assert data['current_streak'] == 0
    assert status == '100% ACC'
    assert color == _AMBER
    assert _duration_or_dash(data['best_clean_time']) == '—'


def test_duplicate_day_prefers_clean_result_even_when_it_is_slower():
    day = dt.date(2030, 1, 7)
    fast_imperfect = _row(
        day, 20, perfect=False, accuracy=99, message_id=1)
    slow_clean = _row(day, 80, perfect=True, message_id=2)

    data = _akari_dashboard_data(
        [fast_imperfect, slow_clean], as_of_date=day)

    assert data['results'] == [slow_clean]
    assert data['total'] == 1
    assert data['clean'] == 1
    assert data['best_clean_time'] == 80
    assert data['week_rows'][day] is slow_clean


def test_best_result_grouping_is_by_day_not_bad_puzzle_number():
    day = dt.date(2030, 1, 7)
    imperfect = _row(
        day, 20, perfect=False, accuracy=99, message_id=1,
        puzzle_number=100)
    clean = _row(
        day, 80, perfect=True, message_id=2, puzzle_number=999)

    data = _akari_dashboard_data([imperfect, clean], as_of_date=day)

    assert data['results'] == [clean]
    assert data['total'] == 1


def test_weekday_filter_keeps_seven_slots_and_filtered_streak():
    monday = dt.date(2030, 1, 7)
    wednesday = monday + dt.timedelta(days=2)

    data = _akari_dashboard_data(
        [_row(monday), _row(wednesday, message_id=2)],
        weekdays={0, 2},
        as_of_date=wednesday,
    )

    assert [day.weekday() for day in data['week_days']] == list(range(7))
    assert data['week_rows'][monday] is not None
    assert data['week_rows'][monday + dt.timedelta(days=1)] is None
    assert data['week_rows'][wednesday] is not None
    assert data['current_streak'] == 2
    assert data['longest_streak'] == 2


def test_weekday_stats_combine_clean_rate_with_clean_only_pace():
    first_monday = dt.date(2030, 1, 7)
    second_monday = first_monday + dt.timedelta(days=7)
    rows = [
        _row(first_monday, 60, message_id=1),
        _row(
            second_monday, 15, perfect=False, accuracy=96, message_id=2),
    ]

    data = _akari_dashboard_data(rows, as_of_date=second_monday)

    assert data['weekday_stats'][0] == {
        'count': 2,
        'clean': 1,
        'clean_rate': 50,
        'clean_median': 60,
    }
    assert data['clean_times'] == [60]
    assert 15 not in data['clean_times']


def test_zero_clean_results_never_invent_a_clean_pace():
    monday = dt.date(2030, 1, 7)
    data = _akari_dashboard_data([
        _row(monday, 18, perfect=False, accuracy=95, message_id=1),
        _row(
            monday + dt.timedelta(days=1), 25, perfect=False, accuracy=88,
            message_id=2),
    ], as_of_date=monday + dt.timedelta(days=1))

    assert data['clean'] == 0
    assert data['best_clean_time'] is None
    assert data['median_clean_time'] is None
    assert all(
        item['clean_median'] is None for item in data['weekday_stats'])


def test_as_of_date_controls_logical_week_not_host_clock():
    logical_today = dt.date(2042, 12, 17)
    monday = logical_today - dt.timedelta(days=2)

    data = _akari_dashboard_data(
        [_row(monday)], as_of_date=logical_today)

    assert data['week_start'] == dt.date(2042, 12, 15)
    assert data['view_date'] == logical_today
    assert logical_today in data['week_days']


def test_old_history_anchors_to_latest_populated_week():
    logical_today = dt.date(2042, 12, 17)
    historical_day = dt.date(2027, 5, 13)

    data = _akari_dashboard_data(
        [_row(historical_day)], as_of_date=logical_today)

    assert data['view_date'] == historical_day
    assert data['week_start'] == dt.date(2027, 5, 10)
    assert data['week_rows'][historical_day] is not None
    assert _week_status(
        None, historical_day + dt.timedelta(days=1),
        data['view_date'])[0] == 'UP NEXT'


def test_chart_cap_preserves_normal_values_and_clips_extreme_outlier():
    values = [18, 45, 70, 190, 280, 3827]

    cap, clipped = _chart_time_cap(values)

    assert clipped is True
    assert max(values[:-1]) <= cap < values[-1]

    normal_cap, normal_clipped = _chart_time_cap([20, 30, 40, 50])
    assert normal_cap == 50
    assert normal_clipped is False


@pytest.mark.parametrize('value', ['🌟🧩', '💡\ufe0f\u200d🌟'])
def test_safe_player_name_preserves_emoji_only_names(value):
    assert _safe_player_name(value) == value


def test_safe_player_name_preserves_long_cjk_and_emoji_without_truncating():
    raw = '非常に長いアカリプレイヤー名' * 5 + ' 🌟🧩🌍'

    assert _safe_player_name(raw) == raw


def test_public_stats_module_reexports_dashboard_renderer_and_signature():
    from tle.cogs import _akari_stats_dashboard
    from tle.cogs import _minigame_stats

    assert (
        _minigame_stats.plot_akari_stats
        is _akari_stats_dashboard.plot_akari_stats
    )
    signature = inspect.signature(_minigame_stats.plot_akari_stats)
    assert signature.parameters['weekdays'].default is None
    assert signature.parameters['as_of_date'].default is None

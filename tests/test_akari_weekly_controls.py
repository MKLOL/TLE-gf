import asyncio
import datetime as dt
from collections import namedtuple
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from tests.minigames_test_utils import _FakeChannel, _FakeGuild, db
from tle.cogs._mgimpl_akari_weekly import (
    parse_weekly_post_time,
    weekly_period_dates,
)


def test_parse_weekly_post_time_requires_strict_24_hour_value():
    assert parse_weekly_post_time('07:05')[0] == '07:05'
    assert parse_weekly_post_time('23:59')[1] == dt.time(23, 59)
    for value in ('7:05', '24:00', '12:60', 'noon'):
        with pytest.raises(ValueError):
            parse_weekly_post_time(value)


def test_weekly_period_rolls_over_at_configured_monday_time():
    zone = ZoneInfo('America/New_York')
    monday = dt.datetime(2026, 8, 31, 8, 29, tzinfo=zone)

    assert weekly_period_dates(monday, '08:30') == (
        dt.date(2026, 8, 30), dt.date(2026, 8, 24))
    assert weekly_period_dates(
        monday.replace(minute=30), '08:30') == (
            dt.date(2026, 8, 31), dt.date(2026, 8, 31))


def test_mod_can_set_weekly_time_and_current_thread(db, monkeypatch):
    from tle.cogs import minigames as minigames_module
    from tle.util import codeforces_common as cf_common

    monkeypatch.setattr(cf_common, 'user_db', db)
    cog = minigames_module.Minigames(bot=None)
    channel = _FakeChannel(123)
    ctx = SimpleNamespace(guild=_FakeGuild(1), channel=channel,
                          send=channel.send)

    asyncio.run(cog._cmd_akari_weekly_post(ctx, ('here',)))
    asyncio.run(cog._cmd_akari_weekly_post(ctx, ('time', '08:30')))

    assert db.get_guild_config(1, 'akari_weekly_post_channel') == '123'
    assert db.get_guild_config(1, 'akari_weekly_post_time') == '08:30'


def test_weekly_announcement_posts_once_to_configured_target(db, monkeypatch):
    from tle.cogs import minigames as minigames_module
    from tle.util import codeforces_common as cf_common
    from tle.util.akari_rating import RatingState

    monkeypatch.setattr(cf_common, 'user_db', db)
    db.set_guild_config(1, 'akari', '1')
    db.set_guild_config(1, 'akari_weekly_post_channel', '123')
    monkeypatch.setattr(
        db, 'get_akari_registrants',
        lambda _guild: {'10', '11', '12', '13'})
    target = _FakeChannel(123)
    cog = minigames_module.Minigames(bot=None)
    Standing = namedtuple(
        'Standing',
        'user_id score days_played perfects total_time week_start week_end')
    start = dt.date(2026, 8, 24)
    standings = [
        Standing(str(user_id), 16.0 - user_id, 7, 7, 420, start,
                 start + dt.timedelta(days=6))
        for user_id in range(10, 14)
    ]
    ratings = [RatingState('10', 1600, 1, 1600, 100, 0, 500)]

    async def preview(*_args, **_kwargs):
        return ratings, standings

    async def resolve(channel_id):
        assert channel_id == 123
        return target

    monkeypatch.setattr(cog, '_akari_weekly_preview', preview)
    monkeypatch.setattr(cog, '_resolve_channel', resolve)
    monkeypatch.setattr(cog, '_active_ranking_rows',
                        lambda rows, **_kwargs: rows)
    monkeypatch.setattr(
        minigames_module, '_get_akari_weekly_table_image_file',
        lambda _guild, rows, **kwargs:
            ('standings', [row.user_id for row in rows], kwargs['title']))
    monkeypatch.setattr(
        minigames_module, '_get_akari_rating_table_image_file',
        lambda *_args, **kwargs: ('ratings', kwargs['title']))

    assert asyncio.run(cog._check_akari_weekly_announcement(
        _FakeGuild(1), start)) is True
    assert asyncio.run(cog._check_akari_weekly_announcement(
        _FakeGuild(1), start)) is False
    assert len(target.sent) == 1
    files = target.sent[0]['kwargs']['files']
    assert [item[0] for item in files] == ['standings', 'ratings']
    assert files[0][1] == ['10', '11', '12', '13']
    assert 'Final standings' in target.sent[0]['content']

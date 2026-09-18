import asyncio
import datetime as dt
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


@pytest.mark.parametrize('player_count', [5, 45, 405])
def test_weekly_announcement_posts_every_player_once_to_configured_target(
        db, monkeypatch, player_count):
    from tle.cogs import minigames as minigames_module
    from tle.util import codeforces_common as cf_common
    from tle.util.akari_rating import RatingState
    from tle.util.akari_weekly import WeeklyStanding
    from tle.cogs._minigame_tables import _get_akari_weekly_table_image_file

    monkeypatch.setattr(cf_common, 'user_db', db)
    db.set_guild_config(1, 'akari', '1')
    db.set_guild_config(1, 'akari_weekly_post_channel', '123')
    user_ids = [str(10 + index) for index in range(player_count)]
    monkeypatch.setattr(db, 'get_akari_registrants',
                        lambda _guild: set(user_ids) | {'banned'})
    target = _FakeChannel(123)
    cog = minigames_module.Minigames(bot=None)
    start = dt.date(2026, 8, 24)
    # Equal scores span page boundaries; their shared rank must be preserved.
    standings = [WeeklyStanding(user_id, 6.0 - index // 3 * 0.01,
                               7, 7, 420, start,
                               start + dt.timedelta(days=6))
                 for index, user_id in enumerate(
                     user_ids + ['opted-out', 'banned'])]
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
    monkeypatch.setattr(cog, '_akari_banned_user_ids', lambda _guild: {'banned'})
    monkeypatch.setattr(
        minigames_module, '_get_akari_weekly_table_image_file',
        lambda guild, rows, **kwargs: _get_akari_weekly_table_image_file(
            guild, rows, name_fn=lambda _guild, row: row.user_id,
            identity_fn=lambda _guild, row: '-', **kwargs))
    monkeypatch.setattr(
        minigames_module, '_get_akari_puzzle_table_image',
        lambda rows, **kwargs: ('standings', rows, kwargs))
    monkeypatch.setattr(
        minigames_module, '_get_akari_rating_table_image_file',
        lambda *_args, **kwargs: ('ratings', kwargs['title']))

    assert asyncio.run(cog._check_akari_weekly_announcement(
        _FakeGuild(1), start)) is True
    assert asyncio.run(cog._check_akari_weekly_announcement(
        _FakeGuild(1), start)) is False
    assert all(len(message['kwargs']['files']) <= 10 for message in target.sent)
    files = [file for message in target.sent
             for file in message['kwargs']['files']]
    pages = files[:-1]
    assert files[-1][0] == 'ratings'
    assert len(pages) == (player_count + 39) // 40
    assert all(page[0] == 'standings' and len(page[1]) <= 40 for page in pages)
    assert [row[1] for page in pages for row in page[1]] == user_ids
    assert [row[0] for page in pages for row in page[1]] == [
        index - index % 3 + 1 for index in range(player_count)]
    assert all(row[4] == 7 for page in pages for row in page[1])
    assert len({page[2]['filename'] for page in pages}) == len(pages)
    assert all(page[2]['header'][-1] == 'Days' for page in pages)
    assert all('Final Rankings' in page[2]['title'] for page in pages)
    assert 'Final rankings' in target.sent[0]['content']

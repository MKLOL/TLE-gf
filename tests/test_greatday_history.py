"""Tests for ``;greatday latest`` and ``;greatday history``."""
import asyncio

from tle.cogs import greatday as greatday_module
from tle.cogs.greatday import GreatDay, _format_pick_time
from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import GUILD, USER_A, USER_B, FakeGreatDayDb


class _Member:
    def __init__(self, user_id, display_name):
        self.id = int(user_id)
        self.display_name = display_name


class _Context:
    def __init__(self, author):
        self.guild = type('_Guild', (), {'id': int(GUILD)})()
        self.channel = object()
        self.author = author
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))


def _run_command(command, cog, ctx, member=None):
    return asyncio.run(command.__wrapped__(cog, ctx, member))


class TestPickHistoryDb:
    def test_empty_latest_and_history(self):
        db = FakeGreatDayDb()
        assert db.greatday_get_latest_pick(GUILD, USER_A) is None
        assert db.greatday_get_pick_history(GUILD, USER_A) == []

    def test_latest_returns_newest_pick(self):
        db = FakeGreatDayDb()
        db.greatday_record_picks(GUILD, [USER_A], 'older', 1000.0)
        db.greatday_record_picks(GUILD, [USER_A], 'newest', 3000.0)
        db.greatday_record_picks(GUILD, [USER_A], 'middle', 2000.0)

        row = db.greatday_get_latest_pick(GUILD, USER_A)
        assert (row.message_id, row.picked_at) == ('newest', 3000.0)

    def test_history_is_newest_first_and_scoped(self):
        db = FakeGreatDayDb()
        db.greatday_record_picks(GUILD, [USER_A], 'older', 1000.0)
        db.greatday_record_picks(GUILD, [USER_A], 'newer', 2000.0)
        db.greatday_record_picks(GUILD, [USER_B], 'other-user', 3000.0)
        db.greatday_record_picks('999', [USER_A], 'other-guild', 4000.0)

        rows = db.greatday_get_pick_history(GUILD, USER_A)
        assert [(row.message_id, row.picked_at) for row in rows] == [
            ('newer', 2000.0),
            ('older', 1000.0),
        ]

    def test_equal_timestamps_use_newer_snowflake_first(self):
        db = FakeGreatDayDb()
        db.greatday_record_picks(GUILD, [USER_A], '200', 1000.0)
        db.greatday_record_picks(GUILD, [USER_A], '100', 1000.0)

        rows = db.greatday_get_pick_history(GUILD, USER_A)
        assert [row.message_id for row in rows] == ['200', '100']


class TestPickTimeFormatting:
    def test_uses_absolute_and_relative_discord_timestamps(self):
        assert _format_pick_time(1234.9) == '<t:1234:F> (<t:1234:R>)'


class TestLatestCommand:
    def test_defaults_to_invoker_and_renders_latest_time(self, monkeypatch):
        db = FakeGreatDayDb()
        db.greatday_record_picks(GUILD, [USER_A], 'not-shown', 1234.0)
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _Context(_Member(USER_A, 'Invoker'))

        _run_command(GreatDay.latest, GreatDay(bot=None), ctx)

        embed = ctx.sent[0][1]['embed']
        assert embed.title == 'Latest Great Day — Invoker'
        assert embed.description == 'Last selected: <t:1234:F> (<t:1234:R>)'
        assert 'not-shown' not in embed.description

    def test_accepts_another_member(self, monkeypatch):
        db = FakeGreatDayDb()
        db.greatday_record_picks(GUILD, [USER_B], 'pick', 5678.0)
        monkeypatch.setattr(cf_common, 'user_db', db)
        ctx = _Context(_Member(USER_A, 'Invoker'))
        target = _Member(USER_B, 'Target')

        _run_command(GreatDay.latest, GreatDay(bot=None), ctx, target)

        embed = ctx.sent[0][1]['embed']
        assert embed.title == 'Latest Great Day — Target'
        assert '<t:5678:F> (<t:5678:R>)' in embed.description

    def test_empty_state(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', FakeGreatDayDb())
        ctx = _Context(_Member(USER_A, 'Invoker'))

        _run_command(GreatDay.latest, GreatDay(bot=None), ctx)

        embed = ctx.sent[0][1]['embed']
        assert 'No Great Day picks have been recorded' in embed.description


class TestHistoryCommand:
    def test_empty_state(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', FakeGreatDayDb())
        ctx = _Context(_Member(USER_A, 'Invoker'))

        _run_command(GreatDay.history, GreatDay(bot=None), ctx)

        embed = ctx.sent[0][1]['embed']
        assert embed.title == 'Great Day history — Invoker'
        assert 'No Great Day history has been recorded' in embed.description

    def test_paginates_15_per_page_newest_first(self, monkeypatch):
        db = FakeGreatDayDb()
        for timestamp in range(1000, 1016):
            db.greatday_record_picks(
                GUILD, [USER_A], f'message-{timestamp}', float(timestamp))
        monkeypatch.setattr(cf_common, 'user_db', db)
        captured = {}
        monkeypatch.setattr(
            greatday_module.paginator, 'paginate',
            lambda bot, channel, pages, **kwargs:
                captured.update(pages=pages, kwargs=kwargs))
        ctx = _Context(_Member(USER_A, 'Invoker'))

        _run_command(GreatDay.history, GreatDay(bot=None), ctx)

        pages = captured['pages']
        assert len(pages) == 2
        first_lines = pages[0][1].description.splitlines()
        second_lines = pages[1][1].description.splitlines()
        assert len(first_lines) == 15
        assert first_lines[0] == '**15.** <t:1015:F> (<t:1015:R>)'
        assert first_lines[-1] == '**1.** <t:1001:F> (<t:1001:R>)'
        assert second_lines == ['**0.** <t:1000:F> (<t:1000:R>)']
        assert captured['kwargs']['author_id'] == int(USER_A)
        descriptions = '\n'.join(page[1].description for page in pages)
        assert 'message-' not in descriptions
        assert 'discord.com/channels' not in descriptions

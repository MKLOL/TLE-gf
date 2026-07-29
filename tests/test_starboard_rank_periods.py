"""Calendar-period behavior for ``;starboard rank``.

The parser is shared by the starboard leaderboard commands, while these
command-level checks specifically protect the ``rank`` alias' bounds, visible
scope, and emoji-alias normalization.
"""
import asyncio
import datetime as dt
import itertools
from types import SimpleNamespace

import pytest

from tle.cogs import starboard as starboard_module
from tle.cogs.starboard import (
    Starboard,
    _NO_TIME_BOUND,
    _parse_starboard_args,
)
from tle.util import codeforces_common as cf_common
from tle.util import paginator

from tests.starboard_test_utils import GUILD, STAR, THUMBS_UP


_NOW = dt.datetime(2026, 7, 29, 15, 42)

_PERIODS = [
    ('day', 'Today', dt.datetime(2026, 7, 29)),
    ('today', 'Today', dt.datetime(2026, 7, 29)),
    ('dtd', 'Today', dt.datetime(2026, 7, 29)),
    ('week', 'Week to date', dt.datetime(2026, 7, 27)),
    ('wtd', 'Week to date', dt.datetime(2026, 7, 27)),
    ('month', 'Month to date', dt.datetime(2026, 7, 1)),
    ('mtd', 'Month to date', dt.datetime(2026, 7, 1)),
    ('year', 'Year to date', dt.datetime(2026, 1, 1)),
    ('ytd', 'Year to date', dt.datetime(2026, 1, 1)),
]


@pytest.mark.parametrize(('token', 'label', 'expected_start'), _PERIODS)
def test_period_aliases_have_deterministic_calendar_boundaries(
        token, label, expected_start):
    emoji, dlo, dhi, scope = _parse_starboard_args(
        (token,), include_label=True, now=_NOW)

    assert emoji == STAR
    assert dt.datetime.fromtimestamp(dlo) == expected_start
    assert dhi == _NO_TIME_BOUND
    assert scope == label


@pytest.mark.parametrize('token', ('all', 'alltime', 'lifetime'))
def test_all_time_aliases_are_unbounded(token):
    assert _parse_starboard_args(
        (token,), include_label=True, now=_NOW,
    ) == (STAR, 0, _NO_TIME_BOUND, 'All time')


def test_parser_keeps_legacy_three_tuple_by_default():
    parsed = _parse_starboard_args(('year',), now=_NOW)

    assert parsed == (
        STAR,
        dt.datetime(2026, 1, 1).timestamp(),
        _NO_TIME_BOUND,
    )
    assert len(parsed) == 3


@pytest.mark.parametrize(
    'args',
    ((THUMBS_UP, 'ytd'), ('ytd', THUMBS_UP)),
)
def test_emoji_can_come_before_or_after_period(args):
    emoji, _dlo, _dhi, scope = _parse_starboard_args(
        args, include_label=True, now=_NOW)

    assert emoji == THUMBS_UP
    assert scope == 'Year to date'


@pytest.mark.parametrize(
    'periods',
    tuple(itertools.permutations(('year', 'month', 'week', 'day'))),
)
def test_combined_periods_choose_narrowest_start_order_independently(periods):
    _emoji, dlo, dhi, scope = _parse_starboard_args(
        periods, include_label=True, now=_NOW)

    assert dt.datetime.fromtimestamp(dlo) == dt.datetime(2026, 7, 29)
    assert dhi == _NO_TIME_BOUND
    assert scope == 'Today'


@pytest.mark.parametrize('args', (('all', 'week'), ('week', 'alltime')))
def test_all_time_token_does_not_widen_another_period(args):
    _emoji, dlo, _dhi, scope = _parse_starboard_args(
        args, include_label=True, now=_NOW)

    assert dt.datetime.fromtimestamp(dlo) == dt.datetime(2026, 7, 27)
    assert scope == 'Week to date'


def _unwrap(command):
    while hasattr(command, '__wrapped__'):
        command = command.__wrapped__
    return command


class _RankDb:
    """Small command spy with a configured main emoji and one alias."""

    def __init__(self, *, default_emoji=STAR):
        self.default_emoji = default_emoji
        self.query_calls = []
        self.entry = SimpleNamespace(
            channel_id='123', threshold=3, color=0xffaa10)

    def get_user_starboard_default(self, guild_id, user_id):
        assert (guild_id, user_id) == (GUILD, 99)
        return self.default_emoji

    def get_guild_config(self, guild_id, key):
        assert (guild_id, key) == (GUILD, 'starboard_leaderboard')
        return '1'

    def get_starboard_entry(self, guild_id, emoji):
        assert guild_id == GUILD
        return self.entry if emoji == STAR else None

    def resolve_alias(self, guild_id, emoji):
        assert guild_id == GUILD
        return STAR if emoji == THUMBS_UP else None

    def get_starboard_star_leaderboard(
            self, guild_id, emoji, dlo=0, dhi=_NO_TIME_BOUND):
        self.query_calls.append((guild_id, emoji, dlo, dhi))
        return [SimpleNamespace(author_id='99', total_stars=12)]


class _Guild:
    id = GUILD

    @staticmethod
    def get_member(user_id):
        return SimpleNamespace(id=user_id, mention=f'<@{user_id}>')


def _ctx():
    return SimpleNamespace(
        guild=_Guild(),
        author=SimpleNamespace(id=99),
        channel=object(),
    )


@pytest.mark.parametrize(
    ('raw_emoji', 'args'),
    ((STAR, ('year',)), (THUMBS_UP, (THUMBS_UP, 'year'))),
)
def test_rank_passes_bounds_labels_title_and_normalizes_alias(
        monkeypatch, raw_emoji, args):
    db = _RankDb()
    monkeypatch.setattr(cf_common, 'user_db', db)
    parse_calls = []

    def fake_parse(
            supplied, default_emoji=STAR, *, include_label=False, **_kwargs):
        parse_calls.append((supplied, default_emoji, include_label))
        selected = raw_emoji if raw_emoji != STAR else default_emoji
        parsed = (selected, 111.0, 222.0)
        return parsed + ('Year to date',) if include_label else parsed

    monkeypatch.setattr(
        starboard_module, '_parse_starboard_args', fake_parse)
    sent = {}

    def capture_pages(_bot, _channel, pages, **_kwargs):
        sent['pages'] = pages

    monkeypatch.setattr(paginator, 'paginate', capture_pages)
    cog = Starboard.__new__(Starboard)
    cog.bot = object()

    asyncio.run(_unwrap(Starboard.star_leaderboard)(cog, _ctx(), *args))

    assert parse_calls == [(args, STAR, True)]
    assert db.query_calls == [(GUILD, STAR, 111.0, 222.0)]
    title = sent['pages'][0][1].title
    assert title.startswith(f'{STAR} Star Leaderboard')
    assert 'Year to date' in title

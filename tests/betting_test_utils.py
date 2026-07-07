"""Shared fixtures, constants and fakes for the betting test suite.

Split out of ``test_betting.py`` so the per-area test modules
(``test_betting_*.py``) can import a single set of helpers via
``from tests.betting_test_utils import ...``.
"""
import sqlite3  # noqa: F401 (re-exported for test convenience)

import pytest

from tle.util.db.user_db_conn import (  # noqa: F401
    UserDbConn, namedtuple_factory, bet_fixture_key,
)


def _raw_event(**over):
    raw = {
        'id': 'evt1', 'sport_key': 'soccer_epl',
        'commence_time': '2026-06-20T15:00:00Z',
        'home_team': 'Spain', 'away_team': 'Cape Verde',
        'bookmakers': [
            {'key': 'b1', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.5},
                {'name': 'Cape Verde', 'price': 6.0},
                {'name': 'Draw', 'price': 4.0}]}]},
            {'key': 'b2', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.6},
                {'name': 'Cape Verde', 'price': 6.5},
                {'name': 'Draw', 'price': 4.2}]}]},
        ],
    }
    raw.update(over)
    return raw



class _FakeResp:
    def __init__(self, data, status=200, text='', headers=None):
        self._data = data
        self.status = status
        self._text = text
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._data

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self, data, headers=None):
        self._data = data
        self.calls = []
        self._headers = headers or {}

    def get(self, url, params=None):
        self.calls.append((url, params))
        return _FakeResp(self._data, headers=self._headers)


GUILD = '111'
CH = '222'
THREAD = '333'
USER_A = '100'
USER_B = '200'


@pytest.fixture
def db():
    return UserDbConn(':memory:')


def _make_market(db, commence=10_000.0, odds=(2.0, 3.0, 4.0)):
    mid = db.bet_market_create(
        GUILD, CH, 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', commence,
        odds[0], odds[1], odds[2], USER_A, 0.0)
    return mid


class _FakeThread:
    def __init__(self, tid):
        self.id = tid
        self.sent = []
        self._messages = {}
        self.archived = False
        self.locked = False

    async def send(self, embed=None, **kw):
        m = _FakeMsg(embed=embed, **kw)
        self.sent.append(m)
        self._messages[m.id] = m
        return m

    async def fetch_message(self, mid):
        return self._messages[mid]

    async def edit(self, **kw):
        self.archived = kw.get('archived', self.archived)
        self.locked = kw.get('locked', self.locked)


class _FakeMsg:
    _n = 5000

    def __init__(self, content=None, embed=None, **kw):
        _FakeMsg._n += 1
        self.id = _FakeMsg._n
        self.content = content
        self.embed = embed
        self.kw = kw
        self.thread = None
        self.deleted = False
        self.edited_embed = None

    async def create_thread(self, name=None, auto_archive_duration=None):
        self.thread = _FakeThread(self.id + 100000)
        return self.thread

    async def edit(self, embed=None, **kw):
        self.edited_embed = embed
        self.embed = embed

    async def delete(self):
        self.deleted = True


class _FakeChannel:
    def __init__(self, cid):
        self.id = cid
        self.mention = f'<#{cid}>'
        self.sent = []
        self._messages = {}

    async def send(self, content=None, embed=None, **kw):
        m = _FakeMsg(content=content, embed=embed, **kw)
        self.sent.append(m)
        self._messages[m.id] = m
        return m

    async def fetch_message(self, mid):
        return self._messages[mid]


class _FakeGuild:
    def __init__(self, gid, channel, roles=None):
        self.id = gid
        self._channel = channel
        self._roles = {int(role.id): role for role in roles or []}

    def get_channel(self, cid):
        return self._channel

    def get_role(self, rid):
        return self._roles.get(int(rid))


class _FakeBot:
    def __init__(self, guilds, channels):
        self.guilds = guilds
        self._channels = channels
        self.user = type('U', (), {'id': 999})()

    def get_channel(self, cid):
        return self._channels.get(cid)


def _wc_event(event_id='evtWC', home='Spain', away='Cape Verde', commence=None):
    return {
        'event_id': event_id, 'sport_key': 'soccer_fifa_world_cup',
        'home_team': home, 'away_team': away,
        'commence_time': commence,
        'odds': {'home': 1.25, 'draw': 5.5, 'away': 12.0},
    }


def _fixture_key(event):
    return bet_fixture_key(
        event['sport_key'], event['home_team'], event['away_team'],
        event['commence_time'])


class _FakeBetMessage:
    def __init__(self, content, channel_id=333):
        self.content = content
        self.author = type('A', (), {'bot': False, 'id': 1})()
        self.guild = type('G', (), {'id': int(GUILD)})()
        self.channel = type('C', (), {'id': channel_id})()
        self.reactions = []

    async def add_reaction(self, emoji):
        self.reactions.append(emoji)

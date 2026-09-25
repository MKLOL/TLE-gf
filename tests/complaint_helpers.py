"""Small DB and Discord fakes shared by complaint workflow tests."""
import sqlite3
from collections import namedtuple
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tle.util.db.api_token_db import ApiTokenDbMixin, create_api_token_schema
from tle.util.db.complaint_db import ComplaintDbMixin

COMMIT = 'https://github.com/MKLOL/TLE-gf/commit/' + 'a' * 40


class ComplaintDb(ComplaintDbMixin, ApiTokenDbMixin):
    def __init__(self, path=':memory:'):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = lambda cur, row: namedtuple(
            'Row', [col[0] for col in cur.description], rename=True)(*row)
        self._create_complaint_tables()
        create_api_token_schema(self.conn)
        self.conn.commit()


def fake_bot():
    admin = SimpleNamespace(id=10, roles=[SimpleNamespace(name='Admin')])
    guild = SimpleNamespace(id=1, get_member=lambda uid: admin if uid == 10 else None)
    guild.fetch_member = AsyncMock(return_value=admin)
    message = SimpleNamespace(jump_url='https://discord.com/channels/1/2/999')
    channel = SimpleNamespace(id=2, guild=guild, send=AsyncMock(return_value=message))
    user = SimpleNamespace(id=20, send=AsyncMock(return_value=message))
    return SimpleNamespace(
        is_ready=lambda: True, guild=guild, admin=admin, channel=channel, user=user,
        get_guild=lambda gid: guild if gid == 1 else None,
        get_channel=lambda cid: channel if cid == 2 else None,
        get_user=lambda uid: user, fetch_channel=AsyncMock(return_value=channel),
        fetch_user=AsyncMock(return_value=user))

"""Shared in-memory games API fixture with the real Minigames implementation."""
from types import SimpleNamespace

import pytest

from tests.minigames_test_utils import (
    FakeMinigameDb, _FakeDiscordMember, _FakeGuild,
)
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common
from tle.util.db.api_token_db import ApiTokenDbMixin, create_api_token_schema
from tle.util.db.games_token_db import GamesTokenDbMixin, create_games_token_schema
from tle.util.games_import import GamesImportService
from tle.util.db.games_submission_db import GamesSubmissionDbMixin, create_games_submission_schema


class GamesDb(FakeMinigameDb, GamesTokenDbMixin, ApiTokenDbMixin, GamesSubmissionDbMixin):
    def __init__(self):
        super().__init__()
        create_games_token_schema(self.conn)
        create_games_submission_schema(self.conn)
        create_api_token_schema(self.conn)
        self.conn.commit()


@pytest.fixture
def games(monkeypatch):
    db = GamesDb()
    monkeypatch.setattr(cf_common, 'user_db', db)
    member = _FakeDiscordMember(10, 'mod', 'Mod', roles=[SimpleNamespace(name='Moderator')])
    alice = _FakeDiscordMember(11, 'alice', 'Alice')
    guild = _FakeGuild(1, [member, alice])
    guild.name = 'Test server'
    cog = Minigames(None)
    bot = SimpleNamespace(get_cog=lambda name: cog if name == 'Minigames' else None)
    for game in ('queens', 'tango'):
        db.set_guild_config(1, game, '1')
        db.set_minigame_channel(1, game, 20)
    db.set_minigame_player_link(1, 'linkedin', 10, 'Importer', 'importer', None, 1, 10)
    db.set_minigame_player_link(1, 'linkedin', 11, 'Alice LinkedIn', 'alice linkedin', None, 1, 10)
    token_id, raw = db.create_games_token(1, 10)
    token = db.authenticate_games_token(raw)
    service = GamesImportService(bot, lambda: db)
    yield SimpleNamespace(db=db, bot=bot, cog=cog, guild=guild, member=member,
                          service=service, token=token, raw=raw, token_id=token_id)
    db.close()


def payload(game='tango', **changes):
    return {
        'game': game, 'puzzle_date': '2026-09-04', 'puzzle_number': None,
        'leaderboard': 'Alice LinkedIn\n🤓💎 No hints & no mistakes!\n0:10\n'
                       'Importer\nYou\n🤓 No hints!\n0:12\nUnknown Player\n0:20',
        **changes,
    }


def preview(env, **changes):
    return env.service.preview(env.token, env.guild, env.member, payload(**changes))


def confirm(env, data):
    return env.service.confirm(env.token, env.guild, env.member, data['preview_id'])

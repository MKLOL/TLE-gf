import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from discord.ext import commands

from tests.games_helpers import games
from tle.cogs._games_tokens import GamesTokenMixin
from tle.util.db.user_db_conn import UserDbConn
from tle.util.db.user_db_upgrades import registry


def test_tokens_are_isolated_hashed_scoped_and_revocable(games):
    db = games.db
    assert db.authenticate_complaint_token(games.raw) is None
    _, complaint = db.create_complaint_token(1, 10)
    assert db.authenticate_games_token(complaint) is None
    row = db.conn.execute('SELECT * FROM games_api_token').fetchone()
    assert games.raw not in repr(row) and len(row.token_hash) == 64
    assert not db.list_games_tokens(1, 11) and not db.list_games_tokens(2, 10)
    assert not db.revoke_games_token(games.token_id, 2, 10)
    assert not db.revoke_games_token(games.token_id, 1, 11)
    assert db.authenticate_games_token(games.raw)
    assert db.revoke_games_token(games.token_id, 1, 10)
    assert db.authenticate_games_token(games.raw) is None


def test_expiration_and_bad_tokens(games):
    for raw in ('', None, 'x' * 10000, 'tlegames_' + 'x' * 42, "' OR 1=1 --"):
        assert games.db.authenticate_games_token(raw) is None
    games.db.conn.execute('UPDATE games_api_token SET expires_at = 0')
    assert games.db.authenticate_games_token(games.raw) is None
    for days in (0, 366, True, 1.2):
        with pytest.raises(ValueError):
            games.db.create_games_token(1, 10, days)


def test_mint_is_moderator_only_and_failed_dm_revokes(games, monkeypatch):
    import discord
    monkeypatch.setattr(discord, 'AllowedMentions',
                        SimpleNamespace(none=lambda: None), raising=False)
    ctx = SimpleNamespace(guild=games.guild, author=games.member, send=AsyncMock())
    ctx.author.send = AsyncMock()
    games.member.roles = []
    with pytest.raises(commands.CheckFailure):
        asyncio.run(GamesTokenMixin.make_games_token(games.cog, ctx))
    games.db.set_guild_config(1, 'tango_admin_user_ids', '["10"]')
    with pytest.raises(commands.CheckFailure):
        asyncio.run(GamesTokenMixin.make_games_token(games.cog, ctx))
    games.member.roles = [SimpleNamespace(name='Moderator')]
    asyncio.run(GamesTokenMixin.make_games_token(games.cog, ctx))
    assert 'tlegames_' in ctx.author.send.call_args.args[0]
    assert 'tlegames_' not in repr(ctx.send.call_args_list)
    count = len(games.db.list_games_tokens(1, 10))
    ctx.author.send.side_effect = OSError('DM failed')
    asyncio.run(GamesTokenMixin.make_games_token(games.cog, ctx))
    assert len(games.db.list_games_tokens(1, 10)) == count


def test_new_schema_upgrade_and_token_survives_restart(tmp_path):
    filename = str(tmp_path / 'user.db')
    db = UserDbConn(filename)
    db.conn.execute('DROP TABLE games_api_token')
    registry.set_version(db.conn, '1.63.0')
    db.conn.commit()
    db.conn.close()
    db = UserDbConn(filename)
    _, raw = db.create_games_token(1, 10)
    assert registry.get_current_version(db.conn) == registry.latest_version
    db.conn.close()
    db = UserDbConn(filename)
    assert db.authenticate_games_token(raw).user_id == '10'
    db.conn.close()


def test_games_tokens_are_redacted_from_transcripts_and_logs(monkeypatch):
    from tests.test_llm_secret_logging import _load_discord_common
    from tle.cogs._llm_transcript import redact_secrets
    token = 'tlegames_' + 'a' * 43
    assert token not in redact_secrets(f'Here is `{token}`')
    assert token not in _load_discord_common(monkeypatch).redact_credentials(token)

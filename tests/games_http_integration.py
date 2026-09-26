"""Real aiohttp/discord.py checks, outside pytest's dependency stubs."""
import asyncio
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

try:
    import aiohttp
    from aiohttp import web
    import discord
    from discord.ext import commands
except ImportError as exc:
    print(str(exc))
    sys.exit(77)

package = ModuleType('tle.util.db')
package.__path__ = [str(Path(__file__).resolve().parents[1] / 'tle' / 'util' / 'db')]
sys.modules['tle.util.db'] = package
from tests.complaint_helpers import ComplaintDb, fake_bot
from tle.util.db.games_token_db import GamesTokenDbMixin, create_games_token_schema
from tle.util.complaint_http import ComplaintHttpServer
from tle.util.complaints import ComplaintService


class Db(ComplaintDb, GamesTokenDbMixin):
    def __init__(self):
        super().__init__()
        create_games_token_schema(self.conn)
        self.conn.commit()


async def check_http():
    db, bot = Db(), fake_bot()
    cog = SimpleNamespace(
        _linkedin_games=lambda: ['queens', 'tango'],
        _has_server_mod_role=lambda member: bool(member.roles),
        _has_linkedin_mod_access=lambda gid, game, member: bool(member.roles))
    bot.get_cog = lambda name: cog
    server = ComplaintHttpServer(bot, ComplaintService(bot, lambda: db))
    _, complaint = db.create_complaint_token(1, 10)
    tid, token = db.create_games_token(1, 10)
    headers = {'Authorization': 'Bearer ' + token}
    server.games.service.catalog = lambda *args: {'games': ['queens', 'tango']}
    calls = []
    server.games.service.preview = lambda *args: calls.append(args) or {'preview_id': 'preview'}
    server.games.service.confirm = lambda *args: calls.append(args) or {'registered': 1}
    runner = web.AppRunner(server.create_app(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    base = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
    async with aiohttp.ClientSession() as session:
        async def request(method, route, expected=200, **kwargs):
            kwargs.setdefault('headers', headers)
            async with session.request(method, base + route, **kwargs) as response:
                data = await response.json()
                assert response.status == expected, (response.status, data)
                assert response.headers['Cache-Control'] == 'no-store'
                assert 'Access-Control-Allow-Origin' not in response.headers
                return data

        try:
            assert (await request('GET', '/v1/games'))['games'] == ['queens', 'tango']
            await request('GET', '/v1/games', 401, headers={})
            await request('GET', '/v1/games', 401,
                          headers={'Authorization': 'Bearer ' + complaint})
            await request('GET', '/v1/complaints', 401)
            await request('GET', '/v1/games', 401,
                          headers=[('Authorization', 'Bearer ' + token)] * 2)
            await request('GET', '/v1/games?token=' + token, 400)
            await request('POST', '/v1/games', 405)
            body = {'game': 'tango', 'puzzle_date': '2026-09-26',
                    'puzzle_number': None, 'leaderboard': 'You\n0:10'}
            route = '/v1/games/imports/preview'
            await request('POST', route, json=body)
            assert calls[-1][0].user_id == '10' and calls[-1][-1] == body
            await request('POST', route, 400, json={**body, 'user_id': '99'})
            await request('POST', route, 400, json=[])
            await request('POST', route, 415, data='{}')
            await request('POST', route, 413, json={**body, 'leaderboard': 'a' * 20000})
            confirm = '/v1/games/imports/preview/confirm'
            await request('POST', confirm, 400, json={'leaderboard': 'replacement'})
            await request('POST', confirm, json={})
            bot.admin.roles = []
            await request('POST', confirm, 403, json={})
            bot.admin.roles = [SimpleNamespace(name='Moderator')]
            bot.is_ready = lambda: False
            await request('GET', '/v1/games', 503)
            bot.is_ready = lambda: True
            # Revoke the token during the awaited body read; confirmation must fail.
            original = server._body

            async def revoked_body(*args):
                result = await original(*args)
                db.revoke_games_token(tid, 1, 10)
                return result

            server._body = revoked_body
            before = len(calls)
            await request('POST', confirm, 401, json={})
            assert len(calls) == before
            await request('GET', '/v1/games', 401)
        finally:
            await runner.cleanup()
            db.conn.close()


async def check_commands():
    common = ModuleType('tle.util.codeforces_common')
    common.user_db = Db()
    sys.modules[common.__name__] = common
    from tle.cogs._games_tokens import GamesTokenMixin

    class GameCog(GamesTokenMixin, commands.Cog):
        def _has_server_mod_role(self, member):
            return any(role.name == 'Moderator' for role in member.roles)

        def _linkedin_games(self):
            return ['queens', 'tango']

        def _has_linkedin_mod_access(self, guild_id, game, member):
            return any(role.name == 'Moderator' for role in member.roles)

    bot = commands.Bot(command_prefix=';', intents=discord.Intents.none())
    cog = GameCog()
    await bot.add_cog(cog)
    ctx = SimpleNamespace(guild=SimpleNamespace(id=1), send=AsyncMock(),
                          author=SimpleNamespace(id=10, roles=[], send=AsyncMock()))
    command = bot.get_command('make-games-token')
    assert command and command.checks
    try:
        await command.callback(cog, ctx)
    except commands.CheckFailure:
        pass
    else:
        raise AssertionError('Non-moderator minted token')
    ctx.author.roles = [SimpleNamespace(name='Moderator')]
    await command.callback(cog, ctx)
    assert 'tlegames_' in ctx.author.send.call_args.args[0]
    assert 'tlegames_' not in repr(ctx.send.call_args_list)
    rows = common.user_db.list_games_tokens(1, 10)
    assert len(rows) == 1
    ctx.author.send.side_effect = discord.Forbidden(SimpleNamespace(status=403, reason='DM disabled'), '')
    await command.callback(cog, ctx)
    assert len(common.user_db.list_games_tokens(1, 10)) == 1
    await bot.get_command('revoke-games-token').callback(cog, ctx, rows[0].id)
    assert not common.user_db.list_games_tokens(1, 10)
    await bot.close()
    common.user_db.conn.close()


if __name__ == '__main__':
    asyncio.run(check_http())
    asyncio.run(check_commands())
    print('Real games HTTP authorization and Discord commands passed.')

"""Real socket/discord.py smoke checks, launched outside the stubbed pytest process."""
import asyncio
import os
from pathlib import Path
import socket
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

try:
    import aiohttp
    from aiohttp import web
    import discord
    from discord.ext import commands
except ImportError as exc:
    print(str(exc))
    sys.exit(77)

# Only the DB package initializer pulls unrelated Codeforces dependencies in.
# Keep the actual mixins, SQLite, HTTP, Discord, and application code intact.
package = ModuleType('tle.util.db')
package.__path__ = [str(Path(__file__).resolve().parents[1] / 'tle' / 'util' / 'db')]
sys.modules['tle.util.db'] = package
from tests.complaint_helpers import COMMIT, ComplaintDb, fake_bot
from tle.util.complaint_http import ComplaintHttpServer
from tle.util.complaints import ComplaintService


async def check_http():
    db, bot = ComplaintDb(), fake_bot()
    service = ComplaintService(bot, lambda: db)
    server = ComplaintHttpServer(bot, service)
    token_id, token = db.create_complaint_token(1, 10)
    headers = {'Authorization': 'Bearer ' + token}
    cid = db.add_complaint(1, 20, 'broken', 'https://discord.com/channels/1/2/3')
    second = db.add_complaint(1, 20, 'another')
    foreign = db.add_complaint(2, 30, 'other server')
    removed = db.add_complaint(1, 20, 'removed')
    db.delete_complaint(removed)
    runner = web.AppRunner(server.create_app(), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    base = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
    async with aiohttp.ClientSession() as session:
        async def request(method, path, expected=200, **kwargs):
            kwargs.setdefault('headers', headers)
            async with session.request(method, base + path, **kwargs) as response:
                body = await response.json()
                assert response.status == expected, (response.status, body)
                assert response.headers['Cache-Control'] == 'no-store'
                assert 'Access-Control-Allow-Origin' not in response.headers
                if expected == 401:
                    assert response.headers['WWW-Authenticate'] == 'Bearer'
                return body

        try:
            await request('GET', '/v1/complaints', 401, headers={})
            await request('GET', '/v1/complaints?token=' + token, 401, headers={})
            await request('GET', '/v1/complaints', 401,
                          headers={'Authorization': 'Bearer tlegf_' + 'x' * 43})
            await request('GET', '/v1/complaints', 401,
                          headers=[('Authorization', 'Bearer ' + token)] * 2)
            listed = await request('GET', '/v1/complaints?limit=1')
            assert listed['complaints'][0]['id'] == second
            assert listed['next_before'] == second
            listed = await request('GET', '/v1/complaints?before=' + str(second))
            assert [row['id'] for row in listed['complaints']] == [cid]
            assert listed['next_before'] is None
            for query in ('status=bad', 'limit=0', 'limit=101', 'before=-1',
                          'limit=1%20OR%201=1', 'before=' + '9' * 100, 'sql=SELECT+1'):
                await request('GET', '/v1/complaints?' + query, 400)
            for target in (foreign, removed, 99999):
                await request('GET', f'/v1/complaints/{target}', 404)
                await request('POST', f'/v1/complaints/{target}/resolve', 404,
                              json={'resolution': 'fixed', 'commit_url': COMMIT})
            await request('GET', '/v1/complaints/not-an-id', 400)
            await request('POST', '/v1/complaints', 405)
            await request('GET', '/v1/sql', 404)
            resolve = f'/v1/complaints/{cid}/resolve'
            valid = {'resolution': 'fixed @everyone', 'commit_url': COMMIT}
            await request('POST', resolve, 415, data='{}')
            await request('POST', resolve, 400, data='{',
                          headers={**headers, 'Content-Type': 'application/json'})
            for payload in ([], {}, {**valid, 'sql': 'delete'},
                            {**valid, 'resolution': 2}, {**valid, 'resolution': ' '},
                            {**valid, 'commit_url': 'https://evil.test/commit/abc1234'}):
                await request('POST', resolve, 400, json=payload)
            await request('POST', resolve, 413,
                          json={**valid, 'resolution': 'x' * 20000})
            resolved = await request('POST', resolve, json=valid)
            assert resolved['complaint']['status'] == 'resolved'
            # API resolutions are queued until the commit is verified to be
            # running; nothing reaches the complainant yet.
            assert resolved['complaint']['notification_status'] == 'queued'
            assert bot.channel.send.await_count == 0
            await request('POST', resolve, json=valid)
            assert bot.channel.send.await_count == 0
            await request('POST', resolve, 409, json={**valid, 'resolution': 'different'})
            detail = await request('GET', f'/v1/complaints/{cid}')
            assert detail['events'][0]['token_id'] == token_id
            listed = await request('GET', '/v1/complaints?status=resolved')
            assert [row['id'] for row in listed['complaints']] == [cid]
            await request('POST', f'/v1/complaints/{cid}/reopen', json={})
            assert db.get_complaint(cid).resolved_at is None
            assert len(db.get_complaint_events(cid, 1)) == 2
            # Revocation and role removal take effect even after admission,
            # while a mutation is waiting behind an in-flight notification.
            original_resolve = service.resolve
            original_reopen = service.reopen
            for action, lose_role in (('resolve', False), ('reopen', False),
                                      ('resolve', True), ('reopen', True)):
                if action == 'reopen':
                    db.resolve_complaint(cid, 1, 10, 'fixed', COMMIT)
                local_id, local_token = db.create_complaint_token(1, 10)
                admitted = asyncio.Event()
                original = original_resolve if action == 'resolve' else original_reopen

                async def waiting(*args, **kwargs):
                    admitted.set()
                    return await original(*args, **kwargs)

                setattr(service, action, waiting)
                await service._lock.acquire()
                pending = asyncio.create_task(request(
                    'POST', f'/v1/complaints/{cid}/{action}', 403 if lose_role else 401,
                    headers={'Authorization': 'Bearer ' + local_token},
                    json=valid if action == 'resolve' else {}))
                await asyncio.wait_for(admitted.wait(), 5)
                if lose_role:
                    bot.admin.roles = []
                else:
                    db.revoke_complaint_token(local_id, 1)
                service._lock.release()
                await pending
                assert (db.get_complaint(cid).resolved_at is not None) == (action == 'reopen')
                bot.admin.roles = [SimpleNamespace(name='Admin')]
                setattr(service, action, original)
                db.reopen_complaint(cid, 1, 10)
            bot.admin.roles = []
            await request('GET', '/v1/complaints', 403)
            bot.admin.roles = [SimpleNamespace(name='Moderator')]
            await request('GET', '/v1/complaints', 403)
            bot.admin.guild_permissions = SimpleNamespace(administrator=True)
            await request('GET', '/v1/complaints')
            bot.is_ready = lambda: False
            await request('GET', '/v1/complaints', 503)
            bot.is_ready = lambda: True
            server._active = 8
            await request('GET', '/v1/complaints', 503)
            server._active = 0
            db.revoke_complaint_token(token_id, 1)
            await request('GET', '/v1/complaints', 401)
        finally:
            await runner.cleanup()
            await service.close()
            db.conn.close()
    # The socket must close with the runner.
    try:
        await asyncio.open_connection('127.0.0.1', int(base.rsplit(':', 1)[1]))
    except OSError:
        pass
    else:
        raise AssertionError('HTTP socket was left open')


async def check_commands():
    # Real discord.py command construction/checks, only unrelated bot utilities stubbed.
    common = ModuleType('tle.util.codeforces_common')
    common.user_db = ComplaintDb()
    embeds = ModuleType('tle.util.discord_common')
    embeds.embed_neutral = lambda text: discord.Embed(description=text)
    embeds.embed_alert = embeds.embed_neutral
    embeds.embed_success = embeds.embed_neutral
    sys.modules[common.__name__] = common
    sys.modules[embeds.__name__] = embeds
    from tle.cogs.complain import Complain
    bot = commands.Bot(command_prefix=';', intents=discord.Intents.none())
    await bot.add_cog(Complain(bot))
    cog = bot.get_cog('Complain')
    ctx = SimpleNamespace(guild=SimpleNamespace(id=1), send=AsyncMock(),
                          author=SimpleNamespace(id=10, roles=[], send=AsyncMock()))
    for name in ('maketoken', 'tokens', 'revoketoken', 'complain resolve', 'complain reopen'):
        command = bot.get_command(name)
        assert command is not None and command.checks
        if ' ' not in name:
            assert command.hidden
        for check in command.checks:
            try:
                await check(ctx)
            except commands.CheckFailure:
                pass
            else:
                raise AssertionError('Non-admin admitted to ' + name)
    ctx.author.roles = [SimpleNamespace(name='Admin')]
    await bot.get_command('maketoken').callback(cog, ctx, 30)
    rows = common.user_db.list_complaint_tokens(1)
    assert len(rows) == 1
    assert 'tlegf_' in ctx.author.send.call_args.args[0]
    assert 'tlegf_' not in repr(ctx.send.call_args_list)
    ctx.author.send.side_effect = discord.Forbidden(SimpleNamespace(status=403, reason='DM disabled'), '')
    await bot.get_command('maketoken').callback(cog, ctx, 30)
    assert len(common.user_db.list_complaint_tokens(1)) == 1
    await bot.get_command('revoketoken').callback(cog, ctx, rows[0].id)
    assert common.user_db.list_complaint_tokens(1) == []
    await bot.close()
    common.user_db.conn.close()


async def check_lifecycle():
    db, bot = ComplaintDb(), fake_bot()
    service = ComplaintService(bot, lambda: db)
    server = ComplaintHttpServer(bot, service)
    with patch.dict(os.environ, {'COMPLAINT_API_ENABLED': '0'}):
        await server.start()
        assert server.runner is None
    with patch.dict(os.environ, {'COMPLAINT_API_ENABLED': '1',
                                'COMPLAINT_API_HOST': '127.0.0.1',
                                'COMPLAINT_API_PORT': '65536'}):
        try:
            await server.start()
        except Exception as exc:
            assert 'port' in str(exc)
        else:
            raise AssertionError('Invalid port accepted')
    # Exercise the production start() path, not just create_app(). Older
    # aiohttp can bind a socket successfully yet reject its handler options
    # only when the first connection arrives.
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    with patch.dict(os.environ, {'COMPLAINT_API_ENABLED': '1',
                                'COMPLAINT_API_HOST': '127.0.0.1',
                                'COMPLAINT_API_PORT': str(port)}):
        await server.start()
        runner = server.runner
        await server.start()
        assert server.runner is runner
        try:
            async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5)) as client:
                async with client.get(f'http://127.0.0.1:{port}/v1/complaints') as response:
                    assert response.status == 401
                    assert (await response.json())['error'] == 'Invalid or expired bearer token.'
        finally:
            await server.close()
        assert server.runner is None
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
        except OSError:
            pass
        else:
            writer.close()
            await writer.wait_closed()
            raise AssertionError('Production HTTP listener was left open')
    service.start()
    task = service._worker
    service.start()
    assert task is service._worker
    await service.close()
    assert task.done()
    await server.close()
    db.conn.close()


async def main():
    await check_http()
    await check_commands()
    await check_lifecycle()
    print('Real HTTP, command authorization, notifications, and lifecycle checks passed.')


if __name__ == '__main__':
    asyncio.run(main())

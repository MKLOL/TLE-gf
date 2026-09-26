"""Small authenticated complaints API sharing the bot's event loop and DB."""
import asyncio
import json
import logging
import os

import discord
from aiohttp import web

from tle.util.db.complaint_db import normalize_tag
from tle.util.complaints import ComplaintError, complaint_json, is_complaint_admin

logger = logging.getLogger(__name__)


def positive_int(value, name, maximum=2**63 - 1):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise ComplaintError(400, f'{name} must be a positive integer.')
    if len(value) > 19 or not 1 <= int(value) <= maximum:
        raise ComplaintError(400, f'{name} is out of range.')
    return int(value)


class ComplaintHttpServer:
    def __init__(self, bot, service):
        self.bot = bot
        self.service = service
        self.runner = None
        self._active = 0

    async def _authenticate(self, request):
        if not self.bot.is_ready():
            raise ComplaintError(503, 'Bot is not ready.')
        values = request.headers.getall('Authorization', [])
        pieces = values[0].split() if len(values) == 1 else []
        token = None
        if len(pieces) == 2 and pieces[0].lower() == 'bearer':
            token = self.service.db.authenticate_complaint_token(pieces[1])
        if token is None:
            raise ComplaintError(401, 'Invalid or expired bearer token.')
        guild = self.bot.get_guild(int(token.guild_id))
        if guild is None:
            raise ComplaintError(403, 'Token server is unavailable.')
        member = guild.get_member(int(token.user_id))
        if member is None:
            try:
                member = await asyncio.wait_for(guild.fetch_member(int(token.user_id)), 5)
            except discord.NotFound:
                raise ComplaintError(403, 'Token issuer is no longer an admin.') from None
            except (discord.HTTPException, OSError, asyncio.TimeoutError):
                raise ComplaintError(503, 'Cannot verify token issuer.') from None
        if not is_complaint_admin(member):
            raise ComplaintError(403, 'Token issuer is no longer an admin.')
        # Member lookup may await Discord: recheck expiry/revocation afterwards.
        if self.service.db.authenticate_complaint_token(pieces[1]) is None:
            raise ComplaintError(401, 'Invalid or expired bearer token.')
        request['token'] = token

    async def _dispatch(self, request, handler):
        self._active += 1
        try:
            if self._active > 8:
                raise ComplaintError(503, 'API is busy; retry later.')
            await self._authenticate(request)
            response = await handler(request)
        except ComplaintError as exc:
            response = web.json_response({'error': str(exc)}, status=exc.status)
            if exc.status == 401:
                response.headers['WWW-Authenticate'] = 'Bearer'
        except web.HTTPException as exc:
            response = web.json_response({'error': exc.reason}, status=exc.status)
            if 'Allow' in exc.headers:
                response.headers['Allow'] = exc.headers['Allow']
        except Exception:
            logger.exception('Complaint API request failed')
            response = web.json_response({'error': 'Internal server error.'}, status=500)
        finally:
            self._active -= 1
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    def create_app(self):
        @web.middleware
        async def middleware(request, handler):
            return await self._dispatch(request, handler)

        app = web.Application(middlewares=[middleware], client_max_size=16 * 1024)
        app.router.add_get('/v1/complaints', self._list)
        app.router.add_get('/v1/complaints/{id}', self._get)
        app.router.add_post('/v1/complaints/{id}/resolve', self._resolve)
        app.router.add_post('/v1/complaints/{id}/reopen', self._reopen)
        return app

    async def _list(self, request):
        if set(request.query) - {'status', 'limit', 'before', 'tag'}:
            raise ComplaintError(400, 'Unknown query parameter.')
        status = request.query.get('status', 'open')
        if status not in ('open', 'resolved', 'all'):
            raise ComplaintError(400, 'Status must be open, resolved, or all.')
        limit = positive_int(request.query.get('limit', '50'), 'limit', 100)
        before = request.query.get('before')
        if before is not None:
            before = positive_int(before, 'before')
        # Unlike ;complain list, the API returns tagged complaints by default:
        # automation that has always fetched status=all must keep seeing
        # everything, and `tags` on each row says what a moderator parked.
        # `tag=untagged` is the Discord default view; a name selects one tag.
        tag, include_tagged = request.query.get('tag'), True
        if tag in (None, 'all'):
            tag = None
        elif tag == 'untagged':
            tag, include_tagged = None, False
        else:
            try:
                tag = normalize_tag(tag)
            except ValueError as exc:
                raise ComplaintError(400, str(exc)) from None
        db = self.service.db
        rows = db.get_complaints(request['token'].guild_id, status, limit + 1,
                                 before, tag=tag, include_tagged=include_tagged)
        page = rows[:limit]
        tags = db.get_tags_for_complaints([row.id for row in page])
        return web.json_response({
            'complaints': [complaint_json(row, tags=tags.get(row.id, ()))
                           for row in page],
            'next_before': rows[limit - 1].id if len(rows) > limit else None,
        })

    def _target(self, request):
        return request['token'], positive_int(request.match_info['id'], 'id')

    async def _get(self, request):
        token, complaint_id = self._target(request)
        row = self.service.get(token.guild_id, complaint_id)
        return web.json_response({
            'complaint': complaint_json(
                row, include_context=True,
                tags=self.service.db.get_complaint_tags(complaint_id)),
            'events': [event._asdict() for event in self.service.db.get_complaint_events(
                complaint_id, token.guild_id)],
        })

    async def _body(self, request, fields):
        if request.content_type != 'application/json':
            raise ComplaintError(415, 'Use Content-Type: application/json.')
        try:
            body = await asyncio.wait_for(request.json(), 5)
        except (ValueError, UnicodeError, json.JSONDecodeError):
            raise ComplaintError(400, 'Invalid JSON body.') from None
        except asyncio.TimeoutError:
            raise ComplaintError(408, 'Request body timed out.') from None
        if not isinstance(body, dict) or set(body) != set(fields):
            raise ComplaintError(400, 'Expected JSON fields: ' + ', '.join(fields))
        return body

    async def _resolve(self, request):
        token, complaint_id = self._target(request)
        body = await self._body(request, ('resolution', 'commit_url'))
        row = await self.service.resolve(
            token.guild_id, complaint_id, token.user_id,
            body['resolution'], body['commit_url'], token.id,
            authorize=lambda: self._authenticate(request),
            defer_notification=True)
        return web.json_response({'complaint': self._json(row)})

    async def _reopen(self, request):
        token, complaint_id = self._target(request)
        await self._body(request, ())
        row = await self.service.reopen(
            token.guild_id, complaint_id, token.user_id, token.id,
            authorize=lambda: self._authenticate(request))
        return web.json_response({'complaint': self._json(row)})

    def _json(self, row):
        return complaint_json(row, tags=self.service.db.get_complaint_tags(row.id))

    async def start(self):
        if self.runner is not None or os.environ.get('COMPLAINT_API_ENABLED', '1') == '0':
            return
        host = os.environ.get('COMPLAINT_API_HOST', '0.0.0.0')
        port = positive_int(os.environ.get('COMPLAINT_API_PORT', '8080'), 'port', 65535)
        # aiohttp 3.8 forwards unknown runner options to RequestHandler: passing
        # shutdown_timeout here binds successfully but resets every connection.
        runner = web.AppRunner(self.create_app(), access_log=None)
        try:
            await runner.setup()
            await web.TCPSite(runner, host, port).start()
        except BaseException:
            await runner.cleanup()
            raise
        self.runner = runner
        logger.info('Complaint API listening on %s:%s', host, port)

    async def close(self):
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

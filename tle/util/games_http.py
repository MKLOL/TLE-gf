"""Games routes on the complaint API listener, with separate credentials."""
import asyncio

import discord
from aiohttp import web

from tle.util.complaints import ComplaintError
from tle.util.games_import import GamesImportService
from tle.util.games_submit import GamesSubmissionService


class GamesHttpRoutes:
    def __init__(self, server):
        self.server = server
        self.service = GamesImportService(server.bot, lambda: server.service.db)
        self.submissions = GamesSubmissionService(self.service)

    def add_routes(self, app):
        app.router.add_get('/v1/games', self.catalog)
        app.router.add_post('/v1/games/results', self.submit)
        app.router.add_post('/v1/games/imports/preview', self.preview)
        app.router.add_post('/v1/games/imports/{id}/confirm', self.confirm)

    async def authenticate(self, request):
        bot, db = self.server.bot, self.service.db
        if not bot.is_ready():
            raise ComplaintError(503, 'Bot is not ready.')
        values = request.headers.getall('Authorization', [])
        parts = values[0].split() if len(values) == 1 else []
        token = db.authenticate_games_token(parts[1]) if (
            len(parts) == 2 and parts[0].lower() == 'bearer') else None
        if token is None:
            raise ComplaintError(401, 'Invalid or expired games token.')
        guild = bot.get_guild(int(token.guild_id))
        if guild is None:
            raise ComplaintError(403, 'Token server is unavailable.')
        member = guild.get_member(int(token.user_id))
        if member is None:
            try:
                member = await asyncio.wait_for(guild.fetch_member(int(token.user_id)), 5)
            except discord.NotFound:
                raise ComplaintError(403, 'Token owner has left the server.') from None
            except (discord.HTTPException, OSError, asyncio.TimeoutError):
                raise ComplaintError(503, 'Cannot verify token owner.') from None
        if db.authenticate_games_token(parts[1]) is None:
            raise ComplaintError(401, 'Invalid or expired games token.')
        if (request.path.startswith('/v1/games/imports/')
                and not self.service.cog._has_server_mod_role(member)):
            raise ComplaintError(403, 'Token owner no longer has game import access.')
        request['token'], request['guild'], request['member'] = token, guild, member
        if request.query:
            raise ComplaintError(400, 'Unknown query parameter.')

    async def catalog(self, request):
        return web.json_response(self.service.catalog(request['guild'], request['member']))

    async def preview(self, request):
        body = await self.server._body(
            request, ('game', 'puzzle_date', 'puzzle_number', 'leaderboard'))
        # Reading the body awaits IO; roles and token revocation may have changed.
        await self.authenticate(request)
        return web.json_response(self.service.preview(
            request['token'], request['guild'], request['member'], body))

    async def confirm(self, request):
        await self.server._body(request, ())
        await self.authenticate(request)
        return web.json_response(self.service.confirm(
            request['token'], request['guild'], request['member'], request.match_info['id']))

    async def submit(self, request):
        body = await self.server._body(request, (
            'game', 'puzzle_date', 'puzzle_number', 'time_seconds', 'accuracy', 'is_perfect'))
        await self.authenticate(request)
        return web.json_response(await self.submissions.submit(
            request['guild'], request['member'], body,
            reauthenticate=lambda: self.authenticate(request)))

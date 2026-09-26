"""Personal games API token commands, composed into the Minigames cog."""
import asyncio

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common


class GamesTokenMixin:
    @commands.command(name='make-games-token')
    @commands.guild_only()
    async def make_games_token(self, ctx, days: int = 30):
        """DM a personal games import token (1–365 days; default 30)."""
        if not self._has_server_mod_role(ctx.author):
            raise commands.CheckFailure('Server Admin or Moderator role required.')
        if not 1 <= days <= 365:
            raise commands.BadArgument('Token lifetime must be 1 to 365 days.')
        db = cf_common.user_db
        token_id, token = db.create_games_token(ctx.guild.id, ctx.author.id, days)
        delivered = False
        try:
            await asyncio.wait_for(ctx.author.send(
                f'Games token #{token_id} for server {ctx.guild.id}; expires in '
                f'{days} days. Paste it into the TLE LinkedIn Games Chrome '
                'extension. Your existing game import permissions still apply. '
                'Register your own LinkedIn name with `;queens register NAME` '
                'or `;tango register NAME` first.\n\n'
                f'`{token}`\n\nKeep this private. Revoke it in that server with '
                f'`;revoke-games-token {token_id}`.',
                allowed_mentions=discord.AllowedMentions.none()), 20)
            delivered = True
        except (discord.HTTPException, OSError, asyncio.TimeoutError):
            pass
        finally:
            if not delivered:
                db.revoke_games_token(token_id, ctx.guild.id, ctx.author.id)
        await ctx.send(
            f'Games token #{token_id} sent by DM.' if delivered else
            'Could not DM you. Enable DMs and try again; the token was revoked.')

    @commands.command(name='games-tokens')
    @commands.guild_only()
    async def games_tokens(self, ctx):
        """List your active games token IDs in this server."""
        rows = cf_common.user_db.list_games_tokens(ctx.guild.id, ctx.author.id)
        lines = [f'#{row.id} — expires <t:{int(row.expires_at)}:R>' for row in rows]
        for start in range(0, max(1, len(lines)), 15):
            await ctx.send('\n'.join(lines[start:start + 15]) or
                           'You have no active games tokens in this server.')

    @commands.command(name='revoke-games-token')
    @commands.guild_only()
    async def revoke_games_token(self, ctx, token_id: int):
        """Revoke one of your games tokens in this server."""
        if not 1 <= token_id < 2**63:
            raise commands.BadArgument('Invalid token ID.')
        revoked = cf_common.user_db.revoke_games_token(
            token_id, ctx.guild.id, ctx.author.id)
        await ctx.send('Games token revoked.' if revoked else 'Games token not found.')

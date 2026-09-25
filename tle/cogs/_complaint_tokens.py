"""Hidden admin-only commands for complaint API credentials."""
import asyncio

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common, discord_common
from tle.util.complaints import is_complaint_admin


def require_complaint_admin(ctx):
    if ctx.guild is None or not is_complaint_admin(ctx.author):
        raise commands.CheckFailure('Server Admin role or Administrator permission required.')
    return True


class ComplaintTokenMixin:
    @commands.command(hidden=True)
    @commands.check(require_complaint_admin)
    async def maketoken(self, ctx, days: int = 30):
        """DM a complaints API token for this server (1–365 days; default 30)."""
        require_complaint_admin(ctx)
        if not 1 <= days <= 365:
            await ctx.send('Token lifetime must be 1 to 365 days.')
            return
        db = cf_common.user_db
        token_id, token = db.create_complaint_token(ctx.guild.id, ctx.author.id, days)
        delivered = False
        try:
            await asyncio.wait_for(ctx.author.send(
                f'Complaints API token #{token_id} for server {ctx.guild.id}. '
                f'Expires in {days} days. Keep it private; it permits reading and '
                f'resolving this server\'s complaints.\n\n'
                f'`{token}`\n\nUse `Authorization: Bearer <token>`. '
                f'Revoke with `;revoketoken {token_id}` in that server.',
                allowed_mentions=discord.AllowedMentions.none()), 20)
            delivered = True
        except (discord.HTTPException, OSError, asyncio.TimeoutError):
            pass
        finally:
            if not delivered:
                db.revoke_complaint_token(token_id, ctx.guild.id)
        if delivered:
            await ctx.send(f'Token #{token_id} sent by DM. Use `;revoketoken {token_id}` to revoke it.')
        else:
            await ctx.send('Could not DM you. Enable DMs and try again; the token was revoked.')

    @commands.command(hidden=True)
    @commands.check(require_complaint_admin)
    async def tokens(self, ctx):
        """List up to 100 active complaint API token IDs for this server."""
        require_complaint_admin(ctx)
        rows = cf_common.user_db.list_complaint_tokens(ctx.guild.id)
        lines = [f'#{row.id} — issuer {row.user_id} — expires <t:{int(row.expires_at)}:R>'
                 for row in rows]
        for start in range(0, max(1, len(lines)), 15):
            await ctx.send(embed=discord_common.embed_neutral(
                '\n'.join(lines[start:start + 15]) or 'No active complaint API tokens.'))

    @commands.command(hidden=True)
    @commands.check(require_complaint_admin)
    async def revoketoken(self, ctx, token_id: int):
        """Revoke a complaint API token by its non-secret ID."""
        require_complaint_admin(ctx)
        if not 1 <= token_id < 2**63:
            raise commands.BadArgument('Invalid token ID.')
        revoked = cf_common.user_db.revoke_complaint_token(token_id, ctx.guild.id)
        await ctx.send('Token revoked.' if revoked else 'Token not found in this server.')

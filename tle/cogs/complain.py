import datetime
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.complaints import ComplaintError, ComplaintService
from tle.cogs._complaint_tokens import ComplaintTokenMixin, require_complaint_admin

logger = logging.getLogger(__name__)

_RATE_LIMIT = 5
_RATE_WINDOW = 6 * 3600  # 6 hours in seconds
_MAX_COMPLAINT_LENGTH = 500


class Complain(ComplaintTokenMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.service = ComplaintService(bot, lambda: cf_common.user_db)

    @commands.group(brief='Complaints', invoke_without_command=True)
    async def complain(self, ctx, *, text: str = None):
        """Add a complaint or list subcommands.

        This feature is used for admins to track improvements for the server,
        please don't abuse.

        Usage:
          ;complain <text>             — file a complaint
          ;complain list               — view all complaints
          ;complain withdraw <id>      — withdraw your own complaint
          ;complain remove <id or ids> — remove complaints (admin only)
          ;complain resolve <id> <commit_url> <summary> — resolve (admin only)
          ;complain reopen <id>        — reopen (admin only)
          ;complain list resolved      — view resolved complaints
        """
        if text is None:
            await ctx.send_help(ctx.command)
            return

        # Rate limit check for non-privileged users
        author = ctx.author
        is_privileged = any(
            r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
            for r in author.roles
        )
        if not is_privileged:
            since = time.time() - _RATE_WINDOW
            count = cf_common.user_db.count_recent_complaints(
                ctx.guild.id, author.id, since
            )
            if count >= _RATE_LIMIT:
                await ctx.send(embed=discord_common.embed_alert(
                    f'Rate limit reached ({_RATE_LIMIT} complaints per 6 hours). '
                    'Please wait before filing another.'
                ))
                return

        if len(text) > _MAX_COMPLAINT_LENGTH:
            await ctx.send(embed=discord_common.embed_alert(
                f'Complaint too long (max {_MAX_COMPLAINT_LENGTH} characters).'
            ))
            return

        message_link = getattr(ctx.message, 'jump_url', None)
        complaint_id = cf_common.user_db.add_complaint(
            ctx.guild.id, author.id, text, message_link
        )
        logger.info(f'Complaint #{complaint_id} added by {author.id} in guild {ctx.guild.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Complaint #{complaint_id} filed. '
            f'You can withdraw it with `;complain withdraw {complaint_id}`.'
        ))

    @complain.command(brief='List complaints')
    async def list(self, ctx, status: str = 'open'):
        """View complaints: open (default), resolved, or all."""
        if status not in ('open', 'resolved', 'all'):
            raise commands.BadArgument('Status must be open, resolved, or all.')
        complaints = cf_common.user_db.get_complaints(ctx.guild.id, status)
        if not complaints:
            await ctx.send(embed=discord_common.embed_neutral('No complaints filed.'))
            return

        lines = []
        for c in complaints:
            ts = datetime.datetime.fromtimestamp(c.created_at).strftime('%Y-%m-%d %H:%M')
            link = getattr(c, 'message_link', None)
            header = f'**#{c.id}** by <@{c.user_id}> ({ts})'
            if link:
                header += f' — [context]({link})'
            detail = f'{header}\n{c.text}'
            if c.resolved_at is not None:
                detail += f'\n**Resolved:** {c.resolution}\n[Commit]({c.commit_url})'
            lines.append(detail)

        # Paginate in chunks to stay under 4096 embed limit
        pages = []
        current = []
        length = 0
        for line in lines:
            if length + len(line) + 2 > 3900 and current:
                pages.append('\n\n'.join(current))
                current = []
                length = 0
            current.append(line)
            length += len(line) + 2
        if current:
            pages.append('\n\n'.join(current))

        for i, page in enumerate(pages):
            title = 'Complaints' if len(pages) == 1 else f'Complaints (page {i+1}/{len(pages)})'
            embed = discord.Embed(title=title, description=page, color=0xffaa10)
            await ctx.send(embed=embed)

    @complain.command(brief='Resolve a complaint and notify its author')
    @commands.check(require_complaint_admin)
    async def resolve(self, ctx, complaint_id: int, commit_url: str, *, resolution: str):
        """Resolve with a GitHub commit: ;complain resolve <id> <commit_url> <summary>."""
        require_complaint_admin(ctx)
        try:
            row = await self.service.resolve(ctx.guild.id, complaint_id, ctx.author.id,
                                             resolution, commit_url)
        except ComplaintError as exc:
            await ctx.send(embed=discord_common.embed_alert(str(exc)))
            return
        notice = ('Author notified.' if row.notification_status == 'sent' else
                  'Author notification failed; delivery will be retried. '
                  'Repeat this command to retry manually.')
        await ctx.send(embed=discord_common.embed_success(
            f'Complaint #{row.id} resolved. {notice}'))

    @complain.command(brief='Reopen a resolved complaint')
    @commands.check(require_complaint_admin)
    async def reopen(self, ctx, complaint_id: int):
        """Reopen a complaint while retaining its resolution audit history."""
        require_complaint_admin(ctx)
        try:
            await self.service.reopen(ctx.guild.id, complaint_id, ctx.author.id)
        except ComplaintError as exc:
            await ctx.send(embed=discord_common.embed_alert(str(exc)))
            return
        await ctx.send(embed=discord_common.embed_success(f'Complaint #{complaint_id} reopened.'))

    @complain.command(brief='Withdraw your own complaint')
    async def withdraw(self, ctx, complaint_id: int):
        """Withdraw a complaint you filed.

        Usage:
          ;complain withdraw <id>
        """
        complaint = cf_common.user_db.get_complaint(complaint_id)
        if complaint is None or str(complaint.guild_id) != str(ctx.guild.id):
            await ctx.send(embed=discord_common.embed_alert(
                f'Complaint #{complaint_id} not found.'
            ))
            return
        if str(complaint.user_id) != str(ctx.author.id):
            await ctx.send(embed=discord_common.embed_alert(
                f'Complaint #{complaint_id} is not yours to withdraw.'
            ))
            return
        cf_common.user_db.delete_complaint(complaint_id)
        logger.info(f'Complaint #{complaint_id} withdrawn by {ctx.author.id} in guild {ctx.guild.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Complaint #{complaint_id} withdrawn.'
        ))

    @complain.command(brief='Remove complaint(s)', aliases=['delete'])
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def remove(self, ctx, *, ids: str):
        """Remove one or more complaints by ID. Admin/Moderator only.

        Usage:
          ;complain remove 5
          ;complain remove 1,2,3,4,5
        """
        # Parse comma/space-separated IDs
        raw_parts = ids.replace(',', ' ').split()
        parsed_ids = []
        for part in raw_parts:
            try:
                parsed_ids.append(int(part))
            except ValueError:
                await ctx.send(embed=discord_common.embed_alert(
                    f'Invalid complaint ID: `{part}`'
                ))
                return

        if not parsed_ids:
            await ctx.send(embed=discord_common.embed_alert('No complaint IDs provided.'))
            return

        if len(parsed_ids) == 1:
            # Single ID — use the original path for a specific not-found message
            complaint_id = parsed_ids[0]
            complaint = cf_common.user_db.get_complaint(complaint_id)
            if complaint is None or str(complaint.guild_id) != str(ctx.guild.id):
                await ctx.send(embed=discord_common.embed_alert(
                    f'Complaint #{complaint_id} not found.'
                ))
                return
            cf_common.user_db.delete_complaint(complaint_id)
            logger.info(f'Complaint #{complaint_id} removed by {ctx.author.id} in guild {ctx.guild.id}')
            await ctx.send(embed=discord_common.embed_success(
                f'Complaint #{complaint_id} removed.'
            ))
        else:
            # Bulk delete
            deleted = cf_common.user_db.delete_complaints(parsed_ids, ctx.guild.id)
            id_list = ', '.join(f'#{i}' for i in parsed_ids)
            logger.info(
                f'Bulk complaint removal by {ctx.author.id} in guild {ctx.guild.id}: '
                f'requested {id_list}, deleted {deleted}'
            )
            await ctx.send(embed=discord_common.embed_success(
                f'Removed {deleted} of {len(parsed_ids)} complaints.'
            ))


async def setup(bot):
    await bot.add_cog(Complain(bot))

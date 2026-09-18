"""Core Discord commands for the Great Day cog."""
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common


_DEFAULT_TIME = '10:00'
_DEFAULT_TZ = 'US/Eastern'


class GreatDayCogError(commands.CommandError):
    pass


def _event_identity(message):
    return message.created_at.timestamp(), message.id


async def _send_membership_result(ctx, user_id, embed):
    """Send a recoverable no-ping result tied to its invoking command."""
    if hasattr(embed, 'set_footer'):
        embed.set_footer(text=f'Great Day user ID: {user_id}')
    await ctx.send(embed=embed, reference=ctx.message, mention_author=False)


class GreatDayCommandsMixin:
    """Core membership and configuration commands inherited by ``GreatDay``."""

    @commands.group(name='greatday', brief='Great Day commands',
                    invoke_without_command=True)
    async def greatday(self, ctx):
        await ctx.send_help(ctx.command)

    @greatday.command(name='signup', brief='Sign up for daily great day pings')
    async def signup(self, ctx):
        if cf_common.user_db.greatday_is_banned(ctx.guild.id, ctx.author.id):
            raise GreatDayCogError('You are banned from great day.')
        added = cf_common.user_db.greatday_signup_with_event(
            ctx.guild.id, ctx.author.id, *_event_identity(ctx.message))
        if added:
            await _send_membership_result(
                ctx, ctx.author.id, discord_common.embed_success(
                    'You have been signed up for great day pings!'))
        else:
            await _send_membership_result(
                ctx, ctx.author.id,
                discord_common.embed_alert('You are already signed up.'))

    @greatday.command(name='remove', brief='Remove yourself from the list')
    async def remove(self, ctx):
        removed = cf_common.user_db.greatday_remove_with_event(
            ctx.guild.id, ctx.author.id, *_event_identity(ctx.message))
        if removed:
            await _send_membership_result(
                ctx, ctx.author.id, discord_common.embed_success(
                    'You have been removed from great day pings.'))
        else:
            await _send_membership_result(
                ctx, ctx.author.id,
                discord_common.embed_alert('You are not signed up.'))

    @greatday.command(name='add', brief='Add a user to the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def add_user(self, ctx, member: discord.Member):
        if cf_common.user_db.greatday_is_banned(ctx.guild.id, member.id):
            name = discord.utils.escape_mentions(member.display_name)
            raise GreatDayCogError(
                f'`{name}` is banned from great day. Unban them first.')
        added = cf_common.user_db.greatday_signup_with_event(
            ctx.guild.id, member.id, *_event_identity(ctx.message))
        name = discord.utils.escape_mentions(member.display_name)
        if added:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_success(
                    f'`{name}` has been added to great day pings.'))
        else:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_alert(
                    f'`{name}` is already signed up.'))

    @greatday.command(name='kick', brief='Remove a user from the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def kick_user(self, ctx, member: discord.Member):
        removed = cf_common.user_db.greatday_remove_with_event(
            ctx.guild.id, member.id, *_event_identity(ctx.message))
        name = discord.utils.escape_mentions(member.display_name)
        if removed:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_success(
                    f'`{name}` has been removed from great day pings.'))
        else:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_alert(
                    f'`{name}` is not signed up.'))

    @greatday.command(name='ban', brief='Ban a user from great day (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def ban_user(self, ctx, member: discord.Member):
        banned = cf_common.user_db.greatday_ban_with_event(
            ctx.guild.id, member.id, *_event_identity(ctx.message))
        name = discord.utils.escape_mentions(member.display_name)
        if banned:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_success(
                    f'`{name}` has been banned from great day.'))
        else:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_alert(
                    f'`{name}` is already banned.'))

    @greatday.command(name='unban', brief='Unban a user from great day (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def unban_user(self, ctx, member: discord.Member):
        unbanned = cf_common.user_db.greatday_unban(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if unbanned:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_success(
                    f'`{name}` has been unbanned from great day.'))
        else:
            await _send_membership_result(
                ctx, member.id, discord_common.embed_alert(
                    f'`{name}` is not banned.'))

    @greatday.command(name='banlist', brief='Show users banned from great day')
    async def banlist(self, ctx):
        rows = cf_common.user_db.greatday_get_banned(ctx.guild.id)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral(
                'No one is banned from great day.'))
            return
        lines = [f'<@{r.user_id}>' for r in rows]
        embed = discord.Embed(title='Great day ban list',
                              description='\n'.join(lines))
        await ctx.send(embed=embed,
                       allowed_mentions=discord.AllowedMentions.none())

    @greatday.command(name='here', brief='Set the great day channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        cf_common.user_db.set_guild_config(
            ctx.guild.id, 'greatday_channel', str(ctx.channel.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Great day channel set to {ctx.channel.mention}'))

    @greatday.command(name='now', brief='Send a great day message now')
    @commands.has_role(constants.TLE_ADMIN)
    async def now(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        if not channel_id:
            raise GreatDayCogError(
                'No great day channel set. Use `;greatday here` first.')
        rows = cf_common.user_db.greatday_get_signups(ctx.guild.id)
        if not rows:
            raise GreatDayCogError('No one has signed up yet.')
        sent = await self._send_greatday(ctx.guild)
        if sent:
            today = datetime.now(ZoneInfo(_DEFAULT_TZ)).strftime('%Y-%m-%d')
            cf_common.user_db.kvs_set(f'greatday_last:{ctx.guild.id}', today)
            await ctx.send(embed=discord_common.embed_success(
                'Great day message sent!'))
        else:
            raise GreatDayCogError('Could not send great day message.')

    @greatday.command(name='time', brief='Set the daily time (HH:MM US/Eastern)',
                      usage='HH:MM')
    @commands.has_role(constants.TLE_ADMIN)
    async def set_time(self, ctx, time_str: str):
        parts = time_str.split(':')
        if len(parts) != 2:
            raise GreatDayCogError(
                'Time must be in HH:MM format (e.g. `10:00`).')
        try:
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            raise GreatDayCogError(
                'Time must be in HH:MM format (e.g. `10:00`).')

        formatted = f'{hour:02d}:{minute:02d}'
        cf_common.user_db.set_guild_config(
            ctx.guild.id, 'greatday_time', formatted)
        await ctx.send(embed=discord_common.embed_success(
            f'Great day time set to **{formatted}** US/Eastern.'))

    @greatday.command(name='show', brief='Show current settings')
    async def show(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        time_str = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_time') or _DEFAULT_TIME
        rows = cf_common.user_db.greatday_get_signups(ctx.guild.id)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'Channel: {channel}',
            f'Time: **{time_str}** US/Eastern',
            f'Signed up: **{len(rows)}** user(s)',
        ]
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

import logging
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds between checks
_DEFAULT_TIME = '10:00'
_DEFAULT_TZ = 'US/Eastern'
_PICK_COUNT = 5


class GreatDayCogError(commands.CommandError):
    pass


class GreatDay(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._check_task.start()

    @tasks.task_spec(name='GreatDayCheck',
                     waiter=tasks.Waiter.fixed_delay(_CHECK_INTERVAL))
    async def _check_task(self, _):
        now = datetime.now(ZoneInfo(_DEFAULT_TZ))
        current_time = now.strftime('%H:%M')
        today = now.strftime('%Y-%m-%d')

        for guild in self.bot.guilds:
            configured_time = cf_common.user_db.get_guild_config(
                guild.id, 'greatday_time') or _DEFAULT_TIME
            if current_time != configured_time:
                continue

            kvs_key = f'greatday_last:{guild.id}'
            last_sent = cf_common.user_db.kvs_get(kvs_key)
            if last_sent == today:
                continue

            if await self._send_greatday(guild):
                cf_common.user_db.kvs_set(kvs_key, today)

    async def _send_greatday(self, guild):
        """Pick random users and send a great day message. Returns True if sent."""
        channel_id = cf_common.user_db.get_guild_config(
            guild.id, 'greatday_channel')
        if not channel_id:
            return False
        channel = guild.get_channel(int(channel_id))
        if channel is None:
            return False

        rows = cf_common.user_db.greatday_get_signups(guild.id)
        if not rows:
            return False

        user_ids = [r.user_id for r in rows]
        picked = random.sample(user_ids, min(_PICK_COUNT, len(user_ids)))
        mentions = ' '.join(f'<@{uid}>' for uid in picked)
        verb = 'is' if len(picked) == 1 else 'are'
        await channel.send(f'I hope {mentions} {verb} having a great day!')
        return True

    # ── Commands ───────────────────────────────────────────────────────

    @commands.group(name='greatday', brief='Great Day commands',
                    invoke_without_command=True)
    async def greatday(self, ctx):
        await ctx.send_help(ctx.command)

    @greatday.command(name='signup', brief='Sign up for daily great day pings')
    async def signup(self, ctx):
        added = cf_common.user_db.greatday_signup(ctx.guild.id, ctx.author.id)
        if added:
            await ctx.send(embed=discord_common.embed_success(
                'You have been signed up for great day pings!'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'You are already signed up.'))

    @greatday.command(name='remove', brief='Remove yourself from the list')
    async def remove(self, ctx):
        removed = cf_common.user_db.greatday_remove(ctx.guild.id, ctx.author.id)
        if removed:
            await ctx.send(embed=discord_common.embed_success(
                'You have been removed from great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'You are not signed up.'))

    @greatday.command(name='add', brief='Add a user to the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def add_user(self, ctx, member: discord.Member):
        added = cf_common.user_db.greatday_signup(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if added:
            await ctx.send(embed=discord_common.embed_success(
                f'`{name}` has been added to great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{name}` is already signed up.'))

    @greatday.command(name='kick', brief='Remove a user from the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def kick_user(self, ctx, member: discord.Member):
        removed = cf_common.user_db.greatday_remove(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if removed:
            await ctx.send(embed=discord_common.embed_success(
                f'`{name}` has been removed from great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{name}` is not signed up.'))

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
            raise GreatDayCogError('Time must be in HH:MM format (e.g. `10:00`).')
        try:
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            raise GreatDayCogError('Time must be in HH:MM format (e.g. `10:00`).')

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

    # ── Error handler ──────────────────────────────────────────────────

    @discord_common.send_error_if(GreatDayCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(GreatDay(bot))

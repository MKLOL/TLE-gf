import asyncio
import datetime
import logging
import os
import subprocess
import sys
import time
import textwrap

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util.codeforces_common import pretty_time_format
from tle.util import discord_common

logger = logging.getLogger(__name__)

_KNOWN_FEATURES = ['starboard_leaderboard']

RESTART = 42


# Adapted from numpy sources.
# https://github.com/numpy/numpy/blob/master/setup.py#L64-85
def git_history():
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(cmd, stdout = subprocess.PIPE, env=env).communicate()[0]
        return out
    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', '--abbrev-ref', 'HEAD'])
        branch = out.strip().decode('ascii')
        out = _minimal_ext_cmd(['git', 'log', '--oneline', '-5'])
        history = out.strip().decode('ascii')
        return (
            'Branch:\n' +
            textwrap.indent(branch, '  ') +
            '\nCommits:\n' +
            textwrap.indent(history, '  ')
        )
    except OSError:
        return "Fetching git info failed"


class Meta(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.start_time = time.time()

    @commands.group(brief='Bot control', invoke_without_command=True)
    async def meta(self, ctx):
        """Command the bot or get information about the bot."""
        await ctx.send_help(ctx.command)

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        """Edit the restart message once the bot is back up."""
        for _ in range(30):
            if cf_common.user_db is not None:
                break
            await asyncio.sleep(1)
        if cf_common.user_db is None:
            return

        val = cf_common.user_db.kvs_get('restart_message')
        if val is None:
            return

        try:
            channel_id, message_id = val.split(':')
            channel = self.bot.get_channel(int(channel_id))
            if channel is None:
                channel = await self.bot.fetch_channel(int(channel_id))
            msg = await channel.fetch_message(int(message_id))
            now = datetime.datetime.now().strftime('%H:%M:%S')
            await msg.edit(content=f'`{now}`: Restart complete.')
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
            logger.warning(f'meta: Could not edit restart message: {e}')
        finally:
            cf_common.user_db.kvs_delete('restart_message')

    @meta.command(brief='Restarts TLE')
    @commands.has_role(constants.TLE_ADMIN)
    async def restart(self, ctx):
        """Restarts the bot."""
        now = datetime.datetime.now().strftime('%H:%M:%S')
        msg = await ctx.send(f'`{now}`: Restarting...')
        cf_common.user_db.kvs_set('restart_message', f'{ctx.channel.id}:{msg.id}')
        os._exit(RESTART)

    @meta.command(brief='Kill TLE')
    @commands.has_role(constants.TLE_ADMIN)
    async def kill(self, ctx):
        """Restarts the bot."""
        await ctx.send('Dying...')
        os._exit(0)

    @meta.command(brief='Is TLE up?')
    async def ping(self, ctx):
        """Replies to a ping."""
        start = time.perf_counter()
        message = await ctx.send(':ping_pong: Pong!')
        end = time.perf_counter()
        duration = (end - start) * 1000
        await message.edit(content=f'REST API latency: {int(duration)}ms\n'
                                   f'Gateway API latency: {int(self.bot.latency * 1000)}ms')

    @meta.command(brief='Get git information')
    async def git(self, ctx):
        """Replies with git information."""
        await ctx.send('```yaml\n' + git_history() + '```')

    @meta.command(brief='Prints bot uptime')
    async def uptime(self, ctx):
        """Replies with how long TLE has been up."""
        await ctx.send('TLE has been running for ' +
                       pretty_time_format(time.time() - self.start_time))

    @meta.command(brief='Print bot guilds')
    @commands.has_role(constants.TLE_ADMIN)
    async def guilds(self, ctx):
        "Replies with info on the bot's guilds"
        msg = [f'Guild ID: {guild.id} | Name: {guild.name} | Owner: {guild.owner.id} | Icon: {guild.icon}'
                for guild in self.bot.guilds]
        await ctx.send('```' + '\n'.join(msg) + '```')

    @meta.group(brief='Feature configuration', invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def config(self, ctx):
        """List or toggle feature flags for this guild.
        Known features: starboard_leaderboard"""
        configs = cf_common.user_db.get_all_guild_configs(ctx.guild.id)
        if not configs:
            await ctx.send(embed=discord_common.embed_neutral('No features enabled.'))
            return
        lines = [f'`{c.key}` = `{c.value}`' for c in configs]
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @config.command(brief='Enable a feature')
    @commands.has_role(constants.TLE_ADMIN)
    async def enable(self, ctx, feature: str):
        """Enable a feature for this guild."""
        if feature not in _KNOWN_FEATURES:
            await ctx.send(embed=discord_common.embed_alert(
                f'Unknown feature `{feature}`. Known features: {", ".join(_KNOWN_FEATURES)}'
            ))
            return
        cf_common.user_db.set_guild_config(ctx.guild.id, feature, '1')
        logger.info(f'CMD config enable: guild={ctx.guild.id} feature={feature} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Feature `{feature}` enabled.'))

    @config.command(brief='Disable a feature')
    @commands.has_role(constants.TLE_ADMIN)
    async def disable(self, ctx, feature: str):
        """Disable a feature for this guild."""
        if feature not in _KNOWN_FEATURES:
            await ctx.send(embed=discord_common.embed_alert(
                f'Unknown feature `{feature}`. Known features: {", ".join(_KNOWN_FEATURES)}'
            ))
            return
        cf_common.user_db.delete_guild_config(ctx.guild.id, feature)
        logger.info(f'CMD config disable: guild={ctx.guild.id} feature={feature} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Feature `{feature}` disabled.'))


async def setup(bot):
    await bot.add_cog(Meta(bot))

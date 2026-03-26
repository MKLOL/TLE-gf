import asyncio
import datetime
import io
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
from tle.cogs._starboard_helpers import _parse_jump_url

logger = logging.getLogger(__name__)

_KNOWN_FEATURES = ['starboard_leaderboard', 'dailyakari']

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
            await msg.edit(content=f'{msg.content}\n`{now}`: Restart complete.')
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
        Known features: starboard_leaderboard, dailyakari"""
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


    @meta.command(brief='Dump Discord message data')
    @commands.has_role(constants.TLE_ADMIN)
    async def log(self, ctx, message_ref: str):
        """Fetch a message by jump URL and dump all its metadata.

        Usage: ;meta log https://discord.com/channels/guild/channel/msg
        """
        parsed = _parse_jump_url(message_ref)
        if parsed is None:
            await ctx.send(embed=discord_common.embed_alert('Invalid message link.'))
            return
        guild_id, channel_id, message_id = parsed

        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except discord.NotFound:
                await ctx.send(embed=discord_common.embed_alert('Channel not found.'))
                return
        try:
            msg = await channel.fetch_message(message_id)
        except discord.NotFound:
            await ctx.send(embed=discord_common.embed_alert('Message not found.'))
            return

        lines = []
        lines.append(f'id:          {msg.id}')
        lines.append(f'type:        {msg.type}')
        lines.append(f'author:      {msg.author} (id={msg.author.id})')
        lines.append(f'created_at:  {msg.created_at}')
        lines.append(f'content:     {msg.content[:300]!r}')
        lines.append(f'attachments: {len(msg.attachments)}')
        lines.append(f'embeds:      {len(msg.embeds)}')
        lines.append(f'reactions:   {len(msg.reactions)}')
        lines.append('')

        for i, att in enumerate(msg.attachments):
            lines.append(f'--- attachment[{i}] ---')
            lines.append(f'  filename:     {att.filename}')
            lines.append(f'  url:          {att.url}')
            lines.append(f'  content_type: {getattr(att, "content_type", None)}')
            lines.append(f'  size:         {att.size}')
            lines.append('')

        for i, e in enumerate(msg.embeds):
            lines.append(f'--- embed[{i}] ---')
            lines.append(f'  type:        {e.type}')
            lines.append(f'  url:         {e.url}')
            lines.append(f'  title:       {e.title}')
            lines.append(f'  description: {e.description!r}' if e.description else '  description: None')
            if e.thumbnail:
                lines.append(f'  thumbnail:')
                for k, v in (e.thumbnail.__dict__ or {}).items():
                    lines.append(f'    {k}: {v}')
            else:
                lines.append(f'  thumbnail:   None')
            if e.video:
                lines.append(f'  video:')
                for k, v in (e.video.__dict__ or {}).items():
                    lines.append(f'    {k}: {v}')
            else:
                lines.append(f'  video:       None')
            if e.image:
                lines.append(f'  image:')
                for k, v in (e.image.__dict__ or {}).items():
                    lines.append(f'    {k}: {v}')
            else:
                lines.append(f'  image:       None')
            if e.provider:
                lines.append(f'  provider:')
                for k, v in (e.provider.__dict__ or {}).items():
                    lines.append(f'    {k}: {v}')
            else:
                lines.append(f'  provider:    None')
            if e.author:
                lines.append(f'  author:')
                for k, v in (e.author.__dict__ or {}).items():
                    lines.append(f'    {k}: {v}')
            else:
                lines.append(f'  author:      None')
            if e.fields:
                for j, f in enumerate(e.fields):
                    lines.append(f'  field[{j}]:   {f.name} = {f.value!r}')
            lines.append('')

        for i, r in enumerate(msg.reactions):
            lines.append(f'reaction[{i}]: {r.emoji} count={r.count}')

        dump = '\n'.join(lines)

        # Send as file if too long for a codeblock, otherwise codeblock
        if len(dump) > 1900:
            await ctx.send(
                file=discord.File(io.StringIO(dump), filename=f'msg_{message_id}.txt')
            )
        else:
            await ctx.send(f'```\n{dump}\n```')


async def setup(bot):
    await bot.add_cog(Meta(bot))

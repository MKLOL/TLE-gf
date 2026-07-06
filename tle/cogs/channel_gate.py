"""Channel command gate: ``;disallow`` / ``;allow``.

``;disallow`` blocks bot (prefix) commands in the current channel (and its
threads). ``;disallow thread`` creates a thread and blocks commands in the
channel's main timeline only — commands stay allowed in *any* thread of that
channel, not just the bot-created one. A global check enforces the gate for
every prefix command; on a blocked attempt it drops a short, auto-deleting
notice (linking the thread when there is one). ``;allow`` reverts.

``;rpoll`` (and its subcommands) is exempt from the gate — rating-weighted
polls run in any channel, even one where bot commands are otherwise blocked.

Only prefix commands are gated — the minigame slash commands run through a
separate app-command path that ``bot.add_check`` does not cover.
"""
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.cogs._channel_gate_helpers import ChannelGateError, gate_decision

logger = logging.getLogger(__name__)

_THREAD_NAME = 'bot-commands'
_THREAD_AUTO_ARCHIVE = 1440  # minutes (1 day)
_NOTICE_DELETE_AFTER = 15    # seconds — the notice auto-deletes so it doesn't pile up
# ``disallow``/``allow`` must stay usable so an admin can always lift the gate;
# ``rpoll`` is exempt so rating-weighted polls can be run in any channel,
# including ones where bot commands are otherwise blocked.
_EXEMPT_COMMANDS = frozenset({'disallow', 'allow', 'rpoll'})


class ChannelGate(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        bot.add_check(self._gate_check)

    def cog_unload(self):
        self.bot.remove_check(self._gate_check)

    # ── enforcement ────────────────────────────────────────────────────
    @staticmethod
    def _location(channel):
        """``(parent_channel_id, current_thread_id)`` for a command's channel.
        A thread maps to its parent; a normal channel maps to itself."""
        if isinstance(channel, discord.Thread):
            return channel.parent_id, channel.id
        return channel.id, None

    async def _gate_check(self, ctx):
        """Global check: runs before every prefix command in a guild."""
        if ctx.guild is None or cf_common.user_db is None:
            return True
        command = ctx.command
        if command is not None \
                and (command.root_parent or command).name in _EXEMPT_COMMANDS:
            return True  # exempt commands always run (see _EXEMPT_COMMANDS)
        parent_id, current_thread_id = self._location(ctx.channel)
        if parent_id is None:
            return True
        gate = cf_common.user_db.get_command_gate(ctx.guild.id, parent_id)
        allowed, allowed_thread_id = gate_decision(gate, current_thread_id)
        if allowed:
            return True
        await self._notify_blocked(ctx, allowed_thread_id)
        raise discord_common.FeatureDisabledSilent()

    async def _notify_blocked(self, ctx, allowed_thread_id):
        if allowed_thread_id is not None:
            text = (f'{ctx.author.mention} bot commands here only work in '
                    f'threads — try <#{allowed_thread_id}>.')
        else:
            text = (f'{ctx.author.mention} bot commands are disabled in this '
                    f'channel.')
        try:
            await ctx.send(
                text, delete_after=_NOTICE_DELETE_AFTER,
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, roles=False, users=True))
        except discord.HTTPException:
            pass

    # ── commands ───────────────────────────────────────────────────────
    def _gate_channel(self, ctx):
        """The text channel a ``;disallow`` / ``;allow`` acts on — the parent
        channel when the command was run inside a thread."""
        channel = ctx.channel
        if isinstance(channel, discord.Thread):
            parent = channel.parent
            if parent is None:
                raise ChannelGateError(
                    "Could not resolve this thread's parent channel.")
            return parent
        return channel

    @commands.command(brief='Disallow bot commands in this channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def disallow(self, ctx, mode: str = None):
        """Disallow bot commands in this channel.

        `;disallow` blocks bot commands in this channel and its threads.
        `;disallow thread` creates a thread and blocks commands in the channel's
        main timeline only — commands stay allowed in any thread of the channel.
        """
        make_thread = mode is not None and mode.lower() == 'thread'
        if mode is not None and not make_thread:
            raise ChannelGateError('Usage: `;disallow` or `;disallow thread`.')
        if make_thread and not isinstance(ctx.channel, discord.TextChannel):
            # A thread can only be spun off a normal text channel — not a forum,
            # voice, or category channel, nor from inside another thread (those
            # would otherwise raise an uncaught error from create_thread).
            raise ChannelGateError(
                '`;disallow thread` only works in a regular text channel.')

        channel = self._gate_channel(ctx)
        thread = await self._create_thread(channel) if make_thread else None

        cf_common.user_db.set_command_gate(
            ctx.guild.id, channel.id, thread.id if thread else None)

        if thread is not None:
            await ctx.send(embed=discord_common.embed_success(
                f'Bot commands are now disabled in {channel.mention} — use a '
                f'thread (e.g. {thread.mention}) instead.'))
        else:
            await ctx.send(embed=discord_common.embed_success(
                f'Bot commands are now disabled in {channel.mention}.'))

    async def _create_thread(self, channel):
        try:
            thread = await channel.create_thread(
                name=_THREAD_NAME, type=discord.ChannelType.public_thread,
                auto_archive_duration=_THREAD_AUTO_ARCHIVE)
        except discord.Forbidden:
            raise ChannelGateError(
                'I need the **Create Public Threads** permission to do that.')
        except discord.HTTPException as e:
            logger.warning('command-gate thread create failed: %s', e)
            raise ChannelGateError('Could not create the thread, try again.')
        try:
            await thread.send(embed=discord_common.embed_neutral(
                'Bot commands are restricted to this thread.'))
        except discord.HTTPException:
            pass
        return thread

    @commands.command(brief='Re-allow bot commands in this channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def allow(self, ctx):
        """Re-enable bot commands in this channel (reverts `;disallow`)."""
        channel = self._gate_channel(ctx)
        removed = cf_common.user_db.clear_command_gate(ctx.guild.id, channel.id)
        if removed:
            await ctx.send(embed=discord_common.embed_success(
                f'Bot commands are now allowed in {channel.mention}.'))
        else:
            await ctx.send(embed=discord_common.embed_neutral(
                f'Bot commands were not disabled in {channel.mention}.'))

    @discord_common.send_error_if(ChannelGateError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(ChannelGate(bot))

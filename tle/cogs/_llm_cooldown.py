"""Moderator command helpers for persistent shared LLM cooldowns."""

import logging

from tle.cogs import _llm_access as llm_access
from tle.util import discord_common
from tle.util.db.llm_cooldown_db import MAX_LLM_COOLDOWN_SECONDS


logger = logging.getLogger(__name__)


async def configure(cog, ctx, arguments):
    """Show or change the current-channel/server LLM cooldown."""
    if not await cog._require_guild_moderator(ctx):
        return
    seconds, global_scope, error = _parse(arguments)
    if error is not None:
        await ctx.send(embed=discord_common.embed_alert(error))
        return

    database = cog._llm_db()
    channel_id = llm_access.scope_channel_id(ctx.channel)
    if seconds is None:
        settings = database.llm_get_cooldown_settings(
            ctx.guild.id, channel_id)
        channel = _duration(settings.get('channel'))
        global_value = _duration(settings.get('global'))
        await ctx.send(embed=discord_common.embed_neutral(
            f'Channel cooldown: **{channel}** (includes its threads).\n'
            f'Server-wide cooldown: **{global_value}**.\n'
            'When both are configured, a prompt must clear both.'))
        return

    target_channel = None if global_scope else channel_id
    if target_channel is None and not global_scope:
        await ctx.send(embed=discord_common.embed_alert(
            'Could not identify this channel.'))
        return
    database.llm_set_cooldown(
        ctx.guild.id, seconds, channel_id=target_channel)
    scope = ('server-wide' if global_scope
             else 'for this channel and its threads')
    logger.warning(
        'LLM cooldown changed by user=%s guild=%s channel=%s seconds=%s',
        ctx.author.id, ctx.guild.id, target_channel, seconds)
    if seconds == 0:
        message = f'The shared LLM cooldown {scope} was removed.'
    else:
        message = (
            f'LLM requests now have a shared {seconds}-second cooldown '
            f'{scope}. Each accepted prompt attempt starts it; blocked retries '
            'do not extend it.')
    await ctx.send(embed=discord_common.embed_success(message))


def _parse(arguments):
    seconds = None
    global_scope = False
    for argument in arguments:
        value = str(argument).strip()
        if value.casefold() == '+global':
            if global_scope:
                return None, False, _usage()
            global_scope = True
            continue
        if seconds is not None:
            return None, False, _usage()
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            return None, False, _usage()
    if seconds is not None and not 0 <= seconds <= MAX_LLM_COOLDOWN_SECONDS:
        return None, False, (
            f'Cooldown must be from 0 to {MAX_LLM_COOLDOWN_SECONDS} seconds; '
            'use 0 to remove it.')
    return seconds, global_scope, None


def _usage():
    return ('Usage: `;ai cooldown <seconds> [+global]`. Use 0 to remove '
            'that cooldown, or omit seconds to inspect the settings.')


def _duration(seconds):
    if not seconds:
        return 'off'
    noun = 'second' if seconds == 1 else 'seconds'
    return f'{seconds} {noun}'

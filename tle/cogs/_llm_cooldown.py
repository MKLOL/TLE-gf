"""Moderator command helpers for persistent shared LLM cooldowns."""

import logging

from tle.cogs import _llm_access as llm_access
from tle.util import discord_common
from tle.util.db.llm_cooldown_db import MAX_LLM_COOLDOWN_SECONDS


logger = logging.getLogger(__name__)


async def configure(cog, ctx, arguments):
    """Show or change exact, channel-family, or server LLM cooldowns."""
    if not await cog._require_guild_moderator(ctx):
        return
    seconds, selected_scope, error = _parse(arguments)
    if error is not None:
        await ctx.send(embed=discord_common.embed_alert(error))
        return

    database = cog._llm_db()
    channel_id = llm_access.scope_channel_id(ctx.channel)
    family_id = llm_access.family_channel_id(ctx.channel)
    local_label = ('thread' if llm_access.is_thread_channel(ctx.channel)
                   else 'channel')
    if seconds is None:
        settings = database.llm_get_cooldown_settings(
            ctx.guild.id, channel_id, family_id=family_id)
        local_value = _duration(settings.get('channel'))
        threads_value = _duration(settings.get('threads'))
        global_value = _duration(settings.get('global'))
        await ctx.send(embed=discord_common.embed_neutral(
            f'{local_label.title()} cooldown: **{local_value}**.\n'
            f'Channel + threads cooldown: **{threads_value}**.\n'
            f'Server-wide cooldown: **{global_value}**.\n'
            'Every configured scope must clear before a prompt can run.'))
        return

    kwargs = {}
    if selected_scope == 'channel':
        if channel_id is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Could not identify this channel.'))
            return
        kwargs['channel_id'] = channel_id
        scope_label = f'for this {local_label}'
        logged_scope = channel_id
    elif selected_scope == 'threads':
        if family_id is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Could not identify this channel family.'))
            return
        kwargs['family_id'] = family_id
        scope_label = 'for this channel and all of its threads'
        logged_scope = f'family:{family_id}'
    else:
        scope_label = 'server-wide'
        logged_scope = None

    database.llm_set_cooldown(ctx.guild.id, seconds, **kwargs)
    logger.warning(
        'LLM cooldown changed by user=%s guild=%s scope=%s seconds=%s',
        ctx.author.id, ctx.guild.id, logged_scope, seconds)
    if seconds == 0:
        message = f'The shared LLM cooldown {scope_label} was removed.'
    else:
        message = (
            f'LLM requests now have a shared {seconds}-second cooldown '
            f'{scope_label}. Each accepted prompt attempt starts it; blocked '
            'retries do not extend it.')
    await ctx.send(embed=discord_common.embed_success(message))


def _parse(arguments):
    seconds = None
    selected_scope = 'channel'
    saw_scope_flag = False
    for argument in arguments:
        value = str(argument).strip()
        folded = value.casefold()
        if folded in ('+global', '+threads'):
            if saw_scope_flag:
                return None, 'channel', _usage()
            selected_scope = 'global' if folded == '+global' else 'threads'
            saw_scope_flag = True
            continue
        if seconds is not None:
            return None, 'channel', _usage()
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            return None, 'channel', _usage()
    if seconds is not None and not 0 <= seconds <= MAX_LLM_COOLDOWN_SECONDS:
        return None, 'channel', (
            f'Cooldown must be from 0 to {MAX_LLM_COOLDOWN_SECONDS} seconds; '
            'use 0 to remove it.')
    return seconds, selected_scope, None


def _usage():
    return ('Usage: `;ai cooldown <seconds> [+threads|+global]`. Use 0 '
            'to remove that cooldown, or omit seconds to inspect all scopes.')


def _duration(seconds):
    if not seconds:
        return 'off'
    noun = 'second' if seconds == 1 else 'seconds'
    return f'{seconds} {noun}'

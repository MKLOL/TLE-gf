"""Persistent request access policy shared by every LLM entry point."""

import math
import re

import discord

from tle.util import discord_common

GUILD_DISABLED_KEY = 'llm_disabled'
_CHANNEL_DISABLED_PREFIX = 'llm_disabled_channel:'
_ENABLED = '0'
_DISABLED = '1'


class LlmAccessDeniedError(Exception):
    """Raised when policy changes after a request's initial preflight."""


def channel_disabled_key(channel_id):
    return f'{_CHANNEL_DISABLED_PREFIX}{channel_id}'


def access_scope(scope):
    if scope is None or scope.casefold() in ('guild', 'server'):
        return 'guild'
    if scope.casefold() in ('here', 'channel'):
        return 'channel'
    return None


def member_label(member):
    label = getattr(member, 'display_name', str(getattr(member, 'id', 'user')))
    return discord.utils.escape_markdown(discord.utils.escape_mentions(label))


def user_target(ctx, target):
    """Resolve a member object, mention, or numeric ID without requiring cache."""
    if target is None:
        return None
    if getattr(target, 'id', None) is not None:
        return int(target.id), member_label(target)
    match = re.fullmatch(r'(?:<@!?(\d+)>|(\d+))', str(target).strip())
    if match is None:
        return None
    user_id = int(match.group(1) or match.group(2))
    getter = getattr(ctx.guild, 'get_member', None)
    member = getter(user_id) if getter is not None else None
    label = member_label(member) if member is not None else f'User {user_id}'
    return user_id, label


def scope_channel_id(channel):
    """Map a thread to its parent so ``disable here`` covers its threads."""
    parent_id = getattr(channel, 'parent_id', None)
    return parent_id or getattr(channel, 'id', None)


def disabled_scope(database, guild_id, channel_id=None):
    """Return ``guild``/``channel`` when requests are disabled, else ``None``."""
    getter = getattr(database, 'get_guild_config', None)
    if getter is None:
        return None
    channel_policy = (getter(guild_id, channel_disabled_key(channel_id))
                      if channel_id is not None else None)
    if channel_policy == _ENABLED:
        return None
    if channel_policy == _DISABLED:
        return 'channel'
    if getter(guild_id, GUILD_DISABLED_KEY) == _DISABLED:
        return 'guild'
    return None


def _has_enabled_channel_override(database, guild_id):
    """Whether a disabled server has been selectively reopened anywhere."""
    getter = getattr(database, 'get_all_guild_configs', None)
    if getter is None:
        return False
    for row in getter(guild_id):
        key, value = (
            getattr(row, 'key', row[0]),
            getattr(row, 'value', row[1]),
        )
        if key.startswith(_CHANNEL_DISABLED_PREFIX) and value == _ENABLED:
            return True
    return False


def set_disabled(database, guild_id, channel_id=None, *, disabled, scope):
    """Apply a server baseline or a channel override.

    Server-wide commands clear every channel override so they really affect
    all channels. A channel enable stores an explicit allow only while the
    server is disabled; otherwise it simply clears that channel's disable.
    """
    if scope == 'guild':
        database.delete_guild_configs_by_prefix(
            guild_id, _CHANNEL_DISABLED_PREFIX)
        if disabled:
            database.set_guild_config(guild_id, GUILD_DISABLED_KEY, _DISABLED)
        else:
            database.delete_guild_config(guild_id, GUILD_DISABLED_KEY)
        return
    if scope != 'channel' or channel_id is None:
        raise ValueError('Invalid LLM disable scope')

    key = channel_disabled_key(channel_id)
    if disabled:
        database.set_guild_config(guild_id, key, _DISABLED)
    elif database.get_guild_config(guild_id, GUILD_DISABLED_KEY) == _DISABLED:
        database.set_guild_config(guild_id, key, _ENABLED)
    else:
        database.delete_guild_config(guild_id, key)


def request_block_reason(database, guild_id, channel_id, user_id):
    """Return a public-safe reason when an LLM request must not proceed."""
    reason = _policy_block_reason(database, guild_id, channel_id, user_id)
    if reason is not None:
        return reason
    checker = getattr(database, 'llm_cooldown_retry', None)
    denial = (checker(guild_id, channel_id)
              if checker is not None else None)
    return _cooldown_block_reason(denial)


def _policy_block_reason(database, guild_id, channel_id, user_id):
    scope = disabled_scope(database, guild_id, channel_id)
    if scope == 'guild':
        if (channel_id is not None
                and _has_enabled_channel_override(database, guild_id)):
            return 'LLM requests are disabled in this channel.'
        return 'LLM requests are disabled in this server.'
    if scope == 'channel':
        return 'LLM requests are disabled in this channel.'
    if database.llm_is_user_banned(guild_id, user_id):
        return 'You are not allowed to use LLM requests in this server.'
    return None


def _cooldown_block_reason(denial):
    if denial is None:
        return None
    stamp = max(0, int(math.ceil(denial.retry_at)))
    description = ('a shared server-wide cooldown'
                   if denial.scope == 'global'
                   else 'a shared cooldown in this channel')
    return (f'LLM requests are on {description}. '
            f'Try again <t:{stamp}:R> (<t:{stamp}:F>).')


def context_block_reason(database, ctx):
    channel_id = scope_channel_id(getattr(ctx, 'channel', None))
    return request_block_reason(
        database, ctx.guild.id, channel_id, ctx.author.id)


async def allow_request_or_notify(database, ctx):
    """Run a request preflight and send its denial reason when blocked."""
    reason = context_block_reason(database, ctx)
    if reason is None:
        return True
    await ctx.send(embed=discord_common.embed_alert(reason))
    return False


def raise_if_request_blocked(database, ctx):
    """Recheck policy and atomically claim cooldowns inside the runtime queue."""
    channel_id = scope_channel_id(getattr(ctx, 'channel', None))
    reason = _policy_block_reason(
        database, ctx.guild.id, channel_id, ctx.author.id)
    if reason is None:
        claimer = getattr(database, 'llm_claim_cooldowns', None)
        denial = (claimer(ctx.guild.id, channel_id)
                  if claimer is not None else None)
        reason = _cooldown_block_reason(denial)
    if reason is not None:
        raise LlmAccessDeniedError(reason)

"""Persistent request access policy shared by every LLM entry point."""

import math
import re

import discord

from tle.util import discord_common

GUILD_DISABLED_KEY = 'llm_disabled'
_CHANNEL_DISABLED_PREFIX = 'llm_disabled_channel:'  # Legacy family scope.
_LOCAL_DISABLED_PREFIX = 'llm_disabled_local:'
_FAMILY_DISABLED_PREFIX = 'llm_disabled_family:'
_ENABLED = '0'
_DISABLED = '1'


class LlmAccessDeniedError(Exception):
    """Raised when policy changes after a request's initial preflight."""


def channel_disabled_key(channel_id):
    """Return the legacy parent-channel override key."""
    return f'{_CHANNEL_DISABLED_PREFIX}{channel_id}'


def local_disabled_prefix(family_id):
    return f'{_LOCAL_DISABLED_PREFIX}{family_id}:'


def local_disabled_key(family_id, channel_id):
    return f'{local_disabled_prefix(family_id)}{channel_id}'


def family_disabled_key(family_id):
    return f'{_FAMILY_DISABLED_PREFIX}{family_id}'


def access_scope(arguments):
    """Parse guild, exact-local, and channel-plus-threads access scopes."""
    if arguments is None:
        values = ()
    elif isinstance(arguments, str):
        values = (arguments.casefold(),)
    else:
        values = tuple(
            str(argument).strip().casefold()
            for argument in arguments
            if str(argument).strip()
        )
    if not values or values in (('guild',), ('server',)):
        return 'guild'
    if values[0] not in ('here', 'channel'):
        return None
    if len(values) == 1:
        return 'channel'
    if len(values) == 2 and values[1] == '+threads':
        return 'family'
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


def is_thread_channel(channel):
    """Return whether ``channel`` is a Discord thread-like object."""
    return getattr(channel, 'parent_id', None) is not None


def scope_channel_id(channel):
    """Return the current channel ID without collapsing threads."""
    return getattr(channel, 'id', None)


def family_channel_id(channel):
    """Return the parent ID for a thread, otherwise the current channel ID."""
    parent_id = getattr(channel, 'parent_id', None)
    return parent_id or getattr(channel, 'id', None)


def _family_policy(database, guild_id, family_id):
    if family_id is None:
        return None
    getter = getattr(database, 'get_guild_config', None)
    if getter is None:
        return None
    policy = getter(guild_id, family_disabled_key(family_id))
    if policy is None:
        # Before exact thread scoping, parent-channel keys represented the
        # parent plus every child thread. Preserve that meaning on upgrade.
        policy = getter(guild_id, channel_disabled_key(family_id))
    return policy


def disabled_scope(database, guild_id, channel_id=None, family_id=None):
    """Return the effective disabled scope, or ``None`` when requests pass."""
    getter = getattr(database, 'get_guild_config', None)
    if getter is None:
        return None
    if family_id is None:
        family_id = channel_id

    local_policy = None
    if channel_id is not None and family_id is not None:
        local_policy = getter(
            guild_id, local_disabled_key(family_id, channel_id))
        if local_policy is None and channel_id != family_id:
            # Compatibility with the short-lived exact-thread key format.
            local_policy = getter(guild_id, channel_disabled_key(channel_id))
    if local_policy == _ENABLED:
        return None
    if local_policy == _DISABLED:
        return 'channel'

    family_policy = _family_policy(database, guild_id, family_id)
    if family_policy == _ENABLED:
        return None
    if family_policy == _DISABLED:
        return 'family'
    if getter(guild_id, GUILD_DISABLED_KEY) == _DISABLED:
        return 'guild'
    return None


def _has_enabled_override(database, guild_id):
    """Whether a disabled server has been selectively reopened anywhere."""
    getter = getattr(database, 'get_all_guild_configs', None)
    if getter is None:
        return False
    prefixes = (
        _CHANNEL_DISABLED_PREFIX,
        _LOCAL_DISABLED_PREFIX,
        _FAMILY_DISABLED_PREFIX,
    )
    for row in getter(guild_id):
        key, value = (
            getattr(row, 'key', row[0]),
            getattr(row, 'value', row[1]),
        )
        if key.startswith(prefixes) and value == _ENABLED:
            return True
    return False


def _inherited_is_disabled(database, guild_id, family_id):
    family_policy = _family_policy(database, guild_id, family_id)
    if family_policy == _ENABLED:
        return False
    if family_policy == _DISABLED:
        return True
    return database.get_guild_config(
        guild_id, GUILD_DISABLED_KEY) == _DISABLED


def set_disabled(database, guild_id, channel_id=None, family_id=None, *,
                 disabled, scope):
    """Apply a server, exact-local, or parent-plus-threads access policy."""
    if scope == 'guild':
        for prefix in (
                _CHANNEL_DISABLED_PREFIX,
                _LOCAL_DISABLED_PREFIX,
                _FAMILY_DISABLED_PREFIX):
            database.delete_guild_configs_by_prefix(guild_id, prefix)
        if disabled:
            database.set_guild_config(guild_id, GUILD_DISABLED_KEY, _DISABLED)
        else:
            database.delete_guild_config(guild_id, GUILD_DISABLED_KEY)
        return

    if family_id is None:
        family_id = channel_id
    if family_id is None:
        raise ValueError('Invalid LLM disable scope')

    if scope == 'family':
        # A family-level command resets exact exceptions in that family so the
        # operation immediately applies to the parent and all child threads.
        database.delete_guild_configs_by_prefix(
            guild_id, local_disabled_prefix(family_id))
        database.delete_guild_config(
            guild_id, channel_disabled_key(family_id))
        if channel_id is not None and channel_id != family_id:
            database.delete_guild_config(
                guild_id, channel_disabled_key(channel_id))
        key = family_disabled_key(family_id)
        if disabled:
            database.set_guild_config(guild_id, key, _DISABLED)
        elif database.get_guild_config(
                guild_id, GUILD_DISABLED_KEY) == _DISABLED:
            database.set_guild_config(guild_id, key, _ENABLED)
        else:
            database.delete_guild_config(guild_id, key)
        return

    if scope != 'channel' or channel_id is None:
        raise ValueError('Invalid LLM disable scope')

    key = local_disabled_key(family_id, channel_id)
    if channel_id != family_id:
        database.delete_guild_config(
            guild_id, channel_disabled_key(channel_id))
    if disabled:
        database.set_guild_config(guild_id, key, _DISABLED)
    elif _inherited_is_disabled(database, guild_id, family_id):
        database.set_guild_config(guild_id, key, _ENABLED)
    else:
        database.delete_guild_config(guild_id, key)


def request_block_reason(database, guild_id, channel_id, user_id,
                         family_id=None):
    """Return a public-safe reason when an LLM request must not proceed."""
    reason = _policy_block_reason(
        database, guild_id, channel_id, user_id, family_id)
    if reason is not None:
        return reason
    checker = getattr(database, 'llm_cooldown_retry', None)
    denial = (checker(guild_id, channel_id, family_id=family_id)
              if checker is not None else None)
    return _cooldown_block_reason(denial)


def _policy_block_reason(database, guild_id, channel_id, user_id,
                         family_id=None):
    scope = disabled_scope(database, guild_id, channel_id, family_id)
    if scope == 'guild':
        if (channel_id is not None
                and _has_enabled_override(database, guild_id)):
            return 'LLM requests are disabled in this channel.'
        return 'LLM requests are disabled in this server.'
    if scope == 'family':
        return 'LLM requests are disabled in this channel and its threads.'
    if scope == 'channel':
        return 'LLM requests are disabled in this channel.'
    if database.llm_is_user_banned(guild_id, user_id):
        return 'You are not allowed to use LLM requests in this server.'
    return None


def _cooldown_block_reason(denial):
    if denial is None:
        return None
    stamp = max(0, int(math.ceil(denial.retry_at)))
    descriptions = {
        'global': 'a shared server-wide cooldown',
        'threads': 'a shared cooldown for this channel and its threads',
        'channel': 'a shared cooldown in this channel',
    }
    description = descriptions.get(
        denial.scope, 'a shared cooldown in this channel')
    return (f'LLM requests are on {description}. '
            f'Try again <t:{stamp}:R> (<t:{stamp}:F>).')


def context_block_reason(database, ctx):
    channel = getattr(ctx, 'channel', None)
    channel_id = scope_channel_id(channel)
    family_id = family_channel_id(channel)
    return request_block_reason(
        database, ctx.guild.id, channel_id, ctx.author.id, family_id)


async def allow_request_or_notify(database, ctx):
    """Run a request preflight and send its denial reason when blocked."""
    reason = context_block_reason(database, ctx)
    if reason is None:
        return True
    await ctx.send(embed=discord_common.embed_alert(reason))
    return False


def raise_if_request_blocked(database, ctx):
    """Recheck policy and atomically claim cooldowns inside the runtime queue."""
    channel = getattr(ctx, 'channel', None)
    channel_id = scope_channel_id(channel)
    family_id = family_channel_id(channel)
    reason = _policy_block_reason(
        database, ctx.guild.id, channel_id, ctx.author.id, family_id)
    if reason is None:
        claimer = getattr(database, 'llm_claim_cooldowns', None)
        denial = (claimer(ctx.guild.id, channel_id, family_id=family_id)
                  if claimer is not None else None)
        reason = _cooldown_block_reason(denial)
    if reason is not None:
        raise LlmAccessDeniedError(reason)

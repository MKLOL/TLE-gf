"""Shared cog-side helpers for the minigames cog.

Holds the error type, the mod-only check, the custom converters, the
``Context``-shaped adapters used by slash commands and scheduled jobs, and a
handful of small name/score formatting helpers.  These live here (rather than
in ``minigames.py``) so the cog module stays small; ``minigames.py`` re-exports
everything the test suite imports by name.
"""

import datetime as dt
import sys

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util.akari_rating import rank_for_rating
from tle.cogs._minigame_common import format_duration, normalize_puzzle_date


def _mg():
    """Return the live ``minigames`` module.

    Used so code in this module resolves monkeypatch-sensitive names (e.g.
    ``plot_akari_rating``) through ``tle.cogs.minigames``'s namespace, which is
    what the test suite patches.  Imported lazily via ``sys.modules`` to avoid
    a circular import at module load.
    """
    return sys.modules['tle.cogs.minigames']


class MinigameCogError(commands.CommandError):
    pass


def queens_mod_only():
    async def predicate(ctx):
        cog = getattr(ctx, 'cog', None)
        if cog is not None and cog._has_queens_mod_access(
                ctx.guild.id, ctx.author):
            return True
        raise MinigameCogError(_mg().Minigames._mod_role_error_message())

    check = getattr(commands, 'check', None)
    if check is None:
        return lambda func: func
    return check(predicate)


class ChannelOrThread(commands.Converter):
    """Converter that finds text channels, threads, and archived threads.

    discord.py's built-in converters only search the guild cache, so
    archived threads (not in cache) can't be found by name or ID.
    This falls back to bot.fetch_channel() for IDs and mentions.
    """

    async def convert(self, ctx, argument):
        # Try the built-in converters first (handles mentions, cached channels/threads)
        for converter in (commands.TextChannelConverter, commands.ThreadConverter):
            try:
                return await converter().convert(ctx, argument)
            except commands.BadArgument:
                continue

        # Fall back to fetch_channel for raw IDs (handles archived threads)
        try:
            channel_id = int(argument.strip('<#>'))
        except ValueError:
            raise commands.BadArgument(f'Channel or thread "{argument}" not found.')
        try:
            return await ctx.bot.fetch_channel(channel_id)
        except discord.NotFound:
            raise commands.BadArgument(f'Channel or thread "{argument}" not found.')
        except discord.Forbidden:
            raise commands.BadArgument(f'I don\'t have access to channel "{argument}".')


class CaseInsensitiveMember(commands.MemberConverter):
    """MemberConverter with a case-insensitive fallback on name/display_name."""

    async def convert(self, ctx, argument):
        try:
            return await super().convert(ctx, argument)
        except commands.BadArgument:
            pass
        lowered = argument.lower()
        for member in ctx.guild.members:
            if member.name.lower() == lowered or member.display_name.lower() == lowered:
                return member
        raise commands.BadArgument(f'Member "{argument}" not found.')


def _safe_member_name(member):
    return discord.utils.escape_mentions(member.display_name)


class _FollowupChannel:
    """Channel-like wrapper that sends via interaction followups.

    Lets code that reads ``ctx.channel.id`` / ``.mention`` or calls
    ``ctx.channel.send()`` (e.g. the paginator) work unchanged.
    """

    def __init__(self, interaction):
        self._interaction = interaction
        self.id = interaction.channel_id
        self.mention = f'<#{interaction.channel_id}>'

    async def send(self, content=None, *, embed=None, view=None,
                   delete_after=None, **kw):
        return await self._interaction.followup.send(
            content, embed=embed, view=view, wait=True, **kw)


class _SlashCtx:
    """Adapter that wraps a *deferred* ``Interaction`` to look like ``commands.Context``.

    Create this **after** calling ``interaction.response.defer()`` so that
    ``followup.send()`` works immediately.
    """

    def __init__(self, interaction):
        self.interaction = interaction
        self.guild = interaction.guild
        self.author = interaction.user
        self.channel = _FollowupChannel(interaction)
        self.bot = interaction.client
        # Discord interaction IDs are globally unique snowflakes, so they're
        # safe to use anywhere a per-invocation message_id is expected (e.g.
        # /akari add storing the row keyed on this id).  ``import-start``
        # overrides this with the real bot reply's id after deferring.
        self.message = type('_Msg', (), {'id': interaction.id})()

    async def send(self, content=None, *, embed=None, **kw):
        return await self.interaction.followup.send(
            content, embed=embed, wait=True, **kw)

    async def send_help(self, command=None):
        pass


class _ScheduledCtx:
    """Minimal ctx for scheduled jobs that send into a configured channel."""

    def __init__(self, bot, guild, channel):
        self.bot = bot
        self.guild = guild
        self.channel = channel
        self.author = getattr(guild, 'me', None) or getattr(bot, 'user', None)
        self.message = type('_Msg', (), {'id': f'scheduled:{guild.id}:{channel.id}'})()

    async def send(self, content=None, *, embed=None, **kw):
        return await self.channel.send(content, embed=embed, **kw)


def _safe_user_name(guild, user_id):
    member = guild.get_member(int(user_id))
    if member is not None:
        return _safe_member_name(member)
    return f'user `{user_id}`'


def _safe_cf_handle(guild, user_id):
    if cf_common.user_db is None:
        return '-'
    handle = cf_common.user_db.get_handle(user_id, guild.id)
    return handle or '-'


def _legend_name_for(guild, member):
    """Pick a matplotlib-safe display name for the rating/perf graph legend.

    Prefers the user's CF handle (ASCII-only by CF's rules → no emoji → no
    matplotlib tofu boxes); falls back to their Discord display name when no
    handle is linked.  See the discussion of why matplotlib can't render emoji
    the way Pango can in the leaderboard image.
    """
    handle = _safe_cf_handle(guild, member.id)
    if handle != '-':
        return handle
    return _safe_member_name(member)


def _format_score(score):
    return f'{score:.3f}'.rstrip('0').rstrip('.')


def _format_akari_history_line(point):
    """One CF-style line of ``;mg akari history`` for one contest day.

    ``**#446** · 2026-06-03 · 🌟 1:34 · 1234 ─ **+12** → 1246 (CM) · perf 1289``

    The horizontal bar / right-arrow combo mirrors ``;handles updates``
    (handles.py:884-889), the canonical CF rating-change format in this
    codebase. Solo days are filtered out by the caller, so ``performance`` is
    guaranteed to be non-None here.
    """
    new_rating = round(point.rating)
    old_rating = round(point.rating - point.delta)
    delta = round(point.delta)
    rank_abbr = rank_for_rating(new_rating).title_abbr
    if point.is_perfect:
        result_str = f'\N{GLOWING STAR} {format_duration(point.time_seconds)}'
    else:
        result_str = f'{point.accuracy}% {format_duration(point.time_seconds)}'
    date_str = normalize_puzzle_date(point.puzzle_date).isoformat()
    return (
        f'**#{point.puzzle_number}** \N{MIDDLE DOT} {date_str} '
        f'\N{MIDDLE DOT} {result_str} '
        f'\N{MIDDLE DOT} {old_rating} \N{HORIZONTAL BAR} **{delta:+}** '
        f'\N{LONG RIGHTWARDS ARROW} {new_rating} ({rank_abbr}) '
        f'\N{MIDDLE DOT} perf {round(point.performance)}'
    )


def _format_minigame_history_line(point):
    """One rating-history line for date-keyed minigames such as Queens."""
    new_rating = round(point.rating)
    old_rating = round(point.rating - point.delta)
    delta = round(point.delta)
    rank_abbr = rank_for_rating(new_rating).title_abbr
    result_str = format_duration(point.time_seconds)
    if point.is_perfect:
        result_str = f'{result_str} clean'
    date_str = normalize_puzzle_date(point.puzzle_date).isoformat()
    performance_str = (
        f'perf {round(point.performance)}'
        if point.performance is not None
        else 'solo'
    )
    return (
        f'**{date_str}** \N{MIDDLE DOT} {result_str} '
        f'\N{MIDDLE DOT} {old_rating} \N{HORIZONTAL BAR} **{delta:+}** '
        f'\N{LONG RIGHTWARDS ARROW} {new_rating} ({rank_abbr}) '
        f'\N{MIDDLE DOT} {performance_str}'
    )


def _format_akari_ban_line(guild, row):
    """One line of ``;mg akari bans``: who's banned, when, by whom, why.

    Example:
        ``• **Alice** \N{MIDDLE DOT} banned 2026-06-03 by **mod1** \N{MIDDLE DOT} spamming``
    """
    target = _safe_user_name(guild, row.user_id)
    banner = _safe_user_name(guild, row.banned_by)
    date_str = dt.datetime.fromtimestamp(row.banned_at).date().isoformat()
    reason_part = f' \N{MIDDLE DOT} {row.reason}' if row.reason else ''
    return (f'\N{BULLET} **{target}** \N{MIDDLE DOT} banned {date_str} '
            f'by **{banner}**{reason_part}')

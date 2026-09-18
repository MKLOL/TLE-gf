"""Persistent counting channels with decimal, binary, and hex input."""
import asyncio
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.cogs._counting_parser import (
    CORRECT,
    classify_count_attempt,
    could_be_count_attempt,
)
from tle.cogs._counting_stats import (
    build_counting_stats,
    format_counting_stats,
)
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.db.counting_db import CountingStateConflict

logger = logging.getLogger(__name__)

_GOOD_REACTION = '✅'
_BAD_REACTION = '❌'


class CountingCogError(commands.CommandError):
    pass


def _message_timestamp(message):
    created_at = getattr(message, 'created_at', None)
    try:
        return float(created_at.timestamp())
    except (AttributeError, OSError, TypeError, ValueError):
        return time.time()


def _author_name(author):
    name = getattr(author, 'display_name', None)
    if name is None:
        name = getattr(author, 'name', None)
    if name is None:
        name = str(getattr(author, 'id', 'Unknown'))
    return str(name)


def _attempt_record(message, expected, result, recorded_at=None):
    return {
        'message_id': getattr(message, 'id'),
        'user_id': getattr(message.author, 'id'),
        'author_name': _author_name(message.author),
        'content': message.content or '',
        'created_at': _message_timestamp(message),
        'recorded_at': time.time() if recorded_at is None else recorded_at,
        'expected_value': expected,
        'submitted_value': result.value,
        'accepted': result.is_correct,
        'radix': result.radix,
        'reason': result.status,
    }


def _next_number_text(number):
    return (f'decimal `{number}`, binary `{number:b}`, '
            f'hex `{number:x}`')


class Counting(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._channel_locks = {}

    def _lock_for(self, guild_id, channel_id):
        key = str(guild_id), str(channel_id)
        lock = self._channel_locks.get(key)
        if lock is None:
            lock = self._channel_locks[key] = asyncio.Lock()
        return lock

    @commands.group(name='counting', aliases=['count'],
                    brief='Counting-channel commands',
                    invoke_without_command=True)
    async def counting(self, ctx):
        await ctx.send_help(ctx.command)

    @counting.command(name='here', brief='Enable counting in this channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        """Enable counting here after replaying the channel's full history."""
        if cf_common.user_db is None:
            raise CountingCogError('The database is not ready yet.')

        lock = self._lock_for(ctx.guild.id, ctx.channel.id)
        async with lock:
            attempts, current_count, last_message_id, scanned = \
                await self._scan_channel_history(
                    ctx.channel, before=ctx.message)
            try:
                cf_common.user_db.counting_sync_history(
                    ctx.guild.id,
                    ctx.channel.id,
                    current_count,
                    last_message_id,
                    attempts,
                    configured_by=ctx.author.id,
                )
            except Exception as exc:
                logger.exception(
                    'Could not configure counting guild=%s channel=%s',
                    ctx.guild.id, ctx.channel.id)
                raise CountingCogError(
                    'Could not save the counting-channel state.') from exc
            # Keep queued live attempts behind the confirmation so its
            # current/next values cannot already be stale when it is sent.
            next_number = current_count + 1
            await ctx.send(embed=discord_common.embed_success(
                f'Counting is active in {ctx.channel.mention}. Scanned all '
                f'**{scanned}** earlier messages and saved **{len(attempts)}** '
                f'numeric attempts.\nCurrent count: **{current_count}**. '
                f'Next: {_next_number_text(next_number)}.'))

    async def _scan_channel_history(self, channel, *, before=None):
        expected = 1
        attempts = []
        last_message_id = None
        scanned = 0
        recorded_at = time.time()
        try:
            history_options = {'limit': None, 'oldest_first': True}
            if before is not None:
                history_options['before'] = before
            history = channel.history(**history_options)
            async for message in history:
                scanned += 1
                if getattr(getattr(message, 'author', None), 'bot', False):
                    continue
                result = classify_count_attempt(message.content or '', expected)
                if not (result.is_correct or result.is_bad_attempt):
                    continue
                attempts.append(_attempt_record(
                    message, expected, result, recorded_at=recorded_at))
                if result.is_correct:
                    last_message_id = message.id
                    expected += 1
        except (discord.Forbidden, discord.HTTPException, AttributeError) as exc:
            raise CountingCogError(
                'I could not read the complete history of this channel. '
                'Check my **Read Message History** permission.') from exc
        return attempts, expected - 1, last_message_id, scanned

    @counting.command(name='stats', brief='Show counting statistics')
    async def stats(self, ctx):
        if cf_common.user_db is None:
            raise CountingCogError('The database is not ready yet.')
        state = cf_common.user_db.counting_get_channel(
            ctx.guild.id, ctx.channel.id)
        if state is None:
            raise CountingCogError(
                'Counting is not active here. An admin can run '
                '`;counting here` first.')
        rows = cf_common.user_db.counting_get_attempts(
            ctx.guild.id, ctx.channel.id)
        summary = build_counting_stats(rows, state.current_count)
        description, fields = format_counting_stats(summary)
        embed = discord.Embed(
            title='Counting stats', description=description, color=0x28A745)
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)
        embed.set_footer(text=f'Next number: {state.current_count + 1}')
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message):
        author = getattr(message, 'author', None)
        if author is None or getattr(author, 'bot', False):
            return
        guild = getattr(message, 'guild', None)
        channel = getattr(message, 'channel', None)
        if guild is None or channel is None or cf_common.user_db is None:
            return
        content = message.content or ''
        if content.lstrip().startswith(';') or not could_be_count_attempt(content):
            return

        reaction = None
        lock = self._lock_for(guild.id, channel.id)
        async with lock:
            try:
                saved, result = self._record_live_attempt(message, guild, channel)
                if saved is None or not saved.inserted:
                    return
                reaction = (_GOOD_REACTION
                            if result.status == CORRECT else _BAD_REACTION)
            except Exception:
                logger.exception(
                    'Counting attempt failed guild=%s channel=%s message=%s',
                    guild.id, channel.id, getattr(message, 'id', None))
                return

        try:
            await message.add_reaction(reaction)
        except (discord.Forbidden, discord.HTTPException, AttributeError):
            logger.warning(
                'Could not react to counting message guild=%s channel=%s '
                'message=%s', guild.id, channel.id,
                getattr(message, 'id', None))

    def _record_live_attempt(self, message, guild, channel):
        """Classify and save once, retrying a cross-process state race."""
        for attempt_number in range(2):
            state = cf_common.user_db.counting_get_channel(
                guild.id, channel.id)
            if state is None:
                return None, None
            if cf_common.user_db.counting_get_attempt(
                    guild.id, channel.id, message.id) is not None:
                return None, None

            expected = state.current_count + 1
            result = classify_count_attempt(message.content or '', expected)
            if not (result.is_correct or result.is_bad_attempt):
                return None, result
            record = _attempt_record(message, expected, result)
            try:
                saved = cf_common.user_db.counting_record_attempt(
                    guild.id,
                    channel.id,
                    record['message_id'],
                    record['user_id'],
                    record['author_name'],
                    record['content'],
                    record['created_at'],
                    expected_value=expected,
                    submitted_value=record['submitted_value'],
                    accepted=record['accepted'],
                    radix=record['radix'],
                    reason=record['reason'],
                    recorded_at=record['recorded_at'],
                )
                return saved, result
            except CountingStateConflict:
                if attempt_number:
                    raise
        return None, None

    @discord_common.send_error_if(CountingCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Counting(bot))

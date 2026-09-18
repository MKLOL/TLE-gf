"""Resolve and restore rating polls using their persisted votes."""
import re
import time

import discord

from tle.util import codeforces_common as cf_common
from tle.cogs._rpoll_helpers import (
    RpollError, _build_disabled_view, _build_poll_embed, _build_results_embed,
    _compute_totals_map,
)
from tle.cogs._rpoll_views import RpollView
from tle.cogs._rpoll_locks import poll_lock

_MESSAGE_LINK = re.compile(
    r'https?://(?:(?:canary|ptb)\.)?discord(?:app)?\.com/channels/'
    r'([0-9]{1,20})/([0-9]{1,20})/([0-9]{1,20})'
)


def _check_channel(ctx, guild_id, channel_id):
    # Keep retrieval in the source channel: checking only the caller's access
    # would still let them publish private-channel results in a public channel.
    if str(ctx.guild.id) != str(guild_id) or str(ctx.channel.id) != str(channel_id):
        raise RpollError('Use this command in the poll\'s original channel or thread.')


async def _fetch_message(ctx, message_id):
    try:
        return await ctx.channel.fetch_message(int(message_id))
    except discord.NotFound as exc:
        raise RpollError(
            'That message was deleted or was not found. To recover results, use '
            ';rpoll results with the poll ID or the original poll message link.'
        ) from exc
    except discord.Forbidden as exc:
        raise RpollError('I need permission to read that message and its history.') from exc
    except discord.HTTPException as exc:
        raise RpollError('I could not fetch that message. Please try again.') from exc


def _result_heading(poll):
    state = 'Final' if _ended(poll) else 'Current'
    return f'{state} results for poll #{poll.poll_id}'


def _ended(poll):
    return bool(poll.closed or poll.expires_at <= time.time())


async def resolve_poll(ctx, target=None):
    """Return (poll, targeted message ID), including legacy completion replies.

    Looking up original messages in SQLite first allows results recovery even
    when the original message was deleted. Result replies are authenticated by
    their bot author and content/reference, never by their suppressed embed.
    """
    if ctx.guild is None:
        raise RpollError('Use this command in the poll\'s server channel.')
    if cf_common.user_db is None:
        raise RpollError('Bot is still starting up. Please try again shortly.')
    db = cf_common.user_db
    if target is None:
        ref = ctx.message.reference
        if ref is None or ref.message_id is None:
            raise RpollError('Reply to a poll or its results, or provide a poll ID or message link.')
        _check_channel(ctx, ref.guild_id, ref.channel_id)
        message_id = ref.message_id
    else:
        target = target.strip().strip('<>')
        link = _MESSAGE_LINK.fullmatch(target)
        if link:
            guild_id, channel_id, message_id = link.groups()
            _check_channel(ctx, guild_id, channel_id)
        elif re.fullmatch(r'#?[0-9]{1,19}', target):
            number = int(target.lstrip('#'))
            if number > 2**63 - 1:
                raise RpollError('Invalid poll ID or message ID.')
            poll = db.get_rpoll(number)
            if poll is not None:
                _check_channel(ctx, poll.guild_id, poll.channel_id)
                if poll.message_id is None:
                    raise RpollError('That poll was never posted.')
                return poll, int(poll.message_id)
            if target.startswith('#'):
                raise RpollError('Poll not found.')
            message_id = number
        else:
            raise RpollError('Provide a poll ID or Discord message link, or reply to a poll.')

    poll = db.get_rpoll_by_message_id(message_id)
    if poll is None:
        message = await _fetch_message(ctx, message_id)
        ref = message.reference
        if message.author.id != ctx.bot.user.id:
            raise RpollError('That message is not a rating poll or its results.')
        heading = re.fullmatch(r'(?:Current|Final) results for poll #([0-9]{1,18})', message.content)
        if ref is not None and ref.message_id is not None:
            _check_channel(ctx, ref.guild_id, ref.channel_id)
            poll = db.get_rpoll_by_message_id(ref.message_id)
            valid = poll is not None and (
                message.content == 'Poll done!' or heading and int(heading[1]) == poll.poll_id
            )
        else:
            # Discord drops the reference when fail_if_not_exists=False is used
            # for a deleted original. Our own exact heading still identifies it.
            poll = db.get_rpoll(int(heading[1])) if heading else None
            valid = poll is not None
        if not valid:
            raise RpollError('That message is not a rating poll or its results.')
    _check_channel(ctx, poll.guild_id, poll.channel_id)
    return poll, int(message_id)


def _stored_data(poll):
    db = cf_common.user_db
    options = [(row.option_index, row.label) for row in db.get_rpoll_options(poll.poll_id)]
    totals = _compute_totals_map(poll.poll_id, poll.formula)
    return options, totals, db.get_rpoll_vote_count(poll.poll_id)


def _results_embed(poll):
    options, totals, count = _stored_data(poll)
    return _build_results_embed(
        poll.question, options, totals, count, formula=poll.formula, closed=_ended(poll),
    )


async def send_results(ctx, target=None):
    poll, _ = await resolve_poll(ctx, target)
    reference = discord.MessageReference(
        message_id=int(poll.message_id), channel_id=int(poll.channel_id),
        guild_id=int(poll.guild_id), fail_if_not_exists=False,
    )
    await ctx.send(
        _result_heading(poll), embed=_results_embed(poll), reference=reference,
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def fix_poll(ctx, target=None):
    poll, message_id = await resolve_poll(ctx, target)
    async with poll_lock(poll.poll_id):
        await _fix_message(ctx, poll.poll_id, message_id)
    await ctx.send('Poll message restored.', allowed_mentions=discord.AllowedMentions.none())


async def _fix_message(ctx, poll_id, message_id):
    message = await _fetch_message(ctx, message_id)
    # Re-read inside the shared edit lock: expiry or votes may have completed
    # while this command resolved its target or waited for another edit.
    poll = cf_common.user_db.get_rpoll(poll_id)
    kwargs = dict(suppress=False, allowed_mentions=discord.AllowedMentions.none())
    if message_id == int(poll.message_id):
        options, totals, count = _stored_data(poll)
        voters = None
        if not poll.anonymous:
            voters = {}
            for row in cf_common.user_db.get_rpoll_voters(poll.poll_id):
                voters.setdefault(row.option_index, []).append(int(row.user_id))
        kwargs['embed'] = _build_poll_embed(
            poll.question, options, totals, count, voters,
            expires_at=poll.expires_at, closed=_ended(poll), formula=poll.formula,
            color=message.embeds[0].color if message.embeds else None,
        )
        kwargs['view'] = (_build_disabled_view(poll.poll_id, len(options)) if _ended(poll)
                          else RpollView(poll.poll_id, len(options)))
    else:
        kwargs.update(content=_result_heading(poll), embed=_results_embed(poll))
    try:
        await message.edit(**kwargs)
    except discord.NotFound as exc:
        raise RpollError('That message was deleted. Use ;rpoll results to recover the results.') from exc
    except discord.Forbidden as exc:
        raise RpollError('I do not have permission to restore that message.') from exc
    except discord.HTTPException as exc:
        raise RpollError('I could not restore that message. Please try again.') from exc

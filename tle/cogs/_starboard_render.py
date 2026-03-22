"""Starboard rendering / building helpers.

Extracted from starboard.py to keep the cog file focused on event handling
and commands.  Everything here is pure logic with no bot state.
"""
import datetime
import logging
import time

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.cogs._starboard_helpers import _emoji_str

logger = logging.getLogger(__name__)

_TIMELINE_KEYWORDS = {'week', 'month', 'year'}

# No time bound sentinel — matches the DB layer constant
_NO_TIME_BOUND = 10 ** 10

# Muted embed color for reply context
_REPLY_EMBED_COLOR = discord.Color.from_rgb(47, 49, 54)

# Image extensions that Discord can render inline in embeds
_IMAGE_EXTENSIONS = ('png', 'jpeg', 'jpg', 'gif', 'webp')

# Video extensions that need to be re-uploaded as files
_VIDEO_EXTENSIONS = ('mp4', 'mov', 'webm')


def _starboard_content(emoji_str, count, jump_url):
    """Build the header line for a starboard message.

    Format: ⭐ **5** | jump_url
    The jump URL is auto-linked by Discord in message content.
    """
    return f'{emoji_str} **{count}** | {jump_url}'


def _parse_starboard_args(args, default_emoji=constants._DEFAULT_STAR):
    """Parse args for starboard leaderboard/top commands.

    Returns (emoji, dlo, dhi) where dlo/dhi are unix timestamps (seconds).
    Supports:
      - timeline keywords: week, month, year
      - date ranges: d>=[[dd]mm]yyyy  d<[[dd]mm]yyyy
      - emoji (anything else that isn't a keyword or date arg)
    If no emoji is provided, defaults to default_emoji.
    If no time filter is provided, dlo=0 and dhi=_NO_TIME_BOUND.
    """
    emoji = None
    dlo = 0
    dhi = _NO_TIME_BOUND

    for arg in args:
        lower = arg.lower()
        if lower in _TIMELINE_KEYWORDS:
            now = datetime.datetime.now()
            if lower == 'week':
                # Monday of this week at 00:00
                monday = now - datetime.timedelta(days=now.weekday())
                dlo = time.mktime(monday.replace(hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'month':
                dlo = time.mktime(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'year':
                dlo = time.mktime(now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
        elif lower.startswith('d>='):
            dlo = max(dlo, cf_common.parse_date(arg[3:]))
        elif lower.startswith('d<'):
            dhi = min(dhi, cf_common.parse_date(arg[2:]))
        else:
            emoji = arg

    if emoji is None:
        emoji = default_emoji
        logger.debug(f'No emoji specified, falling back to default: {default_emoji!r}')
    return emoji, dlo, dhi


async def build_starboard_message(message, emoji_str, count, color):
    """Build content, embeds, and files for a starboard message.

    Returns (content, embeds, files) where:
      - content: the header line with emoji count and jump URL
      - embeds: list of Embed objects (reply context + main + carried-over)
      - files: list of discord.File for video attachments

    For videos: author goes in the content header so the rendering
    order is: content (author) → file attachment (video) → embeds.
    For non-video: author goes in the main embed via set_author.
    """
    content = _starboard_content(emoji_str, count, message.jump_url)
    embeds = []
    files = []

    # Scan attachments to categorise them
    image_url = None
    video_attachments = []
    other_attachments = []
    for att in message.attachments:
        ext = att.filename.lower().rsplit('.', 1)[-1] if '.' in att.filename else ''
        if ext in _IMAGE_EXTENSIONS:
            if image_url is None:
                image_url = att.url
        elif ext in _VIDEO_EXTENSIONS:
            video_attachments.append(att)
        else:
            other_attachments.append(att)

    has_video = bool(video_attachments)

    # For video messages, put author in the content header so it
    # appears above the video player (file attachments render after
    # content but before embeds).
    if has_video:
        safe_name = discord.utils.escape_mentions(message.author.display_name)
        content = (
            f'{emoji_str} **{count}** \u00b7 **{safe_name}** '
            f'| {message.jump_url}'
        )
        for att in video_attachments:
            try:
                files.append(await att.to_file())
            except Exception:
                logger.debug(f'Failed to download video attachment {att.filename}')

    # --- Reply context embed (goes first / above main embed) ---
    if message.reference and message.reference.message_id:
        try:
            ref_msg = message.reference.resolved
            if ref_msg is None or isinstance(ref_msg, discord.DeletedReferencedMessage):
                ref_msg = await message.channel.fetch_message(message.reference.message_id)
            reply_embed = discord.Embed(
                color=_REPLY_EMBED_COLOR,
                timestamp=ref_msg.created_at,
            )
            reply_embed.set_author(
                name=f'Replying to {ref_msg.author.display_name}',
                icon_url=ref_msg.author.display_avatar.url,
            )
            if ref_msg.content:
                text = ref_msg.content
                if len(text) > 4096:
                    text = text[:4093] + '...'
                reply_embed.description = text
            embeds.append(reply_embed)
        except Exception:
            logger.debug(f'Could not fetch referenced message {message.reference.message_id}')

    # --- Main embed ---
    # For video-only messages (no text), skip the main embed entirely.
    has_text = bool(message.content)
    has_other = bool(other_attachments)
    need_embed = not has_video or has_text or has_other or image_url

    if need_embed:
        embed = discord.Embed(color=color, timestamp=message.created_at)
        if not has_video:
            embed.set_author(
                name=message.author.display_name,
                icon_url=message.author.display_avatar.url,
                url=message.jump_url,
            )

        if has_text:
            text = message.content
            if len(text) > 4096:
                text = text[:4093] + '...'
            embed.description = text

        if image_url:
            embed.set_image(url=image_url)

        for att in other_attachments:
            safe_name = att.filename.replace('`', '\u02CB')
            embed.add_field(
                name='Attachment', value=f'`{safe_name}`', inline=False
            )

        # Pull an image from the original message's embeds if we don't
        # already have one from attachments.
        if not image_url and message.embeds:
            for e in message.embeds:
                logger.info(
                    '[gifv-debug] embed type=%r url=%r '
                    'image=%r thumbnail=%r video=%r '
                    'thumbnail.url=%r thumbnail.proxy_url=%r '
                    'video.url=%r',
                    getattr(e, 'type', None),
                    getattr(e, 'url', None),
                    getattr(e, 'image', None),
                    getattr(e, 'thumbnail', None),
                    getattr(e, 'video', None),
                    getattr(getattr(e, 'thumbnail', None), 'url', 'N/A'),
                    getattr(getattr(e, 'thumbnail', None), 'proxy_url', 'N/A'),
                    getattr(getattr(e, 'video', None), 'url', 'N/A'),
                )
                if e.type == 'image' and e.url:
                    embed.set_image(url=e.url)
                    break
                if e.type == 'gifv':
                    # Tenor/Giphy gifv embeds: thumbnail is a static PNG,
                    # video is an MP4 (can't use in set_image).  Swapping
                    # the extension on the proxy URL to .gif gives us the
                    # animated version via Discord's CDN proxy.
                    proxy = getattr(e.thumbnail, 'proxy_url', None) or ''
                    if proxy.rsplit('.', 1)[-1] in ('png', 'jpg', 'jpeg', 'webp'):
                        chosen = proxy.rsplit('.', 1)[0] + '.gif'
                    elif e.thumbnail and e.thumbnail.url:
                        raw = str(e.thumbnail.url)
                        if raw.rsplit('.', 1)[-1] in ('png', 'jpg', 'jpeg', 'webp'):
                            chosen = raw.rsplit('.', 1)[0] + '.gif'
                        else:
                            chosen = raw
                    else:
                        chosen = None
                    logger.info('[gifv-debug] chosen image URL: %r', chosen)
                    if chosen:
                        embed.set_image(url=chosen)
                    break
                if e.type == 'rich' and e.image and e.image.url:
                    embed.set_image(url=e.image.url)
                    break
                if e.thumbnail and e.thumbnail.url:
                    embed.set_image(url=e.thumbnail.url)
                    break

        embeds.append(embed)

    # Carry over rich and link embeds from the original message (bot
    # embeds like Codeforces problem cards, and URL previews like blog
    # post link previews). Skip image/video/gifv auto-embeds.
    for e in message.embeds:
        if e.type in ('rich', 'link', 'article'):
            embeds.append(e)

    # Discord allows a maximum of 10 embeds per message
    embeds = embeds[:10]

    return content, embeds, files

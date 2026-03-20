"""Helpers for the starboard migration cog.

Parsing old bot messages, embed serialization, and fallback message building.
"""
import json
import re

from tle.cogs.starboard import _starboard_content
from tle import constants

_OLD_BOT_RE = re.compile(
    r'^(.+?)\s+\*\*(\d+)\*\*\s*(?:·\s*.+?\s*)?\|\s*(https://discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+))'
)


def parse_old_bot_message(content):
    """Parse an old bot's starboard message content line.

    Expected formats:
        <emoji> **<count>** | <jump_url>
        <emoji> **<count>** · <author> | <jump_url>

    Returns (emoji_str, displayed_count, guild_id, channel_id, message_id) or None.
    All IDs are returned as ints.
    """
    if not content:
        return None
    match = _OLD_BOT_RE.match(content)
    if not match:
        return None
    emoji_str = match.group(1).strip()
    count = int(match.group(2))
    guild_id = int(match.group(4))
    channel_id = int(match.group(5))
    message_id = int(match.group(6))
    return emoji_str, count, guild_id, channel_id, message_id


def serialize_embed_fallback(message):
    """Serialize a Discord message to JSON — content + embeds via to_dict().

    Uses discord.py's native embed.to_dict() so nothing is lost.
    """
    result = {}
    if hasattr(message, 'content') and message.content:
        result['content'] = message.content
    embeds = []
    for embed in message.embeds:
        if hasattr(embed, 'to_dict'):
            d = embed.to_dict()
            if d:
                embeds.append(d)
        elif isinstance(embed, dict) and embed:
            embeds.append(embed)
    if embeds:
        result['embeds'] = embeds
    return json.dumps(result)


def build_fallback_message(entry, fallback_json, emoji_str):
    """Rebuild a (content, embeds) tuple from serialized old bot message.

    Uses discord.py's Embed.from_dict() so the embeds are exact copies.
    Falls back to a basic content line only if fallback_json is missing.
    """
    import discord

    data = {}
    if fallback_json:
        try:
            data = json.loads(fallback_json)
        except (json.JSONDecodeError, TypeError):
            data = {}

    # Use the old bot's original content line directly
    content = data.get('content')
    if not content:
        count = entry.star_count if entry.star_count is not None else 0
        guild_id = getattr(entry, 'guild_id', None) or '0'
        channel_id = getattr(entry, 'source_channel_id', None) or '0'
        jump_url = f'https://discord.com/channels/{guild_id}/{channel_id}/{entry.original_msg_id}'
        content = _starboard_content(emoji_str, count, jump_url)

    # Rebuild embeds using discord.py's native from_dict
    embeds = [discord.Embed.from_dict(d) for d in data.get('embeds', [])]

    return content, embeds

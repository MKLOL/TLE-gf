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
    """Serialize a Discord message's embeds + content to JSON for fallback rendering.

    Returns a JSON string containing the message content and embed data.
    """
    embeds = []
    for embed in message.embeds:
        data = {}
        if hasattr(embed, 'title') and embed.title:
            data['title'] = embed.title
        if hasattr(embed, 'description') and embed.description:
            data['description'] = embed.description
        if hasattr(embed, 'color') and embed.color is not None:
            data['color'] = int(embed.color)
        if hasattr(embed, 'image') and embed.image:
            url = getattr(embed.image, 'url', None)
            if url:
                data['image_url'] = url
        if hasattr(embed, 'author_data') and embed.author_data:
            data['author'] = embed.author_data
        elif hasattr(embed, 'author') and embed.author:
            author = embed.author
            data['author'] = {
                'name': getattr(author, 'name', None),
                'icon_url': getattr(author, 'icon_url', None),
                'url': getattr(author, 'url', None),
            }
        if hasattr(embed, 'fields') and embed.fields:
            fields = []
            for f in embed.fields:
                if isinstance(f, dict):
                    fields.append(f)
                else:
                    fields.append({
                        'name': getattr(f, 'name', None),
                        'value': getattr(f, 'value', None),
                        'inline': getattr(f, 'inline', True),
                    })
            if fields:
                data['fields'] = fields
        if hasattr(embed, 'footer') and embed.footer:
            if isinstance(embed.footer, dict):
                data['footer'] = embed.footer
            else:
                data['footer'] = {
                    'text': getattr(embed.footer, 'text', None),
                    'icon_url': getattr(embed.footer, 'icon_url', None),
                }
        if hasattr(embed, 'timestamp') and embed.timestamp:
            data['timestamp'] = str(embed.timestamp)
        if data:
            embeds.append(data)

    result = {}
    if hasattr(message, 'content') and message.content:
        result['content'] = message.content
    if embeds:
        result['embeds'] = embeds
    return json.dumps(result)


def build_fallback_message(entry, fallback_json, emoji_str):
    """Build a (content, embeds) tuple for a deleted original message.

    Uses the serialized fallback data to reconstruct a reasonable starboard
    message for messages whose originals are no longer fetchable.

    entry: a DB row with .star_count and .original_msg_id
    fallback_json: JSON string from serialize_embed_fallback()
    emoji_str: the emoji string for the starboard content line
    """
    import discord

    count = entry.star_count if entry.star_count is not None else 0
    guild_id = getattr(entry, 'guild_id', None) or '0'
    channel_id = getattr(entry, 'source_channel_id', None) or '0'
    jump_url = f'https://discord.com/channels/{guild_id}/{channel_id}/{entry.original_msg_id}'
    content = _starboard_content(emoji_str, count, jump_url)

    embeds = []
    if fallback_json:
        try:
            data = json.loads(fallback_json)
        except (json.JSONDecodeError, TypeError):
            data = {}

        desc = data.get('content')
        embed_list = data.get('embeds', [])
        if desc or embed_list:
            main_embed = discord.Embed(
                color=constants._DEFAULT_STAR_COLOR,
                description=desc or None,
            )
            if embed_list:
                first = embed_list[0]
                if first.get('author'):
                    author = first['author']
                    main_embed.set_author(
                        name=author.get('name', 'Unknown'),
                        icon_url=author.get('icon_url'),
                        url=author.get('url'),
                    )
                if first.get('image_url'):
                    main_embed.set_image(url=first['image_url'])
                for field in first.get('fields', []):
                    main_embed.add_field(
                        name=field.get('name', ''),
                        value=field.get('value', ''),
                        inline=field.get('inline', True),
                    )
            embeds.append(main_embed)

    return content, embeds

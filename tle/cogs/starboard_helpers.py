"""Shared helpers used by both starboard.py and starboard_backfill.py."""
import re

_JUMP_URL_PATTERN = re.compile(r'discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)')


def _emoji_str(emoji):
    """Normalize a discord emoji to its string representation."""
    return str(emoji)


def _parse_jump_url(text):
    """Extract (guild_id, channel_id, message_id) from a Discord jump URL string.

    Returns a tuple of ints (guild_id, channel_id, message_id) or None.
    Works with both discord.com and discordapp.com URLs.
    """
    match = _JUMP_URL_PATTERN.search(text)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return None

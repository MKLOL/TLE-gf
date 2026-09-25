"""Capture the messages that preceded a complaint.

Most complaints are unactionable on their own — "fix graphs", "formatting",
"fix this picture somehow". The few messages before the report usually carry
the screenshot, the command that misbehaved, or the argument that prompted it,
so they are stored with the complaint and served to automation through the
complaint API.

Capture is best-effort by design: a complaint must never fail to file because
the bot could not read history.
"""
import json
import logging

from tle.cogs._llm_message_text import message_text

logger = logging.getLogger(__name__)

CONTEXT_MESSAGE_COUNT = 5
_MAX_TEXT = 400
_MAX_TOTAL = 4000


def _truncate(text, limit=_MAX_TEXT):
    text = (text or '').strip()
    if len(text) <= limit:
        return text
    return text[:limit - 1].rstrip() + '…'


def _timestamp(message):
    created = getattr(message, 'created_at', None)
    try:
        return created.timestamp()
    except AttributeError:
        return None


def serialize_messages(messages):
    """Render messages, oldest first, as the stored JSON payload.

    Embeds and attachment names are included: the bot answers in embeds, so
    ``content`` is empty for its own output and a transcript built from
    ``content`` alone would drop exactly the messages a complaint is about.

    Returns ``None`` when nothing is worth storing, so the column stays NULL
    rather than holding an empty list.
    """
    entries = []
    for message in messages:
        text = _truncate(message_text(message))
        if not text:
            continue
        author = getattr(message, 'author', None)
        entries.append({
            'id': str(getattr(message, 'id', '') or ''),
            'author_id': str(getattr(author, 'id', '') or ''),
            'author': str(getattr(author, 'display_name', '') or ''),
            'bot': bool(getattr(author, 'bot', False)),
            'at': _timestamp(message),
            'text': text,
        })
    # Trim from the front: the messages nearest the complaint are the ones
    # most likely to explain it.
    while len(entries) > 1 and len(json.dumps(entries)) > _MAX_TOTAL:
        entries.pop(0)
    return json.dumps(entries) if entries else None


async def capture(channel, before_message, *, limit=CONTEXT_MESSAGE_COUNT):
    """Serialize the messages immediately before a complaint, or None.

    Every failure degrades to no context — missing Read Message History, a
    thread the bot cannot see, an API hiccup. Filing the complaint matters
    more than decorating it.
    """
    try:
        history = [message async for message
                   in channel.history(limit=limit, before=before_message)]
    except Exception:
        logger.warning('Could not read complaint context', exc_info=True)
        return None
    return serialize_messages(reversed(history))

"""Capture the messages that preceded a complaint.

Most complaints are unactionable on their own — "fix graphs", "formatting",
"fix this picture somehow". The few messages before the report usually carry
the screenshot, the command that misbehaved, or the argument that prompted it,
so they are stored with the complaint and served to automation through the
complaint API.

Capture is best-effort by design: a complaint must never fail to file because
the bot could not read history.
"""
import asyncio
import json
import logging

from tle.cogs._llm_message_text import message_text
from tle.cogs._llm_transcript import redact_secrets

logger = logging.getLogger(__name__)

CONTEXT_MESSAGE_COUNT = 5
_MAX_TEXT = 400
_MAX_AUTHOR = 80
_MAX_TOTAL = 4000  # characters of stored JSON
_CAPTURE_TIMEOUT = 5


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


def _dump(entries):
    r"""Serialize without \uXXXX escaping.

    ``json.dumps`` escapes non-ASCII by default, which measures Cyrillic and
    CJK at six characters per code point and emoji at twelve. Budgeting
    against that trimmed a Russian or Chinese transcript to a single message
    while an ASCII one of the same real size kept all five.
    """
    return json.dumps(entries, ensure_ascii=False)


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
        # Redacted for the same reason the LLM transcript is, only more so:
        # this copy is persisted in user.db and served over HTTP.
        text = _truncate(redact_secrets(message_text(message)))
        if not text:
            continue
        author = getattr(message, 'author', None)
        entries.append({
            'id': str(getattr(message, 'id', '') or ''),
            'author_id': str(getattr(author, 'id', '') or ''),
            'author': _truncate(
                redact_secrets(getattr(author, 'display_name', '') or ''),
                _MAX_AUTHOR),
            'bot': bool(getattr(author, 'bot', False)),
            'at': _timestamp(message),
            'text': text,
        })
    # Trim from the front: the messages nearest the complaint are the ones
    # most likely to explain it.
    # Measured in characters, not bytes or escapes: budgeting by either one
    # charges Cyrillic, CJK and emoji two to twelve times per character and
    # silently keeps fewer messages for exactly the members least likely to
    # be writing English.
    while len(entries) > 1 and len(_dump(entries)) > _MAX_TOTAL:
        entries.pop(0)
    return _dump(entries) if entries else None


async def _read(channel, before_message, limit):
    return [message async for message
            in channel.history(limit=limit, before=before_message)]


async def capture(channel, before_message, *, limit=CONTEXT_MESSAGE_COUNT,
                  timeout=_CAPTURE_TIMEOUT):
    """Serialize the messages immediately before a complaint, or None.

    Every failure degrades to no context — missing Read Message History, a
    thread the bot cannot see, an API hiccup. Filing the complaint matters
    more than decorating it, so serialization is inside the guard too and the
    fetch is bounded: discord.py sleeps out rate limits and retries 5xx, which
    would otherwise stall the command.

    Missing permission is an expected steady state, not an anomaly, so this
    logs without a traceback — WARNING and above is relayed to the moderator
    log channel, and a channel the bot cannot read would spam it on every
    complaint.
    """
    try:
        history = await asyncio.wait_for(
            _read(channel, before_message, limit), timeout)
        return serialize_messages(reversed(history))
    except asyncio.TimeoutError:
        logger.info('Complaint context timed out after %ss', timeout)
        return None
    except Exception as error:
        logger.info('Could not read complaint context: %s',
                    type(error).__name__)
        return None

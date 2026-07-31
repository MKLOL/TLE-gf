"""Channel history collection for ``;llm``.

Adapted from the approach in MKLOL/TLE-gf#10 (AhmadKashmar): rather than only
ever seeing the message you replied to, a question that depends on the
conversation pulls a window of it. Two shapes:

* **recent** — the last N messages before the command, within a time window,
  for questions like "what were they arguing about?"
* **reply window** — messages around a specific replied-to message, so a reply
  carries the exchange it sat in rather than one isolated line.

Both are time-boxed as well as count-boxed. A channel that was quiet for a day
should not drag yesterday's conversation into today's answer just because the
message count is low.

Collection is deliberately conservative about what it forwards: author display
name, text, and attachment *filenames*. Attachment contents are handled
separately (see ``_llm_context.read_images``) and only for the focused message.
"""
import logging

logger = logging.getLogger(__name__)

# Per-message text budget inside a transcript. Long pastes are the common case
# in a competitive-programming channel and would otherwise crowd out context.
_MAX_MESSAGE_CHARS = 600
# Whole-transcript budget, so a busy channel cannot blow up the prompt.
_MAX_TRANSCRIPT_CHARS = 12000


def _embed_text(embed):
    """Extract the human-readable parts of a Discord embed."""
    pieces = []
    author = getattr(getattr(embed, 'author', None), 'name', None)
    if author:
        pieces.append(f'Embed author: {author}')
    title = getattr(embed, 'title', None)
    if title:
        pieces.append(f'Embed title: {title}')
    description = getattr(embed, 'description', None)
    if description:
        pieces.append(description)
    for field in getattr(embed, 'fields', None) or []:
        name = getattr(field, 'name', None)
        value = getattr(field, 'value', None)
        if name and value:
            pieces.append(f'{name}: {value}')
        elif value:
            pieces.append(value)
        elif name:
            pieces.append(name)
    footer = getattr(getattr(embed, 'footer', None), 'text', None)
    if footer:
        pieces.append(f'Embed footer: {footer}')
    url = getattr(embed, 'url', None)
    if url and not pieces:
        pieces.append(f'Embed URL: {url}')
    return '\n'.join(str(piece).strip() for piece in pieces if str(piece).strip())


def message_text(message):
    """Return text visible in a Discord message, including rich embeds."""
    pieces = []
    content = (getattr(message, 'content', '') or '').strip()
    if content:
        pieces.append(content)
    for embed in getattr(message, 'embeds', None) or []:
        text = _embed_text(embed)
        if text:
            pieces.append(text)
    attachments = getattr(message, 'attachments', None) or []
    names = [getattr(attachment, 'filename', None)
             for attachment in attachments]
    names = [name for name in names if name]
    if attachments:
        if not names:
            names = ['file']
        pieces.append(f'[attached: {", ".join(names)}]')
    return '\n'.join(pieces)


def _is_usable(message, bot_user_id=None, include_bot=False):
    """Skip bot output and empty messages, except an explicitly focused reply."""
    author = getattr(message, 'author', None)
    if not include_bot and author is not None and bot_user_id is not None:
        if getattr(author, 'id', None) == bot_user_id:
            return False
    if not include_bot and getattr(author, 'bot', False):
        return False
    return bool(message_text(message))


async def collect_recent(channel, before=None, limit=50, window_seconds=600,
                         bot_user_id=None):
    """Messages just before ``before``, newest-last.

    Returns [] rather than raising if history is unreadable — an answer
    without context beats no answer.
    """
    after = None
    anchor = getattr(before, 'created_at', None)
    if anchor is not None:
        try:
            from datetime import timedelta
            after = anchor - timedelta(seconds=window_seconds)
        except Exception:  # noqa: BLE001 — stubbed/naive datetimes in tests
            after = None

    collected = []
    try:
        # oldest_first defaults to True whenever `after` is given, which would
        # take the *oldest* `limit` messages in the window and walk forward.
        # We want the ones nearest the command, so force newest-first and
        # reverse below.
        async for message in channel.history(limit=limit, before=before,
                                             after=after, oldest_first=False):
            if _is_usable(message, bot_user_id):
                collected.append(message)
    except Exception:  # noqa: BLE001 — missing Read Message History, etc.
        logger.exception('Could not read channel history for ;llm')
        return []
    collected.reverse()  # newest-first off the wire → oldest-first for the prompt
    return collected


async def collect_reply_window(channel, target, before_count=25,
                               after_count=24, window_seconds=600,
                               bot_user_id=None):
    """Messages surrounding ``target``, oldest-first, including it.

    A reply usually points at one line of a longer exchange; answering well
    needs what came before it and often what came after.
    """
    if target is None:
        return []

    from datetime import timedelta
    anchor = getattr(target, 'created_at', None)
    earlier, later = [], []

    try:
        after = None
        if anchor is not None:
            try:
                after = anchor - timedelta(seconds=window_seconds)
            except Exception:  # noqa: BLE001
                after = None
        # As above: newest-first so these are the messages immediately before
        # the target, not the oldest ones in the window.
        async for message in channel.history(limit=before_count, before=target,
                                             after=after, oldest_first=False):
            if _is_usable(message, bot_user_id):
                earlier.append(message)
        earlier.reverse()

        # No `before` here, so oldest_first already defaults to True — these
        # are the messages immediately after the target, in order.
        async for message in channel.history(limit=after_count, after=target):
            if _is_usable(message, bot_user_id):
                later.append(message)
        later.sort(key=lambda m: getattr(m, 'created_at', 0) or 0)
    except Exception:  # noqa: BLE001
        logger.exception('Could not read reply context for ;llm')
        return [target] if _is_usable(target, bot_user_id,
                                      include_bot=True) else []

    window = earlier
    # The direct reply target is context even when it is the bot's own output.
    if _is_usable(target, bot_user_id, include_bot=True):
        window = window + [target]
    return window + later


def format_transcript(messages, focus=None):
    """Render collected messages as a plain transcript for the prompt.

    ``focus`` (the replied-to message) is marked so the model knows which line
    the question is actually about.
    """
    lines = []
    used = 0
    for message in messages or []:
        author = getattr(getattr(message, 'author', None), 'display_name', None) \
            or 'unknown'
        body = message_text(message).strip()
        if len(body) > _MAX_MESSAGE_CHARS:
            body = body[:_MAX_MESSAGE_CHARS - 1] + '…'

        if not body:
            continue

        is_focus = focus is not None and message is focus
        marker = (' \N{LEFTWARDS ARROW}\N{VARIATION SELECTOR-16} (the message '
                  'being replied to — the one being asked about)'
                  if is_focus else '')
        line = f'{author}: {body}{marker}'
        if used + len(line) > _MAX_TRANSCRIPT_CHARS:
            lines.append('… (earlier messages omitted)')
            break
        lines.append(line)
        used += len(line)
    return '\n'.join(lines)

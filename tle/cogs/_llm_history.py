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
# Discord applies ``limit`` before we can filter bot/empty messages. Scan a
# bounded multiple so nearby useful human messages are not crowded out.
_HISTORY_SCAN_FACTOR = 3
_MAX_AUTHOR_CHARS = 80

_OLDER_OMITTED = '… (older messages omitted)'
_LATER_OMITTED = '… (later messages omitted)'


def _is_usable(message, bot_user_id=None):
    """Skip the bot's own output and empty messages."""
    author = getattr(message, 'author', None)
    if author is not None and bot_user_id is not None:
        if getattr(author, 'id', None) == bot_user_id:
            return False
    if getattr(author, 'bot', False):
        return False
    return bool(getattr(message, 'content', '') or
                getattr(message, 'attachments', None))


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

    wanted = max(0, int(limit))
    collected = []
    try:
        # oldest_first defaults to True whenever `after` is given, which would
        # take the *oldest* `limit` messages in the window and walk forward.
        # We want the ones nearest the command, so force newest-first and
        # reverse below.
        scan_limit = wanted * _HISTORY_SCAN_FACTOR
        async for message in channel.history(limit=scan_limit, before=before,
                                             after=after, oldest_first=False):
            if _is_usable(message, bot_user_id):
                collected.append(message)
                if len(collected) >= wanted:
                    break
    except Exception:  # noqa: BLE001 — missing Read Message History, etc.
        logger.exception('Could not read channel history for ;llm')
        return []
    collected.reverse()  # newest-first off the wire → oldest-first for the prompt
    return collected


async def collect_reply_window(channel, target, before_count=25,
                               after_count=24, window_seconds=600,
                               bot_user_id=None, until=None):
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
        before_limit = max(0, int(before_count))
        async for message in channel.history(
                limit=before_limit * _HISTORY_SCAN_FACTOR, before=target,
                after=after, oldest_first=False):
            if _is_usable(message, bot_user_id):
                earlier.append(message)
                if len(earlier) >= before_limit:
                    break
        earlier.reverse()

        # Stop at both the configured time horizon and the invoking command.
        # Without these bounds a quiet channel can pull in messages from days
        # later, including the command that asked the question.
        later_before = _reply_later_boundary(
            target, until, window_seconds)
        after_limit = max(0, int(after_count))
        async for message in channel.history(
                limit=after_limit * _HISTORY_SCAN_FACTOR, after=target,
                before=later_before, oldest_first=True):
            if _is_usable(message, bot_user_id):
                later.append(message)
                if len(later) >= after_limit:
                    break
        later.sort(key=lambda m: getattr(m, 'created_at', 0) or 0)
    except Exception:  # noqa: BLE001
        logger.exception('Could not read reply context for ;llm')
        return [target] if _is_usable(target, bot_user_id) else []

    window = earlier
    if _is_usable(target, bot_user_id):
        window = window + [target]
    return window + later


def _reply_later_boundary(target, until, window_seconds):
    """Earliest exclusive boundary: command message or target + window."""
    from datetime import timedelta

    anchor = getattr(target, 'created_at', None)
    cutoff = None
    if anchor is not None:
        try:
            cutoff = anchor + timedelta(seconds=window_seconds)
        except Exception:  # noqa: BLE001
            cutoff = None
    if until is None:
        return cutoff
    until_at = getattr(until, 'created_at', None)
    if cutoff is None or (until_at is not None and until_at <= cutoff):
        return until
    return cutoff


def format_transcript(messages, focus=None):
    """Render collected messages as a plain transcript for the prompt.

    ``focus`` (the replied-to message) is marked so the model knows which line
    the question is actually about.
    """
    rendered = []
    focus_position = None
    for message in messages or []:
        line = _render_message(message, focus)
        if line is None:
            continue
        if message is focus:
            focus_position = len(rendered)
        rendered.append(line)
    if not rendered:
        return ''

    if focus_position is None:
        start, end = len(rendered) - 1, len(rendered) - 1
        while start > 0:
            candidate = _compose_transcript(rendered, start - 1, end)
            if len(candidate) > _MAX_TRANSCRIPT_CHARS:
                break
            start -= 1
        return _compose_transcript(rendered, start, end)

    start = end = focus_position
    prefer_left = True
    left_open = start > 0
    right_open = end < len(rendered) - 1
    while left_open or right_open:
        sides = ('left', 'right') if prefer_left else ('right', 'left')
        added = False
        for side in sides:
            if side == 'left' and left_open:
                candidate = _compose_transcript(rendered, start - 1, end)
                if len(candidate) <= _MAX_TRANSCRIPT_CHARS:
                    start -= 1
                    prefer_left = False
                    added = True
                    break
                left_open = False
            elif side == 'right' and right_open:
                candidate = _compose_transcript(rendered, start, end + 1)
                if len(candidate) <= _MAX_TRANSCRIPT_CHARS:
                    end += 1
                    prefer_left = True
                    added = True
                    break
                right_open = False
        left_open = left_open and start > 0
        right_open = right_open and end < len(rendered) - 1
        if not added and not (left_open or right_open):
            break
    return _compose_transcript(rendered, start, end)


def _render_message(message, focus):
    """Render one bounded transcript entry, or ``None`` when empty."""
    author = getattr(getattr(message, 'author', None), 'display_name', None) \
        or 'unknown'
    author = _one_line(author, _MAX_AUTHOR_CHARS)
    body = (getattr(message, 'content', '') or '').strip()

    attachments = getattr(message, 'attachments', None) or []
    names = [_one_line(getattr(item, 'filename', ''), 100)
             for item in attachments]
    names = [name for name in names if name]
    if attachments:
        detail = ', '.join(names[:5]) if names else 'attachment'
        body = (body + ' ' if body else '') + f'[attached: {detail}]'
    if not body:
        return None
    if len(body) > _MAX_MESSAGE_CHARS:
        body = body[:_MAX_MESSAGE_CHARS - 1] + '…'

    marker = (' \N{LEFTWARDS ARROW}\N{VARIATION SELECTOR-16} (the message '
              'being asked about)' if message is focus else '')
    return f'{author}: {body}{marker}'


def _one_line(value, limit):
    text = ' '.join(str(value or '').split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


def _compose_transcript(lines, start, end):
    selected = []
    if start > 0:
        selected.append(_OLDER_OMITTED)
    selected.extend(lines[start:end + 1])
    if end < len(lines) - 1:
        selected.append(_LATER_OMITTED)
    return '\n'.join(selected)

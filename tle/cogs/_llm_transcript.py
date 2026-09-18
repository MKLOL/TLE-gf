"""Credential-safe rendering for bounded ``;llm`` transcripts."""
import json
import re

from tle.cogs._llm_message_text import message_text


# Per-message text budget inside a transcript. Long pastes are the common case
# in a competitive-programming channel and would otherwise crowd out context.
_MAX_MESSAGE_CHARS = 600
# Whole-transcript budget, so a busy channel cannot blow up the prompt.
_MAX_TRANSCRIPT_CHARS = 12000
_MAX_AUTHOR_CHARS = 80

_OLDER_OMITTED = '… (older messages omitted)'
_LATER_OMITTED = '… (later messages omitted)'

_LITERAL_SECRET_PATTERNS = (
    re.compile(r'(?<![\w-])xai-[A-Za-z0-9_-]{12,}', re.IGNORECASE),
    re.compile(r'(?<![\w-])AIza[A-Za-z0-9_-]{20,}'),
    re.compile(r'\bBearer\s+[A-Za-z0-9._~+/=-]{12,}', re.IGNORECASE),
    re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})'),
    re.compile(r'\bAKIA[A-Z0-9]{16}\b'),
    re.compile(r'\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'
               r'\.[A-Za-z0-9_-]{10,}\b'),
    re.compile(r'\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{6}'
               r'\.[A-Za-z0-9_-]{20,}\b'),
)
_ASSIGNED_SECRET = re.compile(
    r'\b(?P<name>[A-Za-z0-9_.-]{0,32}'
    r'(?:api[_-]?key|access[_-]?token|auth[_-]?token|token|secret|'
    r'password|passwd))(?P<separator>\s*[:=]\s*)'
    r'(?P<value>"[^"]*"|\'[^\']*\'|[^\s,;]+)', re.IGNORECASE)


def redact_secrets(value):
    """Remove likely credentials before a transcript leaves Discord."""
    text = str(value or '')
    text = _ASSIGNED_SECRET.sub(
        lambda match: (match.group('name') + match.group('separator') +
                       '[REDACTED]'), text)
    for pattern in _LITERAL_SECRET_PATTERNS:
        text = pattern.sub('[REDACTED]', text)
    return text


def _message_token(message):
    message_id = getattr(message, 'id', None)
    if message_id is not None:
        return 'id', str(message_id)
    return 'object', id(message)


def _reply_target_token(message):
    reference = getattr(message, 'reference', None)
    if reference is None:
        return None
    message_id = getattr(reference, 'message_id', None)
    if message_id is not None:
        return 'id', str(message_id)
    resolved = getattr(reference, 'resolved', None)
    return _message_token(resolved) if resolved is not None else None


def format_transcript(messages, focus=None, structured=False,
                      requester_id=None):
    """Render collected messages as a plain transcript for the prompt.

    ``focus`` (the replied-to message) is marked so the model knows which line
    the question is actually about. ``structured`` adds escaped metadata used
    by the live LLM pipeline while the legacy rendering remains available to
    callers that only need a human-readable preview.
    """
    rendered = []
    focus_position = None
    for message in messages or []:
        line = _render_message(
            message, focus, structured=structured,
            requester_id=requester_id)
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


def _render_message(message, focus, structured=False, requester_id=None):
    """Render one bounded transcript entry, or ``None`` when empty."""
    author = getattr(getattr(message, 'author', None), 'display_name', None) \
        or 'unknown'
    author = _one_line(redact_secrets(author), _MAX_AUTHOR_CHARS)
    body = redact_secrets(message_text(message).strip())
    if not body:
        if message is not focus:
            return None
        body = '(empty message)'
    if len(body) > _MAX_MESSAGE_CHARS:
        body = body[:_MAX_MESSAGE_CHARS - 1] + '…'

    if structured:
        message_author = getattr(message, 'author', None)
        author_id = getattr(message_author, 'id', None)
        message_id = getattr(message, 'id', None)
        reply_to = _reply_target_token(message)
        reply_id = reply_to[1] if reply_to and reply_to[0] == 'id' else None
        timestamp = _format_message_timestamp(
            getattr(message, 'created_at', None))
        return json.dumps({
            'id': str(message_id) if message_id is not None else None,
            'timestamp': timestamp,
            'author': author,
            'author_is_bot': bool(getattr(message_author, 'bot', False)),
            'is_requester': (
                requester_id is not None and author_id is not None
                and str(author_id) == str(requester_id)),
            'reply_to': reply_id,
            'focus': message is focus,
            'content': body,
        }, ensure_ascii=False, separators=(',', ':'))

    marker = (' \N{LEFTWARDS ARROW}\N{VARIATION SELECTOR-16} (the message '
              'being replied to — the one being asked about)'
              if message is focus else '')
    return f'{author}: {body}{marker}'


def _format_message_timestamp(created_at):
    if created_at is None:
        return 'unknown'
    try:
        return created_at.strftime('%Y-%m-%dT%H:%M:%SZ')
    except AttributeError:
        return _one_line(created_at, 80)


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

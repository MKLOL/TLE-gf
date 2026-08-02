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
import json
import logging
import re

from tle.cogs._llm_message_text import _embed_text, message_text

logger = logging.getLogger(__name__)

# Per-message text budget inside a transcript. Long pastes are the common case
# in a competitive-programming channel and would otherwise crowd out context.
_MAX_MESSAGE_CHARS = 600
# Whole-transcript budget, so a busy channel cannot blow up the prompt.
_MAX_TRANSCRIPT_CHARS = 12000
# Discord applies ``limit`` before we can filter bot/empty messages. Scan a
# bounded multiple so nearby useful messages are not crowded out.
_HISTORY_SCAN_FACTOR = 3
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


def _is_usable(message, bot_user_id=None, include_other_bots=False,
               include_bot=False):
    """Skip unwanted bot output unless this is the focused reply target."""
    author = getattr(message, 'author', None)
    if not include_bot and author is not None and bot_user_id is not None:
        if getattr(author, 'id', None) == bot_user_id:
            return False
    if (not include_bot and getattr(author, 'bot', False)
            and not include_other_bots):
        return False
    return bool(message_text(message))


def _outside_session(newer_at, older_at, gap_seconds):
    """Return whether adjacent usable messages cross an inactivity gap."""
    if gap_seconds is None or newer_at is None or older_at is None:
        return False
    try:
        gap = max(0, int(gap_seconds))
        return (newer_at - older_at).total_seconds() > gap
    except (AttributeError, TypeError, ValueError):
        return False


def _speaker_key(message):
    """Return a stable identity for adjacent speaker-turn grouping."""
    author = getattr(message, 'author', None)
    author_id = getattr(author, 'id', None)
    if author_id is not None:
        return ('id', author_id)
    return ('name', getattr(author, 'display_name', None))


async def collect_recent(channel, before=None, limit=50, window_seconds=600,
                         bot_user_id=None, include_other_bots=False,
                         gap_seconds=None):
    """Collect the active conversation before ``before``, oldest-first.

    ``limit`` counts adjacent speaker turns; consecutive usable messages
    from one author consume one turn while all remain in the transcript.
    ``window_seconds`` is the hard maximum age, and ``gap_seconds`` ends
    the session at an inactivity boundary. Unreadable history returns [].
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
    if wanted == 0:
        return []

    collected = []
    # The invoking command bounds the history query, but is not part of the
    # conversation being summarized. Start inactivity-gap detection from the
    # newest usable message before the command instead.
    newer_at = None
    newer_speaker = None
    speaker_turns = 0
    try:
        # The limit counts adjacent speaker turns, not raw Discord
        # messages. With a real time boundary, scan the complete bounded
        # window so a long run from one speaker is not truncated.
        scan_limit = (
            None if after is not None
            else wanted * _HISTORY_SCAN_FACTOR
        )
        async for message in channel.history(
                limit=scan_limit, before=before, after=after,
                oldest_first=False):
            if not _is_usable(message, bot_user_id, include_other_bots):
                continue

            sent_at = getattr(message, 'created_at', None)
            if _outside_session(newer_at, sent_at, gap_seconds):
                break

            speaker = _speaker_key(message)
            if newer_speaker is None:
                speaker_turns = 1
            elif speaker != newer_speaker:
                if speaker_turns >= wanted:
                    break
                speaker_turns += 1

            collected.append(message)
            newer_speaker = speaker
            if sent_at is not None:
                newer_at = sent_at
    except Exception:  # noqa: BLE001 — missing Read Message History, etc.
        logger.exception('Could not read channel history for ;llm')
        return []
    collected.reverse()  # newest-first off the wire → oldest-first for the prompt
    return collected


async def collect_reply_window(channel, target, before_count=25,
                               after_count=24, window_seconds=600,
                               bot_user_id=None, until=None,
                               include_other_bots=False):
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
            if _is_usable(message, bot_user_id, include_other_bots):
                earlier.append(message)
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
            if _is_usable(message, bot_user_id, include_other_bots):
                later.append(message)
        later.sort(key=lambda m: getattr(m, 'created_at', 0) or 0)
    except Exception:  # noqa: BLE001
        logger.exception('Could not read reply context for ;llm')
        return [target]

    # The explicitly selected reply target is context even when this bot sent
    # it or it contains only an embed. Bot filtering applies to neighbors.
    _merge_resolved_ancestors(
        earlier, target, window_seconds, bot_user_id, include_other_bots)
    all_messages = earlier + [target] + later
    earlier = _select_relevant(earlier, target, all_messages, before_limit)
    later = _select_relevant(later, target, all_messages, after_limit)
    return earlier + [target] + later


def _merge_resolved_ancestors(earlier, target, window_seconds, bot_user_id,
                              include_other_bots):
    """Add resolved reply ancestors that a bounded history scan missed."""
    known = {_message_token(message) for message in earlier}
    current = target
    seen = {_message_token(target)}
    for _ in range(len(earlier) + 32):
        resolved = getattr(getattr(current, 'reference', None),
                           'resolved', None)
        if resolved is None:
            break
        token = _message_token(resolved)
        if token in seen:
            break
        seen.add(token)
        if (_is_usable(resolved, bot_user_id, include_other_bots) and
                _within_window(resolved, target, window_seconds) and
                token not in known):
            earlier.append(resolved)
            known.add(token)
        current = resolved
    earlier.sort(key=_chronological_key)


def _select_relevant(candidates, target, all_messages, limit):
    """Take relation-first candidates, then restore chronological order."""
    wanted = max(0, int(limit))
    if wanted == 0 or not candidates:
        return []

    lookup = {_message_token(message): message for message in all_messages}
    target_token = _message_token(target)
    ancestor_tokens = set()
    current = target
    visited = {target_token}
    for _ in range(len(all_messages) + 1):
        parent_token = _reply_target_token(current)
        if parent_token is None or parent_token in visited:
            break
        parent = lookup.get(parent_token)
        if parent is None:
            resolved = getattr(getattr(current, 'reference', None),
                               'resolved', None)
            if resolved is None:
                break
            parent = resolved
            parent_token = _message_token(parent)
        ancestor_tokens.add(parent_token)
        visited.add(parent_token)
        current = parent

    chain_tokens = ancestor_tokens | {target_token}
    direct_tokens = {
        _message_token(message) for message in all_messages
        if _reply_target_token(message) in chain_tokens
    }
    participant_tokens = {
        token for token in (
            _author_token(message) for message in all_messages
            if (_message_token(message) in chain_tokens or
                _message_token(message) in direct_tokens)
        ) if token is not None
    }
    positions = {_message_token(message): index
                 for index, message in enumerate(all_messages)}
    focus_position = positions.get(target_token, 0)

    def rank(message):
        token = _message_token(message)
        if token in ancestor_tokens:
            category = 0
        elif token in direct_tokens:
            category = 1
        elif _author_token(message) in participant_tokens:
            category = 2
        else:
            category = 3
        position = positions.get(token, focus_position)
        return category, abs(position - focus_position), position

    picked = sorted(candidates, key=rank)[:wanted]
    picked.sort(key=_chronological_key)
    return picked


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


def _author_token(message):
    author = getattr(message, 'author', None)
    author_id = getattr(author, 'id', None)
    if author_id is not None:
        return 'id', str(author_id)
    name = getattr(author, 'display_name', None)
    return ('name', str(name).casefold()) if name else None


def _within_window(message, target, window_seconds):
    for attribute in ('channel', 'guild'):
        left = getattr(getattr(message, attribute, None), 'id', None)
        right = getattr(getattr(target, attribute, None), 'id', None)
        if left is not None and right is not None and left != right:
            return False
    message_at = getattr(message, 'created_at', None)
    target_at = getattr(target, 'created_at', None)
    if message_at is None or target_at is None:
        return True
    try:
        age = (target_at - message_at).total_seconds()
        return 0 <= age <= window_seconds
    except (AttributeError, TypeError):
        return True


def _chronological_key(message):
    created_at = getattr(message, 'created_at', None)
    try:
        stamp = created_at.timestamp()
    except (AttributeError, OSError, TypeError, ValueError):
        stamp = 0
    message_id = getattr(message, 'id', 0) or 0
    try:
        message_id = int(message_id)
    except (TypeError, ValueError):
        message_id = 0
    return stamp, message_id


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

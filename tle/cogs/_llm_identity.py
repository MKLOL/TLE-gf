"""Bounded bot-generated identity routing for Grok answer prompts."""

import json

from tle.cogs._llm_history import redact_secrets


_MAX_NAME_CHARS = 80


def build_request_routing(requester, request_message=None, referenced=None):
    """Identify the answer recipient separately from an optional reply subject."""
    requester_record = _author_record(requester)
    target_author = getattr(referenced, 'author', None)
    target_record = None
    if referenced is not None:
        target_record = {
            'message_id': _identifier(getattr(referenced, 'id', None)),
            'author': _author_record(target_author),
            'same_as_requester': _same_author(requester, target_author),
        }
    payload = {
        'requester': requester_record,
        'request_message_id': _identifier(
            getattr(request_message, 'id', None)),
        'focused_reply_target': target_record,
    }
    return json.dumps(
        payload, ensure_ascii=False, separators=(',', ':'))


def _author_record(author):
    return {
        'discord_user_id': _identifier(getattr(author, 'id', None)),
        'display_name': _text(getattr(author, 'display_name', None)),
        'is_bot': bool(getattr(author, 'bot', False)),
    }


def _same_author(left, right):
    if left is None or right is None:
        return False
    left_id = _identifier(getattr(left, 'id', None))
    right_id = _identifier(getattr(right, 'id', None))
    if left_id is not None and right_id is not None:
        return left_id == right_id
    return left is right


def _identifier(value):
    return str(value)[:40] if value is not None else None


def _text(value):
    text = ' '.join(redact_secrets(value).split())
    return text[:_MAX_NAME_CHARS] or None

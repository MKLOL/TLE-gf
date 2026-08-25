"""Parse Great Day membership commands and their exact bot results."""
from __future__ import annotations

from dataclasses import dataclass
import re

from tle.util import discord_common


_COMMAND_RE = re.compile(
    r'^greatday\s+(signup|remove|add|kick|ban|unban)(?:\s+(.*))?$',
    re.DOTALL)
_MENTION_PREFIX_RE = re.compile(r'^<@!?(\d+)> (.*)$', re.DOTALL)
_TARGET_MENTION_RE = re.compile(r'^<@!?(\d+)>$')
_TARGET_ID_RE = re.compile(r'^\d{15,22}$')
_TARGET_FOOTER_RE = re.compile(r'^Great Day user ID: (\d+)$')
_SELF_COMMANDS = frozenset(('signup', 'remove'))

_SELF_RESULTS = {
    'You have been signed up for great day pings!':
        ('signup', True, 'changed'),
    'You are already signed up.': ('signup', False, 'already_signed'),
    'You are banned from great day.': ('signup', False, 'banned'),
    'You have been removed from great day pings.':
        ('remove', True, 'changed'),
    'You are not signed up.': ('remove', False, 'not_signed'),
}
_NAMED_RESULTS = (
    ('` has been added to great day pings.', 'add', True, 'changed'),
    ('` is already signed up.', 'add', False, 'already_signed'),
    ('` is banned from great day. Unban them first.',
     'add', False, 'banned'),
    ('` has been removed from great day pings.', 'kick', True, 'changed'),
    ('` is not signed up.', 'kick', False, 'not_signed'),
    ('` has been banned from great day.', 'ban', True, 'changed'),
    ('` is already banned.', 'ban', False, 'already_banned'),
    ('` has been unbanned from great day.', 'unban', True, 'changed'),
    ('` is not banned.', 'unban', False, 'not_banned'),
)


@dataclass(frozen=True)
class MembershipCommand:
    kind: str
    author_id: str
    target_id: str | None
    target_text: str | None
    at: float
    message_id: str


@dataclass(frozen=True)
class MembershipResult:
    kind: str
    success: bool
    outcome: str
    display_name: str | None
    target_id: str | None
    at: float
    message_id: str
    reference_id: str | None


def message_time(message):
    return float(message.created_at.timestamp())


def _message_author_id(message):
    value = getattr(getattr(message, 'author', None), 'id', None)
    return None if value is None else str(value)


def _command_body(content, bot_user_id):
    text = (content or '').rstrip()
    if text.startswith(';'):
        return text[1:]
    mention = _MENTION_PREFIX_RE.match(text)
    if mention is None or bot_user_id is None:
        return None
    if mention.group(1) != str(bot_user_id):
        return None
    return mention.group(2)


def _first_argument(rest):
    if not rest:
        return None
    rest = rest.strip()
    if not rest:
        return None
    quote_pairs = {'"': '"', "'": "'", '“': '”', '‘': '’'}
    closing = quote_pairs.get(rest[0])
    if closing is not None:
        end = rest.find(closing, 1)
        return rest[1:end] if end >= 1 else rest[1:]
    return rest.split(maxsplit=1)[0]


def parse_membership_command(message, bot_user_id):
    """Parse an actually prefixed membership command without checking roles."""
    # discord.py does not re-run prefix commands when their message is edited.
    # The edited content therefore cannot prove which command, if any, the bot
    # originally executed. This also prevents a user from obtaining a reply
    # from another cog and later editing its prompt into a Great Day command.
    if getattr(message, 'edited_at', None) is not None:
        return None
    author = getattr(message, 'author', None)
    if getattr(author, 'bot', False):
        return None
    author_id = _message_author_id(message)
    if author_id is None or author_id == str(bot_user_id):
        return None
    body = _command_body(getattr(message, 'content', ''), bot_user_id)
    match = _COMMAND_RE.match(body) if body is not None else None
    if match is None:
        return None
    kind, rest = match.groups()
    target_text = None if kind in _SELF_COMMANDS else _first_argument(rest)
    target_id = author_id if kind in _SELF_COMMANDS else None
    if target_text is not None:
        mention = _TARGET_MENTION_RE.match(target_text)
        if mention is not None:
            target_id = mention.group(1)
        elif _TARGET_ID_RE.match(target_text):
            target_id = target_text
    return MembershipCommand(
        kind, author_id, target_id, target_text, message_time(message),
        str(message.id))


def _embed_color_value(embed):
    color = getattr(embed, 'colour', None)
    if color is None:
        color = getattr(embed, 'color', None)
    color = getattr(color, 'value', color)
    try:
        return int(color)
    except (TypeError, ValueError):
        return None


def _proxy_value(value, name):
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _embed_footer(embed):
    footer = getattr(embed, 'footer', None)
    return _proxy_value(footer, 'text'), _proxy_value(footer, 'icon_url')


def _embed_has_extra_metadata(embed):
    """Reject embeds that cannot have been emitted by the Great Day cog."""
    if any((getattr(embed, 'title', None), getattr(embed, 'url', None),
            getattr(embed, 'timestamp', None), getattr(embed, 'fields', None))):
        return True
    author = getattr(embed, 'author', None)
    if _proxy_value(author, 'name'):
        return True
    author_data = getattr(embed, 'author_data', None)  # Test/stub shape.
    if _proxy_value(author_data, 'name'):
        return True
    for name in ('image', 'thumbnail', 'video', 'provider'):
        value = getattr(embed, name, None)
        if _proxy_value(value, 'url') or _proxy_value(value, 'name'):
            return True
    return False


def _parse_result_text(text):
    if not isinstance(text, str):
        return None
    simple = _SELF_RESULTS.get(text)
    if simple is not None:
        return (*simple, None)
    if not text.startswith('`'):
        return None
    for suffix, kind, success, outcome in _NAMED_RESULTS:
        if text.endswith(suffix):
            return kind, success, outcome, text[1:-len(suffix)]
    return None


def _result_reference_id(message):
    reference = getattr(message, 'reference', None)
    value = getattr(reference, 'message_id', None)
    return None if value is None else str(value)


def parse_membership_result(message, bot_user_id):
    """Classify one exact terminal response emitted by the Great Day cog."""
    if (_message_author_id(message) != str(bot_user_id)
            or getattr(message, 'edited_at', None) is not None):
        return None
    # Great Day has always sent exactly one embed with no message content.
    # Legacy results are bare green/amber embeds; current results additionally
    # carry a Great Day target footer and reply to the invoking command.
    if getattr(message, 'content', ''):
        return None
    embeds = list(getattr(message, 'embeds', None) or ())
    if len(embeds) != 1:
        return None
    embed = embeds[0]
    parsed = _parse_result_text(getattr(embed, 'description', None))
    if parsed is None:
        return None
    kind, success, outcome, display_name = parsed
    expected_color = (discord_common._SUCCESS_GREEN if success
                      else discord_common._ALERT_AMBER)
    if (_embed_color_value(embed) != expected_color
            or _embed_has_extra_metadata(embed)):
        return None

    footer_text, footer_icon = _embed_footer(embed)
    footer_match = _TARGET_FOOTER_RE.fullmatch(footer_text or '')
    if footer_text and footer_match is None:
        return None
    if footer_icon:
        return None
    target_id = footer_match.group(1) if footer_match is not None else None
    reference_id = _result_reference_id(message)
    # A footer/reference pair identifies current Great Day replies. Neither is
    # present on genuine legacy results or on GreatDayCogError responses.
    if (target_id is None) != (reference_id is None):
        return None
    return MembershipResult(
        kind, success, outcome, display_name, target_id,
        message_time(message), str(message.id), reference_id)


def _normalized_name(value):
    return str(value or '').replace('@\u200b', '@')


def _member_names(member):
    values = {
        getattr(member, 'name', None),
        getattr(member, 'global_name', None),
        getattr(member, 'nick', None),
        getattr(member, 'display_name', None),
        str(member),
    }
    return {_normalized_name(value) for value in values if value}


def _resolve_named_target(command, result, guild):
    if guild is None or command.target_text is None:
        return None
    members = list(getattr(guild, 'members', None) or ())
    get_named = getattr(guild, 'get_member_named', None)
    named = get_named(command.target_text) if callable(get_named) else None
    if named is not None and all(member.id != named.id for member in members):
        members.append(named)
    target_name = _normalized_name(command.target_text)
    output_name = _normalized_name(result.display_name)
    command_matches = [
        member for member in members if target_name in _member_names(member)
    ]
    output_matches = [
        member for member in members if output_name in _member_names(member)
    ] if output_name else []
    if command_matches and output_matches:
        output_ids = {member.id for member in output_matches}
        overlap = [member for member in command_matches
                   if member.id in output_ids]
        if len(overlap) == 1:
            return str(overlap[0].id)
    if len(command_matches) == 1:
        return str(command_matches[0].id)
    if len(output_matches) == 1:
        return str(output_matches[0].id)
    return None


def resolve_target(command, result, guild):
    if result.target_id is not None:
        return result.target_id, False
    if command.target_id is not None:
        return command.target_id, False
    target_id = _resolve_named_target(command, result, guild)
    return target_id, target_id is not None


def compatible(command, result):
    if command.kind != result.kind:
        return False
    return not (command.target_id and result.target_id
                and command.target_id != result.target_id)

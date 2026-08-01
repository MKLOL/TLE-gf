"""Bounded Codeforces participant metadata for LLM prompts."""
import json
import logging

from tle.cogs._llm_history import redact_secrets

logger = logging.getLogger(__name__)

_MAX_PROFILES = 24
_MAX_TEXT = 80
_RANK_COLORS = {
    'newbie': 'gray',
    'pupil': 'green',
    'specialist': 'cyan',
    'expert': 'blue',
    'candidate master': 'violet',
    'master': 'orange',
    'international master': 'orange',
    'grandmaster': 'red',
    'international grandmaster': 'red',
    'legendary grandmaster': 'red',
    'tourist': 'black',
    'unrated': 'unrated',
}


def build_profiles(database, guild_id, requester, messages=(), focused=None):
    """Return compact JSON for linked participants in request-first order."""
    authors = {}
    order = []

    def remember(author):
        user_id = getattr(author, 'id', None)
        if user_id is None or str(user_id) in authors:
            return
        token = str(user_id)
        authors[token] = author
        order.append(token)

    remember(requester)
    for message in messages or ():
        if message is not None:
            remember(getattr(message, 'author', None))
        if len(order) >= _MAX_PROFILES:
            break

    getter = getattr(database, 'get_cf_users_for_guild_members', None)
    if getter is None or not order:
        return ''
    try:
        rows = getter(guild_id, order[:_MAX_PROFILES])
    except Exception:  # noqa: BLE001 - profile context is optional
        logger.exception('Could not load Codeforces profiles for LLM context')
        return ''

    by_user = {str(user_id): profile for user_id, profile in rows}
    requester_id = str(getattr(requester, 'id', ''))
    focused_id = str(getattr(
        getattr(focused, 'author', None), 'id', ''))
    records = []
    for user_id in order[:_MAX_PROFILES]:
        profile = by_user.get(user_id)
        if profile is None:
            continue
        record = _profile_record(
            user_id, authors[user_id], profile,
            is_requester=user_id == requester_id,
            is_reply_target=bool(focused_id) and user_id == focused_id)
        if record is not None:
            records.append(record)
    return json.dumps(records, ensure_ascii=False, separators=(',', ':')) \
        if records else ''


def _profile_record(user_id, author, profile, *, is_requester=False,
                    is_reply_target=False):
    handle = _text(getattr(profile, 'handle', None))
    if not handle:
        return None
    rating = _integer(getattr(profile, 'rating', None))
    maximum = _integer(getattr(profile, 'maxRating', None))
    rank = _rank(profile)
    title = _text(getattr(rank, 'title', None)) or 'Unrated'
    color_hex = _text(getattr(rank, 'color_graph', None), 16)
    color_name = _RANK_COLORS.get(title.casefold(), 'unknown')
    color = f'{color_name} ({color_hex})' if color_hex else color_name
    return {
        'discord_user_id': user_id,
        'display_name': _text(getattr(author, 'display_name', None)),
        'is_requester': is_requester,
        'is_reply_target': is_reply_target,
        'codeforces_handle': handle,
        'rating': rating,
        'max_rating': maximum,
        'rank': title,
        'rank_abbreviation': _text(getattr(rank, 'title_abbr', None), 12),
        'rank_color': color,
        'country': _text(getattr(profile, 'country', None)),
    }


def _rank(profile):
    try:
        return profile.rank
    except (AttributeError, TypeError, ValueError):
        return None


def _integer(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _text(value, limit=_MAX_TEXT):
    if value is None:
        return None
    text = ' '.join(redact_secrets(value).split())
    return text[:limit] or None

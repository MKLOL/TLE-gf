"""Signup/signout event display and statistics helpers."""

from tle.cogs._greatday_event_backfill import (
    MembershipCommand,
    MembershipResult,
    SignupScanAudit,
    SignupScanResult,
    parse_membership_command,
    parse_membership_result,
    scan_signup_events,
    scan_signup_events_audited,
    scan_signup_events_channels_audited,
)
from tle.cogs._greatday_helpers import _format_pick_time


def embed_texts(msg):
    """Yield the text of a message and of its embeds."""
    yield msg.content or ''
    for embed in getattr(msg, 'embeds', None) or ():
        yield getattr(embed, 'description', None) or ''
        yield getattr(embed, 'title', None) or ''


def is_success_reply(msg, bot_user_id, action):
    """Whether ``msg`` is the bot's confirmation of ``action``."""
    result = parse_membership_result(msg, bot_user_id)
    if result is None or not result.success:
        return False
    kinds = {'signup': ('signup', 'add'), 'signout': ('remove', 'kick')}
    return result.kind in kinds[action]


def parse_signup_command(msg, bot_user_id=None):
    """Return ``(action, user_id)`` for a Great Day membership command.

    ``None`` when the message is not one, or when an admin command does not
    name a member.
    """
    command = parse_membership_command(msg, bot_user_id)
    if command is None or command.target_id is None:
        return None
    actions = {
        'signup': 'signup', 'add': 'signup',
        'remove': 'signout', 'kick': 'signout',
    }
    action = actions.get(command.kind)
    if action is None:
        return None
    return action, command.target_id


def record_event(db, guild_id, user_id, action, message):
    """Record a standalone event and propagate persistence failures."""
    return db.greatday_record_signup_event(
        guild_id, user_id, action, message.created_at.timestamp(), message.id)


def signed_up_post_count(events, post_times, currently_signed_up):
    """Count Great Day posts that happened while the user was signed up.

    ``events`` may be in any order; ``post_times`` are guild-wide post
    timestamps. Returns ``(count, complete)``. Only fully bounded intervals
    and an open interval confirmed by current membership are counted. This
    prevents a missing live event from turning a lower bound into an overcount.
    """
    ordered = sorted(events, key=lambda row: (row.at, str(row.message_id)))
    intervals = []
    start = None
    complete = bool(ordered)
    for row in ordered:
        if row.action == 'signup':
            if start is not None:
                # Two confirmed signups require an unrecorded signout between
                # them. Restart at the later known-active checkpoint.
                complete = False
            start = row.at
        elif start is not None:
            intervals.append((start, row.at))
            start = None
        else:
            # Left without a recorded join.
            complete = False
    if start is not None:
        if currently_signed_up:
            intervals.append((start, float('inf')))
        else:
            # The missing signout could have happened anywhere after start.
            complete = False
    elif currently_signed_up:
        # Current membership requires a later signup that is not in the log.
        complete = False
    count = sum(1 for at in post_times
                if any(lo <= at <= hi for lo, hi in intervals))
    return count, complete


def collapse_events(events):
    """Drop events that do not change signed-up state.

    A second signup while already signed up (or a repeated signout) carries no
    information, so only state transitions are kept. Accepts events in any
    order and returns the kept ones newest first.
    """
    ordered = sorted(events, key=lambda row: (row.at, str(row.message_id)))
    kept = []
    for row in ordered:
        if not kept or kept[-1].action != row.action:
            kept.append(row)
    kept.reverse()
    return kept


def merge_history(picks, events):
    """Interleave picks and signup events into newest-first display lines.

    Picks keep their bare timestamp line; only signup/signout are labelled.
    """
    labels = {'signup': 'Signed up — ', 'signout': 'Signed out — '}
    entries = [(row.picked_at, str(row.message_id), '') for row in picks]
    entries += [(row.at, str(row.message_id), labels[row.action])
                for row in events]
    # Ties break on message id, matching the DB queries' ordering.
    entries.sort(key=lambda e: (e[0], int(e[1]) if e[1].isdigit() else 0),
                 reverse=True)
    return [f'{label}{_format_pick_time(at)}' for at, _, label in entries]

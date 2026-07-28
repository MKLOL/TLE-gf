"""Pure helpers shared by the Great Day commands and backfill."""
import re

from tle.util import ranking

# Stop the backfill once we've walked this far past the most recent
# Great Day match without finding another one. Great Day runs roughly daily,
# so a five-day gap means we have probably collected the full history.
_BACKFILL_STOP_GAP_SECONDS = 5 * 24 * 3600

# Great Day message template: "I hope <@id> <@id> ... having a great day!"
_GREATDAY_RE = re.compile(r'^I hope .*having a great day!\s*$')
_MENTION_RE = re.compile(r'<@!?(\d+)>')


def _personal_rank_line(rows, user_id):
    """Render the invoker's standard-competition rank for the stats command."""
    user_id_str = str(user_id)
    for rank, row in ranking.rank_items(rows, lambda r: r.cnt):
        if str(row.user_id) == user_id_str:
            return (f"Your rank: **#{rank}** — great-day'd "
                    f'**{row.cnt}** time(s).')
    return "You haven't been great-day'd yet."


def _should_stop_backfill(last_match_ts, current_msg_ts, max_gap_seconds):
    """Return whether an older message is too far past the latest match."""
    if last_match_ts is None:
        return False
    return last_match_ts - current_msg_ts > max_gap_seconds


def _parse_greatday_message(msg, bot_user_id):
    """Return mentioned IDs for a genuine bot-authored Great Day post."""
    author_id = getattr(getattr(msg, 'author', None), 'id', None)
    if author_id != bot_user_id:
        return None
    if not _GREATDAY_RE.match(msg.content or ''):
        return None
    uids = _MENTION_RE.findall(msg.content)
    return uids or None


def _target_datetime(now, time_str):
    """Return today's target time as a timezone-aware datetime."""
    hour, minute = map(int, time_str.split(':'))
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _format_pick_time(picked_at):
    """Render a stored timestamp in Discord's absolute and relative forms."""
    timestamp = int(picked_at)
    return f'<t:{timestamp}:F> (<t:{timestamp}:R>)'

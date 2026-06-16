import asyncio
import time
import datetime as dt
from collections import namedtuple

from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import discord_common

_CONTESTS_PER_PAGE = 5
_CONTEST_PAGINATE_WAIT_TIME = 5 * 60
_STANDINGS_PER_PAGE = 15
_STANDINGS_PAGINATE_WAIT_TIME = 2 * 60
_FINISHED_CONTESTS_LIMIT = 5
_WATCHING_RATED_VC_WAIT_TIME = 5 * 60  # seconds
_RATED_VC_EXTRA_TIME = 10 * 60  # seconds
_MIN_RATED_CONTESTANTS_FOR_RATED_VC = 50

_INDEX_CELL_COLOR = (0, 0, 0)


def _cf_rating_color(rating):
    """Per-row text colour for the probratimg table — pulls the official
    CF rank palette from ``cf.rating2rank().color_embed`` so colours stay
    in sync with the rest of the codebase."""
    if rating is None:
        return (10, 10, 10)
    embed = cf.rating2rank(rating).color_embed
    if embed is None:
        return (10, 10, 10)
    return ((embed >> 16) & 0xFF, (embed >> 8) & 0xFF, embed & 0xFF)


def _render_problemratings_image(title, indices, official_ratings, predicted, *, from_cache):
    """Render the probrat table using the same Cairo/Pango table renderer as
    ``;mg akari ratings``. Each rating cell is coloured by its own value's
    CF rank (Official and Predicted may land in different tiers)."""
    # Imported here to avoid pulling cairo/Pango (and the whole minigames cog)
    # into contests' module-load path if it's never invoked.
    from tle.cogs.minigames import _get_akari_puzzle_table_image
    header = ('#', 'Official', 'Predicted (C)' if from_cache else 'Predicted')
    rows = [
        (str(idx),
         '—' if official_ratings[i] is None else str(official_ratings[i]),
         str(predicted[i]))
        for i, idx in enumerate(indices)
    ]
    cell_colors = [
        (_INDEX_CELL_COLOR, _cf_rating_color(official_ratings[i]), _cf_rating_color(predicted[i]))
        for i in range(len(indices))
    ]
    return _get_akari_puzzle_table_image(
        rows, title=title,
        header=header,
        cols=(60, 200, 200),
        right_align_cols=(0, 1, 2),
        cell_colors=cell_colors,
        width=500,
        filename='probrat.png',
    )


class ContestCogError(commands.CommandError):
    pass


_VcRatingChange = namedtuple('VcRatingChange', 'handle oldRating newRating')


def _apply_vc_deltas(db, vc_id, handles, member_ids, ranklist):
    """Apply rated-VC rating deltas after a contest finishes.

    Returns a {handle: _VcRatingChange} dict on success, or None if the
    ranklist contains no VIRTUAL participation rows *for our VC
    handles* — under CF's May 2026 restriction on contest.standings,
    ordinary callers only see CONTESTANT rows, so we cannot compute
    deltas. The caller is expected to finish the VC and notify users
    without touching VC ratings in that case (silently treating every
    None delta as 'did not participate' would wipe everyone's history).

    The check is specific to our handles: even if CF's API ever starts
    surfacing VIRTUAL rows for strangers (e.g. due to a partial
    rollback), that alone shouldn't unlock the rating loop — it has to
    include the actual VC participants.
    """
    handle_set = set(handles)
    has_our_virtual = any(
        row.party.participantType == 'VIRTUAL'
        and row.party.members
        and row.party.members[0].handle in handle_set
        for row in ranklist.standings)
    if not has_our_virtual:
        return None
    rating_change_by_handle = {}
    for handle, member_id in zip(handles, member_ids):
        delta = ranklist.delta_by_handle.get(handle)
        if delta is None:
            db.remove_last_ratedvc_participation(member_id)
            continue
        old_rating = db.get_vc_rating(member_id)
        new_rating = old_rating + delta
        rating_change_by_handle[handle] = _VcRatingChange(
            handle=handle, oldRating=old_rating, newRating=new_rating)
        db.update_vc_rating(vc_id, member_id, new_rating)
    return rating_change_by_handle


def _contest_start_time_format(contest, tz):
    start = dt.datetime.fromtimestamp(contest.startTimeSeconds, tz)
    return f'{start.strftime("%d %b %y, %H:%M")} {tz}'


def _contest_duration_format(contest):
    duration_days, duration_hrs, duration_mins, _ = cf_common.time_format(contest.durationSeconds)
    duration = f'{duration_hrs}h {duration_mins}m'
    if duration_days > 0:
        duration = f'{duration_days}d ' + duration
    return duration


def _get_formatted_contest_desc(id_str, start, duration, url, max_duration_len):
    em = '\N{EN SPACE}'
    sq = '\N{WHITE SQUARE WITH UPPER RIGHT QUADRANT}'
    desc = (f'`{em}{id_str}{em}|'
            f'{em}{start}{em}|'
            f'{em}{duration.rjust(max_duration_len, em)}{em}|'
            f'{em}`[`link {sq}`]({url} "Link to contest page")')
    return desc


def _get_embed_fields_from_contests(contests):
    infos = [(contest.name, str(contest.id), _contest_start_time_format(contest, dt.timezone.utc),
              _contest_duration_format(contest), contest.register_url)
             for contest in contests]

    max_duration_len = max(len(duration) for _, _, _, duration, _ in infos)

    fields = []
    for name, id_str, start, duration, url in infos:
        value = _get_formatted_contest_desc(id_str, start, duration, url, max_duration_len)
        fields.append((name, value))
    return fields


async def _send_reminder_at(channel, role, contests, before_secs, send_time):
    delay = send_time - time.time()
    if delay <= 0:
        return
    await asyncio.sleep(delay)
    values = cf_common.time_format(before_secs)

    def make(value, label):
        tmp = f'{value} {label}'
        return tmp if value == 1 else tmp + 's'

    labels = 'day hr min sec'.split()
    before_str = ' '.join(make(value, label) for label, value in zip(labels, values) if value > 0)
    desc = f'About to start in {before_str}'
    embed = discord_common.cf_color_embed(description=desc)
    for name, value in _get_embed_fields_from_contests(contests):
        embed.add_field(name=name, value=value)
    await channel.send(role.mention, embed=embed)


def _get_ongoing_vc_participants():
    """ Returns a set containing the `member_id`s of users who are registered in an ongoing vc.
    """
    ongoing_vc_ids = cf_common.user_db.get_ongoing_rated_vc_ids()
    ongoing_vc_participants = set()
    for vc_id in ongoing_vc_ids:
        vc_participants = set(cf_common.user_db.get_rated_vc_user_ids(vc_id))
        ongoing_vc_participants |= vc_participants
    return ongoing_vc_participants

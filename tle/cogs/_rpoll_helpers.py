"""Pure formula, scoring, and embed/format helpers for the rpoll cog.

Split out of ``rpoll.py`` so the cog file stays small. The ``Rpoll`` cog and its
button view import everything they need from here, and ``rpoll.py`` re-exports
the public names that the test-suite imports from ``tle.cogs.rpoll``.
"""
import datetime
import re
from collections import Counter

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

# Number emojis for options 0-4
_NUMBER_EMOJIS = ['1\N{COMBINING ENCLOSING KEYCAP}',
                  '2\N{COMBINING ENCLOSING KEYCAP}',
                  '3\N{COMBINING ENCLOSING KEYCAP}',
                  '4\N{COMBINING ENCLOSING KEYCAP}',
                  '5\N{COMBINING ENCLOSING KEYCAP}']

MAX_OPTIONS = 5
_DEFAULT_DURATION = 86400  # 24 hours in seconds
_SAFETY_NET_INTERVAL = 300  # Safety-net sweep every 5 minutes
_DURATION_RE = re.compile(r'^\+(\d+)([mhd])$')
_VALID_FORMULAS = ('sum', 'exp', 'team', 'osu', 'gg', 'mgg', 'fffff',
                   'akari', 'akariexp')
_FORMULA_LABELS = {
    'sum': 'sum of ratings',
    'exp': 'exponential: `2^(rating/400) * 100`',
    'team': 'team Elo: solo rating with 50% win vs all voters',
    'osu': 'osu-style: top vote full, then `0.67x`, `0.67^2x`, ...',
    'gg': 'gitgud: all-time gg score',
    'mgg': 'monthly gitgud: score for poll creation month',
    'fffff': 'scaled linear: `max(0, 1 + (rating - 1900) / 1600) * 100`',
    'akari': 'sum of Daily Akari ratings',
    'akariexp': 'exponential of Daily Akari rating: `2^(rating/400) * 100`',
}

_GITGUD_SCORE_DISTRIB = (1, 2, 3, 5, 8, 12, 17, 23)
_GITGUD_SCORE_DISTRIB_MIN = -400
_GITGUD_SCORE_DISTRIB_MAX = 300
_ONE_WEEK_DURATION = 7 * 24 * 60 * 60
_GITGUD_MORE_POINTS_START_TIME = 1680300000


class RpollError(commands.CommandError):
    pass


def _parse_duration(token):
    """Parse a duration token like +1h, +30m, +2d. Returns seconds."""
    m = _DURATION_RE.match(token)
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2)
    if unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400
    return None


def _apply_formula(formula, ratings):
    """Apply a scoring formula to a list of individual ratings. Returns total score.

    ``akari`` / ``akariexp`` share their composition with ``sum`` / ``exp``;
    only the rating source (the Daily Akari snapshot vs CF) differs, and that
    happens in :func:`_get_vote_weight` before the rating reaches this function.
    """
    if formula in ('exp', 'akariexp'):
        return round(sum(2 ** (r / 400) * 100 for r in ratings))
    if formula == 'team':
        if not ratings:
            return 0
        return _compose_team_rating(ratings)
    if formula == 'osu':
        return _compose_osu_score(ratings)
    if formula == 'fffff':
        return round(sum(max(0, 1 + (r - 1900) / 1600) for r in ratings) * 100)
    return sum(ratings)


def _get_elo_win_probability(player_rating, opponent_rating):
    """Match the team-rating win probability used by ;teamrate."""
    return 1.0 / (1 + 10 ** ((opponent_rating - player_rating) / 400.0))


def _compose_team_rating(ratings):
    """Match the team-rating composition used by ;teamrate."""
    left = -100.0
    right = 10000.0
    rating_counts = Counter(ratings)
    for _ in range(20):
        candidate_rating = (left + right) / 2.0
        win_probability = 1.0
        for rating, count in rating_counts.items():
            win_probability *= _get_elo_win_probability(candidate_rating, rating) ** count
        if win_probability < 0.5:
            left = candidate_rating
        else:
            right = candidate_rating
    return round((left + right) / 2)


def _compose_osu_score(ratings, decay=0.67):
    """Weight sorted ratings by a fixed decay, like osu pp weighting."""
    sorted_ratings = sorted(ratings, reverse=True)
    total = sum(rating * (decay ** index) for index, rating in enumerate(sorted_ratings))
    return round(total)


def _calculate_gitgud_score_for_delta(delta):
    """Match the gg/mgg point distribution."""
    if delta <= _GITGUD_SCORE_DISTRIB_MIN:
        return _GITGUD_SCORE_DISTRIB[0]
    if delta >= _GITGUD_SCORE_DISTRIB_MAX:
        return _GITGUD_SCORE_DISTRIB[-1]
    index = (delta - _GITGUD_SCORE_DISTRIB_MIN) // 100
    return _GITGUD_SCORE_DISTRIB[index]


def _get_monthly_gitgud_score(user_id, created_at):
    """Match ;mgg semantics for the month the poll was created in."""
    created_dt = datetime.datetime.fromtimestamp(created_at)
    start_time, end_time = cf_common.get_start_and_end_of_month(created_dt)
    more_points_time = end_time - _ONE_WEEK_DURATION
    more_points_active = start_time >= _GITGUD_MORE_POINTS_START_TIME
    entries = cf_common.user_db.get_gudgitters_timerange_for_user(user_id, start_time, end_time)

    score = 0
    for rating_delta, issue_time in entries:
        entry_score = _calculate_gitgud_score_for_delta(int(rating_delta))
        if more_points_active and int(issue_time) >= more_points_time:
            score += 2 * entry_score
        else:
            score += entry_score
    return score


def _get_vote_weight(poll, user_id, guild_id):
    """Get the numeric vote weight for the current poll formula."""
    if poll.formula == 'gg':
        return cf_common.user_db.get_gudgitter_score(user_id)
    if poll.formula == 'mgg':
        return _get_monthly_gitgud_score(user_id, poll.created_at)
    if poll.formula in ('akari', 'akariexp'):
        # Respect the user's privacy choice: opted-out users (and banned ones,
        # whose rating may not reflect current play anyway) contribute a
        # zero-weight vote, same as anyone without a snapshot row.  The poll
        # totals must not surface a rating the user has explicitly hidden.
        if cf_common.user_db.is_akari_opted_out(guild_id, user_id):
            return 0
        if cf_common.user_db.is_akari_banned(guild_id, user_id):
            return 0
        row = cf_common.user_db.get_akari_rating(guild_id, user_id)
        if row is None:
            return 0
        return int(round(row.rating))
    return cf_common.user_db.get_rpoll_user_rating(user_id, guild_id)


def _refresh_poll_ratings(poll, guild_id):
    """Recompute every existing voter's weight and persist it.

    Handles can be relinked and CF/gitgud scores change over time; without a
    refresh the stored rating drifts from the current truth and totals go
    stale. Called on every vote toggle so the embed always reflects current
    ratings.
    """
    for row in cf_common.user_db.get_rpoll_voter_ids(poll.poll_id):
        user_id = int(row.user_id)
        fresh_rating = _get_vote_weight(poll, user_id, guild_id)
        cf_common.user_db.update_rpoll_voter_rating(poll.poll_id, user_id, fresh_rating)


def _compute_totals_map(poll_id, formula):
    """Compute per-option totals using the given scoring formula."""
    if formula in {'exp', 'team', 'osu', 'fffff', 'akariexp'}:
        votes = cf_common.user_db.get_rpoll_vote_ratings(poll_id)
        totals = {}
        for vote in votes:
            totals.setdefault(vote.option_index, []).append(vote.rating)
        return {
            option_index: _apply_formula(formula, option_ratings)
            for option_index, option_ratings in totals.items()
        }
    totals = cf_common.user_db.get_rpoll_totals(poll_id)
    return {row.option_index: row.total_rating for row in totals}


def _build_poll_embed(question, options, totals_map, vote_count, voters_map=None,
                      expires_at=None, closed=False, formula='exp', color=None):
    """Build the embed for a rating poll.

    Args:
        question: The poll question.
        options: List of (option_index, label) tuples.
        totals_map: Dict of option_index -> total_rating.
        vote_count: Total number of distinct voters.
        voters_map: Optional dict of option_index -> list of user_ids.
        expires_at: Optional UNIX timestamp when poll expires.
        closed: Whether the poll has ended.
    """
    grand_total = sum(totals_map.get(idx, 0) for idx, _ in options)
    show_pct = grand_total > 0

    lines = []
    for idx, label in options:
        total = totals_map.get(idx, 0)
        emoji = _NUMBER_EMOJIS[idx] if idx < len(_NUMBER_EMOJIS) else f'{idx + 1}.'
        if show_pct:
            pct = round(total / grand_total * 100)
            lines.append(f'{emoji} {label} — **{total}** ({pct}%)')
        else:
            lines.append(f'{emoji} {label} — **{total}**')

    # Winner / tied line
    if grand_total > 0:
        max_total = max(totals_map.get(idx, 0) for idx, _ in options)
        leaders = [label for idx, label in options if totals_map.get(idx, 0) == max_total]
        if len(leaders) == 1:
            second = sorted((totals_map.get(idx, 0) for idx, _ in options), reverse=True)
            lead = max_total - (second[1] if len(second) > 1 else 0)
            lines.append(f'\nLeader: **{leaders[0]}** (+{lead})')
        else:
            lines.append(f'\nTied: {", ".join(f"**{l}**" for l in leaders)}')

    # Voter breakdown per option
    if voters_map:
        lines.append('')
        for idx, label in options:
            user_ids = voters_map.get(idx, [])
            if user_ids:
                mentions = ', '.join(f'<@{uid}>' for uid in user_ids)
                lines.append(f'{label}: {mentions}')

    # Formula description
    formula_label = _FORMULA_LABELS.get(formula, formula)
    lines.append(f'\nScoring: {formula_label}')

    # Expiry info in description
    if closed:
        lines.append('**Poll has ended.**')
    elif expires_at:
        lines.append(f'Ends <t:{int(expires_at)}:R>')

    embed = discord.Embed(
        title=question,
        description='\n'.join(lines),
        color=color if color is not None else discord_common.random_cf_color(),
    )
    embed.set_footer(text=f'{vote_count} vote{"s" if vote_count != 1 else ""}')
    return embed


def _build_results_embed(question, options, totals_map, vote_count, formula='exp'):
    """Build a compact embed for the poll expiry reply."""
    grand_total = sum(totals_map.get(idx, 0) for idx, _ in options)
    parts = []
    for idx, label in options:
        total = totals_map.get(idx, 0)
        if grand_total > 0:
            pct = round(total / grand_total * 100)
            parts.append(f'**{label}** {pct}%')
        else:
            parts.append(f'**{label}** 0')
    votes_str = f'{vote_count} vote{"s" if vote_count != 1 else ""}'
    formula_label = _FORMULA_LABELS.get(formula, formula)

    lines = [f'**{question}**']
    if grand_total > 0:
        max_total = max(totals_map.get(idx, 0) for idx, _ in options)
        winners = [label for idx, label in options if totals_map.get(idx, 0) == max_total]
        if len(winners) == 1:
            lines.append(f'Winner: **{winners[0]}**')
        else:
            lines.append(f'Tied: {", ".join(f"**{w}**" for w in winners)}')
    lines.append(f'{" / ".join(parts)} ({votes_str})')
    lines.append(f'Scoring: {formula_label}')
    return discord.Embed(
        description='\n'.join(lines),
        color=discord_common.random_cf_color(),
    )


def _build_disabled_view(poll_id, option_count):
    """Build a view with all buttons disabled."""
    view = discord.ui.View(timeout=None)
    for i in range(option_count):
        emoji = _NUMBER_EMOJIS[i] if i < len(_NUMBER_EMOJIS) else None
        btn = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            emoji=emoji,
            custom_id=f'rpoll:{poll_id}:{i}',
            disabled=True,
        )
        view.add_item(btn)
    return view

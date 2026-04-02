import asyncio
import datetime
import logging
import re
import time
from collections import Counter

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks

logger = logging.getLogger(__name__)

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
_VALID_FORMULAS = ('sum', 'exp', 'team', 'osu', 'gg', 'mgg', 'fffff')
_FORMULA_LABELS = {
    'sum': 'sum of ratings',
    'exp': 'exponential: `2^(rating/400) * 100`',
    'team': 'team Elo: solo rating with 50% win vs all voters',
    'osu': 'osu-style: top vote full, then `0.67x`, `0.67^2x`, ...',
    'gg': 'gitgud: all-time gg score',
    'mgg': 'monthly gitgud: score for poll creation month',
    'fffff': 'scaled linear: `max(0, 1 + (rating - 1900) / 1600) * 100`',
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
    """Apply a scoring formula to a list of individual ratings. Returns total score."""
    if formula == 'exp':
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
    return cf_common.user_db.get_rpoll_user_rating(user_id, guild_id)


def _compute_totals_map(poll_id, formula):
    """Compute per-option totals using the given scoring formula."""
    if formula in {'exp', 'team', 'osu', 'fffff'}:
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
    return discord.Embed(
        description=f'**{question}**\n{" / ".join(parts)} ({votes_str})\nScoring: {formula_label}',
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


class RpollView(discord.ui.View):
    """Persistent view with buttons for each poll option."""

    def __init__(self, poll_id, option_count):
        super().__init__(timeout=None)
        for i in range(option_count):
            self.add_item(RpollButton(poll_id, i))


class RpollButton(discord.ui.Button):
    """A single poll option button."""

    def __init__(self, poll_id, option_index):
        emoji = _NUMBER_EMOJIS[option_index] if option_index < len(_NUMBER_EMOJIS) else None
        super().__init__(
            style=discord.ButtonStyle.secondary,
            emoji=emoji,
            custom_id=f'rpoll:{poll_id}:{option_index}',
        )
        self.poll_id = poll_id
        self.option_index = option_index

    async def callback(self, interaction: discord.Interaction):
        if cf_common.user_db is None:
            await interaction.response.send_message('Bot is still starting up.', ephemeral=True)
            return

        # Check if poll is closed or expired before allowing vote
        poll = cf_common.user_db.get_rpoll(self.poll_id)
        if poll is None:
            await interaction.response.send_message('Poll not found.', ephemeral=True)
            return
        if poll.closed or poll.expires_at <= time.time():
            await interaction.response.send_message('This poll has ended.', ephemeral=True)
            return

        user_id = interaction.user.id
        guild_id = interaction.guild_id

        rating = _get_vote_weight(poll, user_id, guild_id)
        added = cf_common.user_db.toggle_rpoll_vote(
            self.poll_id, user_id, self.option_index, rating
        )

        options = cf_common.user_db.get_rpoll_options(self.poll_id)
        totals_map = _compute_totals_map(self.poll_id, poll.formula)
        vote_count = cf_common.user_db.get_rpoll_vote_count(self.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(self.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        # Preserve the original embed color
        existing_color = None
        if interaction.message and interaction.message.embeds:
            existing_color = interaction.message.embeds[0].color

        embed = _build_poll_embed(
            poll.question,
            [(opt.option_index, opt.label) for opt in options],
            totals_map,
            vote_count,
            voters_map,
            expires_at=poll.expires_at,
            formula=poll.formula,
            color=existing_color,
        )

        action = 'voted for' if added else 'removed vote from'
        option_label = next((opt.label for opt in options if opt.option_index == self.option_index), '?')
        await interaction.response.edit_message(embed=embed)
        logger.info(f'rpoll: user={user_id} {action} option {self.option_index} '
                    f'({option_label}) on poll={self.poll_id} rating={rating}')


class Rpoll(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._scheduled_timers = {}  # poll_id -> asyncio.Task
        logger.info('Rpoll cog initialized')

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        """Re-register persistent views once the DB is available."""
        # user_db is initialized in the bot's on_ready handler, which may run
        # after cog listeners.  Wait briefly for it to become available.
        for _ in range(30):
            if cf_common.user_db is not None:
                break
            await asyncio.sleep(1)
        if cf_common.user_db is None:
            logger.warning('rpoll: user_db still None after waiting, skipping view registration')
            return
        self._register_persistent_views()
        self._schedule_all_active_polls()
        self._safety_net_task.start()

    def _register_persistent_views(self):
        """Register persistent views for all active polls so buttons work after restart."""
        try:
            polls = cf_common.user_db.get_all_active_rpolls()
            for poll in polls:
                options = cf_common.user_db.get_rpoll_options(poll.poll_id)
                view = RpollView(poll.poll_id, len(options))
                self.bot.add_view(view, message_id=int(poll.message_id))
            if polls:
                logger.info(f'rpoll: Re-registered {len(polls)} persistent poll views')
        except Exception as e:
            logger.error(f'rpoll: Failed to re-register poll views: {e}', exc_info=True)

    def _schedule_all_active_polls(self):
        """On startup, schedule a timer for every open poll."""
        try:
            polls = cf_common.user_db.get_all_active_rpolls()
            for poll in polls:
                self._schedule_expiry(poll.poll_id, poll.expires_at)
            if polls:
                logger.info(f'rpoll: Scheduled expiry timers for {len(polls)} active polls')
        except Exception as e:
            logger.error(f'rpoll: Failed to schedule poll timers: {e}', exc_info=True)

    def _schedule_expiry(self, poll_id, expires_at):
        """Schedule an asyncio task that sleeps until expires_at, then closes the poll."""
        # Cancel existing timer for this poll if any
        old = self._scheduled_timers.pop(poll_id, None)
        if old and not old.done():
            old.cancel()

        delay = max(0, expires_at - time.time())
        task = asyncio.create_task(self._expiry_timer(poll_id, delay))
        self._scheduled_timers[poll_id] = task

    async def _expiry_timer(self, poll_id, delay):
        """Sleep then close a specific poll."""
        try:
            await asyncio.sleep(delay)
            if cf_common.user_db is None:
                return
            poll = cf_common.user_db.get_rpoll(poll_id)
            if poll is None or poll.closed:
                return
            await self._close_poll(poll)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'rpoll: Timer failed for poll {poll_id}: {e}', exc_info=True)
        finally:
            self._scheduled_timers.pop(poll_id, None)

    @tasks.task_spec(name='RpollSafetyNet',
                     waiter=tasks.Waiter.fixed_delay(_SAFETY_NET_INTERVAL))
    async def _safety_net_task(self, _):
        """Safety-net sweep for polls that slipped through (e.g. bot restart race)."""
        if cf_common.user_db is None:
            return
        try:
            expired = cf_common.user_db.get_expired_unclosed_rpolls()
        except Exception as e:
            logger.error(f'rpoll safety net: Failed to query expired polls: {e}', exc_info=True)
            return

        for poll in expired:
            try:
                await self._close_poll(poll)
            except Exception as e:
                logger.error(f'rpoll safety net: Failed to close poll {poll.poll_id}: {e}',
                             exc_info=True)

    async def _close_poll(self, poll):
        """Close an expired poll: mark in DB, edit message, send results."""
        cf_common.user_db.close_rpoll(poll.poll_id)
        logger.info(f'rpoll: Closed expired poll {poll.poll_id}')

        options = cf_common.user_db.get_rpoll_options(poll.poll_id)
        totals_map = _compute_totals_map(poll.poll_id, poll.formula)
        vote_count = cf_common.user_db.get_rpoll_vote_count(poll.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(poll.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        option_pairs = [(opt.option_index, opt.label) for opt in options]
        disabled_view = _build_disabled_view(poll.poll_id, len(options))

        channel = self.bot.get_channel(int(poll.channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(poll.channel_id))
            except Exception:
                logger.warning(f'rpoll: Could not fetch channel {poll.channel_id} for poll {poll.poll_id}')
                return

        # Preserve the original embed color when closing
        existing_color = None
        try:
            msg = await channel.fetch_message(int(poll.message_id))
            if msg.embeds:
                existing_color = msg.embeds[0].color
        except Exception:
            msg = None

        closed_embed = _build_poll_embed(
            poll.question, option_pairs, totals_map, vote_count,
            voters_map, expires_at=poll.expires_at, closed=True,
            formula=poll.formula, color=existing_color,
        )

        # Edit original message to disable buttons and show "ended"
        try:
            if msg is None:
                msg = await channel.fetch_message(int(poll.message_id))
            await msg.edit(embed=closed_embed, view=disabled_view)
        except Exception as e:
            logger.warning(f'rpoll: Could not edit message for poll {poll.poll_id}: {e}')

        # Reply to original message with a compact embed so mentions stay inert.
        try:
            results_embed = _build_results_embed(
                poll.question, option_pairs, totals_map, vote_count,
                formula=poll.formula,
            )
            ref = discord.MessageReference(
                message_id=int(poll.message_id), channel_id=int(poll.channel_id),
                fail_if_not_exists=False,
            )
            await channel.send('Poll done!', embed=results_embed, reference=ref)
        except Exception as e:
            logger.warning(f'rpoll: Could not send results for poll {poll.poll_id}: {e}')

    @commands.command(brief='Create a rating-weighted poll')
    async def rpoll(self, ctx, *, args: str):
        """Create a poll where votes are weighted by Codeforces rating.
        Vote for multiple options, click again to un-vote. No CF handle = 0.

        Example: ;rpoll +anon +2h "Best approach?" BFS,DFS,Dijkstra

        Flags: +anon (hide voters), +Nm/+Nh/+Nd (duration, default 24h).
        Scoring (default +exp):
          +exp: exponential `2^(rating/400) * 100`
          +sum: sum of ratings
          +team: team Elo (solo rating with 50% win vs all)
          +osu: top vote full, then 0.67x decay
          +gg / +mgg: all-time / monthly gitgud score
        """
        args = args.strip()
        # Normalize smart/curly quotes (common on macOS) to straight quotes
        args = args.replace('\u201c', '"').replace('\u201d', '"')
        args = args.replace('\u2018', "'").replace('\u2019', "'")
        anonymous = False
        duration = _DEFAULT_DURATION
        formula = 'exp'

        # Parse flags: +anon, +duration, +formula (in any order, before the question)
        while args.startswith('+'):
            token = args.split(None, 1)[0]
            if token == '+anon':
                anonymous = True
                args = args[len(token):].lstrip()
            elif token.lstrip('+') in _VALID_FORMULAS:
                formula = token.lstrip('+')
                args = args[len(token):].lstrip()
            else:
                parsed = _parse_duration(token)
                if parsed is not None:
                    duration = parsed
                    args = args[len(token):].lstrip()
                else:
                    break  # Not a flag, stop parsing

        # Extract quoted question, then comma-separated options
        if args.startswith('"'):
            end = args.find('"', 1)
            if end == -1:
                raise RpollError('Missing closing quote for question.')
            question = args[1:end]
            options_str = args[end + 1:].lstrip()
        else:
            # No quotes — first word is the question (legacy support)
            parts = args.split(None, 1)
            if len(parts) < 2:
                raise RpollError('Usage: ;rpoll "Question" Option1,Option2')
            question, options_str = parts

        options = [opt.strip() for opt in options_str.split(',')]
        options = [opt for opt in options if opt]  # Remove empty

        if len(options) < 2:
            raise RpollError('Need at least 2 options (comma-separated).')
        if len(options) > MAX_OPTIONS:
            raise RpollError(f'Maximum {MAX_OPTIONS} options allowed.')

        now = time.time()
        expires_at = now + duration

        poll_id = cf_common.user_db.create_rpoll(
            ctx.guild.id, ctx.channel.id, question, options,
            ctx.author.id, now, anonymous=anonymous, expires_at=expires_at,
            formula=formula,
        )

        embed = _build_poll_embed(
            question,
            list(enumerate(options)),
            {},
            0,
            expires_at=expires_at,
            formula=formula,
        )
        view = RpollView(poll_id, len(options))
        msg = await ctx.send(embed=embed, view=view)

        cf_common.user_db.set_rpoll_message_id(poll_id, msg.id)
        self._schedule_expiry(poll_id, expires_at)
        logger.info(f'rpoll: Created poll={poll_id} question={question!r} '
                    f'options={options} duration={duration}s by user={ctx.author.id} msg={msg.id}')

    @discord_common.send_error_if(RpollError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Rpoll(bot))

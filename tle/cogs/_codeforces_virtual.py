"""``;virtual`` — a blind random contest whose solves earn gitgud points.

The bot cannot start a Codeforces virtual for anyone; only the user can, on
the site. What it can do is choose the contest without showing it, hold the
user to that choice once they confirm, and afterwards verify from
``user.status`` that the solves were made in a virtual of that contest which
started after the confirmation: ``Party.startTimeSeconds`` is the virtual's
own start and ``participantType`` says ``VIRTUAL``.

Credited solves become completed gitgud challenges, so ``;gitlog``,
``;gitgudders`` and the monthly board need no special casing.
"""
import random
import time

import discord

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.cogs._codeforces_helpers import (
    CodeforcesCogError,
    _calculateGitgudScoreForDelta,
)

_OFFER_TIMEOUT = 10 * 60   # confirm within ten minutes or the offer lapses
_START_GRACE = 10 * 60     # start the virtual within ten minutes of the reveal
_CLOCK_SLACK = 60          # tolerance between our clock and Codeforces'
_DURATION_BUCKET = 30 * 60  # the offer rounds the length so it identifies less
_DIV1_MARKERS = ('div1', 'global', 'avito', 'goodbye', 'hello')
_DIVISION_LABEL = {'div3': 'Div. 3', 'div2': 'Div. 2', 'div1': 'Div. 1'}


# --- pure helpers ---------------------------------------------------------

def division_markers(rating):
    """Same split ;vc uses, for one handle instead of an average."""
    if rating < 1600:
        return ['div3']
    if rating < 2100:
        return ['div2']
    return list(_DIV1_MARKERS)


def gitgud_base_rating(effective_rating):
    """The rating gitgud measures deltas against."""
    return max(1100, min(3000, round(effective_rating, -2)))


def eligible_contests(contests, markers, excluded, handle):
    """Finished, standard contests for the division the user never touched."""
    return [contest for contest in contests
            if contest.matches(markers)
            and not cf_common.is_nonstandard_contest(contest)
            and not cf_common.is_contest_writer(contest.id, handle)
            and contest.id not in excluded]


def expiry(confirmed_at, duration_seconds):
    """When the session stops accepting solves: start grace plus the contest."""
    return confirmed_at + _START_GRACE + duration_seconds


def start_deadline(confirmed_at):
    """Latest moment the virtual may start.

    The reveal is the moment the problems become readable, so the window
    between reveal and start is exactly the head start a user can give
    themselves. It is kept short for that reason.
    """
    return confirmed_at + _START_GRACE


def virtual_solves(submissions, session, duration_seconds, credited=()):
    """Problems solved inside a virtual of the session's contest, by index.

    A solve counts only if it was made as a VIRTUAL participant, alone, in a
    virtual that started inside the reveal-to-start window, and within the
    contest's running time — which together bound the wall clock to
    ``expires_at`` plus slack. Problems already credited are skipped so a
    repeated claim cannot double-pay.
    """
    solved = {}
    for sub in submissions:
        if sub.contestId != session.contest_id or sub.verdict != 'OK':
            continue
        party = sub.author
        if party.participantType != 'VIRTUAL' or len(party.members) != 1:
            continue
        started = party.startTimeSeconds
        if started is None or started < session.confirmed_at - _CLOCK_SLACK:
            continue
        if started > start_deadline(session.confirmed_at) + _CLOCK_SLACK:
            continue
        elapsed = sub.relativeTimeSeconds
        if elapsed is None or elapsed > duration_seconds:
            continue
        index = sub.problem.index
        if index in credited or index in solved:
            continue
        solved[index] = sub.problem
    return [solved[index] for index in sorted(solved)]


def session_is_over(session, now):
    """Past the last moment a legal solve could land, slack included."""
    return now > session.expires_at + _CLOCK_SLACK


def _hours(seconds):
    hours, minutes = divmod(int(seconds) // 60, 60)
    return f'{hours}h{minutes:02d}' if hours else f'{minutes} min'


def _rough_hours(seconds):
    """Length rounded to the bucket, so the offer narrows the pool less."""
    rounded = max(_DURATION_BUCKET,
                  round(seconds / _DURATION_BUCKET) * _DURATION_BUCKET)
    return 'about ' + _hours(rounded)


# --- confirmation view ----------------------------------------------------

class VirtualOfferView(discord.ui.View):
    """Confirm-or-cancel for a contest the user has not been shown."""

    def __init__(self, cog, ctx, handle, contest, markers, timeout=_OFFER_TIMEOUT):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.handle = handle
        self.contest = contest
        self.markers = markers
        self.message = None
        self.decided = False
        self.add_item(_OfferButton(self, 'Confirm', discord.ButtonStyle.success,
                                   confirm=True))
        self.add_item(_OfferButton(self, 'Cancel', discord.ButtonStyle.secondary,
                                   confirm=False))

    async def interaction_check(self, interaction):
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                'This offer is for someone else.', ephemeral=True)
            return False
        return True

    async def decide(self, interaction, confirm):
        if self.decided:
            return
        self.decided = True
        for item in self.children:
            item.disabled = True
        if confirm:
            await self.cog._confirm_virtual(interaction, self)
        else:
            await interaction.response.edit_message(
                embed=discord.Embed(title='Virtual cancelled',
                                    description='Nothing was revealed. Run '
                                                '`;virtual` for a fresh pick.'),
                view=self)
        self.stop()

    async def on_timeout(self):
        if self.decided:
            return
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class _OfferButton(discord.ui.Button):
    def __init__(self, offer, label, style, *, confirm):
        super().__init__(label=label, style=style)
        self.offer = offer
        self.confirm = confirm

    async def callback(self, interaction):
        await self.offer.decide(interaction, self.confirm)


# --- cog mixin ------------------------------------------------------------

class CodeforcesVirtualMixin:
    async def _virtual_handle(self, ctx):
        handle, = await cf_common.resolve_handles(
            ctx, self.converter, ('!' + str(ctx.author.id),))
        return handle

    def _offer_embed(self, markers, contest):
        division = next((_DIVISION_LABEL[m] for m in markers
                         if m in _DIVISION_LABEL), 'Div. 1-level')
        embed = discord.Embed(
            title='Virtual contest — confirm blind',
            description=(
                f'A random **{division}** round you have never touched, '
                f'**{_rough_hours(contest.durationSeconds)}** long. The '
                f'contest stays hidden until you confirm.\n\n'
                f'After confirming you have **{_START_GRACE // 60} minutes** to '
                f'start the virtual on Codeforces; every problem you solve while '
                f'it runs earns gitgud points. There is no cancelling once '
                f'revealed — an untouched virtual just expires.'),
            color=0x3498DB)
        embed.set_footer(text=f'Offer expires in {_OFFER_TIMEOUT // 60} minutes.')
        return embed

    def _reveal_embed(self, session_contest, confirmed_at, expires_at, points=0):
        start_by = int(start_deadline(confirmed_at))
        embed = discord.Embed(
            title=session_contest.name, url=session_contest.url,
            description=(
                f'[Start the virtual]({cf.CONTEST_BASE_URL}{session_contest.id}/virtual) '
                f'by <t:{start_by}:t> (<t:{start_by}:R>). Solves count until '
                f'<t:{int(expires_at)}:t> (<t:{int(expires_at)}:R>).\n'
                f'Run `;virtual claim` to credit what you have solved — any '
                f'time, and again when you finish.'),
            color=0x2ECC71)
        embed.add_field(name='Length', value=_hours(session_contest.durationSeconds))
        embed.add_field(name='Points so far', value=str(points))
        return embed

    def _pending_offers(self):
        offers = getattr(self, '_virtual_offers', None)
        if offers is None:
            offers = self._virtual_offers = {}
        return offers

    def _summary_lines(self, names, unrated):
        lines = list(names)
        if unrated:
            lines.append(f'Skipped unrated problem(s): {", ".join(unrated)}.')
        return lines

    async def _finalize_expired(self, ctx, session):
        """Credit late solves and close an expired session after a valid fetch.

        The session's handle may no longer resolve on Codeforces (renamed or
        removed). That must not leave the session active forever — nothing
        else can close it and the one-active rule would lock the user out of
        ``;virtual`` for good — so a missing handle closes it with no credit
        and says so. Transient API failures leave the session claimable.
        """
        try:
            points, names, unrated = await self._credit_virtual(ctx, session)
        except cf.HandleNotFoundError as error:
            cf_common.user_db.finish_virtual_session(session.id)
            return (f'Your previous virtual ({session.contest_name}) is over, '
                    f'but Codeforces would not return submissions for '
                    f'`{session.handle}` ({error}); it was closed without credit.')
        cf_common.user_db.finish_virtual_session(session.id)
        lines = self._summary_lines(names, unrated)
        return (f'Your previous virtual ({session.contest_name}) is over: '
                f'{len(names)} problem(s) credited for {points} points.'
                + ('\n' + '\n'.join(lines) if lines else ''))

    async def _virtual_impl(self, ctx):
        session = cf_common.user_db.get_active_virtual_session(
            ctx.guild.id, ctx.author.id)
        now = time.time()
        if session is not None:
            if not session_is_over(session, now):
                contest = cf_common.cache2.contest_cache.get_contest(session.contest_id)
                await ctx.send(
                    f'You already have a virtual running for `{session.handle}`.',
                    embed=self._reveal_embed(contest, session.confirmed_at,
                                             session.expires_at, session.points))
                return
            # Expired: credit any late solves before offering a new one, so
            # forgetting to claim never loses a finished virtual.
            await ctx.send(await self._finalize_expired(ctx, session))

        # One offer at a time: rerolling reveals nothing, but it is not free
        # for the API either, and a pile of live offers is a foot-gun.
        pending = self._pending_offers().get(ctx.author.id)
        if pending is not None and not pending.decided \
                and now < pending.offered_at + _OFFER_TIMEOUT:
            raise CodeforcesCogError(
                'You already have an offer waiting — confirm or cancel it first.')

        handle = await self._virtual_handle(ctx)
        user = cf_common.user_db.fetch_cf_user(handle)
        markers = division_markers(user.effective_rating)
        excluded = await cf_common.get_visited_contests([handle])
        excluded |= cf_common.user_db.virtual_session_contest_ids(
            ctx.guild.id, ctx.author.id)
        contests = cf_common.cache2.contest_cache.get_contests_in_phase('FINISHED')
        candidates = eligible_contests(contests, markers, excluded, handle)
        if not candidates:
            raise CodeforcesCogError(
                f'No untouched {", ".join(markers)} contests left for `{handle}`.')
        contest = random.choice(candidates)

        view = VirtualOfferView(self, ctx, handle, contest, markers)
        view.offered_at = now
        view.base_rating = gitgud_base_rating(user.effective_rating)
        # Registered only once it is actually on screen: a failed send must
        # not leave a phantom offer blocking the user for ten minutes.
        view.message = await ctx.send(embed=self._offer_embed(markers, contest),
                                      view=view)
        self._pending_offers()[ctx.author.id] = view

    async def _confirm_virtual(self, interaction, offer):
        now = time.time()
        contest = offer.contest
        expires_at = expiry(now, contest.durationSeconds)
        session_id = cf_common.user_db.start_virtual_session(
            offer.ctx.guild.id, offer.ctx.author.id, offer.handle, contest.id,
            contest.name, now, expires_at, offer.base_rating)
        if session_id is None:
            await interaction.response.edit_message(
                embed=discord.Embed(title='Already running',
                                    description='You already have an active '
                                                'virtual. Finish that one first.'),
                view=offer)
            return
        await interaction.response.edit_message(
            content=f'Virtual confirmed for `{offer.handle}`.',
            embed=self._reveal_embed(contest, now, expires_at), view=offer)

    async def _credit_virtual(self, ctx, session):
        """Credit new solves for a session. Returns (points, names, unrated).

        Everything is read from the session — the handle it was confirmed
        for and the rating it was confirmed at — so re-identifying to a
        stronger account after the reveal, or a rating change mid-session,
        changes nothing.
        """
        submissions = await cf.user.status(handle=session.handle)
        contest = cf_common.cache2.contest_cache.get_contest(session.contest_id)
        credited = cf_common.user_db.credited_virtual_problems(session.id)
        base = session.base_rating
        now = int(time.time())

        points, names, unrated = 0, [], []
        for problem in virtual_solves(submissions, session,
                                      contest.durationSeconds, credited):
            if problem.rating is None or cf_common.is_nonstandard_problem(problem):
                # Unrated and *special problems are never gitgud material.
                unrated.append(problem.index)
                continue
            delta = problem.rating - base
            score = _calculateGitgudScoreForDelta(delta)
            if cf_common.user_db.credit_virtual_solve(
                    session.id, ctx.author.id, problem, delta, score, now):
                points += score
                names.append(f'{problem.index}. {problem.name} (+{score})')
        return points, names, unrated

    async def _virtual_claim_impl(self, ctx):
        session = cf_common.user_db.get_active_virtual_session(
            ctx.guild.id, ctx.author.id)
        if session is None:
            raise CodeforcesCogError(
                'No virtual to claim. Run `;virtual` to get one.')
        finished = session_is_over(session, time.time())
        if finished:
            await ctx.send(await self._finalize_expired(ctx, session))
            return
        points, names, unrated = await self._credit_virtual(ctx, session)

        lines = self._summary_lines(
            names or ['Nothing new since the last claim.'], unrated)
        handle = session.handle
        total = session.points + points
        title = f'{session.contest_name} — {total} points so far'
        await ctx.send(f'`{handle}` claimed {points} point(s).',
                       embed=discord.Embed(title=title,
                                           description='\n'.join(lines),
                                           color=0x2ECC71))

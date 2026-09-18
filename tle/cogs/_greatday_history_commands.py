"""History, statistics, and backfill commands for the Great Day cog."""
import discord
from discord.ext import commands

from tle import constants
from tle.cogs._greatday_commands import (
    GreatDayCogError,
    GreatDayCommandsMixin,
)
from tle.cogs._greatday_event_channels import discover_guild_history_channels
from tle.cogs._greatday_events import (
    collapse_events,
    merge_history,
    scan_signup_events_audited,
    scan_signup_events_channels_audited,
    signed_up_post_count,
)
from tle.cogs._greatday_helpers import (
    _BACKFILL_STOP_GAP_SECONDS,
    _format_pick_time,
    _parse_greatday_message,
    _personal_rank_line,
    _should_stop_backfill,
)
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import ranking
from tle.util.db.greatday_db import SIGNUP_HISTORY_AUDIT_KEY


_STATS_PER_PAGE = 15
_HISTORY_PER_PAGE = 15
# Edit the backfill progress embed every N scanned messages. Discord rate-
# limits message edits to ~5/5s — 250 is a comfortable cadence even for
# multi-thousand-message channels.
_BACKFILL_PROGRESS_INTERVAL = 250


class GreatDayHistoryCommandsMixin:
    """History and backfill subcommands inherited by ``GreatDay``."""

    @GreatDayCommandsMixin.greatday.command(
        name='latest', aliases=['last'],
        brief='Show the latest time a user was great-day\'d',
        usage='[@user]')
    async def latest(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(target.display_name))
        row = cf_common.user_db.greatday_get_latest_pick(
            ctx.guild.id, target.id)
        description = ('No Great Day picks have been recorded for this user.'
                       if row is None else
                       f'Last selected: {_format_pick_time(row.picked_at)}')
        await ctx.send(embed=discord.Embed(
            title=f'Latest Great Day — {name}',
            description=description,
            color=0x00aaff,
        ))

    @GreatDayCommandsMixin.greatday.command(
        name='history', brief='Show a user\'s Great Day history',
        usage='[@user]')
    async def history(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(target.display_name))
        picks = cf_common.user_db.greatday_get_pick_history(
            ctx.guild.id, target.id)
        events = collapse_events(cf_common.user_db.greatday_get_signup_events(
            ctx.guild.id, target.id))
        entries = merge_history(picks, events)
        title = f'Great Day history — {name}'
        if not entries:
            await ctx.send(embed=discord.Embed(
                title=title,
                description='No Great Day history has been recorded for this user.',
                color=0x00aaff,
            ))
            return

        # Newest first, but numbered from the oldest entry (= 0) so an
        # entry's number never changes as new history is added.
        numbered = list(zip(range(len(entries) - 1, -1, -1), entries))
        pages = []
        for chunk in paginator.chunkify(numbered, _HISTORY_PER_PAGE):
            lines = [f'**{number}.** {entry}' for number, entry in chunk]
            pages.append((None, discord.Embed(
                title=title,
                description='\n'.join(lines),
                color=0x00aaff,
            )))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @GreatDayCommandsMixin.greatday.command(
        name='stats', brief='Show how many times users have been great-day\'d',
        usage='[@user]')
    async def stats(self, ctx, member: discord.Member = None):
        if member is not None:
            await ctx.send(embed=discord_common.embed_neutral(
                self._member_stats(ctx.guild, member)))
            return

        rows = cf_common.user_db.greatday_get_stats(ctx.guild.id)
        if not rows:
            raise GreatDayCogError(
                'No picks recorded yet. Admins can run `;greatday backfill` '
                'to seed history from the channel.')

        personal = (_personal_rank_line(rows, ctx.author.id) + '\n'
                    + self._signup_stat_lines(ctx.guild, ctx.author.id))
        # Rank the whole list once with standard competition ranking so tied
        # counts share a rank (and the tie still numbers correctly across page
        # boundaries), then paginate the (rank, row) pairs.
        ranked = ranking.rank_items(rows, lambda r: r.cnt)
        chunks = paginator.chunkify(ranked, _STATS_PER_PAGE)
        pages = []
        for chunk in chunks:
            lines = []
            for rank, row in chunk:
                m = ctx.guild.get_member(int(row.user_id))
                name = m.mention if m is not None else f'`{row.user_id}`'
                lines.append(f'**#{rank}** {name} — **{row.cnt}**')
            embed = discord.Embed(
                title='Great Day leaderboard',
                description='\n'.join(lines),
                color=0x00aaff,
            )
            pages.append((personal, embed))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    def _member_stats(self, guild, member):
        """Render a single member's pick count, last signup and days signed up."""
        count = cf_common.user_db.greatday_get_count(guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        return (f'`{name}` has been great-day\'d **{count}** time(s).\n'
                + self._signup_stat_lines(guild, member.id))

    def _signup_stat_lines(self, guild, user_id):
        """Render the last-signup and days-signed-up lines for a user."""
        events = cf_common.user_db.greatday_get_signup_events(
            guild.id, user_id)
        signed_up = cf_common.user_db.greatday_is_signed_up(guild.id, user_id)
        last_signup = next(
            (row for row in events if row.action == 'signup'), None)
        days, complete = signed_up_post_count(
            events, cf_common.user_db.greatday_get_post_times(guild.id),
            signed_up)
        audit_status = cf_common.user_db.get_guild_config(
            guild.id, SIGNUP_HISTORY_AUDIT_KEY) or ''
        audited = audit_status.startswith('clean:')
        complete = complete and audited
        lines = []
        if last_signup is None:
            # Signups predating the event log are unknown, not absent.
            lines.append('Last signup: not recorded'
                         + ('' if signed_up else ' (not signed up)'))
        else:
            lines.append(f'Last signup: {_format_pick_time(last_signup.at)}')
        if complete:
            suffix = ' (inferred from audited message history)'
        elif audit_status.startswith('incomplete:'):
            suffix = ' (inferred — backfill audit found warnings)'
        else:
            suffix = ' (at least — history incomplete or not fully audited)'
        lines.append(f'Days signed up: **{days}**{suffix}')
        return '\n'.join(lines)

    @GreatDayCommandsMixin.greatday.command(
        name='backfill',
        brief='Seed pick history from the greatday channel (admin/mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def backfill(self, ctx):
        """Walk the greatday channel's history and insert one pick row per
        matched message and mentioned user. Idempotent — safe to re-run.
        """
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        if not channel_id:
            raise GreatDayCogError(
                'No great day channel set. Use `;greatday here` first.')
        channel = ctx.guild.get_channel(int(channel_id))
        if channel is None:
            raise GreatDayCogError(
                'Configured great day channel is not accessible.')

        progress = await ctx.send(embed=discord_common.embed_neutral(
            f'Backfilling from {channel.mention}… (scanned **0**, matched **0**)'))

        bot_user_id = self.bot.user.id if self.bot and self.bot.user else None
        scanned = 0
        matched = 0
        inserted = 0
        last_match_ts = None
        stopped_early = False
        # Newest first — leaderboard updates with recent picks immediately,
        # and if the admin aborts (bot restart) the most relevant history
        # is already saved.
        async for msg in channel.history(limit=None, oldest_first=False):
            scanned += 1
            msg_ts = msg.created_at.timestamp()
            uids = _parse_greatday_message(msg, bot_user_id)
            if uids is not None:
                matched += 1
                inserted += cf_common.user_db.greatday_record_picks(
                    ctx.guild.id, uids, msg.id, msg_ts)
                last_match_ts = msg_ts
            elif _should_stop_backfill(last_match_ts, msg_ts,
                                        _BACKFILL_STOP_GAP_SECONDS):
                stopped_early = True
                break

            if scanned % _BACKFILL_PROGRESS_INTERVAL == 0:
                try:
                    await progress.edit(embed=discord_common.embed_neutral(
                        f'Backfilling from {channel.mention}… '
                        f'scanned **{scanned}**, matched **{matched}**, '
                        f'inserted **{inserted}** so far.'))
                except discord.HTTPException:
                    # Rate-limited or message deleted — keep scanning either way.
                    pass

        gap_days = _BACKFILL_STOP_GAP_SECONDS // 86400
        tail = (f' Stopped early after a {gap_days}-day gap with no further '
                'greatday messages — assumed full history captured.'
                if stopped_early else '')
        await progress.edit(embed=discord_common.embed_success(
            f'Backfill complete. Scanned **{scanned}** message(s), '
            f'matched **{matched}**, inserted **{inserted}** new pick row(s).'
            + tail))
        # Fresh ping so the invoker sees completion even if the progress
        # message has scrolled out of view.
        await ctx.send(f'{ctx.author.mention} `;greatday backfill` finished — '
                       f'inserted **{inserted}** new pick row(s).')

    @GreatDayCommandsMixin.greatday.command(
        name='backfillsignups',
        brief='Seed signup/signout history server-wide (admin/mod)',
        usage='[#channel]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def backfill_signups(self, ctx, channel: discord.TextChannel = None):
        """Match exact command results and replay them across the whole guild.

        Without an argument every readable message channel and discoverable
        thread is merged chronologically. The optional channel argument keeps
        targeted recovery available, but cannot certify complete guild history.
        """
        targeted = channel is not None
        scope_label = (channel.mention if targeted else 'all server channels')

        progress = await ctx.send(embed=discord_common.embed_neutral(
            f'Scanning {scope_label} for signup history… '
            '(scanned **0**, matched **0**)'))

        async def report(scanned, matched):
            try:
                await progress.edit(embed=discord_common.embed_neutral(
                    f'Scanning {scope_label} for signup history… '
                    f'scanned **{scanned}**, matched **{matched}** so far.'))
            except discord.HTTPException:
                # Rate-limited or message deleted — keep scanning either way.
                pass

        bot_user_id = self.bot.user.id if self.bot and self.bot.user else None
        current_signups = {
            str(row.user_id)
            for row in cf_common.user_db.greatday_get_signups(ctx.guild.id)
        }
        current_bans = {
            str(row.user_id)
            for row in cf_common.user_db.greatday_get_banned(ctx.guild.id)
        }
        if targeted:
            try:
                result = await scan_signup_events_audited(
                    channel, ctx.guild.id, bot_user_id, guild=ctx.guild,
                    current_signup_ids=current_signups,
                    current_ban_ids=current_bans, progress=report,
                    progress_interval=_BACKFILL_PROGRESS_INTERVAL)
            except discord.Forbidden:
                raise GreatDayCogError(
                    f'Missing permission to read {channel.mention} history.')
            scan_status = ('partial' if result.audit.trustworthy
                           else 'incomplete')
            scope_id = channel.id
        else:
            scope = await discover_guild_history_channels(
                ctx.guild, (ctx.channel,))
            if not scope.channels:
                raise GreatDayCogError(
                    'No readable server channels were found to scan.')
            result = await scan_signup_events_channels_audited(
                scope.channels, ctx.guild.id, bot_user_id, guild=ctx.guild,
                current_signup_ids=current_signups,
                current_ban_ids=current_bans, progress=report,
                progress_interval=_BACKFILL_PROGRESS_INTERVAL,
                discovery_failures=scope.discovery_failures)
            scan_status = ('clean' if result.audit.trustworthy
                           else 'incomplete')
            scope_id = 'guild'
        inserted = cf_common.user_db.greatday_record_signup_backfill(
            result.events, ctx.guild.id, f'{scan_status}:{scope_id}')
        stored_status = cf_common.user_db.get_guild_config(
            ctx.guild.id, SIGNUP_HISTORY_AUDIT_KEY) or 'incomplete:unknown'
        status = stored_status.split(':', 1)[0]
        summary = (
            f'Signup backfill complete. Scanned **{result.scanned}** '
            f'message(s), recovered **{len(result.events)}** event(s), '
            f'inserted **{inserted}** new event(s).\n'
            + result.audit.summary())
        if status != scan_status:
            summary += (f'\nStored history remains **{status}** because the '
                        'existing audit status takes precedence.')
        embed_factory = (discord_common.embed_success
                         if status == 'clean'
                         else discord_common.embed_alert)
        await progress.edit(embed=embed_factory(summary))
        await ctx.send(f'{ctx.author.mention} `;greatday backfillsignups` '
                       f'finished — inserted **{inserted}** new event(s); '
                       f'audit **{status}**.')

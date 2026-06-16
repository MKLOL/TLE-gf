import time
import datetime as dt
from collections import defaultdict

import discord
from discord.ext import commands
from matplotlib import pyplot as plt

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import discord_common
from tle.util import paginator
from tle.util import table
from tle.util import tasks
from tle.util import graph_common as gc

from tle.cogs._contests_helpers import (
    ContestCogError,
    _apply_vc_deltas,
    _contest_duration_format,
    _contest_start_time_format,
    _get_ongoing_vc_participants,
    _MIN_RATED_CONTESTANTS_FOR_RATED_VC,
    _RATED_VC_EXTRA_TIME,
    _WATCHING_RATED_VC_WAIT_TIME,
)


class RatedVcMixin:
    """Mixin holding rated-VC commands, the background watcher task, and the
    associated embed/ranklist helpers."""

    @staticmethod
    def _make_contest_embed_for_vc_ranklist(ranklist, vc_start_time=None, vc_end_time=None):
        contest = ranklist.contest
        embed = discord_common.cf_color_embed(title=contest.name, url=contest.url)
        embed.set_author(name='VC Standings')
        now = time.time()
        if vc_start_time and vc_end_time:
            en = '\N{EN SPACE}'
            elapsed = cf_common.pretty_time_format(now - vc_start_time, shorten=True)
            remaining = cf_common.pretty_time_format(max(0, vc_end_time - now), shorten=True)
            msg = f'{elapsed} elapsed{en}|{en}{remaining} remaining'
            embed.add_field(name='Tick tock', value=msg, inline=False)
        return embed

    @commands.command(brief='Start a rated vc.', usage='<contest_id> <@user1 @user2 ...>')
    async def ratedvc(self, ctx, contest_id: int, *members: discord.Member):
        ratedvc_channel_id = cf_common.user_db.get_rated_vc_channel(ctx.guild.id)
        if not ratedvc_channel_id or ctx.channel.id != ratedvc_channel_id:
            raise ContestCogError('You must use this command in ratedvc channel.')
        if not members:
            raise ContestCogError('Missing members')
        contest = cf_common.cache2.contest_cache.get_contest(contest_id)
        try:
            (await cf.contest.ratingChanges(contest_id=contest_id))[_MIN_RATED_CONTESTANTS_FOR_RATED_VC - 1]
        except (cf.RatingChangesUnavailableError, IndexError):
            error = (f'`{contest.name}` was not rated for at least {_MIN_RATED_CONTESTANTS_FOR_RATED_VC} contestants'
                    ' or the ratings changes are not published yet.')
            raise ContestCogError(error)

        ongoing_vc_member_ids = _get_ongoing_vc_participants()
        this_vc_member_ids = {str(member.id) for member in members}
        intersection = this_vc_member_ids & ongoing_vc_member_ids
        if intersection:
            busy_members = ", ".join([ctx.guild.get_member(int(member_id)).mention for member_id in intersection])
            error = f'{busy_members} are registered in ongoing ratedvcs.'
            raise ContestCogError(error)

        handles = cf_common.members_to_handles(members, ctx.guild.id)
        visited_contests = await cf_common.get_visited_contests(handles)
        if contest_id in visited_contests:
            raise ContestCogError(f'Some of the handles: {", ".join(handles)} have submissions in the contest')
        start_time = time.time()
        finish_time = start_time + contest.durationSeconds + _RATED_VC_EXTRA_TIME
        cf_common.user_db.create_rated_vc(contest_id, start_time, finish_time, ctx.guild.id, [member.id for member in members])
        title = f'Starting {contest.name} for:'
        msg = "\n".join(f'[{discord.utils.escape_markdown(handle)}]({cf.PROFILE_BASE_URL}{handle})' for handle in handles)
        embed = discord_common.cf_color_embed(title=title, description=msg, url=contest.url)
        await ctx.send(embed=embed)
        embed = discord_common.embed_alert(f'You have {int(finish_time - start_time) // 60} minutes to complete the vc!')
        embed.set_footer(text='GL & HF')
        await ctx.send(embed=embed)

    @staticmethod
    def _make_vc_rating_changes_embed(guild, contest_id, change_by_handle):
        """Make an embed containing a list of rank changes and rating changes for ratedvc participants.
        """
        contest = cf_common.cache2.contest_cache.get_contest(contest_id)
        user_id_handle_pairs = cf_common.user_db.get_handles_for_guild(guild.id)
        member_handle_pairs = [(guild.get_member(int(user_id)), handle)
                               for user_id, handle in user_id_handle_pairs]
        member_change_pairs = [(member, change_by_handle[handle])
                               for member, handle in member_handle_pairs
                               if member is not None and handle in change_by_handle]

        member_change_pairs.sort(key=lambda pair: pair[1].newRating, reverse=True)
        rank_to_role = {role.name: role for role in guild.roles}

        def rating_to_displayable_rank(rating):
            rank = cf.rating2rank(rating).title
            role = rank_to_role.get(rank)
            return role.mention if role else rank

        rank_changes_str = []
        for member, change in member_change_pairs:
            if len(cf_common.user_db.get_vc_rating_history(member.id)) == 1:
                # If this is the user's first rated contest.
                old_role = 'Unrated'
            else:
                old_role = rating_to_displayable_rank(change.oldRating)
            new_role = rating_to_displayable_rank(change.newRating)
            if new_role != old_role:
                rank_change_str = (f'{member.mention} [{discord.utils.escape_markdown(change.handle)}]({cf.PROFILE_BASE_URL}{change.handle}): {old_role} '
                                   f'\N{LONG RIGHTWARDS ARROW} {new_role}')
                rank_changes_str.append(rank_change_str)

        member_change_pairs.sort(key=lambda pair: pair[1].newRating - pair[1].oldRating,
                                 reverse=True)
        rating_changes_str = []
        for member, change in member_change_pairs:
            delta = change.newRating - change.oldRating
            rating_change_str = (f'{member.mention} [{discord.utils.escape_markdown(change.handle)}]({cf.PROFILE_BASE_URL}{change.handle}): {change.oldRating} '
                            f'\N{HORIZONTAL BAR} **{delta:+}** \N{LONG RIGHTWARDS ARROW} '
                            f'{change.newRating}')
            rating_changes_str.append(rating_change_str)

        desc = '\n'.join(rank_changes_str) or 'No rank changes'
        embed = discord_common.cf_color_embed(title=contest.name, url=contest.url, description=desc)
        embed.set_author(name='VC Results')
        embed.add_field(name='Rating Changes',
                        value='\n'.join(rating_changes_str) or 'No rating changes',
                        inline=False)
        return embed

    async def _watch_rated_vc(self, vc_id: int):
        vc = cf_common.user_db.get_rated_vc(vc_id)
        channel_id = cf_common.user_db.get_rated_vc_channel(vc.guild_id)
        if channel_id is None:
            raise ContestCogError('No Rated VC channel')
        channel = self.bot.get_channel(int(channel_id))
        member_ids = cf_common.user_db.get_rated_vc_user_ids(vc_id)
        handles = [cf_common.user_db.get_handle(member_id, channel.guild.id) for member_id in member_ids]
        handle_to_member_id = {handle: member_id for handle, member_id in zip(handles, member_ids)}
        now = time.time()
        ranklist = await cf_common.cache2.ranklist_cache.generate_vc_ranklist(vc.contest_id, handle_to_member_id)

        async def has_running_subs(handle):
            return [sub for sub in await cf.user.status(handle=handle)
                    if sub.verdict == 'TESTING' and
                       sub.problem.contestId == vc.contest_id and
                       sub.relativeTimeSeconds <= vc.finish_time - vc.start_time]

        running_subs_flag = any([await has_running_subs(handle) for handle in handles])
        if running_subs_flag:
            msg = 'Some submissions are still being judged'
            await channel.send(embed=discord_common.embed_alert(msg), delete_after=_WATCHING_RATED_VC_WAIT_TIME)
        if now < vc.finish_time or running_subs_flag:
            # Display current standings
            await channel.send(embed=self._make_contest_embed_for_vc_ranklist(ranklist, vc.start_time, vc.finish_time), delete_after=_WATCHING_RATED_VC_WAIT_TIME)
            await self._show_ranklist(channel, vc.contest_id, handles, ranklist=ranklist, vc=True, delete_after=_WATCHING_RATED_VC_WAIT_TIME)
            return
        rating_change_by_handle = _apply_vc_deltas(
            cf_common.user_db, vc_id, handles, member_ids, ranklist)
        cf_common.user_db.finish_rated_vc(vc_id)
        if rating_change_by_handle is None:
            await channel.send(embed=discord_common.embed_alert(
                "Rated VC complete, but rating changes can't be applied — "
                "CF's standings API no longer returns virtual participations "
                "for ordinary callers. VC ratings have been preserved."))
            return
        await channel.send(embed=self._make_vc_rating_changes_embed(channel.guild, vc.contest_id, rating_change_by_handle))
        await self._show_ranklist(channel, vc.contest_id, handles, ranklist=ranklist, vc=True)

    @tasks.task_spec(name='WatchRatedVCs',
                     waiter=tasks.Waiter.fixed_delay(_WATCHING_RATED_VC_WAIT_TIME))
    async def _watch_rated_vcs_task(self, _):
        ongoing_rated_vcs = cf_common.user_db.get_ongoing_rated_vc_ids()
        if ongoing_rated_vcs is None:
            return
        for rated_vc_id in ongoing_rated_vcs:
            await self._watch_rated_vc(rated_vc_id)

    @commands.command(brief='Unregister this user from an ongoing ratedvc', usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def _unregistervc(self, ctx, user: discord.Member):
        """ Unregister this user from an ongoing ratedvc.
        """
        ongoing_vc_member_ids = _get_ongoing_vc_participants()
        if str(user.id) not in ongoing_vc_member_ids:
            raise ContestCogError(f'{user.mention} has no ongoing ratedvc!')
        cf_common.user_db.remove_last_ratedvc_participation(user.id)
        await ctx.send(embed=discord_common.embed_success(f'Successfully unregistered {user.mention} from the ongoing vc.'))

    @commands.command(brief='Set the rated vc channel to the current channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def set_ratedvc_channel(self, ctx):
        """ Sets the rated vc channel to the current channel.
        """
        cf_common.user_db.set_rated_vc_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success('Rated VC channel saved successfully'))

    @commands.command(brief='Get the rated vc channel')
    async def get_ratedvc_channel(self, ctx):
        """ Gets the rated vc channel.
        """
        channel_id = cf_common.user_db.get_rated_vc_channel(ctx.guild.id)
        channel = ctx.guild.get_channel(channel_id)
        if channel is None:
            raise ContestCogError('There is no rated vc channel')
        embed = discord_common.embed_success('Current rated vc channel')
        embed.add_field(name='Channel', value=channel.mention)
        await ctx.send(embed=embed)

    @commands.command(brief='Show vc ratings')
    async def vcratings(self, ctx):
        users = [(await self.member_converter.convert(ctx, str(member_id)), handle, cf_common.user_db.get_vc_rating(member_id, default_if_not_exist=False))
                 for member_id, handle in cf_common.user_db.get_handles_for_guild(ctx.guild.id)]
        # Filter only rated users. (Those who entered at least one rated vc.)
        users = [(member, handle, rating)
                 for member, handle, rating in users
                 if rating is not None]
        users.sort(key=lambda user: -user[2])

        _PER_PAGE = 10

        def make_page(chunk, page_num):
            style = table.Style('{:>}  {:<}  {:<}  {:<}')
            t = table.Table(style)
            t += table.Header('#', 'Name', 'Handle', 'Rating')
            t += table.Line()
            for index, (member, handle, rating) in enumerate(chunk):
                rating_str = f'{rating} ({cf.rating2rank(rating).title_abbr})'
                t += table.Data(_PER_PAGE * page_num + index, f'{member.display_name}', handle, rating_str)

            table_str = f'```\n{t}\n```'
            embed = discord_common.cf_color_embed(description=table_str)
            return 'VC Ratings', embed

        if not users:
            raise ContestCogError('There are no active VCers.')

        pages = [make_page(chunk, k) for k, chunk in enumerate(paginator.chunkify(users, _PER_PAGE))]
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    @commands.command(brief='Plot vc rating for a list of at most 5 users', usage='@user1 @user2 ..')
    async def vcrating(self, ctx, *members: discord.Member):
        """Plots VC rating for at most 5 users."""
        members = members or (ctx.author, )
        if len(members) > 5:
            raise ContestCogError('Cannot plot more than 5 VCers at once.')
        plot_data = defaultdict(list)

        min_rating = 1100
        max_rating = 1800

        for member in members:
            rating_history = cf_common.user_db.get_vc_rating_history(member.id)
            if not rating_history:
                raise ContestCogError(f'{member.mention} has no vc history.')
            for vc_id, rating in rating_history:
                vc = cf_common.user_db.get_rated_vc(vc_id)
                date = dt.datetime.fromtimestamp(vc.finish_time)
                plot_data[member.display_name].append((date, rating))
                min_rating = min(min_rating, rating)
                max_rating = max(max_rating, rating)

        plt.clf()
        # plot at least from mid gray to mid purple
        for rating_data in plot_data.values():
            x, y = zip(*rating_data)
            plt.plot(x, y,
                     linestyle='-',
                     marker='o',
                     markersize=4,
                     markerfacecolor='white',
                     markeredgewidth=0.5)

        gc.plot_rating_bg(cf.RATED_RANKS)
        plt.gcf().autofmt_xdate()

        plt.ylim(min_rating - 100, max_rating + 200)
        labels = [
            gc.StrWrap('{} ({})'.format(
                member_display_name,
                rating_data[-1][1]))
            for member_display_name, rating_data in plot_data.items()
        ]
        plt.legend(labels, loc='upper left', prop=gc.fontprop)

        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='VC rating graph')
        discord_common.attach_image(embed, discord_file)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed, file=discord_file)

    @commands.command(brief='Plot vc performance for a list of at most 5 users', aliases=['vcperf'], usage='@user1 @user2 ..')
    async def vcperformance(self, ctx, *members: discord.Member):
        """Plots VC performance for at most 5 users."""
        members = members or (ctx.author, )
        if len(members) > 5:
            raise ContestCogError('Cannot plot more than 5 VCers at once.')
        plot_data = defaultdict(list)

        min_rating = 1100
        max_rating = 1800

        for member in members:
            rating_history = cf_common.user_db.get_vc_rating_history(member.id)
            if not rating_history:
                raise ContestCogError(f'{member.mention} has no vc history.')
            ratingbefore = 1500
            for vc_id, rating in rating_history:
                vc = cf_common.user_db.get_rated_vc(vc_id)
                perf = ratingbefore + (rating - ratingbefore)*4
                date = dt.datetime.fromtimestamp(vc.finish_time)
                plot_data[member.display_name].append((date, perf))
                min_rating = min(min_rating, perf)
                max_rating = max(max_rating, perf)
                ratingbefore = rating

        plt.clf()
        # plot at least from mid gray to mid purple
        for rating_data in plot_data.values():
            x, y = zip(*rating_data)
            plt.plot(x, y,
                     linestyle='-',
                     marker='o',
                     markersize=4,
                     markerfacecolor='white',
                     markeredgewidth=0.5)

        gc.plot_rating_bg(cf.RATED_RANKS)
        plt.gcf().autofmt_xdate()

        plt.ylim(min_rating - 100, max_rating + 200)
        labels = [
            gc.StrWrap('{} ({})'.format(
                member_display_name,
                ratingbefore))
            for member_display_name, rating_data in plot_data.items()
        ]
        plt.legend(labels, loc='upper left', prop=gc.fontprop)

        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='VC performance graph')
        discord_common.attach_image(embed, discord_file)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed, file=discord_file)

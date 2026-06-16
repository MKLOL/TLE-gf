import random
import datetime
import discord
import asyncio

from discord.ext import commands
from collections import defaultdict
from matplotlib import pyplot as plt

from tle import constants
from tle.util.db.user_db_conn import Duel, DuelType, Winner
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import paginator
from tle.util import discord_common
from tle.util import table
from tle.util import graph_common as gc

from tle.cogs._duel_helpers import (
    DUEL_RANKS,
    DuelCogError,
    DuelRank,
    complete_duel,
    elo_delta,
    elo_prob,
    get_cf_user,
    logger,
    parse_nohandicap,
    rating2rank,
    _get_coefficient,
    _DUEL_CHECK_ONGOING_INTERVAL,
    _DUEL_EXPIRY_TIME,
    _DUEL_INVALIDATE_TIME,
    _DUEL_MAX_DUEL_DURATION,
    _DUEL_MAX_RATIO,
    _DUEL_NO_DRAW_TIME,
    _DUEL_OFFICIAL_CUTOFF,
    _DUEL_RATING_DELTA,
    _DUEL_STATUS_TESTING,
    _DUEL_STATUS_UNSOLVED,
)
from tle.cogs._duel_impl import DuelImplMixin


class Dueling(DuelImplMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()
        self.draw_offers = {}

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        asyncio.create_task(self._check_ongoing_duels())

    @commands.group(brief='Duel commands',
                    invoke_without_command=True)
    async def duel(self, ctx):
        """Group for commands pertaining to duels"""
        await ctx.send_help(ctx.command)

    def _checkIfCorrectChannel(self, ctx):
        duel_channel_id = cf_common.user_db.get_duel_channel(
            ctx.guild.id)
        if not duel_channel_id or ctx.channel.id != duel_channel_id:
            raise DuelCogError(
                'You must use this command in duel channel.')

    @duel.command(brief='Set the duel channel to the current channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)  # OK
    async def set_channel(self, ctx):
        """ Sets the duel channel to the current channel.
        """
        cf_common.user_db.set_duel_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success('Duel channel saved successfully'))

    @duel.command(brief='Get the duel channel')
    async def get_channel(self, ctx):
        """ Gets the duel channel.
        """
        channel_id = cf_common.user_db.get_duel_channel(ctx.guild.id)
        channel = ctx.guild.get_channel(channel_id)
        if channel is None:
            raise DuelCogError('There is no duel channel. Set one with ;duel set_channel')
        embed = discord_common.embed_success('Current duel channel')
        embed.add_field(name='Channel', value=channel.mention)
        await ctx.send(embed=embed)

    @duel.command(brief='Challenge to a duel', usage='opponent [rating] [+tag..] [~tag..] [+divX] [~divX] [nohandicap] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def challenge(self, ctx, opponent: discord.Member, *args):
        """Challenge another server member to a duel. Problem difficulty will be the lesser of duelist ratings minus 400. You can alternatively specify a different rating.
        All duels will be rated. The challenge expires if ignored for 5 minutes.
        The bot will allow the lower rated duelist to take more time for the duel.
        If the keyword 'nohandicap' is added there will be no handicap for the higher rated duelist."""
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)
        await self._challenge_impl(ctx, opponent, args)

    @duel.command(brief='Decline a duel challenge. Can be used to decline a challenge as challengee.')
    async def decline(self, ctx):
        active = cf_common.user_db.check_duel_decline(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(
                f'{ctx.author.mention}, you are not being challenged!')

        duelid, challenger = active
        challenger = ctx.guild.get_member(challenger)
        cf_common.user_db.cancel_duel(duelid, ctx.guild.id, Duel.DECLINED)
        message = f'`{ctx.author.mention}` declined a challenge by {challenger.mention}.'
        embed = discord_common.embed_alert(message)
        await ctx.send(embed=embed)

    @duel.command(brief='Withdraw a duel challenge. Can be used to revert the challenge as challenger.')
    async def withdraw(self, ctx):
        active = cf_common.user_db.check_duel_withdraw(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(
                f'{ctx.author.mention}, you are not challenging anyone.')

        duelid, challengee = active
        challengee = ctx.guild.get_member(challengee)
        cf_common.user_db.cancel_duel(duelid, ctx.guild.id, Duel.WITHDRAWN)
        message = f'{ctx.author.mention} withdrew a challenge to `{challengee.mention}`.'
        embed = discord_common.embed_alert(message)
        await ctx.send(embed=embed)

    @duel.command(brief='Accept a duel challenge. This starts the duel.')
    async def accept(self, ctx):
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        active = cf_common.user_db.check_duel_accept(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(
                f'{ctx.author.mention}, you are not being challenged.')

        duelid, challenger_id, name = active
        challenger = ctx.guild.get_member(challenger_id)
        await ctx.send(f'Duel between {challenger.mention} and {ctx.author.mention} starting in 15 seconds!')
        await asyncio.sleep(15)

        start_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.start_duel(duelid, ctx.guild.id, start_time)
        if rc != 1:
            raise DuelCogError(
                f'Unable to start the duel between {challenger.mention} and {ctx.author.mention}.')

        problem = cf_common.cache2.problem_cache.problem_by_name[name]
        title = f'{problem.index}. {problem.name}'
        desc = cf_common.cache2.contest_cache.get_contest(
            problem.contestId).name
        embed = discord.Embed(title=title, url=problem.url, description=desc)
        embed.add_field(name='Rating', value=problem.rating)
        await ctx.send(f'Starting duel: {challenger.mention} vs {ctx.author.mention}', embed=embed)

    @duel.command(brief='Give up the duel (only for duels with handicap). Can only be used by the lower rated duelist after the higher rated duelist has solved the problem.')
    async def giveup(self, ctx):
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        active = cf_common.user_db.check_duel_giveup(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        duelid, challenger_id, challengee_id, start_timestamp, problem_name, contest_id, index, dtype = active

        # get discord member
        challenger = ctx.guild.get_member(challenger_id)
        challengee = ctx.guild.get_member(challengee_id)

         # get cf handles and cf.Users
        userids = [challenger_id, challengee_id]
        handles = [cf_common.user_db.get_handle(
            userid, ctx.guild.id) for userid in userids]
        users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]

        highrated_user = users[0] if users[0].effective_rating > users[1].effective_rating else users[1]
        lowrated_user = users[1] if users[0].effective_rating > users[1].effective_rating else users[0]
        highrated_member = challenger if users[0].effective_rating > users[1].effective_rating else challengee
        lowrated_member = challengee if users[0].effective_rating > users[1].effective_rating else challenger

        highrated_timestamp = await self._get_solve_time(highrated_user.handle, contest_id, index)
        lowrated_timestamp = await self._get_solve_time(lowrated_user.handle, contest_id, index)

        lowerrated_id = userids[1] if users[0].effective_rating > users[1].effective_rating else userids[0]

        # only low rated user can invoke the command
        if ctx.author.id != lowerrated_id:
            await ctx.send(f'Only the lower rated user can give up the duel.')
            return

        # no pending submissions allowed
        if highrated_timestamp == _DUEL_STATUS_TESTING or lowrated_timestamp == _DUEL_STATUS_TESTING:
            await ctx.send(f'Wait a bit, {ctx.author.mention}. A submission is still being judged.')
            return

        # only if the high rated has already finished
        if highrated_timestamp == _DUEL_STATUS_UNSOLVED:
            await ctx.send(f'You can\'t give up the duel if the higher rated user has not finished the problem.')
            return

        # end the duel and declare high rated as winner
        winner = highrated_member
        loser = lowrated_member
        win_status = Winner.CHALLENGER if winner == challenger else Winner.CHALLENGEE
        win_time = highrated_timestamp
        embed = complete_duel(duelid, ctx.guild.id, win_status,
                            winner, loser, win_time, 1, dtype)
        await ctx.send(f'{loser.mention} gave up. {winner.mention} won the duel against {loser.mention}!', embed=embed)

    @duel.command(brief='Complete a duel. Can be used after the problem was solved by one of the duelists.')
    async def complete(self, ctx):
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        active = cf_common.user_db.check_duel_complete(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        await self._check_duel_complete(ctx.guild, ctx.channel, active)

    @duel.command(brief='Offer a draw or accept a draw offer.')
    async def draw(self, ctx):
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        active = cf_common.user_db.check_duel_draw(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        duelid, challenger_id, challengee_id, start_time, dtype = active
        now = datetime.datetime.now().timestamp()
        if now - start_time < _DUEL_NO_DRAW_TIME:
            draw_time = cf_common.pretty_time_format(
                start_time + _DUEL_NO_DRAW_TIME - now)
            await ctx.send(f'Think more {ctx.author.mention}. You can offer a draw in {draw_time}.')
            return

        if not duelid in self.draw_offers:
            self.draw_offers[duelid] = ctx.author.id
            offeree_id = challenger_id if ctx.author.id != challenger_id else challengee_id
            offeree = ctx.guild.get_member(offeree_id)
            await ctx.send(f'{ctx.author.mention} is offering a draw to {offeree.mention}!')
            return

        if self.draw_offers[duelid] == ctx.author.id:
            await ctx.send(f'{ctx.author.mention}, you\'ve already offered a draw.')
            return

        offerer = ctx.guild.get_member(self.draw_offers[duelid])
        embed = complete_duel(duelid, ctx.guild.id, Winner.DRAW,
                              offerer, ctx.author, now, 0.5, dtype)
        await ctx.send(f'{ctx.author.mention} accepted draw offer by {offerer.mention}.', embed=embed)

    @duel.command(brief='Show duelist profile page')
    async def profile(self, ctx, member: discord.Member = None):
        await self._profile_impl(ctx, member)

    @duel.command(brief='Print head to head dueling history',
                  aliases=['versushistory'])
    async def vshistory(self, ctx, member1: discord.Member = None, member2: discord.Member = None):
        if not member1:
            raise DuelCogError(
                f'You need to specify one or two discord members.')

        member2 = member2 or ctx.author
        data = cf_common.user_db.get_pair_duels(member1.id, member2.id, ctx.guild.id)
        w, l, d = 0, 0, 0
        for _, _, _, _, challenger, challengee, winner in data:
            if winner != Winner.DRAW:
                winnerid = challenger if winner == Winner.CHALLENGER else challengee
                if winnerid == member1.id:
                    w += 1
                else:
                    l += 1
            else:
                d += 1
        message = discord.utils.escape_mentions(f'`{member1.display_name}` ({w}/{d}/{l}) `{member2.display_name}`')
        pages = self._paginate_duels(
            data, message, ctx.guild.id, False)
        paginator.paginate(self.bot, ctx.channel, pages,
                           wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    @duel.command(brief='Print user dueling history')
    async def history(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        data = cf_common.user_db.get_duels(member.id, ctx.guild.id)
        message = discord.utils.escape_mentions(f'dueling history of `{member.display_name}`')
        pages = self._paginate_duels(
            data, message, ctx.guild.id, False)
        paginator.paginate(self.bot, ctx.channel, pages,
                           wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    @duel.command(brief='Print a list of recent duels.')
    async def recent(self, ctx):
        data = cf_common.user_db.get_recent_duels(ctx.guild.id)
        pages = self._paginate_duels(
            data, 'list of recent duels', ctx.guild.id, True)
        paginator.paginate(self.bot, ctx.channel, pages,
                           wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    @duel.command(brief='Print list of ongoing duels.')
    async def ongoing(self, ctx, member: discord.Member = None):
        def make_line(entry):
            _, challenger, challengee, start_time, name, _, _, _ = entry
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            now = datetime.datetime.now().timestamp()
            when = cf_common.pretty_time_format(
                now - start_time, shorten=True, always_seconds=True)
            challenger = get_cf_user(challenger, ctx.guild.id)
            challengee = get_cf_user(challengee, ctx.guild.id)
            return f'[{challenger.handle}]({challenger.url}) vs [{challengee.handle}]({challengee.url}): [{name}]({problem.url}) [{problem.rating}] {when}'

        def make_page(chunk):
            message = f'List of ongoing duels:'
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        member = member or ctx.author
        data = cf_common.user_db.get_ongoing_duels(ctx.guild.id)
        if not data:
            raise DuelCogError('There are no ongoing duels.')

        pages = [make_page(chunk) for chunk in paginator.chunkify(data, 7)]
        paginator.paginate(self.bot, ctx.channel, pages,
                           wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

    @duel.command(brief="Show duelists")
    async def ranklist(self, ctx):
        """Show the list of duelists with their duel rating."""
        await self._ranklist_impl(ctx)

    @duel.command(brief='Invalidate the duel. Can be used within 5 minutes after the duel has been started.')
    async def invalidate(self, ctx): # @@@ TODO: broken with new duel types
        """Declare your duel invalid. Use this if you've solved the problem prior to the duel.
        You can only use this functionality during the first 120 seconds of the duel."""
        # check if we are in the correct channel
        self._checkIfCorrectChannel(ctx)

        active = cf_common.user_db.check_duel_complete(ctx.author.id, ctx.guild.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        duelid, challenger_id, challengee_id, start_time, _, _, _, _ = active
        if datetime.datetime.now().timestamp() - start_time > _DUEL_INVALIDATE_TIME:
            raise DuelCogError(
                f'{ctx.author.mention}, you can no longer invalidate your duel.')
        await self.invalidate_duel(ctx, duelid, challenger_id, challengee_id)

    @duel.command(brief='Invalidate a duel', usage='[duelist]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def _invalidate(self, ctx, member: discord.Member):
        """Declare an ongoing duel invalid."""
        active = cf_common.user_db.check_duel_complete(member.id, ctx.guild.id)
        if not active:
            raise DuelCogError(f'{member.mention} is not in a duel.')

        duelid, challenger_id, challengee_id, _, _, _, _, _ = active
        await self.invalidate_duel(ctx, duelid, challenger_id, challengee_id)

    # TODO: Add _invalidate by cfhandle

    # rating does not plot rating changes through lockouts
    @duel.command(brief='Plot rating', usage='[duelist]')
    async def rating(self, ctx, *members: discord.Member):
        """Plot duelist's rating."""
        members = members or (ctx.author, )
        if len(members) > 5:
            raise DuelCogError(f'Cannot plot more than 5 duelists at once.')

        duelists = [member.id for member in members]
        duels = cf_common.user_db.get_complete_official_duels(ctx.guild.id)
        rating = dict()
        plot_data = defaultdict(list)
        time_tick = 0
        for challenger, challengee, winner, finish_time in duels:
            challenger_r = rating.get(challenger, 1500)
            challengee_r = rating.get(challengee, 1500)
            if winner == Winner.CHALLENGER:
                delta = round(elo_delta(challenger_r, challengee_r, 1))
            elif winner == Winner.CHALLENGEE:
                delta = round(elo_delta(challenger_r, challengee_r, 0))
            else:
                delta = round(elo_delta(challenger_r, challengee_r, 0.5))

            rating[challenger] = challenger_r + delta
            rating[challengee] = challengee_r - delta
            if challenger in duelists or challengee in duelists:
                if challenger in duelists:
                    plot_data[challenger].append(
                        (time_tick, rating[challenger]))
                if challengee in duelists:
                    plot_data[challengee].append(
                        (time_tick, rating[challengee]))
                time_tick += 1

        if time_tick == 0:
            raise DuelCogError(f'Nothing to plot.')

        plt.clf()
        # plot at least from mid gray to mid purple
        min_rating = 1350
        max_rating = 1550
        for rating_data in plot_data.values():
            for tick, rating in rating_data:
                min_rating = min(min_rating, rating)
                max_rating = max(max_rating, rating)

            x, y = zip(*rating_data)
            plt.plot(x, y,
                     linestyle='-',
                     marker='o',
                     markersize=2,
                     markerfacecolor='white',
                     markeredgewidth=0.5)

        gc.plot_rating_bg(DUEL_RANKS)
        plt.xlim(0, time_tick - 1)
        plt.ylim(min_rating - 100, max_rating + 100)

        labels = [
            gc.StrWrap('{} ({})'.format(
                ctx.guild.get_member(duelist).display_name,
                rating_data[-1][1]))
            for duelist, rating_data in plot_data.items()
        ]
        plt.legend(labels, loc='upper left', prop=gc.fontprop)

        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='Duel rating graph')
        discord_common.attach_image(embed, discord_file)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed, file=discord_file)

    @discord_common.send_error_if(DuelCogError, cf_common.ResolveHandleError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Dueling(bot))

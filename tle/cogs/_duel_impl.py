import random
import datetime
import asyncio

import discord

from tle.util.db.user_db_conn import Duel, DuelType, Winner
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import paginator
from tle.util import discord_common
from tle.util import table

from tle.cogs._duel_helpers import (
    DuelCogError,
    complete_duel,
    get_cf_user,
    logger,
    parse_nohandicap,
    rating2rank,
    _get_coefficient,
    _DUEL_CHECK_ONGOING_INTERVAL,
    _DUEL_EXPIRY_TIME,
    _DUEL_MAX_DUEL_DURATION,
    _DUEL_OFFICIAL_CUTOFF,
    _DUEL_RATING_DELTA,
    _DUEL_STATUS_TESTING,
    _DUEL_STATUS_UNSOLVED,
)


class DuelImplMixin:
    """Mixin holding the non-command implementation logic for duels: the
    ongoing-duel watcher, duel completion checks, solve-time lookup, the duel
    history paginator, and invalidation."""

    async def _check_ongoing_duels(self):
        try:
            for guild in self.bot.guilds:
                await self._check_ongoing_duels_for_guild(guild)
        except Exception as exception:
            # we need to handle exceptions on our own -> put them into server log for now (TODO: logging channel would be better)
            msg = 'Ignoring exception in command {}:'.format("_check_round_complete")
            exc_info = type(exception), exception, exception.__traceback__
            extra = {}
            logger.exception(msg, exc_info=exc_info, extra=extra)
        await asyncio.sleep(_DUEL_CHECK_ONGOING_INTERVAL)
        asyncio.create_task(self._check_ongoing_duels())

    async def _check_ongoing_duels_for_guild(self, guild):
        logger.info(f'_check_ongoing_duels_for_guild: running for {guild.id}')
        # check for ongoing duels that are older than _DUEL_MAX_DUEL_DURATION
        data = cf_common.user_db.get_ongoing_duels(guild.id)
        channel_id = cf_common.user_db.get_duel_channel(guild.id)
        if channel_id == None:
            logger.warn(f'_check_ongoing_duels_for_guild: duel channel is not set.')
            return

        channel = self.bot.get_channel(channel_id)
        if channel is None:
            logger.warn(f'_check_ongoing_duels_for_guild: duel channel is not found on the server.')
            return

        for entry in data:
            duelid, challenger_id, challengee_id, start_timestamp, problem_name, _, _, dtype = entry
            now = datetime.datetime.now().timestamp()
            if now - start_timestamp >= _DUEL_MAX_DUEL_DURATION:
                challenger = guild.get_member(challenger_id)
                if challenger is None:
                    logger.warn(f'_check_ongoing_duels_for_guild: member with {challenger_id} could not be retrieved.')
                challengee = guild.get_member(challengee_id)
                if challengee is None:
                    logger.warn(f'_check_ongoing_duels_for_guild: member with {challengee_id} could not be retrieved.')

                embed = complete_duel(duelid, guild.id, Winner.DRAW,
                                challenger, challengee, now, 0.5, dtype)
                timelimit = cf_common.pretty_time_format(_DUEL_MAX_DUEL_DURATION)
                await channel.send(f'Auto draw of duel between {challenger.mention} and {challengee.mention} since it was active for more than {timelimit}.', embed=embed)

        # check for duels that can be completed
        for entry in data:
            await self._check_duel_complete(guild, channel, entry, True)

    async def _get_solve_time(self, handle, contest_id, index):
        subs = [sub for sub in await cf.user.status(handle=handle)
                if (sub.verdict == 'OK' or sub.verdict == 'TESTING')
                and sub.problem.contestId == contest_id
                and sub.problem.index == index]

        if not subs:
            return _DUEL_STATUS_UNSOLVED
        if 'TESTING' in [sub.verdict for sub in subs]:
            return _DUEL_STATUS_TESTING
        return min(subs, key=lambda sub: sub.creationTimeSeconds).creationTimeSeconds

    async def _check_duel_complete(self, guild, channel, data, isAutoComplete=False):
        duelid, challenger_id, challengee_id, start_timestamp, problem_name, contest_id, index, dtype = data

        # get discord member
        challenger = guild.get_member(challenger_id)
        challengee = guild.get_member(challengee_id)

         # get cf handles and cf.Users
        userids = [challenger_id, challengee_id]
        handles = [cf_common.user_db.get_handle(
            userid, guild.id) for userid in userids]
        users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]

        highrated_user = users[0] if users[0].effective_rating > users[1].effective_rating else users[1]
        lowrated_user = users[1] if users[0].effective_rating > users[1].effective_rating else users[0]
        highrated_member = challenger if users[0].effective_rating > users[1].effective_rating else challengee
        lowrated_member = challengee if users[0].effective_rating > users[1].effective_rating else challenger
        higherrated_rating, lowerrated_rating = highrated_user.effective_rating, lowrated_user.effective_rating
        highrated_timestamp = await self._get_solve_time(highrated_user.handle, contest_id, index)
        lowrated_timestamp = await self._get_solve_time(lowrated_user.handle, contest_id, index)

        # no pending submissions allowed
        if highrated_timestamp == _DUEL_STATUS_TESTING or lowrated_timestamp == _DUEL_STATUS_TESTING:
            if not isAutoComplete:
                await channel.send(f'Wait a bit. A submission is still being judged.')
            return

        # get problem including rating
        problem = [prob for prob in cf_common.cache2.problem_cache.problems
                   if prob.name == problem_name]

        adjusted = False
        coeff = 1.0

        #for adjusted duels we calc coefficient and set flag
        if dtype == DuelType.ADJUNOFFICIAL or dtype == DuelType.ADJOFFICIAL:
            coeff = _get_coefficient(problem[0].rating, lowerrated_rating, higherrated_rating)
            adjusted = True

        # if lower rated finished first -> win for him
        # if higher rated finished first
        #       if lower rated is also done -> check times and announce winner
        #       if lower rated is still missing -> make timer till his time is over and check again
        if highrated_timestamp and lowrated_timestamp:
            highrated_duration = highrated_timestamp - start_timestamp
            lowerrated_duration = lowrated_timestamp - start_timestamp
            if highrated_duration*coeff != lowerrated_duration:
                if highrated_duration * coeff < lowerrated_duration:
                    winner = highrated_member
                    loser = lowrated_member
                    win_time = highrated_timestamp
                else:
                    winner = lowrated_member
                    loser = highrated_member
                    win_time = lowrated_timestamp

                diff = cf_common.pretty_time_format(
                abs(highrated_duration * coeff - lowerrated_duration), always_seconds=True)
                win_status = Winner.CHALLENGER if winner == challenger else Winner.CHALLENGEE
                embed = complete_duel(duelid, guild.id, win_status, winner, loser, win_time, 1, dtype)
                if adjusted:
                    await channel.send(f"Both {challenger.mention} and {challengee.mention} solved it. But {winner.mention} was {diff} faster than the adjusted time limit!", embed=embed)
                else:
                    await channel.send(f'Both {challenger.mention} and {challengee.mention} solved it but {winner.mention} was {diff} faster!', embed=embed)
            else:
                embed = complete_duel(duelid, guild.id, Winner.DRAW,
                                      challenger, challengee, highrated_timestamp, 0.5, dtype)
                if adjusted:
                    await channel.send(f"{challenger.mention} and {challengee.mention} solved the problem with the same adjusted time! It's a draw!", embed=embed)
                else:
                    await channel.send(f"{challenger.mention} and {challengee.mention} solved the problem in the exact same amount of time! It's a draw!", embed=embed)
        elif highrated_timestamp: # special handling since we cant know if lowrated will still solve within time
            highrated_duration = highrated_timestamp - start_timestamp
            lowerrated_duration = highrated_duration * coeff
            current_duration = datetime.datetime.now().timestamp() - start_timestamp
            if current_duration >= lowerrated_duration: # we can make a decision, higher rated won
                winner = highrated_member
                loser = lowrated_member
                win_status = Winner.CHALLENGER if winner == challenger else Winner.CHALLENGEE
                win_time = highrated_timestamp
                embed = complete_duel(duelid, guild.id, win_status,
                                    winner, loser, win_time, 1, dtype)
                await channel.send(f'{winner.mention} beat {loser.mention} in a duel!', embed=embed)
            else:
                time_remaining = lowerrated_duration - current_duration
                time_remaining_formatted = cf_common.pretty_time_format(
                    time_remaining, always_seconds=True)
                if not isAutoComplete:
                    await channel.send(f'{highrated_member.mention} solved it but {lowrated_member.mention} still has {time_remaining_formatted} to solve the problem! Bot will check automatically if the problem has been solved or time is up. {lowrated_member.mention} can also invoke `;duel giveup` if they want to give up.')

        elif lowrated_timestamp:
            winner = lowrated_member
            loser = highrated_member
            win_status = Winner.CHALLENGER if winner == challenger else Winner.CHALLENGEE
            win_time = lowrated_timestamp
            embed = complete_duel(duelid, guild.id, win_status,
                                  winner, loser, win_time, 1, dtype)
            await channel.send(f'{winner.mention} beat {loser.mention} in a duel!', embed=embed)
        else:
            if not isAutoComplete:
                await channel.send('Nobody solved the problem yet.')

    def _paginate_duels(self, data, message, guild_id, show_id):
        def make_line(entry):
            duelid, start_time, finish_time, name, challenger, challengee, winner = entry
            duel_time = cf_common.pretty_time_format(
                finish_time - start_time, shorten=True, always_seconds=True)
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            when = cf_common.days_ago(start_time)
            idstr = f'{duelid}: '
            if winner != Winner.DRAW:
                loser = get_cf_user(challenger if winner ==
                                    Winner.CHALLENGEE else challengee, guild_id)
                winner = get_cf_user(challenger if winner ==
                                     Winner.CHALLENGER else challengee, guild_id)
                if (winner == None and loser == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] won by [unknown] vs [unknown] {when} in {duel_time}'
                if (loser == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] won by [{winner.handle}]({winner.url}) vs [unknown] {when} in {duel_time}'
                if (winner == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] won by [unknown] vs [{loser.handle}]({loser.url}) {when} in {duel_time}'
                return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] won by [{winner.handle}]({winner.url}) vs [{loser.handle}]({loser.url}) {when} in {duel_time}'
            else:
                challenger = get_cf_user(challenger, guild_id)
                challengee = get_cf_user(challengee, guild_id)
                if (challenger == None and challengee == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] drawn by [unknown] vs [unknown] {when} after {duel_time}'
                if (challenger == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] drawn by [unknown] vs [{challengee.handle}]({challengee.url}) {when} after {duel_time}'
                if (challengee == None):
                    return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] drawn by [{challenger.handle}]({challenger.url}) vs [unknown] {when} after {duel_time}'
                return f'{idstr if show_id else str()}[{name}]({problem.url}) [{problem.rating}] drawn by [{challenger.handle}]({challenger.url}) and [{challengee.handle}]({challengee.url}) {when} after {duel_time}'

        def make_page(chunk):
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        if not data:
            raise DuelCogError('There are no duels to show.')

        return [make_page(chunk) for chunk in paginator.chunkify(data, 7)]

    async def invalidate_duel(self, ctx, duelid, challenger_id, challengee_id):
        rc = cf_common.user_db.invalidate_duel(duelid, ctx.guild.id)
        if rc == 0:
            raise DuelCogError(f'Unable to invalidate duel {duelid}.')

        challenger = ctx.guild.get_member(challenger_id)
        challenger_mention = challenger.mention if challenger is not None else str(challenger_id)
        challengee = ctx.guild.get_member(challengee_id)
        challengee_mention = challengee.mention if challengee is not None else str(challengee_id)
        await ctx.send(f'Duel between {challenger_mention} and {challengee_mention} has been invalidated.')

    async def _challenge_impl(self, ctx, opponent, args):
        challenger_id = ctx.author.id
        challengee_id = opponent.id

        await cf_common.resolve_handles(ctx, self.converter, ('!' + str(ctx.author.id), '!' + str(opponent.id)))
        userids = [challenger_id, challengee_id]
        handles = [cf_common.user_db.get_handle(
            userid, ctx.guild.id) for userid in userids]
        submissions = [await cf.user.status(handle=handle) for handle in handles]

        if not cf_common.user_db.is_duelist(challenger_id, ctx.guild.id):
            cf_common.user_db.register_duelist(challenger_id, ctx.guild.id)
        if not cf_common.user_db.is_duelist(challengee_id, ctx.guild.id):
            cf_common.user_db.register_duelist(challengee_id, ctx.guild.id)
        if challenger_id == challengee_id:
            raise DuelCogError(
                f'{ctx.author.mention}, you cannot challenge yourself!')
        if cf_common.user_db.check_duel_challenge(challenger_id, ctx.guild.id):
            raise DuelCogError(
                f'{ctx.author.mention}, you are currently in a duel!')
        if cf_common.user_db.check_duel_challenge(challengee_id, ctx.guild.id):
            raise DuelCogError(
                f'{opponent.mention} is currently in a duel!')

        tags = cf_common.parse_tags(args, prefix='+')
        bantags = cf_common.parse_tags(args, prefix='~')
        rating = cf_common.parse_rating(args)
        nohandicap = parse_nohandicap(args)
        users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]
        lowest_rating = min(user.effective_rating or 0 for user in users)
        suggested_rating = round(lowest_rating, -2) + _DUEL_RATING_DELTA
        rating = round(rating, -2) if rating else suggested_rating
        rating = min(3500, max(rating, 800))
        unofficial = rating > _DUEL_OFFICIAL_CUTOFF #suggested_rating
        dlo,dhi = cf_common.parse_daterange(args)
        if not nohandicap:
            dtype = DuelType.ADJUNOFFICIAL if unofficial else DuelType.ADJOFFICIAL
        else:
            dtype = DuelType.UNOFFICIAL if unofficial else DuelType.OFFICIAL

        solved = {
            sub.problem.name for subs in submissions for sub in subs if sub.verdict != 'COMPILATION_ERROR'}
        seen = {name for userid in userids for name,
                in cf_common.user_db.get_duel_problem_names(userid, ctx.guild.id)} # maybe guild id is not needed here

        def get_problems(rating):
            return [prob for prob in cf_common.cache2.problem_cache.problems
                    if prob.rating == rating and prob.name not in solved and prob.name not in seen
                    and not any(cf_common.is_contest_writer(prob.contestId, handle) for handle in handles)
                    and not cf_common.is_nonstandard_problem(prob)
                    and prob.matches_all_tags(tags)
                    and not prob.matches_any_tag(bantags)
                    and dlo <= cf_common.cache2.contest_cache.get_contest(prob.contestId).startTimeSeconds < dhi]

        for problems in map(get_problems, range(rating, 400, -100)):
            if problems:
                break

        rstr = f'{rating} rated ' if rating else ''
        if not problems:
            raise DuelCogError(
                f'No unsolved {rstr}problems left for {ctx.author.mention} vs {opponent.mention}.')

        problems.sort(key=lambda problem: cf_common.cache2.contest_cache.get_contest(
            problem.contestId).startTimeSeconds)

        choice = max(random.randrange(len(problems)) for _ in range(5))
        problem = problems[choice]

        issue_time = datetime.datetime.now().timestamp()
        duelid = cf_common.user_db.create_duel(
            challenger_id, challengee_id, issue_time, problem, dtype, ctx.guild.id)

        if not nohandicap:
            # get cf handles and cf.Users
            userids = [challenger_id, challengee_id]
            handles = [cf_common.user_db.get_handle(
                userid, ctx.guild.id) for userid in userids]
            users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]

            # get discord member
            challenger = ctx.guild.get_member(challenger_id)
            challengee = ctx.guild.get_member(challengee_id)

            highrated_user = users[0] if users[0].effective_rating > users[1].effective_rating else users[1]
            lowrated_user = users[1] if users[0].effective_rating > users[1].effective_rating else users[0]
            highrated_member = challenger if users[0].effective_rating > users[1].effective_rating else challengee
            lowrated_member = challengee if users[0].effective_rating > users[1].effective_rating else challenger
            higherrated_rating, lowerrated_rating = highrated_user.effective_rating, lowrated_user.effective_rating
            coeff = _get_coefficient(problem.rating, lowerrated_rating, higherrated_rating)
            percentage = round((coeff - 1.0)*100,1)
            ostr = 'an **unofficial** ' if unofficial else 'a '
            diff = cf_common.pretty_time_format(600 * coeff-600, always_seconds=True)
            if lowerrated_rating == higherrated_rating:
                await ctx.send(f'{ctx.author.mention} is challenging {opponent.mention} to {ostr} {rstr}duel with handicap! Since {lowrated_member.mention} and {highrated_member.mention} have same rating no one will get a time bonus.' )
            else:
                await ctx.send(f'{ctx.author.mention} is challenging {opponent.mention} to {ostr} {rstr}duel with handicap! {lowrated_member.mention} is lower rated and will get {percentage} % more time (bonus of {diff} for every 10 minutes of duel duration).' )
        else:
            ostr = 'an **unofficial**' if unofficial else 'a'
            await ctx.send(f'{ctx.author.mention} is challenging {opponent.mention} to {ostr} {rstr}duel!')
        await asyncio.sleep(_DUEL_EXPIRY_TIME)
        if cf_common.user_db.cancel_duel(duelid, ctx.guild.id, Duel.EXPIRED):
            message = f'{ctx.author.mention}, your request to duel {opponent.mention} has expired!'
            embed = discord_common.embed_alert(message)
            await ctx.send(embed=embed)

    async def _profile_impl(self, ctx, member):
        member = member or ctx.author

        if not cf_common.user_db.is_duelist(member.id, ctx.guild.id):
            raise DuelCogError(
                f'{member.mention} has not done any duels.')

        user = get_cf_user(member.id, ctx.guild.id)
        rating = cf_common.user_db.get_duel_rating(member.id, ctx.guild.id)
        desc = f'Duelist profile of {rating2rank(rating).title} {member.mention} aka **[{user.handle}]({user.url})**'
        embed = discord.Embed(
            description=desc, color=rating2rank(rating).color_embed)
        embed.add_field(name='Rating', value=rating, inline=True)

        wins = cf_common.user_db.get_duel_wins(member.id, ctx.guild.id)
        num_wins = len(wins)
        embed.add_field(name='Wins', value=num_wins, inline=True)
        num_losses = cf_common.user_db.get_num_duel_losses(member.id, ctx.guild.id)
        embed.add_field(name='Losses', value=num_losses, inline=True)
        num_draws = cf_common.user_db.get_num_duel_draws(member.id, ctx.guild.id)
        embed.add_field(name='Draws', value=num_draws, inline=True)
        num_declined = cf_common.user_db.get_num_duel_declined(member.id, ctx.guild.id)
        embed.add_field(name='Declined', value=num_declined, inline=True)
        num_rdeclined = cf_common.user_db.get_num_duel_rdeclined(member.id, ctx.guild.id)
        embed.add_field(name='Got declined', value=num_rdeclined, inline=True)

        def duel_to_string(duel):
            start_time, finish_time, problem_name, challenger, challengee = duel
            duel_time = cf_common.pretty_time_format(
                finish_time - start_time, shorten=True, always_seconds=True)
            when = cf_common.days_ago(start_time)
            loser_id = challenger if member.id != challenger else challengee
            loser = get_cf_user(loser_id, ctx.guild.id)
            problem = cf_common.cache2.problem_cache.problem_by_name[problem_name]
            return f'**[{problem.name}]({problem.url})** [{problem.rating}] versus [{loser.handle}]({loser.url}) {when} in {duel_time}'

        if wins:
            # sort by finish_time - start_time
            wins.sort(key=lambda duel: duel[1] - duel[0])
            embed.add_field(name='Fastest win',
                            value=duel_to_string(wins[0]), inline=False)
            embed.add_field(name='Slowest win',
                            value=duel_to_string(wins[-1]), inline=False)

        embed.set_thumbnail(url=f'{user.titlePhoto}')
        await ctx.send(embed=embed)

    async def _ranklist_impl(self, ctx):
        users = [(ctx.guild.get_member(user_id), rating)
                 for user_id, rating in cf_common.user_db.get_duelists(ctx.guild.id)]
        users = [(member, cf_common.user_db.get_handle(member.id, ctx.guild.id), rating)
                 for member, rating in users
                 if member is not None and cf_common.user_db.get_num_duel_completed(member.id, ctx.guild.id) > 0]

        _PER_PAGE = 10

        def make_page(chunk, page_num):
            style = table.Style('{:>}  {:<}  {:<}  {:<}')
            t = table.Table(style)
            t += table.Header('#', 'Name', 'Handle', 'Rating')
            t += table.Line()
            for index, (member, handle, rating) in enumerate(chunk):
                rating_str = f'{rating} ({rating2rank(rating).title_abbr})'

                handlestr = 'Unknown'
                if (handle is not None):
                    handlestr = handle
                t += table.Data(_PER_PAGE * page_num + index + 1,
                                f'{member.display_name}', handlestr, rating_str)

            table_str = f'```\n{t}\n```'
            embed = discord_common.cf_color_embed(description=table_str)
            return 'List of duelists', embed

        if not users:
            raise DuelCogError('There are no active duelists.')

        pages = [make_page(chunk, k) for k, chunk in enumerate(
            paginator.chunkify(users, _PER_PAGE))]
        paginator.paginate(self.bot, ctx.channel, pages,
                           wait_time=5 * 60, set_pagenum_footers=True, author_id=ctx.author.id)

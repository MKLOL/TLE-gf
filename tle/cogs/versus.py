import collections

from discord.ext import commands
from matplotlib import pyplot as plt
import numpy as np

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc


class VersusCogError(commands.CommandError):
    pass


def _compute_versus_stats(handles, all_changes):
    """Given a list of handles and a dict {handle: [RatingChange, ...]},
    compute per-handle win counts and placement distributions across shared contests.

    Returns (wins, placements, total_shared) where:
      wins: dict handle -> number of contests where handle had the best rank
      placements: dict handle -> Counter of {place: count}  (1-indexed)
      total_shared: number of shared contests
    """
    # Build contest_id -> {handle: rank} for contests where 2+ handles participated
    contest_ranks = collections.defaultdict(dict)
    for handle in handles:
        for rc in all_changes.get(handle, []):
            contest_ranks[rc.contestId][handle] = rc.rank

    # Filter to contests with at least 2 of the requested handles
    shared_contests = {cid: ranks for cid, ranks in contest_ranks.items()
                       if len(ranks) >= 2}

    wins = {h: 0 for h in handles}
    placements = {h: collections.Counter() for h in handles}

    for cid, ranks in shared_contests.items():
        # Sort participating handles by rank (lower = better)
        sorted_handles = sorted(ranks.keys(), key=lambda h: ranks[h])
        # Use competition ranking: tied users get the same place
        place = 1
        i = 0
        while i < len(sorted_handles):
            current_rank = ranks[sorted_handles[i]]
            # Find all handles sharing this rank
            j = i
            while j < len(sorted_handles) and ranks[sorted_handles[j]] == current_rank:
                placements[sorted_handles[j]][place] += 1
                j += 1
            i = j
            place = j + 1  # Competition ranking: skip places for ties
        # Winner = sole holder of best rank
        best_rank = ranks[sorted_handles[0]]
        winners = [h for h in sorted_handles if ranks[h] == best_rank]
        if len(winners) == 1:
            wins[winners[0]] += 1
        # Tie = no one gets a win

    return wins, placements, len(shared_contests)


class Versus(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()

    @commands.command(brief='Compare contest results between users',
                      usage='handle1 handle2 [handle3 ...]')
    async def versus(self, ctx, *handles: str):
        """Show head-to-head contest win counts among the given users.
        Use ! prefix for Discord users (e.g. !username), otherwise treated as CF handle."""
        if len(handles) < 2:
            raise VersusCogError('Please provide at least 2 handles.')

        handles = await cf_common.resolve_handles(ctx, self.converter, handles,
                                                  mincnt=2, maxcnt=5)

        cache = cf_common.cache2.rating_changes_cache
        all_changes = {}
        for handle in handles:
            all_changes[handle] = cache.get_rating_changes_for_handle(handle)

        wins, placements, total_shared = _compute_versus_stats(handles, all_changes)

        if total_shared == 0:
            raise VersusCogError('No shared contests found among the given users.')

        lines = []
        # Sort by wins descending
        for handle in sorted(handles, key=lambda h: wins[h], reverse=True):
            w = wins[handle]
            word = 'win' if w == 1 else 'wins'
            lines.append(f'**{handle}**: {w} {word}')

        desc = '\n'.join(lines)
        desc += f'\n\n*Across {total_shared} shared contest{"s" if total_shared != 1 else ""}*'

        embed = discord_common.cf_color_embed(title='Versus — Head to Head', description=desc)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed)

    @commands.command(brief='Plot placement distribution between users',
                      aliases=['plotvs'],
                      usage='handle1 handle2 [handle3 ...]')
    async def plotversus(self, ctx, *handles: str):
        """Plot how often each user placed 1st, 2nd, 3rd, etc. among the group
        across all shared contests. Use ! prefix for Discord users."""
        if len(handles) < 2:
            raise VersusCogError('Please provide at least 2 handles.')

        handles = await cf_common.resolve_handles(ctx, self.converter, handles,
                                                  mincnt=2, maxcnt=5)

        cache = cf_common.cache2.rating_changes_cache
        all_changes = {}
        for handle in handles:
            all_changes[handle] = cache.get_rating_changes_for_handle(handle)

        wins, placements, total_shared = _compute_versus_stats(handles, all_changes)

        if total_shared == 0:
            raise VersusCogError('No shared contests found among the given users.')

        num_users = len(handles)
        max_place = num_users  # Placements go from 1 to num_users

        plt.clf()
        plt.axes().set_prop_cycle(gc.rating_color_cycler)

        x = np.arange(1, max_place + 1)
        bar_width = 0.8 / num_users
        offsets = np.linspace(-(num_users - 1) / 2 * bar_width,
                              (num_users - 1) / 2 * bar_width,
                              num_users)

        for i, handle in enumerate(handles):
            counts = [placements[handle].get(p, 0) for p in range(1, max_place + 1)]
            plt.bar(x + offsets[i], counts, bar_width, label=handle)

        place_labels = []
        for p in range(1, max_place + 1):
            if p == 1:
                place_labels.append('1st')
            elif p == 2:
                place_labels.append('2nd')
            elif p == 3:
                place_labels.append('3rd')
            else:
                place_labels.append(f'{p}th')

        plt.xticks(x, place_labels)
        plt.xlabel('Placement (among group)')
        plt.ylabel('Number of contests')
        plt.title(f'Placement Distribution ({total_shared} shared contests)')
        plt.legend()

        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='Versus — Placement Distribution')
        discord_common.attach_image(embed, discord_file)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed, file=discord_file)


    @discord_common.send_error_if(VersusCogError, cf_common.ResolveHandleError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Versus(bot))

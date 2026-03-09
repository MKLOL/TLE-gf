import collections
import time

from discord.ext import commands
from matplotlib import pyplot as plt
import numpy as np

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc

# Alias cache staleness: 14 days normally, 2 days in Dec/Jan (rename season)
_STALE_SECONDS = 14 * 24 * 60 * 60
_STALE_SECONDS_RENAME_SEASON = 2 * 24 * 60 * 60


class VersusCogError(commands.CommandError):
    pass


def _compute_versus_stats(handles, all_changes, strict=False):
    """Given a list of handles and a dict {handle: [RatingChange, ...]},
    compute per-handle win counts and placement distributions across shared contests.

    If strict=True, only contests where ALL handles participated are counted.
    Otherwise, contests where at least 2 handles participated are counted.

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

    min_participants = len(handles) if strict else 2
    shared_contests = {cid: ranks for cid, ranks in contest_ranks.items()
                       if len(ranks) >= min_participants}

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


def _normalize_handles(handles, cache):
    """Resolve handle casing to match what's stored in the rating changes cache.
    The cache stores handles with CF's canonical casing, but users may type any case."""
    canonical = {h.lower(): h for h in cache.handle_rating_cache}
    result = []
    for handle in handles:
        result.append(canonical.get(handle.lower(), handle))
    return result


def _alias_is_stale(resolved_at):
    """Check if an alias resolution is stale based on current month."""
    if resolved_at is None:
        return True
    import datetime
    now = datetime.datetime.now()
    max_age = _STALE_SECONDS_RENAME_SEASON if now.month in (12, 1) else _STALE_SECONDS
    return (time.time() - resolved_at) > max_age


async def _resolve_aliases(handle, cache_db):
    """Get all handles for a person. Uses cached alias table if fresh,
    otherwise makes one cf.user.rating API call and caches the result."""
    aliases, resolved_at = cache_db.get_handle_aliases(handle)

    if aliases is not None and not _alias_is_stale(resolved_at):
        return aliases

    # Cache miss or stale — call CF API once to discover all handle names
    try:
        api_changes = await cf.user.rating(handle=handle)
    except cf.HandleNotFoundError:
        # Unknown handle — cache it as mapping to itself so we don't retry
        now = int(time.time())
        cache_db.save_handle_aliases({handle: handle}, now)
        return {handle}

    # Extract all distinct handles from the API response
    discovered = {rc.handle for rc in api_changes}
    if not discovered:
        discovered = {handle}

    # The current handle is whatever CF uses for the most recent entry,
    # or the input handle if no entries
    current = api_changes[-1].handle if api_changes else handle

    # Build alias map: every discovered handle → current handle
    alias_map = {h: current for h in discovered}
    # Also map the input handle in case it differs from all discovered ones
    alias_map[handle] = current

    now = int(time.time())
    cache_db.save_handle_aliases(alias_map, now)
    return set(alias_map.keys())


async def _get_all_changes(handles, cache):
    """Fetch rating changes for each handle, merging data from all historical
    handle names (CF renames). Uses cached alias table, only hits CF API if stale."""
    cache_db = cache.cache_master.conn
    all_changes = {}
    for handle in handles:
        aliases = await _resolve_aliases(handle, cache_db)
        merged = []
        seen_contests = set()
        for alias in aliases:
            for rc in cache.get_rating_changes_for_handle(alias):
                if rc.contestId not in seen_contests:
                    seen_contests.add(rc.contestId)
                    merged.append(rc)
        all_changes[handle] = merged
    return all_changes


class Versus(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()

    @commands.command(brief='Compare contest results between users',
                      usage='[+all] handle1 handle2 [handle3 ...]')
    async def versus(self, ctx, *args: str):
        """Show head-to-head contest win counts among the given users.
        Use ! prefix for Discord users (e.g. !username), otherwise treated as CF handle.
        Use +all to only count contests where every listed user participated."""
        (strict,), handles = cf_common.filter_flags(args, ['+all'])

        if len(handles) < 2:
            raise VersusCogError('Please provide at least 2 handles.')

        handles = await cf_common.resolve_handles(ctx, self.converter, handles,
                                                  mincnt=2, maxcnt=5)

        cache = cf_common.cache2.rating_changes_cache
        handles = _normalize_handles(handles, cache)
        all_changes = await _get_all_changes(handles, cache)

        wins, placements, total_shared = _compute_versus_stats(handles, all_changes,
                                                               strict=strict)

        if total_shared == 0:
            msg = 'No contests found where all users participated.' if strict else \
                  'No shared contests found among the given users.'
            raise VersusCogError(msg)

        lines = []
        # Sort by wins descending
        for handle in sorted(handles, key=lambda h: wins[h], reverse=True):
            w = wins[handle]
            word = 'win' if w == 1 else 'wins'
            lines.append(f'**{handle}**: {w} {word}')

        mode = 'all-participated' if strict else 'shared'
        desc = '\n'.join(lines)
        desc += f'\n\n*Across {total_shared} {mode} contest{"s" if total_shared != 1 else ""}*'

        embed = discord_common.cf_color_embed(title='Versus — Head to Head', description=desc)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed)

    @commands.command(brief='Plot placement distribution between users',
                      aliases=['plotvs'],
                      usage='[+all] handle1 handle2 [handle3 ...]')
    async def plotversus(self, ctx, *args: str):
        """Plot how often each user placed 1st, 2nd, 3rd, etc. among the group
        across all shared contests. Use ! prefix for Discord users.
        Use +all to only count contests where every listed user participated."""
        (strict,), handles = cf_common.filter_flags(args, ['+all'])

        if len(handles) < 2:
            raise VersusCogError('Please provide at least 2 handles.')

        handles = await cf_common.resolve_handles(ctx, self.converter, handles,
                                                  mincnt=2, maxcnt=5)

        cache = cf_common.cache2.rating_changes_cache
        handles = _normalize_handles(handles, cache)
        all_changes = await _get_all_changes(handles, cache)

        wins, placements, total_shared = _compute_versus_stats(handles, all_changes,
                                                               strict=strict)

        if total_shared == 0:
            msg = 'No contests found where all users participated.' if strict else \
                  'No shared contests found among the given users.'
            raise VersusCogError(msg)

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
        mode = 'all-participated' if strict else 'shared'
        plt.title(f'Placement Distribution ({total_shared} {mode} contests)')
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

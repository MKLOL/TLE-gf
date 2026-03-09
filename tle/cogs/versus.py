import collections
import time
import datetime

from discord.ext import commands
from matplotlib import pyplot as plt
import numpy as np

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import graph_common as gc

# How long before we re-fetch a handle's rating history from the API.
# 14 days normally, 2 days in Dec/Jan (CF rename season).
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


def _is_stale(resolved_at):
    """Check if a cached resolution is stale based on current month."""
    if resolved_at is None:
        return True
    now = datetime.datetime.now()
    max_age = _STALE_SECONDS_RENAME_SEASON if now.month in (12, 1) else _STALE_SECONDS
    return (time.time() - resolved_at) > max_age


async def _get_rating_changes(handle, cache_db):
    """Get a handle's full rating history. On first call (or when stale),
    fetches from cf.user.rating and writes the results into the rating_change
    cache under the current handle. Subsequent calls are pure DB lookups.

    This handles CF renames: the API returns all contests for the person,
    and we store them under the current handle in the cache."""
    # Check if we've already resolved this handle recently
    row = cache_db.conn.execute(
        'SELECT current_handle, resolved_at FROM handle_alias WHERE handle = ?', (handle,)
    ).fetchone()

    if row:
        current_handle, resolved_at = row
        if not _is_stale(resolved_at):
            # Fresh cache — read under current handle (may differ if renamed)
            return cache_db.get_rating_changes_for_handle(current_handle)

    # Stale or never resolved — fetch from API
    try:
        api_changes = await cf.user.rating(handle=handle)
    except cf.HandleNotFoundError:
        # Old handle that no longer exists — check if we know its current name
        alias_row = cache_db.conn.execute(
            'SELECT current_handle FROM handle_alias WHERE handle = ?', (handle,)
        ).fetchone()
        if alias_row and alias_row[0] != handle:
            # We already know the current handle — use it
            return cache_db.get_rating_changes_for_handle(alias_row[0])
        # Truly unknown handle — cache as resolved so we don't retry
        now = int(time.time())
        cache_db.conn.execute(
            'INSERT OR REPLACE INTO handle_alias (handle, current_handle, resolved_at) '
            'VALUES (?, ?, ?)', (handle, handle, now)
        )
        cache_db.conn.commit()
        return cache_db.get_rating_changes_for_handle(handle)

    # The API returns all contests under the current handle name.
    current = api_changes[-1].handle if api_changes else handle

    if api_changes:
        # If handle was renamed, delete old rows to avoid duplicates.
        # The old handle's rows would otherwise coexist with the new ones,
        # causing issues in contest listings and user counts.
        if handle != current:
            cache_db.conn.execute(
                'DELETE FROM rating_change WHERE handle = ?', (handle,)
            )
        # Also find any OTHER old aliases and clean them up
        old_aliases = cache_db.conn.execute(
            'SELECT handle FROM handle_alias WHERE current_handle = ? AND handle != ?',
            (current, current)
        ).fetchall()
        for (old_handle,) in old_aliases:
            cache_db.conn.execute(
                'DELETE FROM rating_change WHERE handle = ?', (old_handle,)
            )

        cache_db.save_rating_changes(api_changes)

    # Mark as resolved
    now = int(time.time())
    cache_db.conn.execute(
        'INSERT OR REPLACE INTO handle_alias (handle, current_handle, resolved_at) '
        'VALUES (?, ?, ?)', (handle, current, now)
    )
    if handle != current:
        cache_db.conn.execute(
            'INSERT OR REPLACE INTO handle_alias (handle, current_handle, resolved_at) '
            'VALUES (?, ?, ?)', (current, current, now)
        )
    cache_db.conn.commit()

    return cache_db.get_rating_changes_for_handle(current)


async def _get_all_changes(handles, cache):
    """Fetch rating changes for each handle via the cached API approach."""
    cache_db = cache.cache_master.conn
    all_changes = {}
    for handle in handles:
        all_changes[handle] = await _get_rating_changes(handle, cache_db)
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

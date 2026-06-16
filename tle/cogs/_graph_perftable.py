"""Performance-table data and formatting helpers for the Graphs cog.

Split out of ``tle/cogs/graphs.py`` to keep files under the 500-line limit.
``graphs.py`` re-exports these symbols so existing imports keep working.

Note: ``_build_cfvc_rows`` and ``_estimate_perf_from_cache`` read ``cf`` and
``cf_common`` from this module's namespace, so tests that mock the Codeforces
API must patch ``tle.cogs._graph_perftable.cf`` / ``.cf_common``.
"""

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import table


_CONTEST_NAME_MAX = 30


def _truncate_name(name):
    if len(name) > _CONTEST_NAME_MAX:
        return name[:_CONTEST_NAME_MAX - 3] + '...'
    return name


def _build_rated_rows(rating_changes, corrected):
    """Build performance rows from original and corrected rating changes."""
    rows = []
    for i, (orig, corr) in enumerate(zip(rating_changes, corrected)):
        delta = orig.newRating - orig.oldRating
        rows.append({
            'idx': i + 1,
            'contest': _truncate_name(orig.contestName),
            'rank': orig.rank,
            'old': orig.oldRating,
            'new': orig.newRating,
            'delta': delta,
            'perf': corr.newRating,
        })
    return rows


def _build_vc_rows(rating_history, dlo, dhi, get_vc_info):
    """Build performance rows from VC rating history.

    get_vc_info(vc_id) -> (finish_time, contest_name)
    """
    rows = []
    ratingbefore = 1500
    idx = 0
    for vc_id, rating in rating_history:
        finish_time, contest_name = get_vc_info(vc_id)
        if not (dlo <= finish_time < dhi):
            ratingbefore = rating
            continue
        idx += 1
        delta = rating - ratingbefore
        perf = ratingbefore + (rating - ratingbefore) * 4
        rows.append({
            'idx': idx,
            'contest': _truncate_name(contest_name),
            'rank': None,
            'old': ratingbefore,
            'new': rating,
            'delta': delta,
            'perf': perf,
        })
        ratingbefore = rating
    return rows


def _estimate_perf_from_cache(contest_id, virtual_rank):
    """Estimate performance by finding the closest-ranked official contestant in cache.

    Returns estimated performance or None if no cached data.
    """
    changes = cf_common.cache2.rating_changes_cache.get_rating_changes_for_contest(contest_id)
    if not changes:
        return None
    # Sort by rank (should already be, but be safe)
    changes.sort(key=lambda c: c.rank)
    # Binary search for closest rank
    lo, hi = 0, len(changes) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if changes[mid].rank < virtual_rank:
            lo = mid + 1
        else:
            hi = mid
    # Check neighbors for closest
    best = changes[lo]
    if lo > 0 and abs(changes[lo - 1].rank - virtual_rank) < abs(best.rank - virtual_rank):
        best = changes[lo - 1]
    return best.oldRating + 4 * (best.newRating - best.oldRating)


async def _build_cfvc_rows(handle, dlo=0, dhi=10**10):
    """Build performance rows for CF virtual participations (not TLE VCs).

    Under CF's May 2026 contest.standings restriction, anonymous callers
    only see CONTESTANT rows — VIRTUAL ranks cannot be fetched, so this
    function no longer writes to cfvc_cache. Existing cached entries are
    still served (computed pre-restriction); uncached contests are
    counted as missing. Performance is computed from the shared
    rating_changes_cache so all users benefit from one cache.

    Returns (rows, missing_count).
    """
    submissions = await cf.user.status(handle=handle)
    virtual_cids = set()
    for sub in submissions:
        if sub.author.participantType == 'VIRTUAL':
            cid = sub.contestId
            if cid is not None and cid < cf.GYM_ID_THRESHOLD:
                virtual_cids.add(cid)

    if not virtual_cids:
        return [], 0

    # Load cached ranks (per-user)
    cached_cids = cf_common.user_db.get_cfvc_cached_contest_ids(handle)
    cached_ranks = cf_common.user_db.get_cfvc_cache(handle)
    rank_by_cid = {cid: rank for cid, rank in cached_ranks}

    # Under CF's May 2026 contest.standings restriction, anonymous callers
    # only see CONTESTANT rows. There is no way to recover a VIRTUAL rank
    # via the API for an uncached contest — skip the wasted multi-MB
    # request and surface the contest as missing.
    uncached_cids = sorted(virtual_cids - cached_cids)
    missing = len(uncached_cids)

    # Build rows — compute perf on-the-fly from shared rating changes cache
    rows = []
    for cid in sorted(virtual_cids):
        if cid not in rank_by_cid:
            continue
        rank = rank_by_cid[cid]

        # Date filter using contest start time from contest cache
        try:
            contest = cf_common.cache2.contest_cache.get_contest(cid)
            if contest.startTimeSeconds is not None:
                if not (dlo <= contest.startTimeSeconds < dhi):
                    continue
            contest_name = contest.name
        except Exception:
            contest_name = f'Contest {cid}'

        # Perf from shared rating changes cache (benefits all users)
        perf = _estimate_perf_from_cache(cid, rank)
        if perf is None:
            missing += 1
            continue

        rows.append({
            'idx': len(rows) + 1,
            'contest': _truncate_name(contest_name),
            'rank': rank,
            'old': None,
            'new': None,
            'delta': None,
            'perf': perf,
        })

    return rows, missing


def _format_cfvc_table(rows):
    """Format CF virtual contest rows (rank + perf only, no rating columns)."""
    style = table.Style('{:>}  {:<}  {:>}  {:>}')
    t = table.Table(style)
    t += table.Header('#', 'Contest', 'Rank', 'Perf')
    t += table.Line()
    for row in rows:
        t += table.Data(row['idx'], row['contest'], row['rank'], row['perf'])
    return str(t)


def _format_perftable(rows):
    """Format performance rows into a string table."""
    has_rank = any(r['rank'] is not None for r in rows)
    if has_rank:
        style = table.Style('{:>}  {:<}  {:>}  {:>}  {:>}  {:>}  {:>}')
    else:
        style = table.Style('{:>}  {:<}  {:>}  {:>}  {:>}  {:>}')

    t = table.Table(style)
    if has_rank:
        t += table.Header('#', 'Contest', 'Rank', 'Old', 'New', 'Δ', 'Perf')
    else:
        t += table.Header('#', 'Contest', 'Old', 'New', 'Δ', 'Perf')
    t += table.Line()
    for row in rows:
        delta_str = f'+{row["delta"]}' if row['delta'] >= 0 else str(row['delta'])
        if has_rank:
            rank_str = str(row['rank']) if row['rank'] is not None else '-'
            t += table.Data(row['idx'], row['contest'], rank_str,
                            row['old'], row['new'], delta_str, row['perf'])
        else:
            t += table.Data(row['idx'], row['contest'],
                            row['old'], row['new'], delta_str, row['perf'])
    return str(t)

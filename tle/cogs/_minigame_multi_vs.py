"""Round-robin computation for Akari/Queens comparisons with 2+ players."""

from itertools import combinations

from tle.cogs._minigame_common import (
    default_score_matchup,
    pick_best_results,
)


def compute_multi_vs(rows_by_user, *, score_fn=None, missing_is_loss=False,
                     best_result_sort_key_fn=None, group_key_fn=None,
                     missing_result=None):
    """Score every pair of players across one common puzzle set.

    Normal mode compares only puzzles completed by every selected player.
    Missing-is-loss modes compare the union and score missing rows using the
    same semantics as the two-player ``compute_vs`` helper.
    """
    if score_fn is None:
        score_fn = default_score_matchup
    user_ids = list(rows_by_user)
    best_by_user = {
        user_id: pick_best_results(
            rows,
            sort_key_fn=best_result_sort_key_fn,
            group_key_fn=group_key_fn,
        )
        for user_id, rows in rows_by_user.items()
    }

    key_sets = [set(rows) for rows in best_by_user.values()]
    if not key_sets:
        puzzle_keys = set()
    elif missing_is_loss:
        puzzle_keys = set().union(*key_sets)
    else:
        puzzle_keys = set.intersection(*key_sets)

    players = {
        user_id: {
            'user_id': user_id,
            'score': 0.0,
            'wins': 0,
            'losses': 0,
            'ties': 0,
        }
        for user_id in user_ids
    }
    for key in sorted(puzzle_keys):
        for user1, user2 in combinations(user_ids, 2):
            row1 = best_by_user[user1].get(key)
            row2 = best_by_user[user2].get(key)
            if row1 is None and row2 is None:
                continue
            if row1 is None:
                if missing_result is None:
                    points1, points2 = 0.0, 1.0
                else:
                    points1, points2 = score_fn(missing_result, row2)
            elif row2 is None:
                if missing_result is None:
                    points1, points2 = 1.0, 0.0
                else:
                    points1, points2 = score_fn(row1, missing_result)
            else:
                points1, points2 = score_fn(row1, row2)
            players[user1]['score'] += points1
            players[user2]['score'] += points2
            if points1 == points2:
                players[user1]['ties'] += 1
                players[user2]['ties'] += 1
            elif points1 > points2:
                players[user1]['wins'] += 1
                players[user2]['losses'] += 1
            else:
                players[user2]['wins'] += 1
                players[user1]['losses'] += 1

    return {
        'puzzle_count': len(puzzle_keys),
        'pair_count': len(user_ids) * (len(user_ids) - 1) // 2,
        'players': players,
    }

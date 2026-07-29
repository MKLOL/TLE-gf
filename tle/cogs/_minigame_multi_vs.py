"""Pairwise Akari/Queens comparison aggregation for 2+ players."""

from itertools import combinations

from tle.cogs._minigame_common import compute_vs_matchups


def compute_multi_vs(rows_by_user, *, score_fn=None, missing_is_loss=False,
                     best_result_sort_key_fn=None, group_key_fn=None,
                     missing_result=None):
    """Aggregate the ordinary two-player comparison for every user pair.

    Each pair keeps the same intersection/union semantics as ``compute_vs``.
    Adding another player therefore cannot erase puzzles that two existing
    players can compare.
    """
    user_ids = list(rows_by_user)
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
    puzzle_keys = set()
    comparison_count = 0
    for user1, user2 in combinations(user_ids, 2):
        matchups = compute_vs_matchups(
            rows_by_user[user1],
            rows_by_user[user2],
            score_fn=score_fn,
            missing_is_loss=missing_is_loss,
            best_result_sort_key_fn=best_result_sort_key_fn,
            group_key_fn=group_key_fn,
            missing_result=missing_result,
        )
        for matchup in matchups:
            points1 = matchup['score1']
            points2 = matchup['score2']
            puzzle_keys.add(matchup['key'])
            comparison_count += 1
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
        'comparison_count': comparison_count,
        'pair_count': len(user_ids) * (len(user_ids) - 1) // 2,
        'players': players,
    }

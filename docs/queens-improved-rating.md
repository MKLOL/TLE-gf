# Queens improved rating beta

The `+beta` Queens mode is an on-demand, margin-aware multiplayer Elo
replay. It is separate from the persisted Codeforces-style Queens rating:
testing it cannot alter the ordinary leaderboard or anything that consumes the
ordinary rating.

The full research, holdout, simulation, corruption, and pool-dynamics review
is in [the robustness and inflation audit](queens-improved-rating-audit.md).

## Why the model changed

The first beta used Glicko-2. It expanded one daily placement into one result
against every opponent and treated those correlated comparisons much like
independent games. In the supplied snapshot, that produced first-day changes
as large as `-286` and `+189`. One noisy puzzle should not carry that much
evidence.

Queens also has many close and tied times. A one-second lead on a short Monday
should be weaker evidence than a large gap, even when both leads change the
ordinal rank. Research on score-aware rating models likewise finds that score
differences can add information discarded by win/loss-only systems:
[Score-Based Bayesian Skill Learning](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/sbsl_ecml2012.pdf).
The design also borrows the robust-response goal of native multiplayer systems
such as [Elo-MMR](https://arxiv.org/abs/2101.00400), while staying small and
explainable for a 12–20-player community.

## Soft time bracket

For player `i`, transform the time in seconds:

```text
x_i = ln(time_i + 4)
```

The four-second offset is a low-time noise floor. It prevents a one-second gap
on a very fast puzzle from looking like an enormous percentage difference.

Each other time contributes a soft result, and a neutral self-result anchors
the bracket:

```text
z_ij = clip((x_j - x_i) / 0.35, -8, 8)
A_i = [0.5 + sum(j != i, sigmoid(z_ij))] / n
```

Lower time is better. Equal times contribute exactly `0.5`; a wider gap moves
smoothly toward `1` or `0`. Every person contributes at most `1/n`, so one
extreme fastest or slowest time cannot stretch the middle of the field the way
literal min/max normalization would.

The symmetric `±8` evidence limit activates only beyond a 16.4x adjusted-time
ratio. It prevents numerical certainty and extreme long-run pair separation;
it is not a cap on a player's rating change.

Given the pre-day ratings, the expected score for any candidate rating `r` is:

```text
F(r) = mean(j in field, sigmoid((r - rating_j) / (800 / ln(10))))
```

The rating change is:

```text
delta_i = 144 * (A_i - F(rating_i))
```

The wider `800` expectation scale and `144` K-factor are an exact two-times
re-expression of the original beta's rating points around the unchanged 1200
start. Rating scales have arbitrary units—Microsoft's
[TrueSkill explanation](https://www.microsoft.com/en-us/research/project/trueskill-ranking-system/)
likewise calculates on one scale and multiplies into a useful display range.
Applying the same factor to the expectation curve, update, and performance
preserves every prediction, ordering, tie, and convergence property. It simply
lets sustained skill differences use the rank bands already displayed by
Queens.

Consequences:

- A day with fewer than two players is unrated.
- A larger field does not multiply one puzzle into many independent games.
- Every round is zero-sum, so a fixed retained identity pool cannot create
  nominal points. Active or visible averages can still move through churn,
  selective submission, hidden accounts, or sybils.
- There is no post-processing cap. Because both `A_i` and `F(rating_i)` are
  probabilities, the formula itself keeps `|delta_i|` below the K-factor of
  `144`.
- Inactivity never changes skill.
- New players receive the same bounded update rule as established players.
- Playing more days supplies more evidence but does not award rating by itself;
  participation volume belongs to Queens XP rather than skill rating.
- A malformed locked first time is quarantined from the beta replay after
  first-submission deduplication. It cannot become a zero-second win, promote a
  later retry, or break every `+beta` command.

## Performance

The displayed single-day performance is the unique rating `P_i` where:

```text
F(P_i) = A_i
```

The neutral self-result makes the best and worst performances finite. Tied
times receive the same performance, and:

```text
performance_i > pre_rating_i  exactly when  delta_i > 0
```

For the example field `7, 8, 10, 12, 13, 16, 20, 25` seconds, with everyone
starting at 1200:

| Time | Performance | Rating change |
|---:|---:|---:|
| 7 | 1568 | +34.90 |
| 8 | 1492 | +28.57 |
| 10 | 1360 | +16.25 |
| 12 | 1247 | +4.87 |
| 13 | 1196 | -0.38 |
| 16 | 1061 | -14.23 |
| 20 | 908 | -28.61 |
| 25 | 746 | -41.35 |

The performance drop from 12 to 13 seconds is about 51 points; the drop from
13 to 16 is about 136 points. The larger time gap therefore matters about 2.7
times as much without making either result catastrophic.

## Snapshot replay

The supplied snapshot was read without modification. Its exact live/import
first-submission merge contains 1,378 results, 29 observed users, and 442
puzzle days. Of those days, 384 are solo and provide no rating signal. The 58
rated days contain 994 participant-results and fields of 7–21 players.

| Model | Mean absolute change | 95th percentile | Observed range |
|---|---:|---:|---:|
| New improved beta | 19.65 | 47.42 | -74.60 to +70.90 |
| Ordinary Queens | 14.19 | 32.94 | -38.87 to +57.01 |
| Retired Glicko beta | 27.73 | 70.85 | -286.23 to +188.84 |

The beta preserved exactly `29 × 1200 = 34,800` total points. Final observed
ratings ranged from about `852` to `1643`, and the ordering remained close to
the ordinary ladder (Spearman correlation `0.973`).

On chronological comparisons where both players already had five rated days,
the beta predicted the strict faster/slower order 73.90% of the time versus
74.27% for ordinary Queens. That small loss is the intentional cost of treating
close finishes as weaker evidence. Against the soft margin outcome it is built
to model, the beta's log loss was `0.6214` versus `0.6555` for ordinary Queens.

## Commands

Add `+beta` anywhere in the prefix-command arguments:

```text
;queens ratings +beta
;queens rating +beta
;queens perf +beta
;queens history +beta
;queens results +beta
```

The same views expose an `improved` boolean in their slash-command forms.

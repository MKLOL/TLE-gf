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
x_i = ln(time_i)
```

There is no additive time offset. The bracket therefore measures the raw time
ratio: the same absolute gap carries more evidence on a faster puzzle.

Each other time contributes a soft result, and a neutral self-result anchors
the bracket:

```text
z_ij = clip((x_j - x_i) / 0.35, -8, 8)
S_ij = sigmoid(z_ij)
S_ii = 0.5
```

Lower time is better. Equal times contribute exactly `0.5`; a wider gap moves
smoothly toward `1` or `0`. Every person contributes at most `1/n`, so one
extreme fastest or slowest time cannot stretch the middle of the field the way
literal min/max normalization would.

The symmetric `±8` evidence limit activates only beyond a 16.4x raw-time
ratio. It prevents numerical certainty and extreme long-run pair separation;
it is not a cap on a player's rating change.

Given the pre-day ratings, the expected pair score is:

```text
E(r, rating_j) = sigmoid((r - rating_j) / (800 / ln(10)))
E_ij = E(rating_i, rating_j)
```

Each pair then receives a blended proper-score weight:

```text
W_ij = 0.10 + 0.90 * 4 * E_ij * (1 - E_ij)
delta_i = (108 / n) * sum(j != i, W_ij * (S_ij - E_ij))
```

This is the rating-logit gradient of a 10% cross-entropy / 90% Brier
proper-scoring loss. At an even expectation (`E = 0.5`) it is identical to the
old update. Near a very confident `0` or `1` expectation, one contradictory
day has less leverage, while the 10% cross-entropy floor prevents it from
being ignored. It is a smooth formula, not a post-processing delta cap.

The wider `800` expectation scale keeps sustained skill differences visible
across the existing rank bands, while the `108` K-factor reduces the weight
of one noisy daily puzzle. Rating scales have arbitrary units—Microsoft's
[TrueSkill explanation](https://www.microsoft.com/en-us/research/project/trueskill-ranking-system/)
likewise calculates on one scale and multiplies into a useful display range.
Changing K affects convergence speed and day-to-day volatility, not the
expectation curve or its long-run equilibrium.

Consequences:

- A day with fewer than two players is unrated.
- A larger field does not multiply one puzzle into many independent games.
- Every round is zero-sum, so a fixed retained identity pool cannot create
  nominal points. Active or visible averages can still move through churn,
  selective submission, hidden accounts, or sybils.
- There is no post-processing cap. Every pair score and expectation is a
  probability and `0.1 <= W_ij <= 1`, so the formula itself keeps
  `|delta_i| < 108(n - 1)/n`, which is below the K-factor.
- Inactivity never changes skill.
- New players receive the same bounded update rule as established players.
- Playing more days supplies more evidence but does not award rating by itself;
  participation volume belongs to Queens XP rather than skill rating.
- A malformed locked first time is quarantined from the beta replay after
  first-submission deduplication. It cannot become a zero-second win, promote a
  later retry, or break every `+beta` command.

## Performance

Let the player's mean soft result against the day's field be:

```text
A_i = mean(j in field, S_ij)
F(P) = mean(j in field, E(P, rating_j))
```

The displayed performance is the unique rating `P_i` satisfying:

```text
F(P_i) = A_i
```

`F` is strictly increasing. Better effective results therefore always produce
higher performance, and identical results always produce identical
performance, regardless of incoming rating. The neutral self-result keeps the
best and worst values finite.

This display inversion intentionally differs from minimizing the blended
update loss as a function of `P`. That composite can have several local
minima, which can make independently selected branches rank a slower result
above a faster one. Beta performance describes result strength against the
common field; the robust gradient separately determines the rating change.
Because the update gives each opponent an expectation-dependent robustness
weight while performance uses the unweighted field score, an unusual rating
spread can make `performance - pre_rating` and the delta have different signs.
That is not a result-order inversion: performance still ranks the day
monotonically.

For the example field `7, 8, 10, 12, 13, 16, 20, 25` seconds, with everyone
starting at 1200:

| Time | Performance | Rating change |
|---:|---:|---:|
| 7 | 1668 | +31.69 |
| 8 | 1558 | +25.58 |
| 10 | 1381 | +13.79 |
| 12 | 1242 | +3.26 |
| 13 | 1182 | -1.43 |
| 16 | 1025 | -13.34 |
| 20 | 853 | -24.96 |
| 25 | 673 | -34.59 |

The performance drop from 12 to 13 seconds is about 60 points; the drop from
13 to 16 is about 157 points. The larger time gap therefore matters about 2.6
times as much without making either result catastrophic.

## Akari accuracy multiplier

Akari `+beta` feeds an accuracy-adjusted time into this same engine:

```text
effective_time = raw_time * sqrt(101 - accuracy)
```

This makes 100% `1x`, 99% about `1.414x`, 90% about `3.32x`, and 68% about
`5.74x`. A 99% result must be approximately 29% faster to tie a perfect
result. Accuracy therefore matters even at 99%, but there is no hard
perfect/imperfect cliff and an exceptional time can overcome the penalty.

In `;akari results +beta`, rows are sorted by beta performance rather than the
ordinary accuracy-first result key. Performance is shown as a whole number,
so values that round to the same displayed number receive the same competition
rank. This makes the visible order, value, and rank agree.

## Snapshot replay

The supplied snapshot was read without modification. Its exact live/import
first-submission merge contains 1,378 results, 29 observed users, and 442
puzzle days. Of those days, 384 are solo and provide no rating signal. The 58
rated days contain 994 participant-results and fields of 7–21 players.

| Model | Mean absolute change | 95th percentile | Observed range |
|---|---:|---:|---:|
| Previous K=144 beta | 19.25 | 45.10 | -64.69 to +63.69 |
| Ordinary Queens | 14.19 | 32.94 | -38.87 to +57.01 |
| Retired Glicko beta | 27.73 | 70.85 | -286.23 to +188.84 |

The previous K=144 replay preserved exactly `29 × 1200 = 34,800` total points.
Final observed
ratings had mean `1200.00`, standard deviation `198.27`, and range
`814.37–1681.00`. The largest per-day zero-sum rounding error was
`3.38e-14`.

Across the replay there were no performance-order inversions, and every exact
result tie received identical performance.

The earlier chronological prediction figures were measured before the 90%
Brier / 10% log-loss blend. They have not been rerun and are not presented as
current predictive evidence. The older offset-four audit remains useful only
for the historical model and protocol it explicitly labels.

## Commands

Add `+beta` anywhere in the prefix-command arguments:

```text
;queens ratings +beta
;queens rating +beta
;queens perf +beta
;queens history +beta
;queens results +beta
```

The same views expose a `beta` boolean in their slash-command forms.

Akari exposes the matching beta views:

```text
;akari ratings +beta
;akari rating +beta
;akari perf +beta
;akari history +beta
;akari results +beta
```

The beta replay is transient for both games: it does not overwrite the
ordinary Queens ladder or Akari rating snapshot.

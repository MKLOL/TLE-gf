# Queens improved rating: robustness and inflation audit

## Scope and decision

This audit covers the opt-in Queens `+beta` engine. Akari `+beta` reuses its
zero-sum pair evidence and field correction with an accuracy-first,
opponent-relative hybrid score and a separate hierarchical score for displayed
event performance.
Ordinary Queens, ordinary Akari, persisted rating snapshots, registration
policy, and command routing are outside the formula change.

The current player-facing parameters are:

- start rating `1200`;
- time offset `0` seconds;
- soft-margin width `0.35`;
- pair-result blend: 85% continuous margin and 15% hard head-to-head result;
- expectation scale `800 / ln(10)` and K-factor `124` for Queens;
- expectation scale `700 / ln(10)` and K-factor `108.5` for Akari;
- proper-score blend: 10% cross-entropy gradient and 90% Brier gradient;
- field correction: `0.25` points per rated participant;
- no field-size multiplier or post-hoc delta cap.

The original research and robustness tournament used a four-second offset.
The offset was removed by a later user-directed retune, making fast-puzzle
gaps deliberately stronger. Structural proofs in this document still hold,
but empirical sections explicitly marked historical were not rerun and are not
evidence for choosing the raw-time transform. Production hardening remains:

- invalid locked-first times are quarantined from the beta replay instead of
  becoming zero-second wins or breaking the command;
- pair evidence is limited to a logit of `±8`, preventing numerical complete
  separation at absurd time ratios;
- deterministic property tests enforce the mathematical guarantees below.

In the original offset-four audit, the evidence limit changed no rank, changed
the largest final rating by less than `0.05`, and had no meaningful predictive
effect. It remains a numerical and long-run safety rail, not a rating-change
cap.

## Data and evaluation protocol

The supplied `queens_snapshot.db` was read without modification. The live and
imported tables were merged exactly like the bot: a live row supersedes its
matching imported row, then the earliest message for each user and puzzle is
locked.

| Quantity | Value |
|---|---:|
| Merged results | 1,378 |
| Observed players | 29 |
| Puzzle days | 442 |
| Rated days (at least two players) | 58 |
| Rated participant-results | 994 |
| Field size | 7–21 (median 18) |
| Days containing a tied pair | 48 of 58 |

The effective independent sample is much closer to 58 days than to the
thousands of pair comparisons derived from them. The original offset-four
research therefore used:

1. chronological predictions made before each daily update;
2. warm-up requirements for established-player comparisons;
3. chronological development blocks and an untouched final holdout;
4. paired bootstraps that resample whole puzzle days;
5. both strict faster/slower and continuous log-time metrics;
6. injected corruptions, leave-one-day/player replay, null permutations, and
   long stationary simulations.

That protocol was not rerun after the raw-time, 90/10-gradient, or 85/15
head-to-head retunes. The structural proofs below apply to the current formula;
all snapshot, predictive, and corruption figures are explicitly historical. In
the original study, log
loss and Brier score were used because they are proper probabilistic scoring
rules, following
[Gneiting and Raftery (2007)](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf).
Accuracy alone cannot distinguish calibrated confidence from overconfidence.

## Fixed point-scale calibration

The display coordinates were checked across historical replay checkpoints,
not chosen to make one final snapshot equal. Queens retains its established
`2.0` coordinate (`800`-point 10:1 gap, K=`124`); Akari uses the rounded `1.75`
coordinate (`700`-point gap, K=`108.5`). Across all exported participants in
the supplied snapshots, replayed with production formula semantics on
2026-08-05, normal versus beta population standard deviation was `151.81`
versus `153.37` for Akari and `187.87` versus `188.72` for Queens. The Queens
export lacks registration and ban state, so this is not a reconstruction of
the public-board subset. Means and individual ratings still differ because the
evidence, decay, and correction policies differ. No ongoing centering,
variance matching, or leaderboard-dependent multiplier is applied.

## Exact model

For a Queens field of `n` players, transform player `i`'s time:

```text
x_i = ln(time_i)
```

Queens continuous margin evidence and hard result are:

```text
z_ij = clip((x_j - x_i) / 0.35, -8, 8)
M_ij = sigmoid(z_ij)
R_ij = 1 if time_i < time_j, 0 if time_i > time_j, else 0.5
S_ij = 0.85 * M_ij + 0.15 * R_ij
```

Akari uses the same hybrid score for equal accuracy. For unequal accuracy,
identify the lower-accuracy result `L` and higher-accuracy result `H`:

```text
adjusted_time_L = time_L + time_H
M_LH = soft_time(adjusted_time_L, time_H)
M_HL = 1 - M_LH
R_LH = 0
R_HL = 1
S_ij = 0.85 * M_ij + 0.15 * R_ij
```

Equivalently, the lower player's unclipped logit numerator is
`-ln(1 + time_L / time_H)`. Higher accuracy therefore always wins the direct
pair; a faster lower-accuracy result can approach but never cross a tie, and a
slower one is never rewarded. Accuracy must be an integer from 0 through 100.
Every nonzero accuracy difference uses the same tier rule, and the perfect
flag adds no separate tier.

For Queens, expected score and robust update weight are:

```text
E_ij = sigmoid((rating_i - rating_j) / (800 / ln(10)))
W_ij = 0.10 + 0.90 * 4 * E_ij * (1 - E_ij)
```

Akari substitutes `700 / ln(10)` in the expectation. This is a fixed,
rounded coordinate calibration rather than a replay-by-replay normalization.

Lower time is better for Queens and within an Akari accuracy tier. Across
Akari tiers, accuracy determines the winner and time determines the margin.
The pairwise update and final contest delta are:

```text
raw_delta_i = (124 / n) * sum(j != i, W_ij * (S_ij - E_ij))
c = -mean(raw_delta) - 0.25
delta_i = raw_delta_i + c
```

Akari substitutes the proportionally matched K-factor `108.5`. Within the raw
contest model, moving the expectation gap, K-factor, performance search span,
and ratings together preserves the latent probabilities and normalized update
dynamics. The fixed `0.25` anti-churn policy remains a separate displayed-point
shift.

The weight is the rating-logit gradient of a 10% cross-entropy / 90% Brier
blend of two strictly proper scoring losses. It equals `1` at an even matchup
and approaches `0.10` at a very confident expectation. A surprising result is
therefore attenuated smoothly but never ignored.

The implementation includes a neutral self-comparison. Its score and
expectation are both `0.5` at the incoming rating, so it contributes zero to
the update and explains the denominator `n`.

For Queens, the `±8` limit activates when the raw-time ratio exceeds:

```text
exp(0.35 * 8) = 16.445
```

For unequal-accuracy Akari pairs it clips the adjusted ratio
`1 + time_L / time_H`; equivalently, the raw lower/higher time ratio must
exceed `15.445`.

After the 85/15 blend, it bounds one strict rating-update pair score to roughly
`[0.000285, 0.999715]`; exact ties remain `0.5`. It is not a cap on rating
change. The response already saturates well before that point; the limit
prevents a corrupt or repeatedly absurd margin from implying numerical
certainty and unlimited pair separation. Akari's display-only hierarchy may
use exact `0` or `1`; those values never enter a delta, and the neutral
self-score keeps their field means strictly interior.

## Proven guarantees

### Raw conservation and field correction

For every pair:

```text
S_ij + S_ji = 1
E_ij + E_ji = 1
W_ij = W_ji
```

The evidence clip is symmetric, so it preserves this identity. Pair residuals
cancel:

```text
sum(raw_delta_i over the field) = 0
```

The correction recenters floating residue and subtracts the same `0.25` from
each rated participant. It preserves all within-round delta differences and
produces:

```text
sum(delta_i over the field) = -0.25 * n
total rating = 1200 * observed players - 0.25 * rated participations
```

Decay transfers remain zero-sum. Solo days pay no correction. The stronger-
participant Codeforces correction is deliberately absent.

The historical snapshot figures below predate this field policy; their exact
`34,800 = 29 × 1200` total and `3.38e-14` largest daily error describe the
retired zero-sum replay, not current output.

### Natural daily bound

Each pair residual lies strictly between `-1` and `1`, and there are `n - 1`
non-self terms:

```text
abs(delta_i + 0.25) < K * (n - 1) / n
```

Here `K` is `124` for Queens and `108.5` for Akari. The Queens raw component
is below 113.7 points in a 12-player field and below 117.8 in a 20-player
field; final magnitude can be `0.25` larger. There is no post-processing delta
clamp.

### One-time contamination bound

If one participant `k`'s result changes while the field and pre-ratings stay
fixed, every comparison not involving `k` is bit-for-bit unchanged. For any
other player `i`, only one term can move:

```text
abs(delta_i_after - delta_i_before) <= K / n
```

For Queens, the limit is 10.34 points at `n = 12` and 6.2 at `n = 20`; Akari's
limits are proportionally smaller. The changed player's own result affects
`n - 1` terms, so their own update can move by almost the full natural daily
bound. The formula protects the rest of the field more strongly than it
protects the owner of a corrupt record.

These are immediate-day guarantees. A wrong rating can affect later
expectations, so full-history corruption can propagate.

### Monotone event performance

Displayed performance inverts a player's mean field score:

```text
A_i = mean(j in field, Q_ij)
F(P) = mean(j in field, E(P, rating_j))
F(P_i) = A_i
```

For Queens, `Q_ij = S_ij`. Akari deliberately uses a display-only hierarchy:

```text
Q_ij = 1                         if accuracy_i > accuracy_j
Q_ij = 0                         if accuracy_i < accuracy_j
Q_ij = hybrid_time(time_i, time_j) if accuracy_i = accuracy_j
```

This display score never enters the rating delta. `F` is strictly increasing,
so the inverse is unique. Queens performance follows time; Akari performance
follows accuracy descending and then time ascending. The neutral self-score
keeps every target inside `(0, 1)` and every displayed result finite. Exact
result ties share one performance regardless of incoming rating.

For an Akari accuracy tier of `m` players with `B` players in lower tiers, the
unnormalized display total lies between `B + 0.5` and `B + m - 0.5`. Adjacent
tiers are therefore separated by at least one pair point, or `1/n` after the
field mean. Within a tier, the hybrid-time score is strictly monotone. This proves
the stated accuracy/time ordering for every field, not only observed data.

Adding the same constant to every pre-rating leaves all deltas unchanged and
adds that constant to every performance. Equal times share the same
performance; equal-time players with different ratings can still receive
different deltas because their expectations differed.

The display does not minimize the robust update loss over `P`: that composite
can have multiple local minima and independently chosen branches can invert
result order. The unique field inversion avoids that ambiguity. The historical
Queens snapshot preserved every strict result comparison, and every exact
result tie shared one performance under the formula tested at that time.

The delta applies opponent-specific `W_ij` values while performance uses an
unweighted mean display score. In unusually spread fields, delta and
`performance - pre_rating` can therefore have opposite signs without changing
the day's performance order. In the historical merged live/import replay this
occurred in 13 of 994 contested Queens performances (1.31%); the largest
opposite-direction offset was 19.06 rating points.

## Historical snapshot results

These results predate the current 85/15 head-to-head blend. The source snapshot
is not in this repository, so they were not recomputed for this change.

| Measure | Previous K=144 beta |
|---|---:|
| Final mean | 1200.00 |
| Final range | 814.37–1681.00 |
| Final standard deviation | 198.27 |
| Mean absolute daily change | 19.25 |
| 95th percentile absolute change | 45.10 |
| Observed daily range | −64.69 to +63.69 |

The previously reported chronological accuracy, log-loss, and Brier figures
predated the current 90/10 proper-score blend. They were not rerun and are not
valid measurements of the current implementation. The model's probabilities
also represent margin-weighted results rather than literal faster/slower win
odds.

### Akari snapshot cross-check

The supplied Akari snapshot figures below are historical: they use the retired
K=144 square-root accuracy multiplier, not the current additive pair score.
They remain only as a record of the earlier experiment and are not evidence
for the current Akari policy:

| Measure | Previous K=144 Akari beta |
|---|---:|
| Final mean | 1200.00 |
| Final range | 830.59–1878.89 |
| Final standard deviation | 202.43 |
| Loss magnitude, 95th / 99th / worst | 53.75 / 61.91 / 70.49 |
| Gain, 95th / 99th / best | 46.93 / 58.28 / 71.14 |

The largest per-day zero-sum error in that retired replay was `6.39e-14`.
Under its old effective-time order, performance had no strict-order inversions
and every exact result tie shared one performance. Delta/performance direction
differed in 45 of 4,149 contested performances (1.08%); the largest
opposite-direction offset was 54.56 points.

Current `+beta` Akari result tables sort explicitly by accuracy descending and
time ascending. The hierarchical display score proves that exact performance
has the same order; exact `(accuracy, time)` ties share a competition rank.
Ordinary Akari result ordering is unchanged outside `+beta`.

## Historical alternative-model tournament (four-second offset)

The search covered:

- 300 combinations of offset, width, K, and field-size scaling;
- 280 broad and 567 fine combinations including 70–100% soft blends;
- strict rank-only pairwise Elo;
- a partial-ranking Plackett–Luce baseline;
- smooth and clipped robust transforms.

The independent final tournament used four chronological development blocks
and an untouched 12-day holdout.

| Model | Holdout strict LL | Holdout soft LL | Final SD | Corruption p95 |
|---|---:|---:|---:|---:|
| Previous offset-four pure soft | 0.55085 | 0.62712 | 176.80 | 60.66 |
| 95% soft + 5% strict | 0.54911 | 0.62730 | 181.71 | 61.10 |
| Strict rank-only | 0.53051 | 0.64996 | 289.35 | 71.87 |
| Partial Plackett–Luce | 0.54352 | 0.63407 | 230.60 | — |

“Corruption p95” is the 95th percentile of the largest final-player rating
error after replacing one observed time with 1 or 86,400 seconds.

The 95/5 blend's strict-log-loss gain was real but tiny. Accuracy changed by
less than 0.1 percentage point, margin calibration worsened slightly, ordinary
movement increased, and corruption tails did not improve. It also adds a
discontinuous rank step to near-tied times.

That historical 95% soft-result / 5% strict-result experiment used an older
time offset and optimizer. It tests the same broad target family but does not
validate the current user-directed 85/15 choice. The 90% Brier / 10% log-loss
gradient blend is a separate robustness mechanism.

Adding hard-result weight changes the product goal: strict wins matter even
when their time margins are tiny. Recent research also finds that simple Elo
can outperform more complex models on sparse data despite misspecification
([Tang, Wang, and Jin, 2025](https://arxiv.org/abs/2502.10985)).

The literature supports testing score information, but not raw,
uncentered margin multipliers. Kovalchik's tennis study found score-aware Elo
variants useful, while only its separately centered joint-additive variant had
stable, unbiased simulation behavior
([Kovalchik, 2020](https://doi.org/10.1016/j.ijforecast.2020.01.006)).
[G-Elo](https://arxiv.org/abs/2010.11187) likewise derives observed-minus-
expected margin updates from an explicit probability model.

## Historical robustness trials (four-second offset)

- Removing one rated day gave median final-rating RMS movement `3.37`, 95th
  percentile `14.78`, and worst final rank correlation `0.9926`.
- Removing one player gave median RMS movement `5.01`, 95th percentile `13.71`,
  and worst rank correlation `0.9951`. The largest effect on another player was
  `23.68` points.
- A real `3,827`-second outlier was already saturated. Replacing it with
  `1,000,000` seconds changed its own final rating by `0.00004` and anyone
  else's by at most `0.000004`.
- In 30 equal-skill, 10,000-day simulations, the mean stayed 1200 within
  `5e-12` and rating SD stabilized around 52–54 rather than wandering upward.
- Across 1,000 within-day time-permutation null replays, final SD averaged
  `57.4`; its 97.5th percentile was `72.4`. The then-observed `176.8`
  separation was persistent signal rather than a null random walk.
- In 100 synthetic seasons, corrupting 1%, 5%, and 10% of times produced final
  RMS errors of `17.1`, `41.0`, and `59.7`, with rank correlations `0.988`,
  `0.951`, and `0.913`. Total points remained conserved.

## Inflation and manipulation limits

Mark Glickman's analysis of chess ratings distinguishes closed-pool point
conservation from entry/exit and cohort effects
([Chess Rating Systems](https://www.glicko.net/research/crs.pdf)). The same
distinction applies here.

The formula cannot by itself prevent:

- the visible mean rising when low-rated accounts become hidden or inactive;
- a player submitting only unusually good days;
- sybil or colluding accounts donating their starting points;
- peak fields remaining historical maxima even while current rating decays;
- historical rewrites when current registration filters old fields;
- the whole community improving together on an absolute scale.

In the historical pre-correction stress trials, submitting only times no slower than one's trailing personal
median raised ratings by a median `34` and as much as `140`. Ten fresh losing
accounts could give a roughly 1221-rated beneficiary about `64` points in one
day. Their pairwise transfers remain balanced in the current model, while the
field correction additionally removes `0.25` per rated participant. Withheld
bad days still create selection bias.

Stronger defenses require product policy—mandatory capture, activity
requirements, identity trust, or stable anonymized historical competitors.
The beta's active-day decay now limits above-start current-rating parking, but
does not solve selective submission, sybils, or historical peak parking.
Ordinary Queens remains deliberately unaffected.

This is also why richer systems were not transplanted. TrueSkill tracks
uncertainty and handles multiplayer rankings through approximate message
passing ([Herbrich, Minka, and Graepel, 2007](https://www.microsoft.com/en-us/research/publication/trueskilltm-a-bayesian-skill-rating-system/));
Elo-MMR proves robust response and aligned incentives for large ranked fields
([Ebtekar and Liu, 2021](https://arxiv.org/abs/2101.00400)); and score-based
Bayesian models can use extra outcome information
([Guo et al., 2012](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/sbsl_ecml2012.pdf)).
For only 58 rated Queens days, their added uncertainty and parameters did not
outweigh loss of raw pair conservation, transparent field accounting,
time-margin semantics, or simplicity.

## Release invariants

Future changes to `+beta` must retain:

- ordinary Queens isolation and no rating-table writes;
- deterministic first-submission locking;
- invalid-time quarantine after that lock;
- solo days producing no contest delta while remaining eligible to receive a
  zero-sum decay transfer;
- exact ties and tied performance;
- pair complement and raw round point conservation;
- exactly `0.25` field deflation per rated participant, with no
  strongest-player correction;
- concluded-active-day decay only above 1200, with current-day protection and
  equal redistribution to valid participants;
- the 85% continuous-margin / 15% hard-result pair target;
- the `K(n - 1)/n` raw contest-delta bound before field correction or decay;
- the `K/n` one-opponent contamination bound;
- contest-update rating-translation invariance before fixed-anchor decay;
- unique, result-monotone event performance;
- Akari accuracy validation and additive, complementary rating pair scores;
- Akari hierarchical performance ordered by accuracy, then time;
- Akari `+beta` result ordering and exact-tie ranks based on `(accuracy, time)`;
- deterministic replay under arbitrary input ordering.

Any future retuning needs substantially more rated days, a preregistered
chronological holdout, whole-day uncertainty intervals, and a robustness
constraint. A leaderboard that merely looks attractive is not validation.

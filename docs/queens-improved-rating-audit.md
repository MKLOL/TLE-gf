# Queens improved rating: robustness and inflation audit

## Scope and decision

This audit covers only the opt-in Queens `+improved` engine. Ordinary Queens,
Akari, persisted rating snapshots, registration policy, and command routing are
outside the formula change.

The decision is to retain the current model and its player-facing parameters:

- start rating `1200`;
- time offset `4` seconds;
- soft-margin width `0.35`;
- expectation scale `800 / ln(10)`;
- K-factor `144`;
- no field-size multiplier or post-hoc delta cap.

Hundreds of alternatives improved literal win/loss confidence only by making
close times harsher and the ladder wider. None materially improved both
prediction and robustness. The production hardening is deliberately narrow:

- invalid locked-first times are quarantined from the beta replay instead of
  becoming zero-second wins or breaking the command;
- pair evidence is limited to a logit of `±8`, preventing numerical complete
  separation at absurd time ratios;
- deterministic property tests enforce the mathematical guarantees below.

On the supplied snapshot the evidence limit changes no rank, changes the
largest final rating by less than `0.05`, and has no meaningful predictive
effect. It is a numerical and long-run safety rail, not a prediction tweak.

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
thousands of pair comparisons derived from them. Evaluation therefore used:

1. chronological predictions made before each daily update;
2. warm-up requirements for established-player comparisons;
3. chronological development blocks and an untouched final holdout;
4. paired bootstraps that resample whole puzzle days;
5. both strict faster/slower and continuous adjusted-log-time metrics;
6. injected corruptions, leave-one-day/player replay, null permutations, and
   long stationary simulations.

Log loss and Brier score were used because they are proper probabilistic
scoring rules, following
[Gneiting and Raftery (2007)](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf).
Accuracy alone cannot distinguish calibrated confidence from overconfidence.

## Exact model

For a field of `n` players, transform player `i`'s time:

```text
x_i = ln(time_i + 4)
```

For each pair:

```text
z_ij = clip((x_j - x_i) / 0.35, -8, 8)
S_ij = sigmoid(z_ij)
E_ij = sigmoid((rating_i - rating_j) / (800 / ln(10)))
```

Lower time is better. The update is:

```text
delta_i = (144 / n) * sum(j != i, S_ij - E_ij)
```

The implementation includes the player's neutral self-comparison in both
field averages. Both self terms are `0.5`, so they cancel and only explain the
denominator `n`.

The `±8` limit activates only when the adjusted-time ratio exceeds:

```text
exp(0.35 * 8) = 16.445
```

It bounds one pair score to `[0.000335, 0.999665]`. It is not a cap on rating
change. The ordinary probability response already saturates well before that
point; the limit prevents a corrupt or repeatedly absurd margin from implying
numerical certainty and unlimited pair separation.

## Proven guarantees

### Point conservation

For every pair:

```text
S_ij + S_ji = 1
E_ij + E_ji = 1
```

The evidence clip is symmetric, so it preserves this identity. Pair residuals
cancel:

```text
sum(delta_i over the field) = 0
```

Every newly observed player starts at 1200. If all observed identities remain
in the replay, induction gives:

```text
total rating = 1200 * observed player count
```

The snapshot total is exactly `34,800 = 29 × 1200`, up to floating arithmetic
below `1.1e-13` per day.

This is **point conservation**, not a claim that every visible or active
leaderboard is inflation-proof.

### Natural daily bound

Each pair residual lies strictly between `-1` and `1`, and there are `n - 1`
non-self terms:

```text
abs(delta_i) < 144 * (n - 1) / n
```

That is below 132 points in a 12-player field and below 136.8 in a 20-player
field. There is no post-processing delta clamp.

### One-time contamination bound

If one participant `k`'s time changes while the field and pre-ratings stay
fixed, every comparison not involving `k` is bit-for-bit unchanged. For any
other player `i`, only one term can move:

```text
abs(delta_i_after - delta_i_before) <= 144 / n
```

The limit is 12 points at `n = 12` and 7.2 at `n = 20`. The changed player's
own time affects `n - 1` terms, so their own update can move by almost the full
natural daily bound. The formula protects the rest of the field more strongly
than it protects the owner of a corrupt record.

These are immediate-day guarantees. A wrong rating can affect later
expectations, so full-history corruption can propagate.

### Stable performance and updates

Displayed performance is the unique rating `P_i` whose expected field score
equals the actual soft score. The field-expectation function is strictly
increasing, so:

```text
performance_i > pre_rating_i  exactly when  delta_i > 0
```

The derivative of a logistic expectation is at most
`1 / (4 * (800 / ln(10)))`. Therefore an update cannot overshoot its
performance:

```text
abs(delta_i)
    <= 144 / (4 * (800 / ln(10)))
       * abs(performance_i - pre_rating_i)
    < 0.104 * abs(performance_i - pre_rating_i)
```

Adding the same constant to every pre-rating leaves all deltas unchanged and
adds that constant to every performance. Equal times share the same
performance; equal-time players with different ratings can receive different
deltas because their expectations differed.

## Snapshot results

| Measure | Current beta |
|---|---:|
| Final mean | 1200.00 |
| Final range | 851.55–1643.42 |
| Final standard deviation | 176.80 |
| Mean absolute daily change | 19.65 |
| 95th percentile absolute change | 47.42 |
| Observed daily range | −74.60 to +70.90 |

Chronological established-player evaluation produced 6,716 strict pair
comparisons:

| Measure | Result | Whole-day bootstrap 95% interval |
|---|---:|---:|
| Strict accuracy | 73.96% | 72.10–75.53% |
| Strict log loss | 0.54795 | 0.5356–0.5621 |
| Strict Brier score | 0.18275 | — |
| Fixed soft-margin log loss | 0.62143 | 0.6080–0.6344 |
| Fixed soft-margin Brier score | 0.05952 | — |

The model's probabilities represent margin-weighted results, not literal win
odds. They are intentionally underconfident against binary wins while being
well calibrated to the soft-margin target.

## Alternative-model tournament

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
| Current pure soft | 0.55085 | 0.62712 | 176.80 | 60.66 |
| 95% soft + 5% strict | 0.54911 | 0.62730 | 181.71 | 61.10 |
| Strict rank-only | 0.53051 | 0.64996 | 289.35 | 71.87 |
| Partial Plackett–Luce | 0.54352 | 0.63407 | 230.60 | — |

“Corruption p95” is the 95th percentile of the largest final-player rating
error after replacing one observed time with 1 or 86,400 seconds.

The 95/5 blend's strict-log-loss gain was real but tiny. Accuracy changed by
less than 0.1 percentage point, margin calibration worsened slightly, ordinary
movement increased, and corruption tails did not improve. It also adds a
discontinuous rank step to near-tied times.

The aggressive models gain binary confidence by changing the product goal.
They do not provide a better version of the requested close-times-as-near-ties
rating. Recent research also finds that simple Elo can outperform more complex
models on sparse data despite model misspecification
([Tang, Wang, and Jin, 2025](https://arxiv.org/abs/2502.10985)).

The literature supports testing score information, but not raw,
uncentered margin multipliers. Kovalchik's tennis study found score-aware Elo
variants useful, while only its separately centered joint-additive variant had
stable, unbiased simulation behavior
([Kovalchik, 2020](https://doi.org/10.1016/j.ijforecast.2020.01.006)).
[G-Elo](https://arxiv.org/abs/2010.11187) likewise derives observed-minus-
expected margin updates from an explicit probability model.

## Robustness trials

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
  `57.4`; its 97.5th percentile was `72.4`. The observed `176.8` separation is
  persistent signal, not a null random walk.
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
- peak parking when inactivity never changes rating;
- historical rewrites when current registration filters old fields;
- the whole community improving together on an absolute scale.

In stress trials, submitting only times no slower than one's trailing personal
median raised ratings by a median `34` and as much as `140`. Ten fresh losing
accounts could give a roughly 1221-rated beneficiary about `64` points in one
day. Both attacks still conserve total points: balancing losses remain in
submitted-round opponents or donor accounts, while withheld bad days create
selection bias.

Solving those issues requires product policy—mandatory capture, activity
requirements, decay, identity trust, or stable anonymized historical
competitors. Those choices affect registration, privacy, bans, and potentially
ordinary Queens. They are not safe to smuggle into this beta formula.

This is also why richer systems were not transplanted. TrueSkill tracks
uncertainty and handles multiplayer rankings through approximate message
passing ([Herbrich, Minka, and Graepel, 2007](https://www.microsoft.com/en-us/research/publication/trueskilltm-a-bayesian-skill-rating-system/));
Elo-MMR proves robust response and aligned incentives for large ranked fields
([Ebtekar and Liu, 2021](https://arxiv.org/abs/2101.00400)); and score-based
Bayesian models can use extra outcome information
([Guo et al., 2012](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/sbsl_ecml2012.pdf)).
For only 58 rated Queens days, their added uncertainty and parameters did not
outweigh loss of exact conservation, time-margin semantics, or simplicity.

## Release invariants

Future changes to `+improved` must retain:

- ordinary Queens isolation and no rating-table writes;
- deterministic first-submission locking;
- invalid-time quarantine after that lock;
- solo days producing no rating signal;
- exact ties and tied performance;
- pair complement and round point conservation;
- the `144(n - 1)/n` daily bound;
- the `144/n` one-opponent contamination bound;
- rating-translation invariance;
- delta/performance sign agreement and no overshoot;
- deterministic replay under arbitrary input ordering.

Any future retuning needs substantially more rated days, a preregistered
chronological holdout, whole-day uncertainty intervals, and a robustness
constraint. A leaderboard that merely looks attractive is not validation.

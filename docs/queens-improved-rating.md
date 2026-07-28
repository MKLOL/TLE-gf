# Queens improved rating beta

The `+improved` Queens mode is an on-demand Glicko-2 replay. It is separate
from the persisted Codeforces-style Queens rating, so testing it cannot alter
the established leaderboard or any feature that consumes the established
rating.

## Why Glicko-2

The existing model stores one rating number. Glicko-2 additionally tracks a
rating deviation (uncertainty) and volatility for each player. A new or
returning player's result can therefore move their rating more than the same
result would move a well-established player's rating.

Each contested Queens day is one Glicko-2 rating period. A player's placement
is expanded into simultaneous head-to-head outcomes against that day's other
participants: win for a faster time, draw for an equal time, and loss for a
slower time. All players are updated from the same pre-day snapshot. Days with
zero or one participant are not rating periods and have no effect.

The beta uses these parameters:

- Initial rating: `1200`
- Initial rating deviation: `100`
- Initial volatility: `0.06`
- System constant (tau): `0.5`
- Convergence tolerance: `0.000001`

The equations are the ones in the
[official Glicko-2 specification](https://www.glicko.net/glicko/glicko2.pdf),
with the rating scale translated from 1500 to 1200. The beta performance shown
for one day is the specification's estimated pre-to-performance improvement
(`Delta`) converted back to the public rating scale.

Glicko-2 is still fundamentally a match model rather than a native
free-for-all model. Queens pair outcomes from one day are correlated, so this
mode is deliberately labeled `testing beta`. Native multiplayer alternatives
considered were
[TrueSkill](https://www.microsoft.com/en-us/research/publication/trueskilltm-a-bayesian-skill-rating-system-2/)
and the
[Weng-Lin Plackett-Luce model](https://jmlr.csail.mit.edu/papers/volume12/weng11a/weng11a.pdf).

## Snapshot replay

The supplied snapshot was read without modification. Its canonical
first-submission merge contains 1,378 rows, 29 observed users, and 442 puzzle
days. Of those days, 384 are solo and provide no rating signal. The remaining
58 contests have 7–21 participants (17.14 on average). Equal-time finishes
occur on 48 of the 58 contested days, so preserving draws is important.

Chronological next-contest pairwise forecasts over 7,938 non-tied comparisons
gave:

| Model | Accuracy | Brier score | Log loss |
|---|---:|---:|---:|
| Existing Codeforces-style | 72.58% | 0.1827 | 0.5425 |
| Glicko-2 beta | 73.00% | 0.1782 | 0.5308 |

Lower is better for Brier score and log loss. On only the final 28 contests,
the two systems were effectively tied and the existing model was slightly
ahead. The data therefore supports testing Glicko-2 for its uncertainty
handling, not claiming a decisive predictive victory.

After all 58 contested days, replaying every observed identity produced beta
ratings from about 834 to 1,593 (mean 1,122, median 1,058). The snapshot has no
registration table, so these are research aggregates rather than the exact
public leaderboard the bot will show.

## Commands

Add `+improved` anywhere in the prefix-command arguments:

```text
;queens ratings +improved
;queens rating +improved
;queens perf +improved
;queens history +improved
;queens results +improved
```

The same views expose an `improved` boolean in their slash-command forms.

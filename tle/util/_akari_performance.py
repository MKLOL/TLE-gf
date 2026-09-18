"""Codeforces seed primitives and the displayed event performance.

Split out of ``akari_rating`` to keep that module under the 500-line limit;
``akari_rating`` re-exports every name here, so ``akari_rating.<name>`` still
resolves for existing callers.

Two different inversions of the same logistic model live here:

* :func:`_needed_rating` drives the *rating change*.  It targets the
  geometric-mean rank ``sqrt(actual * expected)``, which is Codeforces' own
  half-step toward the observed result.
* :func:`event_performance` produces the *displayed* performance.  It targets
  the observed rank directly, so the number means what a reader assumes it
  means: the rating at which finishing here would have been par.

Keeping them separate is deliberate.  The obvious shortcut — recovering a
performance from the delta as ``2 * need - rating`` — is only a first-order
(log-linear) approximation of the second inversion, and its error is a
function of the player's own rating.  That leaks identity into a number that
should describe the result: two players with the *same* finishing time on the
same day printed performances up to 160 points apart.
"""

# Codeforces logistic scale: a 400-point gap ⇒ ~10x odds.
_RATING_SCALE = 400.0
# Bounds and iteration count for the rating binary searches.  25 bisections
# over [1, 8000] resolve to < 3e-4 — far finer than any rating difference, while
# keeping a full-history replay cheap enough to run on every result change.
_SEARCH_LO = 1.0
_SEARCH_HI = 8000.0
_SEARCH_ITERS = 25


def _pow10(rating):
    """``10 ** (rating / 400)`` — precomputed per player so the seed sums below
    contain no ``pow`` calls (P(b beats a) = x_b / (x_a + x_b) where x = 10^(R/400))."""
    return 10.0 ** (rating / _RATING_SCALE)


def _expected_seed(x_self, pow_others):
    """Codeforces "seed": the expected rank of a player whose ``_pow10`` is ``x_self``.

    seed = 1 + Σ P(other ranks above me) = 1 + Σ x_other / (x_self + x_other).
    ``pow_others`` is the list of the *other* players' ``_pow10`` values.
    Monotonically decreasing in the player's rating.
    """
    seed = 1.0
    for x_other in pow_others:
        seed += x_other / (x_self + x_other)
    return seed


def _needed_rating(pow_others, target_seed):
    """Binary-search the rating whose :func:`_expected_seed` equals ``target_seed``.

    The seed decreases as rating rises, so when the seed at ``mid`` is below the
    target we have overshot and search lower.
    """
    lo, hi = _SEARCH_LO, _SEARCH_HI
    for _ in range(_SEARCH_ITERS):
        mid = (lo + hi) / 2.0
        if _expected_seed(_pow10(mid), pow_others) < target_seed:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2.0


def _expected_losses(x_self, pow_field):
    """Expected number of players who finish above ``x_self``.

    Unlike :func:`_expected_seed`, ``pow_field`` is the *whole* field including
    the player themself, whose own term is ``x_self / (x_self + x_self) = 0.5``
    at their current rating but slides to 0 (or 1) as the trial rating rises
    (or falls).  That sliding term is the entire point — see
    :func:`event_performance`.
    """
    return sum(x_other / (x_self + x_other) for x_other in pow_field)


def event_performance(pow_field, rank):
    """The rating at which finishing ``rank`` in this field would be par.

    Solves ``_expected_losses(P) == rank - 0.5`` for ``P``.  Both sides count
    the player as a half-loss to themself: the ``rank - 1`` players who really
    finished ahead, plus 0.5, against an expectation that also includes their
    own term.

    Three properties follow, and all three are why this replaced the older
    ``2 * need - rating``:

    * **Always finite.** ``_expected_losses`` sweeps the open interval
      ``(0, n)``, so every target in ``0.5 .. n - 0.5`` is reachable.  The
      textbook alternative — solving ``seed(P) == rank`` over the *other*
      players — only sweeps ``(1, n)``, leaving rank 1 and last place
      unreachable; on real fields that is ~13% of results (every winner and
      every last place, every day), where the search would just run to its
      bound.
    * **Identity-free.** The result depends only on the multiset of field
      ratings and on ``rank``.  Two players who tie therefore print exactly the
      same performance — which neither the old formula nor the
      self-excluding ``seed(P) == rank`` manages.
    * **Monotone in rank.** A better finish never prints a worse performance.

    The half-loss alone changes nothing (pinned at a constant 0.5 it cancels
    off both sides and gives back ``seed(P) == rank``); the work is done by the
    self-term *moving* with ``P``.  It equals 0.5 exactly when the answer lands
    on the player's own rating, so this correction vanishes for a par result
    and grows precisely where the alternative degenerates.

    ``pow_field`` is the whole field's ``_pow10`` values; ``rank`` is 1-based
    with ties sharing the lower rank.
    """
    target = rank - 0.5
    lo, hi = _SEARCH_LO, _SEARCH_HI
    for _ in range(_SEARCH_ITERS):
        mid = (lo + hi) / 2.0
        if _expected_losses(_pow10(mid), pow_field) < target:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2.0

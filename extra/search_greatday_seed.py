#!/usr/bin/env python3
"""Search for a string seed that is kind to one target user.

This mirrors greatday's current selection semantics:
  - pick ``min(pick_count, len(users))`` distinct users each day
  - selection is uniform via ``random.sample``

The only change is the proposed deterministic seeding:
  random.Random(sha256(date_iso) + seed)

By default the script builds a synthetic signup list containing:
  - the protected handle ``fffff``
  - 25 randomly generated 5-letter handles

It then samples many candidate seeds and scores them by:
  1. lowest maximum consecutive-day streak for the protected handle
  2. fewest extra consecutive selected days overall
  3. fewest total selected days overall
"""

from __future__ import annotations

import argparse
import hashlib
import random
import string
from dataclasses import dataclass
from datetime import date, timedelta


DEFAULT_TARGET = "fffff"
DEFAULT_OTHER_USERS = 25
DEFAULT_PICK_COUNT = 5
DEFAULT_START_DATE = "2026-04-17"
DEFAULT_DAYS = 3650
DEFAULT_TRIALS = 2000
DEFAULT_POPULATION_SEED = 0
DEFAULT_SEARCH_SEED = 0
DEFAULT_SEED_LENGTH = 16
DEFAULT_OBJECTIVE = "streak"


@dataclass(frozen=True)
class Score:
    max_streak: int
    consecutive_days: int
    total_hits: int


@dataclass(frozen=True)
class Streak:
    start: date
    end: date
    length: int


@dataclass(frozen=True)
class SearchResult:
    seed: str
    score: Score
    streaks: tuple[Streak, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Search candidate string seeds for date-based greatday selection."
        )
    )
    parser.add_argument("--target", default=DEFAULT_TARGET,
                        help=f"Protected handle. Default: {DEFAULT_TARGET}")
    parser.add_argument("--other-users", type=int, default=DEFAULT_OTHER_USERS,
                        help=f"Number of random users besides the target. Default: {DEFAULT_OTHER_USERS}")
    parser.add_argument("--pick-count", type=int, default=DEFAULT_PICK_COUNT,
                        help=f"Users picked per day. Default: {DEFAULT_PICK_COUNT}")
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE,
                        help=f"Start date in YYYY-MM-DD. Default: {DEFAULT_START_DATE}")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"Days to evaluate. Default: {DEFAULT_DAYS}")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS,
                        help=f"Random seeds to test. Default: {DEFAULT_TRIALS}")
    parser.add_argument("--population-seed", type=int, default=DEFAULT_POPULATION_SEED,
                        help=f"RNG seed for synthetic users. Default: {DEFAULT_POPULATION_SEED}")
    parser.add_argument("--search-seed", type=int, default=DEFAULT_SEARCH_SEED,
                        help=f"RNG seed for candidate seed search. Default: {DEFAULT_SEARCH_SEED}")
    parser.add_argument("--seed-length", type=int, default=DEFAULT_SEED_LENGTH,
                        help=f"Length of each candidate seed string. Default: {DEFAULT_SEED_LENGTH}")
    parser.add_argument("--objective", choices=("streak", "total_hits"),
                        default=DEFAULT_OBJECTIVE,
                        help=(
                            "Optimization target. "
                            "'streak' minimizes consecutive picks first; "
                            "'total_hits' minimizes total picks first. "
                            f"Default: {DEFAULT_OBJECTIVE}"
                        ))
    parser.add_argument("--show-top-streaks", type=int, default=10,
                        help="How many longest streaks to print. Default: 10")
    return parser.parse_args()


def build_users(target: str, other_users: int, population_seed: int) -> list[str]:
    rng = random.Random(population_seed)
    alphabet = string.ascii_lowercase
    users = [target]
    while len(users) < other_users + 1:
        handle = "".join(rng.choice(alphabet) for _ in range(5))
        if handle not in users:
            users.append(handle)
    return users


def build_date_hashes(start_date: date, days: int) -> list[str]:
    return [
        hashlib.sha256((start_date + timedelta(days=offset)).isoformat().encode()).hexdigest()
        for offset in range(days)
    ]


def evaluate_seed(
    users: list[str],
    target: str,
    pick_count: int,
    start_date: date,
    date_hashes: list[str],
    seed: str,
    objective: str,
    best_score: Score | None = None,
) -> SearchResult | None:
    current_streak = 0
    streak_start: date | None = None
    max_streak = 0
    consecutive_days = 0
    total_hits = 0
    streaks: list[Streak] = []
    sample_size = min(pick_count, len(users))

    for offset, date_hash in enumerate(date_hashes):
        today = start_date + timedelta(days=offset)
        picked = random.Random(date_hash + seed).sample(users, sample_size)
        hit = target in picked

        if hit:
            total_hits += 1
            if current_streak == 0:
                streak_start = today
            current_streak += 1
            if current_streak > max_streak:
                max_streak = current_streak
            if current_streak >= 2:
                consecutive_days += 1
        else:
            if current_streak:
                streaks.append(Streak(streak_start, today - timedelta(days=1), current_streak))
            current_streak = 0
            streak_start = None

        if best_score is not None:
            current_score = Score(
                max_streak=max_streak,
                consecutive_days=consecutive_days,
                total_hits=total_hits,
            )
            if score_key(current_score, objective) > score_key(best_score, objective):
                return None

    if current_streak:
        streaks.append(
            Streak(
                streak_start,
                start_date + timedelta(days=len(date_hashes) - 1),
                current_streak,
            )
        )

    return SearchResult(
        seed=seed,
        score=Score(
            max_streak=max_streak,
            consecutive_days=consecutive_days,
            total_hits=total_hits,
        ),
        streaks=tuple(streaks),
    )


def search_seed(
    users: list[str],
    target: str,
    pick_count: int,
    start_date: date,
    days: int,
    trials: int,
    search_seed: int,
    seed_length: int,
    objective: str,
) -> SearchResult:
    date_hashes = build_date_hashes(start_date, days)
    rng = random.Random(search_seed)
    alphabet = "0123456789abcdef"
    best: SearchResult | None = None

    for trial in range(trials):
        candidate = "".join(rng.choice(alphabet) for _ in range(seed_length))
        result = evaluate_seed(
            users=users,
            target=target,
            pick_count=pick_count,
            start_date=start_date,
            date_hashes=date_hashes,
            seed=candidate,
            objective=objective,
            best_score=best.score if best else None,
        )
        if result is None:
            continue
        if best is None or score_key(result.score, objective) < score_key(best.score, objective):
            best = result
            print(
                f"[trial {trial:>5}] best seed={best.seed} "
                f"score=(max_streak={best.score.max_streak}, "
                f"consecutive_days={best.score.consecutive_days}, "
                f"total_hits={best.score.total_hits})"
            )

    if best is None:
        raise RuntimeError("Search produced no result.")
    return best


def score_key(score: Score, objective: str) -> tuple[int, int, int]:
    if objective == "streak":
        return (score.max_streak, score.consecutive_days, score.total_hits)
    if objective == "total_hits":
        return (score.total_hits, score.max_streak, score.consecutive_days)
    raise ValueError(f"Unsupported objective: {objective}")


def format_streak(streak: Streak) -> str:
    if streak.length == 1:
        return f"{streak.start.isoformat()} ({streak.length} day)"
    return (
        f"{streak.start.isoformat()} .. {streak.end.isoformat()} "
        f"({streak.length} days)"
    )


def main() -> None:
    args = parse_args()
    if args.other_users < 0:
        raise SystemExit("--other-users must be non-negative")
    if args.pick_count <= 0:
        raise SystemExit("--pick-count must be positive")
    if args.days <= 0:
        raise SystemExit("--days must be positive")
    if args.trials <= 0:
        raise SystemExit("--trials must be positive")
    if args.seed_length <= 0:
        raise SystemExit("--seed-length must be positive")

    users = build_users(args.target, args.other_users, args.population_seed)
    result = search_seed(
        users=users,
        target=args.target,
        pick_count=args.pick_count,
        start_date=args.start_date,
        days=args.days,
        trials=args.trials,
        search_seed=args.search_seed,
        seed_length=args.seed_length,
        objective=args.objective,
    )

    print()
    print("Synthetic users:")
    print(" ".join(users))
    print()
    print("Chosen seed:")
    print(result.seed)
    print()
    print("Summary:")
    print(f"  protected user: {args.target}")
    print(f"  objective: {args.objective}")
    print(f"  picks per day: {min(args.pick_count, len(users))}")
    print(f"  evaluation window: {args.start_date.isoformat()} + {args.days} days")
    print(f"  max streak: {result.score.max_streak}")
    print(f"  consecutive selected days total: {result.score.consecutive_days}")
    print(f"  total selected days: {result.score.total_hits}")
    print()
    print("Longest streaks:")
    longest = sorted(result.streaks, key=lambda streak: (-streak.length, streak.start))
    for streak in longest[:args.show_top_streaks]:
        print(f"  {format_streak(streak)}")


if __name__ == "__main__":
    main()

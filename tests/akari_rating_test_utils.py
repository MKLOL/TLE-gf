"""Shared helpers for the Akari rating engine tests."""
from types import SimpleNamespace


def _row(user_id, puzzle_number, *, perfect=False, accuracy=0, time_seconds=100):
    return SimpleNamespace(
        user_id=user_id,
        puzzle_number=puzzle_number,
        is_perfect=perfect,
        accuracy=accuracy,
        time_seconds=time_seconds,
    )


def _day(puzzle_number, players):
    """players: list of (user_id, perfect, accuracy, time_seconds)."""
    return [
        _row(uid, puzzle_number, perfect=p, accuracy=a, time_seconds=t)
        for uid, p, a, t in players
    ]

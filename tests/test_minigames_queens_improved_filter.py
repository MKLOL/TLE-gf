from tle.cogs import minigames
from tle.cogs._minigame_queens_filters import (
    _split_queens_improved_filter,
)


def test_improved_filter_can_appear_anywhere():
    args = ('first', '+beta', 'second')

    remaining, improved = _split_queens_improved_filter(args)

    assert remaining == ['first', 'second']
    assert improved is True


def test_improved_filter_is_case_insensitive():
    remaining, improved = _split_queens_improved_filter(
        ('+BeTa', 'member'))

    assert remaining == ['member']
    assert improved is True


def test_improved_filter_only_matches_exact_flag():
    args = ('beta', '++beta', '+beta=yes', '+improved', 'member')

    remaining, improved = _split_queens_improved_filter(args)

    assert remaining == list(args)
    assert improved is False


def test_improved_filter_removes_duplicate_flags_idempotently():
    remaining, improved = _split_queens_improved_filter(
        ('+beta', 'member', '+BETA', '+beta'))

    assert remaining == ['member']
    assert improved is True
    assert _split_queens_improved_filter(remaining) == (['member'], False)


def test_improved_filter_is_reexported_from_minigames():
    assert minigames._split_queens_improved_filter is (
        _split_queens_improved_filter
    )

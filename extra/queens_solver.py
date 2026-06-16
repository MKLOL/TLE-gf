"""Pure Queens solver — no browser, no LinkedIn, no I/O.

The board is an N×N grid.  Each cell belongs to one of N "color regions".
A valid solution places exactly one queen such that every row, every
column, and every region holds exactly one queen, AND no two queens are
orthogonally OR diagonally adjacent (no touching, even at a corner).

Since we have exactly one queen per row, two queens can only "touch" when
they're in adjacent rows; checking the previous row's column against the
current candidate's column is therefore sufficient.
"""


def solve_queens(regions):
    """Solve a Queens grid.

    ``regions`` is an N×N list-of-lists of region IDs (any hashable values).
    Returns a list of (row, col) tuples — one per row — or ``None`` if the
    grid has no solution (which shouldn't happen for a well-formed LinkedIn
    puzzle but we surface it cleanly anyway).
    """
    n = len(regions)
    if n == 0 or any(len(row) != n for row in regions):
        return None
    queens = []
    cols_used = set()
    regs_used = set()

    def search(row):
        if row == n:
            return True
        for c in range(n):
            if c in cols_used:
                continue
            reg = regions[row][c]
            if reg in regs_used:
                continue
            if row > 0 and abs(queens[row - 1] - c) <= 1:
                # cols_used already excludes same column (diff == 0); this
                # additionally rejects diff == 1, the diagonal-touch case.
                continue
            queens.append(c)
            cols_used.add(c)
            regs_used.add(reg)
            if search(row + 1):
                return True
            queens.pop()
            cols_used.remove(c)
            regs_used.remove(reg)
        return False

    return [(r, c) for r, c in enumerate(queens)] if search(0) else None


def _validate_solution(regions, sol):
    """Confirm ``sol`` actually satisfies the Queens constraints for ``regions``."""
    n = len(regions)
    if sol is None or len(sol) != n:
        return False
    cols = [c for _, c in sol]
    if len(set(cols)) != n:
        return False
    regs = [regions[r][c] for r, c in sol]
    if len(set(regs)) != n:
        return False
    for i in range(1, n):
        if abs(cols[i] - cols[i - 1]) <= 1:
            return False  # diagonal or same-column touch
    return True


def _solver_self_test():
    """Quick sanity check on degenerate inputs.

    The real solver test is the daily integration run — we don't ship a real
    LinkedIn fixture because we'd have to keep it in sync with the puzzle of
    the day.  These cases just confirm the algorithm doesn't crash on
    trivial inputs and surfaces unsolvable boards as ``None``.
    """
    assert solve_queens([[0]]) == [(0, 0)]
    assert solve_queens([]) is None
    # 3×3 with rows-as-regions is unsolvable: the only ways to fit 3 unique
    # columns with each pair differing by >= 2 require columns 0, 2, 4 — but
    # the third doesn't exist on a 3-wide board.
    assert solve_queens([[r] * 3 for r in range(3)]) is None
    # 5×5 by row regions is solvable; verify the structure of the result.
    grid_5 = [[r] * 5 for r in range(5)]
    sol = solve_queens(grid_5)
    assert _validate_solution(grid_5, sol), f'5x5 solve failed: {sol}'
    return True

"""Persistent cache for Daily Akari's externally supplied difficulty levels."""


class AkariDifficultyDbMixin:
    def get_akari_puzzle_difficulties(self, puzzle_numbers):
        numbers = sorted({int(number) for number in puzzle_numbers})
        if not numbers:
            return {}
        placeholders = ','.join('?' for _ in numbers)
        rows = self.conn.execute(
            f'SELECT puzzle_number, difficulty FROM akari_puzzle_difficulty '
            f'WHERE puzzle_number IN ({placeholders})',
            numbers,
        ).fetchall()
        return {int(row.puzzle_number): int(row.difficulty) for row in rows}

    def upsert_akari_puzzle_difficulties(self, entries, fetched_at):
        rows = [
            (int(number), int(difficulty), float(fetched_at))
            for number, difficulty in entries.items()
            if 1 <= int(difficulty) <= 5
        ]
        with self.conn:
            self.conn.executemany(
                '''
                INSERT INTO akari_puzzle_difficulty
                    (puzzle_number, difficulty, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(puzzle_number) DO UPDATE SET
                    difficulty = excluded.difficulty,
                    fetched_at = excluded.fetched_at
                ''',
                rows,
            )
        return len(rows)

#!/usr/bin/env python3
"""Parse LinkedIn Queens/Tango score messages from a Basic LinkedIn data export.

Reads ``messages.csv`` from a LinkedIn "Basic" data export, finds rows whose
body matches the chosen game's score-share format, and writes a clean JSON
file containing **only** the score data: sender name, sender profile URL,
puzzle number, time, badges, and the puzzle date — the input for
``;queens backfill`` / ``;tango backfill``.

Nothing else is emitted — no message bodies, no recipient lists, no DM
contents, no subject lines.  Safe to share / upload because the only thing
in the output is what would appear on a LinkedIn leaderboard anyway.

Usage:
    python extra/queens_parse_messages.py /path/to/messages.csv [output.json]
    python extra/queens_parse_messages.py /path/to/messages.csv -o queens.json
    python extra/queens_parse_messages.py /path/to/messages.csv --game tango
    python extra/queens_parse_messages.py /path/to/messages.csv --no-dedupe
"""
import argparse
import csv
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path


# One entry per LinkedIn game.  The anchors are the same ones the cog uses
# (``LinkedInDef`` in tle/cogs/_minigame_queens.py / _minigame_tango.py) —
# keep them in sync if LinkedIn ever resets puzzle numbering.
_GAMES = {
    'queens': {'word': 'Queens',
               'anchor_date': date(2026, 6, 8), 'anchor_number': 769},
    'tango': {'word': 'Tango',
              'anchor_date': date(2026, 9, 4), 'anchor_number': 697},
}


def _line_re(word):
    """Body looks like one of::

        Queens #771 | 0:15
        Queens #770 | 0:05 with no mistakes & no hints
        Tango #695
        0:08 🌗

    Anchored at start because we only want the first / leading line; the
    time is either after a ``|`` on the same line or wrapped onto the next.
    The body also contains a trailing ``lnkd.in/<game>.`` link we ignore.
    """
    return re.compile(
        rf'^\s*{word}\s+#(\d+)\s*(?:\|\s*|\n\s*)(\d{{1,2}}):(\d{{2}})([^\n]*)',
        re.IGNORECASE | re.MULTILINE)


def _puzzle_date_for(puzzle_number, game):
    spec = _GAMES[game]
    return spec['anchor_date'] + timedelta(
        days=int(puzzle_number) - spec['anchor_number'])


def _parse_badges(trailing):
    lowered = (trailing or '').lower()
    return ('no hints' in lowered, 'no mistakes' in lowered)


def parse_messages_csv(path, game='queens'):
    """Yield one dict per score message for ``game`` found in ``path``."""
    word = _GAMES[game]['word']
    line_re = _line_re(word)
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Tolerate different column headers across export versions.
        sender_col = next(
            (c for c in ('FROM', 'From', 'SenderName')
             if c in reader.fieldnames),
            'FROM')
        url_col = next(
            (c for c in ('SENDER PROFILE URL', 'SenderProfileUrl',
                         'sender_profile_url')
             if c in reader.fieldnames),
            'SENDER PROFILE URL')
        content_col = next(
            (c for c in ('CONTENT', 'Content', 'Message')
             if c in reader.fieldnames),
            'CONTENT')
        date_col = next(
            (c for c in ('DATE', 'Date') if c in reader.fieldnames),
            'DATE')
        for row in reader:
            content = (row.get(content_col) or '').strip()
            if word.lower() not in content.lower():
                continue
            m = line_re.search(content)
            if not m:
                continue
            puzzle_num = int(m.group(1))
            minutes = int(m.group(2))
            seconds = int(m.group(3))
            no_hints, no_mistakes = _parse_badges(m.group(4))
            time_seconds = minutes * 60 + seconds
            sender_name = (row.get(sender_col) or '').strip()
            if not sender_name:
                continue
            try:
                puzzle_date_iso = _puzzle_date_for(
                    puzzle_num, game).isoformat()
            except (OverflowError, ValueError):
                puzzle_date_iso = None
            yield {
                'linkedin_name': sender_name,
                'linkedin_url': (row.get(url_col) or '').strip(),
                'puzzle_number': puzzle_num,
                'puzzle_date': puzzle_date_iso,
                'time_seconds': time_seconds,
                'no_hints': no_hints,
                'no_mistakes': no_mistakes,
                'is_perfect': no_hints and no_mistakes,
                'sent_at_utc': (row.get(date_col) or '').strip(),
            }


def dedupe_best_per_player_puzzle(results):
    """Collapse duplicate messages (same user, same puzzle) to the best result.

    The same person can post the same score in multiple group chats, so the
    same (player, puzzle) often appears twice.  Keep the entry with the
    lowest time, breaking ties by total badges (more is better).
    """
    by_key = {}
    for r in results:
        # Profile URL is the most reliable identity; fall back to name.
        identity = r['linkedin_url'] or r['linkedin_name']
        key = (identity, r['puzzle_number'])
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = r
            continue
        prev_score = (prev['time_seconds'],
                      -(int(prev['no_hints']) + int(prev['no_mistakes'])))
        new_score = (r['time_seconds'],
                     -(int(r['no_hints']) + int(r['no_mistakes'])))
        if new_score < prev_score:
            by_key[key] = r
    return list(by_key.values())


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        'messages',
        help='Path to messages.csv from a LinkedIn "Basic" data export.')
    p.add_argument(
        'output', nargs='?', default=None,
        help='Output JSON path (default: <game>_history.json).')
    p.add_argument(
        '--game', choices=sorted(_GAMES), default='queens',
        help='Which LinkedIn game to extract (default: queens).')
    p.add_argument(
        '-o', '--output', dest='output_flag', default=None,
        help='Alternative way to specify the output path.')
    p.add_argument(
        '--no-dedupe', action='store_true',
        help='Emit every score-message row, including duplicates from '
             'group-chat cross-posts.')
    p.add_argument(
        '--respect-badges', action='store_true',
        help='Honour the "with no hints / no mistakes" suffix on each '
             'message.  Default: mark every result as clean (no hints + '
             'no mistakes + is_perfect=true), since bare LinkedIn share '
             'messages omit the suffix even for clean solves.')
    args = p.parse_args(argv)

    output_path = Path(
        args.output_flag or args.output or f'{args.game}_history.json'
    ).expanduser().resolve()
    msgs_path = Path(args.messages).expanduser().resolve()
    if not msgs_path.exists():
        print(f'No such file: {msgs_path}', file=sys.stderr)
        return 1

    results = list(parse_messages_csv(msgs_path, args.game))
    raw_count = len(results)
    if not args.respect_badges:
        for r in results:
            r['no_hints'] = True
            r['no_mistakes'] = True
            r['is_perfect'] = True
    if not args.no_dedupe:
        results = dedupe_best_per_player_puzzle(results)
    results.sort(key=lambda r: (r['puzzle_number'], r['time_seconds']))

    print(f'Scanned {msgs_path.name}', file=sys.stderr)
    print(f'  {_GAMES[args.game]["word"]}-message rows: {raw_count}',
          file=sys.stderr)
    if not args.no_dedupe:
        print(f'  After dedupe (best per player/puzzle): {len(results)}',
              file=sys.stderr)

    if results:
        nums = sorted({r['puzzle_number'] for r in results})
        print(f'  Puzzle range: #{nums[0]} – #{nums[-1]}  '
              f'({len(nums)} distinct puzzle(s))', file=sys.stderr)
        by_player = {}
        for r in results:
            by_player[r['linkedin_name']] = by_player.get(
                r['linkedin_name'], 0) + 1
        print(f'\nPlayers ({len(by_player)}):', file=sys.stderr)
        for name, count in sorted(by_player.items(), key=lambda kv: -kv[1]):
            print(f'  {count:>4} × {name}', file=sys.stderr)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'\nWrote {len(results)} result(s) → {output_path}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())

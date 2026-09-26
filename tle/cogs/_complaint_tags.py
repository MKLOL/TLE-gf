"""Bounded tag rendering for complaint embeds."""


def format_tags(tags, limit=5):
    """Keep tags from crowding out the complaint and resolution text."""
    summary = ' '.join(f'`{tag}`' for tag in tags[:limit])
    remaining = len(tags) - limit
    if remaining > 0:
        summary += f' … (+{remaining} more)'
    return summary


def tag_pages(counts, limit=3900):
    """Show every tag in the directory without exceeding an embed's limit."""
    pages, lines, size = [], [], 0
    for row in counts:
        line = f'`{row.tag}` — {row.count}'
        if lines and size + len(line) + 1 > limit:
            pages.append('\n'.join(lines))
            lines, size = [], 0
        lines.append(line)
        size += len(line) + 1
    if lines:
        pages.append('\n'.join(lines))
    return pages

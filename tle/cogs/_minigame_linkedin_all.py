"""Read-only linked-result projection for hypothetical +all ratings."""

from types import SimpleNamespace

from tle.util import codeforces_common as cf_common
from tle.cogs._minigame_common import normalize_puzzle_date
from tle.cogs._minigame_linkedin import linkedin_best_result_sort_key
from tle.cogs._minigame_queens_cog import _linkedin_result_message_id


def all_linked_results(guild_id, game):
    db = cf_common.user_db
    links = {str(link.user_id): link for link in
             db.get_minigame_player_links(guild_id, game.link_key)}
    by_name = {link.normalized_name: link for link in links.values()}
    rows = {}
    # Preserve legacy linked results that have no canonical source yet.
    for row in db.get_minigame_results_for_guild(guild_id, game.name):
        if str(row.user_id) not in links:
            continue
        date = normalize_puzzle_date(row.puzzle_date)
        number = game.linkedin.number_for_date(date)
        key = (str(row.user_id), number)
        if key in rows and linkedin_best_result_sort_key(rows[key]) >= linkedin_best_result_sort_key(row):
            continue
        rows[key] = SimpleNamespace(
            **{**row._asdict(), 'puzzle_number': number})
    sources = {}
    for row in db.get_minigame_unresolved_results_for_guild(guild_id, game.name):
        link = by_name.get(row.normalized_name)
        if link is None:
            continue
        date = normalize_puzzle_date(row.puzzle_date)
        number = game.linkedin.number_for_date(date)
        key = (str(link.user_id), number)
        # Match the official projection's legacy/canonical conflict policy.
        priority = (int(not bool(row.is_rated)),
                    int(int(row.puzzle_number) == number), float(row.stored_at))
        if key in sources and sources[key] >= priority:
            continue
        sources[key] = priority
        rows[key] = SimpleNamespace(
            message_id=row.source_message_id or _linkedin_result_message_id(
                game, guild_id, date, link.user_id),
            guild_id=str(guild_id), game=game.name, channel_id=row.channel_id,
            user_id=str(link.user_id), puzzle_number=number,
            puzzle_date=date.isoformat(), accuracy=int(row.accuracy),
            time_seconds=int(row.time_seconds), is_perfect=int(row.is_perfect),
            raw_content=row.raw_content)
    return list(rows.values())

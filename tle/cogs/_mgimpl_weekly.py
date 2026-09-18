"""`;akari week` / `;queens week` — the server's Monday-Sunday recap.

Aggregation lives in ``_minigame_weekly`` and rendering in
``_weekly_recap_render``; this mixin only resolves per-game inputs (rows,
difficulty weights, display names) and hands them over.
"""

import datetime as dt

from tle.util import codeforces_common as cf_common
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_queens import (
    queens_weekly_difficulty_map,
)
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_common import resolve_scoring
from tle.cogs._minigame_weekly import (
    build_week_recap, daily_rating_changes, week_bounds,
)
from tle.cogs._weekly_recap_render import (
    AKARI_PALETTE, QUEENS_PALETTE, plot_weekly_recap,
)
from tle.cogs._mgimpl_queenscmd import _QueensWeeklyRow


def _parse_week_anchor(args, today):
    """Resolve the week selector: a date, ``last``, or nothing for this week."""
    args = [str(arg).strip() for arg in args if str(arg).strip()]
    if not args:
        return today
    if len(args) > 1:
        raise MinigameCogError(
            'Usage: `;<game> week [YYYY-MM-DD|last]`.')
    token = args[0].casefold()
    if token in ('last', 'prev', 'previous'):
        return week_bounds(today)[0] - dt.timedelta(days=1)
    try:
        return dt.date.fromisoformat(args[0])
    except ValueError as exc:
        raise MinigameCogError(
            f'`{args[0]}` is not a date (`YYYY-MM-DD`) or `last`.') from exc


class ImplWeeklyMixin:
    async def _cmd_week(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        self._sync_minigame_results_for_read(ctx.guild.id, game)
        queens = game.linkedin_identity
        today = _queens_current_puzzle_date() if queens else dt.date.today()
        anchor = _parse_week_anchor(args, today)
        if anchor > today:
            raise MinigameCogError(
                f'`{anchor:%Y-%m-%d}` is in the future; there is nothing to '
                f'recap yet.')

        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, game.name)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, game, rows)
        if queens:
            rows = self._filter_queens_registered_result_rows(
                ctx.guild.id, game, rows)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results have been posted yet.')

        _args, _name, scoring = resolve_scoring(game, ())
        top_kwargs = {
            'is_eligible': scoring.is_eligible_winner,
            'best_result_sort_key_fn': scoring.best_result_sort_key,
            'winner_result_sort_key_fn': scoring.winner_result_sort_key,
            'group_key_fn': scoring.result_group_key,
            # A solo day is not a contest; Queens already excludes those from
            # its winners board, and the recap must agree with it.
            'min_participants': 2 if queens else 1,
        }
        rating_rows, difficulties = await self._week_rating_inputs(
            game, rows, anchor)
        start, end = week_bounds(anchor)
        daily = (
            daily_rating_changes(
                rows, start, end,
                compute_kwargs=self._rating_compute_kwargs(game))
            if game.rating is not None else [])

        recap = build_week_recap(
            rows,
            display_name=game.display_name,
            anchor_date=anchor,
            today=today,
            difficulties=difficulties,
            viewer_id=ctx.author.id,
            top_kwargs=top_kwargs,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            rating_rows=rating_rows,
            daily_ratings=daily,
        )
        if not recap.result_count:
            raise MinigameCogError(
                f'No {game.display_name} results for the week of '
                f'{recap.week_start:%b %d, %Y}.')

        name_fn = self._week_name_fn(ctx.guild, game)
        recap_file = plot_weekly_recap(
            recap, name_fn,
            palette=QUEENS_PALETTE if queens else AKARI_PALETTE)
        await ctx.send(file=recap_file)

    def _week_name_fn(self, guild, game):
        """Cache name lookups; a recap can mention the same player many times."""
        cache = {}

        def resolve(user_id):
            user_id = str(user_id)
            if user_id not in cache:
                cache[user_id] = self._minigame_public_user_name(
                    guild, game, user_id)
            return cache[user_id]

        return resolve

    async def _week_rating_inputs(self, game, rows, anchor):
        """Rows and difficulty weights for the game's weekly rating contest."""
        if game.linkedin_identity:
            # Queens' weekly contest is time-only: share messages do not carry
            # hint/mistake badges, so accuracy would depend on ingestion path.
            scoring_rows = [
                _QueensWeeklyRow(
                    row.user_id, row.puzzle_number, row.puzzle_date,
                    100, row.time_seconds, True)
                for row in rows
            ]
            return scoring_rows, queens_weekly_difficulty_map(scoring_rows)
        if game.name == AKARI_GAME.name:
            # Every puzzle in each touched Monday-Sunday span needs a weight,
            # because a week's scores are normalized across all seven days.
            wanted = set()
            for row in rows:
                try:
                    row_date = dt.date.fromisoformat(str(row.puzzle_date))
                except ValueError:
                    continue
                monday_number = int(row.puzzle_number) - row_date.weekday()
                wanted.update(range(monday_number, monday_number + 7))
            return rows, await self._akari_difficulty_map(wanted)
        return rows, {}

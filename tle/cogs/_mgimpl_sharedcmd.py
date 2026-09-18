"""Shared here/clear/show/vs/top/streak/remove commands. (Minigames cog impl mixin; see minigames.py)."""

import logging

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import ranking

from tle.cogs._minigame_common import (
    compute_vs, compute_vs_matchups, compute_streak, compute_longest_streak,
    compute_top_breakdown, pick_best_results, format_duration, parse_date_args,
    resolve_scoring,
)
from tle.cogs._minigame_akari import (
    AKARI_GAME,
)
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_helpers import (
    MinigameCogError, _safe_member_name, _safe_user_name,
    _format_score,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _filter_queens_weekday_rows,
    _format_queens_weekday_filter,
)

logger = logging.getLogger(__name__)


def _skipped_puzzles(puzzle_numbers, current_puzzle):
    """Return the first submission and missing concluded puzzle numbers."""
    submitted = set()
    for value in puzzle_numbers:
        try:
            puzzle_number = int(value)
        except (TypeError, ValueError):
            continue
        if 0 < puzzle_number <= int(current_puzzle):
            submitted.add(puzzle_number)
    if not submitted:
        return None, []

    first_submission = min(submitted)
    skipped = [
        puzzle_number
        for puzzle_number in range(
            int(current_puzzle) - 1, first_submission, -1)
        if puzzle_number not in submitted
    ]
    return first_submission, skipped


class ImplSharedCmdMixin:
    # ── Delegated-admin list commands (shared by Queens and Akari) ──────

    async def _cmd_minigame_admins(self, ctx, label, get_ids):
        admin_ids = get_ids(ctx.guild.id)
        if not admin_ids:
            await ctx.send(embed=discord_common.embed_neutral(
                f'No extra {label} admins configured.'))
            return
        lines = [
            f'- {_safe_user_name(ctx.guild, user_id)} (`{user_id}`)'
            for user_id in sorted(admin_ids, key=self._user_id_sort_key)
        ]
        await ctx.send(embed=discord_common.embed_neutral(
            f'Extra {label} admins:\n' + '\n'.join(lines)))

    def _require_server_mod_for_admin_list(self, ctx, label):
        if not self._has_server_mod_role(ctx.author):
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                f'can change the {label} admin list.')

    async def _cmd_minigame_admins_add(self, ctx, member, label,
                                       get_ids, set_ids):
        self._require_server_mod_for_admin_list(ctx, label)
        admin_ids = get_ids(ctx.guild.id)
        before = len(admin_ids)
        admin_ids.add(str(member.id))
        if len(admin_ids) == before:
            message = (
                f'`{_safe_member_name(member)}` already has '
                f'{label} admin access.')
        else:
            set_ids(ctx.guild.id, admin_ids)
            message = (
                f'`{_safe_member_name(member)}` can now run '
                f'{label} mod commands.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _cmd_minigame_admins_remove(self, ctx, member, label,
                                          get_ids, set_ids):
        self._require_server_mod_for_admin_list(ctx, label)
        admin_ids = get_ids(ctx.guild.id)
        removed = str(member.id) in admin_ids
        admin_ids.discard(str(member.id))
        if removed:
            set_ids(ctx.guild.id, admin_ids)
            message = (
                f'`{_safe_member_name(member)}` no longer has '
                f'{label} admin access.')
        else:
            message = (
                f'`{_safe_member_name(member)}` was not an extra '
                f'{label} admin.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _send_minigame_skips(
            self, ctx, member, game, first_submission, skipped,
            date_for_puzzle):
        """Render the shared Akari/Queens skipped-day response."""
        member_name = _safe_member_name(member)
        if first_submission is None:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{member_name}`.')

        first_date = date_for_puzzle(first_submission)
        if not skipped:
            await ctx.send(embed=discord_common.embed_success(
                f'`{member_name}` has no skipped {game.display_name} days '
                f'since first submitting **#{first_submission}** on '
                f'**{first_date.isoformat()}**.'))
            return

        lines = []
        for puzzle_number in skipped:
            puzzle_date = date_for_puzzle(puzzle_number)
            lines.append(
                f'**#{puzzle_number}** \N{MIDDLE DOT} '
                f'{puzzle_date.isoformat()} \N{MIDDLE DOT} '
                f'{puzzle_date:%A}')
        day_label = 'day' if len(skipped) == 1 else 'days'
        title = (
            f'{game.display_name} skipped days — '
            f'{member_name} ({len(skipped)} {day_label})')
        tracking_line = (
            f'Since first submission: **#{first_submission}** '
            f'\N{MIDDLE DOT} **{first_date.isoformat()}**')
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=title,
                description=f'{tracking_line}\n\n' + '\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    # ── Shared command implementations ──────────────────────────────────

    async def _cmd_here(self, ctx, game):
        cf_common.user_db.set_minigame_channel(ctx.guild.id, game.name, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel set to {ctx.channel.mention}'
        ))

    async def _cmd_clear(self, ctx, game):
        cf_common.user_db.clear_minigame_channel(ctx.guild.id, game.name)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel cleared.'
        ))

    async def _cmd_show(self, ctx, game):
        enabled = self._is_enabled(ctx.guild.id, game.feature_flag)
        channel_id = self._get_channel(ctx.guild.id, game.name)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            f'channel: {channel}',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {game.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @staticmethod
    def _guessgame_puzzle_url(puzzle_number):
        return f'https://guessthe.game/p/{int(puzzle_number)}'

    @staticmethod
    def _format_guessgame_result(row):
        if row is None:
            return 'no result'

        accuracy = int(getattr(row, 'accuracy', 0))
        yellow_pos = int(getattr(row, 'time_seconds', 7))
        if accuracy > 0:
            green_pos = 7 - accuracy
            if green_pos == 1:
                return 'perfect'
            return f'green {green_pos}'
        if yellow_pos < 7:
            return f'yellow {yellow_pos}'
        return 'no green'

    def _make_guessgame_vs_pages(self, ctx, game, member1, member2, stats, matchups, scoring_name):
        title_suffix = f' ({scoring_name.title()})' if scoring_name else ''
        summary_lines = [
            f'`{_safe_member_name(member1)}`: **{_format_score(stats["score1"])}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{_format_score(stats["score2"])}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ]

        pages = []
        per_page = 10
        ordered_matchups = list(reversed(matchups))
        for chunk in paginator.chunkify(ordered_matchups, per_page):
            embed = discord.Embed(
                title=f'{game.display_name} Head to Head{title_suffix}',
                description='\n'.join(summary_lines),
                color=discord_common.random_cf_color(),
            )

            col1 = []
            col2 = []
            for matchup in chunk:
                row1 = matchup['row1']
                row2 = matchup['row2']
                puzzle_number = int(
                    row1.puzzle_number if row1 is not None else row2.puzzle_number
                )
                puzzle_link = f'[#{puzzle_number}]({self._guessgame_puzzle_url(puzzle_number)})'
                col1.append(
                    f'{puzzle_link} {self._format_guessgame_result(row1)}'
                    f' · {_format_score(matchup["score1"])} pts'
                )
                col2.append(
                    f'{puzzle_link} {self._format_guessgame_result(row2)}'
                    f' · {_format_score(matchup["score2"])} pts'
                )

            embed.add_field(
                name=_safe_member_name(member1),
                value='\n'.join(col1),
                inline=True,
            )
            embed.add_field(
                name=_safe_member_name(member2),
                value='\n'.join(col2),
                inline=True,
            )
            pages.append((None, embed))

        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_guessgame_matchups(self, ctx, member1, member2, *args):
        game = GUESSGAME_GAME
        self._require_enabled(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        stats = compute_vs(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        matchups = compute_vs_matchups(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        self._make_guessgame_vs_pages(
            ctx, game, member1, member2, stats, matchups, scoring_name)

    async def _cmd_streak(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        filter_args = list(args)
        filter_args, weekdays = _split_queens_weekday_filter(filter_args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except MinigameCogError:
                member = ctx.author

        try:
            dlo, dhi, plo, phi = parse_date_args(filter_args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        streak = compute_streak(rows, weekdays)
        longest = compute_longest_streak(rows, weekdays)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        best = pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        weekday_label = _format_queens_weekday_filter(weekdays)
        weekday_suffix = f' ({weekday_label})' if weekday_label else ''
        embed = discord.Embed(
            title=f'{game.display_name} Streak{weekday_suffix}',
            description='\n'.join([
                f'`{_safe_member_name(member)}`: **{streak}** consecutive perfect day(s)',
                f'Longest streak: **{longest}** day(s)',
                f'Latest result: **{latest_status}** in **{format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_top(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        self._sync_minigame_results_for_read(ctx.guild.id, game)
        show_ties = any(
            str(arg).strip().casefold() == '+ties' for arg in args)
        args = tuple(
            arg for arg in args
            if str(arg).strip().casefold() != '+ties')
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            args, weekdays = _split_queens_weekday_filter(args)
            reference_date = (
                _queens_current_puzzle_date()
                if game.linkedin_identity else None)
            dlo, dhi, plo, phi = parse_date_args(
                args, reference_date=reference_date)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, game.name, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, game, rows)
        if game.linkedin_identity:
            rows = self._filter_queens_registered_result_rows(
                ctx.guild.id, game, rows)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        winners = compute_top_breakdown(
            rows,
            is_eligible=scoring.is_eligible_winner,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            winner_result_sort_key_fn=scoring.winner_result_sort_key,
            group_key_fn=scoring.result_group_key,
            min_participants=(2 if game.linkedin_identity else 1),
        )
        if not show_ties:
            # Outright wins only; a player who never won a puzzle alone drops off.
            winners = sorted(
                ((user_id, solo, tied) for user_id, solo, tied in winners
                 if solo),
                key=lambda item: (-item[1], int(item[0])),
            )
        if not winners:
            raise MinigameCogError(
                f'No {game.display_name} '
                f'{"winners" if show_ties else "outright winners"} '
                f'found for this range.')

        suffix_parts = []
        if scoring_name:
            suffix_parts.append(scoring_name.title())
        if show_ties:
            suffix_parts.append('With Ties')
        weekday_label = _format_queens_weekday_filter(weekdays)
        if weekday_label:
            suffix_parts.append(weekday_label)
        title_suffix = f' ({", ".join(suffix_parts)})' if suffix_parts else ''
        # Standard competition ranking so users tied on win count share a rank
        # instead of being split by the secondary (user_id) sort.
        rank_key = (
            (lambda item: item[1] + item[2]) if show_ties
            else (lambda item: item[1]))
        ranked = ranking.rank_items(winners, rank_key)
        pages = []
        per_page = 10
        for chunk in paginator.chunkify(ranked, per_page):
            lines = []
            for rank, (user_id, solo, tied) in chunk:
                name = self._minigame_public_user_name(ctx.guild, game, user_id)
                if show_ties:
                    lines.append(
                        f'**#{rank}** `{name}` — **{solo + tied}** wins '
                        f'({solo} solo, {tied} tied)')
                else:
                    lines.append(f'**#{rank}** `{name}` — **{solo}** wins')
            embed = discord.Embed(
                title=f'{game.display_name} Winners{title_suffix}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_remove(self, ctx, game, member, puzzle_id):
        rc = cf_common.user_db.delete_minigame_result_for_user_puzzle(
            ctx.guild.id, game.name, member.id, puzzle_id)
        if not rc:
            raise MinigameCogError(
                f'No {game.display_name} result found for '
                f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.')
        if game.name == AKARI_GAME.name:
            self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {game.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.'))

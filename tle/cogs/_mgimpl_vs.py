"""Two-player and round-robin Akari/Queens comparison commands."""

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import ranking
from tle.cogs._minigame_common import compute_vs, parse_date_args, resolve_scoring
from tle.cogs._minigame_helpers import MinigameCogError, _format_score
from tle.cogs._minigame_multi_vs import compute_multi_vs
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter,
    _filter_queens_weekday_rows,
    _format_queens_weekday_filter,
)


_MAX_VS_PLAYERS = 10


class ImplVsMixin:
    @staticmethod
    def _looks_like_vs_filter(game, argument):
        if hasattr(argument, 'id'):
            return False
        text = str(argument).strip().casefold()
        if text in {'week', 'month', 'year'}:
            return True
        if text in game.scoring_variants:
            return True
        return text.startswith((
            'd>=', 'd<', 'p>=', 'p<',
            '+dow=', '+day=', '+days=', '+weekday=', '+weekdays=',
        ))

    @staticmethod
    def _validate_vs_members(members):
        if len(members) < 2:
            raise MinigameCogError('Choose at least two users to compare.')
        if len(members) > _MAX_VS_PLAYERS:
            raise MinigameCogError(
                f'You can compare at most {_MAX_VS_PLAYERS} users at once.')
        member_ids = [str(member.id) for member in members]
        if len(member_ids) != len(set(member_ids)):
            raise MinigameCogError(
                'Each user can only appear once in a comparison.')

    async def _resolve_vs_arguments(self, ctx, game, arguments):
        """Resolve leading member arguments and leave trailing filters intact."""
        members = []
        filters = []
        resolving_members = True
        for argument in arguments:
            if not resolving_members:
                filters.append(argument)
                continue
            if (len(members) >= 2
                    and self._looks_like_vs_filter(game, argument)):
                resolving_members = False
                filters.append(argument)
                continue
            if hasattr(argument, 'id'):
                member = argument
            else:
                try:
                    member = await self._resolve_member(ctx, argument)
                except MinigameCogError:
                    if len(members) >= 2:
                        raise
                    resolving_members = False
                    filters.append(argument)
                    continue
            members.append(member)

        if len(members) < 2 and filters:
            raise MinigameCogError(
                'Choose at least two users to compare. Put all users before '
                'the optional filters.')
        self._validate_vs_members(members)
        return members, filters

    async def _cmd_vs(self, ctx, game, member1, member2, *args):
        """Compatibility entry point for existing two-player callers."""
        await self._cmd_vs_members(ctx, game, [member1, member2], *args)

    async def _cmd_vs_members(self, ctx, game, members, *args):
        self._validate_vs_members(members)
        self._require_enabled(ctx.guild.id, game)
        self._sync_minigame_results_for_read(ctx.guild.id, game)
        try:
            args, weekdays = _split_queens_weekday_filter(args)
            args, scoring_name, scoring = resolve_scoring(game, args)
            reference_date = (
                _queens_current_puzzle_date()
                if game.linkedin_identity else None)
            dlo, dhi, plo, phi = parse_date_args(
                args, reference_date=reference_date)
        except ValueError as exc:
            raise MinigameCogError(str(exc)) from exc

        rows_by_user = {}
        for member in members:
            rows = cf_common.user_db.get_minigame_results_for_user(
                ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
            rows = self._filter_minigame_banned_rows(
                ctx.guild.id, game, rows)
            rows_by_user[str(member.id)] = _filter_queens_weekday_rows(
                rows, weekdays)

        missing_is_loss = (
            scoring.missing_is_loss
            if scoring.missing_is_loss is not None
            else game.missing_is_loss
        )
        missing_result = (
            scoring.missing_result
            if scoring.missing_result is not None
            else game.missing_result
        )
        if len(members) == 2:
            await self._send_two_player_vs(
                ctx, game, members, rows_by_user, scoring_name, scoring,
                weekdays, missing_is_loss, missing_result)
            return
        await self._send_multi_player_vs(
            ctx, game, members, rows_by_user, scoring_name, scoring,
            weekdays, missing_is_loss, missing_result)

    @staticmethod
    def _vs_title_suffix(scoring_name, weekdays):
        suffix_parts = []
        if scoring_name:
            suffix_parts.append(scoring_name.title())
        weekday_label = _format_queens_weekday_filter(weekdays)
        if weekday_label:
            suffix_parts.append(weekday_label)
        return f' ({", ".join(suffix_parts)})' if suffix_parts else ''

    async def _send_two_player_vs(
            self, ctx, game, members, rows_by_user, scoring_name, scoring,
            weekdays, missing_is_loss, missing_result):
        member1, member2 = members
        stats = compute_vs(
            rows_by_user[str(member1.id)],
            rows_by_user[str(member2.id)],
            score_fn=scoring.score_matchup,
            missing_is_loss=missing_is_loss,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=missing_result,
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        name1 = self._minigame_public_user_name(ctx.guild, game, member1.id)
        name2 = self._minigame_public_user_name(ctx.guild, game, member2.id)
        description = '\n'.join([
            f'`{name1}`: **{stats["score1"]:g}** points, '
            f'**{stats["wins1"]}** wins',
            f'`{name2}`: **{stats["score2"]:g}** points, '
            f'**{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ])
        await ctx.send(embed=discord.Embed(
            title=(
                f'{game.display_name} Head to Head'
                f'{self._vs_title_suffix(scoring_name, weekdays)}'),
            description=description,
            color=discord_common.random_cf_color(),
        ))

    async def _send_multi_player_vs(
            self, ctx, game, members, rows_by_user, scoring_name, scoring,
            weekdays, missing_is_loss, missing_result):
        stats = compute_multi_vs(
            rows_by_user,
            score_fn=scoring.score_matchup,
            missing_is_loss=missing_is_loss,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=missing_result,
        )
        if stats['puzzle_count'] == 0:
            raise MinigameCogError(
                f'These users have no shared {game.display_name} puzzles '
                'to compare.')

        member_by_id = {str(member.id): member for member in members}
        standings = sorted(
            stats['players'].values(),
            key=lambda row: (
                -row['score'], -row['wins'], row['losses'],
                int(row['user_id']),
            ),
        )
        lines = []
        for rank, row in ranking.rank_items(
                standings, lambda item: item['score']):
            member = member_by_id[row['user_id']]
            name = self._minigame_public_user_name(
                ctx.guild, game, member.id)
            lines.append(
                f'**#{rank}** `{name}` — '
                f'**{_format_score(row["score"])}** points · '
                f'**{row["wins"]}** wins · '
                f'**{row["losses"]}** losses · '
                f'**{row["ties"]}** ties')
        lines.extend([
            '',
            f'Puzzles: **{stats["puzzle_count"]}**',
            f'Comparisons: **{stats["comparison_count"]}**',
        ])
        await ctx.send(embed=discord.Embed(
            title=(
                f'{game.display_name} Head to Head'
                f'{self._vs_title_suffix(scoring_name, weekdays)}'),
            description='\n'.join(lines),
            color=discord_common.random_cf_color(),
        ))

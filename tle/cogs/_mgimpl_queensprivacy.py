"""LinkedIn-game rating opt-out and opt-in command implementations.

Opt-out is per game: a player can stay rated in Queens while sitting out
Tango.  The shared LinkedIn link is untouched either way.
"""

import time

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_common import normalize_puzzle_date
from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_queens_cog import (
    _parse_linkedin_date_or_number,
)


class ImplQueensPrivacyMixin:
    async def _cmd_queens_optout(self, ctx, game, member=None):
        """Store this user's future results as unrated."""
        self._require_enabled(ctx.guild.id, game)
        target = self._resolve_queens_registrar_target(
            ctx, game, member, action='opt out')
        user_id = target.id
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, user_id)
        if link is None:
            raise MinigameCogError(
                f'`{self._queens_public_user_name(ctx.guild, user_id)}` must '
                'register a LinkedIn name before opting out.')
        if cf_common.user_db.is_minigame_opted_out(
                ctx.guild.id, game.name, user_id):
            raise MinigameCogError(
                f'`{self._queens_public_user_name(ctx.guild, user_id)}` is '
                f'already opted out of '
                f'{game.display_name} ratings.')

        # Canonicalize prior history before setting the flag. Those earlier
        # sources stay rated and can return on opt-in; only writes processed
        # after the flag exists become permanently unrated.
        self._migrate_legacy_queens_results_to_external(ctx.guild.id, game)
        cf_common.user_db.optout_minigame_user(
            ctx.guild.id, game.name, user_id, time.time(),
            link.normalized_name)
        self._sync_queens_materialized_results(
            ctx.guild.id, game, migrate_legacy=False)
        self._recompute_minigame_ratings(
            ctx.guild.id, game, sync_results=False)

        target_name = self._queens_public_user_name(ctx.guild, user_id)
        await ctx.send(embed=discord_common.embed_success(
            f'`{target_name}` is opted out of '
            f'{game.display_name} ratings. Their LinkedIn registration '
            'and earlier rated history still count. New submissions/imports '
            'are stored permanently unrated. '
            f'They can run `;{game.name} optin` before their next result to '
            'participate again.'))

    async def _cmd_queens_optin(self, ctx, game):
        """Make this user's future results rated again."""
        self._require_enabled(ctx.guild.id, game)
        user_id = ctx.author.id
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, user_id)
        if link is None:
            raise MinigameCogError(
                'Register a LinkedIn name before opting back into '
                f'{game.display_name} ratings.')
        removed = cf_common.user_db.clear_minigame_optout(
            ctx.guild.id, game.name, user_id)
        if not removed:
            raise MinigameCogError(
                f'You are not opted out of '
                f'{game.display_name} ratings.')

        self._sync_queens_materialized_results(
            ctx.guild.id, game, migrate_legacy=False)
        self._recompute_minigame_ratings(
            ctx.guild.id, game, sync_results=False)
        await ctx.send(embed=discord_common.embed_success(
            f'You are opted into {game.display_name} ratings. '
            'Results stored while you were opted out remain unrated; your '
            'next result will count.'))

    async def _cmd_queens_set_result_rating(
            self, ctx, game, args, *, is_rated, member=None):
        """Moderator-only canonical rating toggle for one player/day."""
        self._require_enabled(ctx.guild.id, game)
        command = 'rate' if is_rated else 'unrate'
        if member is None:
            player_text, puzzle_date = self._parse_queens_remove_args(
                game, args, command=command)
            user_id, label, link = await self._resolve_queens_linked_player(
                ctx, game, player_text)
        else:
            puzzle_date = _parse_linkedin_date_or_number(game, args)
            user_id = str(member.id)
            link = cf_common.user_db.get_minigame_player_link(
                ctx.guild.id, game.link_key, member.id)
            if link is None:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` '
                    f'is not registered for {self._linkedin_games_label()}.')
            label = self._queens_public_user_name(ctx.guild, member.id)
        self._migrate_legacy_queens_results_to_external(ctx.guild.id, game)
        puzzle_numbers = set(
            game.linkedin.puzzle_numbers_for_date(puzzle_date))
        sources = [
            row
            for row in cf_common.user_db.get_minigame_unresolved_results_for_name(
                ctx.guild.id, game.name, link.normalized_name)
            if (
                int(row.puzzle_number) in puzzle_numbers
                or normalize_puzzle_date(row.puzzle_date) == puzzle_date
            )
        ]
        if not sources:
            raise MinigameCogError(
                f'No {game.display_name} result found for `{label}` '
                f'on {puzzle_date.isoformat()}.')
        changed = [
            source for source in sources
            if bool(source.is_rated) != bool(is_rated)
        ]
        if not changed:
            state = 'rated' if is_rated else 'unrated'
            raise MinigameCogError(
                f'`{label}`\'s result on {puzzle_date.isoformat()} is '
                f'already {state}.')

        for source in changed:
            cf_common.user_db.set_minigame_unresolved_result_rating(
                ctx.guild.id, game.name, link.normalized_name,
                source.puzzle_number, is_rated)
        self._sync_queens_materialized_results(
            ctx.guild.id, game, migrate_legacy=False)
        self._recompute_minigame_ratings(
            ctx.guild.id, game, sync_results=False)

        state = 'rated' if is_rated else 'unrated'
        detail = (
            ' This day counts despite their active opt-out; future results '
            'remain unrated.'
            if is_rated and cf_common.user_db.is_minigame_opted_out(
                ctx.guild.id, game.name, user_id)
            else ''
        )
        await ctx.send(embed=discord_common.embed_success(
            f'`{label}`\'s {game.display_name} result on '
            f'{puzzle_date.isoformat()} is now {state}.{detail}'))

"""Queens rating opt-out and opt-in command implementations."""

import time

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_queens import QUEENS_GAME


class ImplQueensPrivacyMixin:
    async def _cmd_queens_optout(self, ctx):
        """Keep a user's identity/source data but exclude rating projections."""
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        user_id = ctx.author.id
        if cf_common.user_db.is_minigame_opted_out(
                ctx.guild.id, QUEENS_GAME.name, user_id):
            raise MinigameCogError(
                f'You are already opted out of '
                f'{QUEENS_GAME.display_name} ratings.')

        # Privacy first: once this row exists, all read/rating paths hide the
        # user even if a later cleanup step fails.
        cf_common.user_db.optout_minigame_user(
            ctx.guild.id, QUEENS_GAME.name, user_id, time.time())
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, user_id)
        self._migrate_legacy_queens_results_to_external(ctx.guild.id)
        if link is not None:
            self._delete_queens_materialized_results_for_link(
                ctx.guild.id, link)
        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        self._recompute_minigame_ratings(
            ctx.guild.id, QUEENS_GAME, sync_results=False)

        await ctx.send(embed=discord_common.embed_success(
            f'You are opted out of {QUEENS_GAME.display_name} ratings. '
            'Your LinkedIn registration and stored results were kept, but '
            'imports will not add them to ratings or public result boards. '
            'Run `;queens optin` to participate again.'))

    async def _cmd_queens_optin(self, ctx):
        """Restore rating projections for a previously opted-out user."""
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        user_id = ctx.author.id
        removed = cf_common.user_db.clear_minigame_optout(
            ctx.guild.id, QUEENS_GAME.name, user_id)
        if not removed:
            raise MinigameCogError(
                f'You are not opted out of '
                f'{QUEENS_GAME.display_name} ratings.')

        self._sync_queens_materialized_results(
            ctx.guild.id, migrate_legacy=False)
        self._recompute_minigame_ratings(
            ctx.guild.id, QUEENS_GAME, sync_results=False)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, user_id)
        detail = (
            'Your stored results now count again.'
            if link is not None
            else 'Register your LinkedIn name to count stored results.'
        )
        await ctx.send(embed=discord_common.embed_success(
            f'You are opted into {QUEENS_GAME.display_name} ratings. '
            f'{detail}'))

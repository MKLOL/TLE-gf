"""Direct LinkedIn-game registration helpers. (Minigames cog impl mixin.)

The player link is shared by every LinkedIn game (``game.link_key``), so a
registration made through any one of them claims stored results and
recomputes ratings for all of them.
"""

import time

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import normalize_queens_name
from tle.cogs._minigame_helpers import MinigameCogError, _safe_member_name
from tle.cogs._minigame_queens_cog import (
    _QUEENS_ANONYMOUS_LABEL,
    _QUEENS_ANONYMOUS_LINK_MARKER,
    _clean_queens_linkedin_name,
    _is_queens_link_anonymous,
    _queens_public_link_name,
)


class ImplQueensRegMixin:
    def _ensure_queens_link_available(
            self, guild, game, member, name, normalized_name, *,
            anonymous=False):
        public_name = _QUEENS_ANONYMOUS_LABEL if anonymous else name
        existing = cf_common.user_db.get_minigame_player_link_by_name(
            guild.id, game.link_key, normalized_name)
        if existing is not None and str(existing.user_id) != str(member.id):
            if anonymous or _is_queens_link_anonymous(existing):
                raise MinigameCogError(
                    f'That {self._linkedin_short_name(game)} name is already '
                    'taken.')
            existing_label = self._queens_public_user_name(
                guild, existing.user_id, {str(existing.user_id): existing})
            raise MinigameCogError(
                f'LinkedIn name `{public_name}` is already linked to '
                f'{existing_label}.')

    def _prepare_queens_registration_link(
            self, guild, game, member, name_text, *, anonymous=False):
        self._ensure_not_minigame_banned(
            guild.id, game, member.id, _safe_member_name(member))
        name = _clean_queens_linkedin_name(name_text)
        normalized = normalize_queens_name(name)
        self._ensure_queens_link_available(
            guild, game, member, name, normalized, anonymous=anonymous)
        external_url = (
            _QUEENS_ANONYMOUS_LINK_MARKER if anonymous else None)
        return name, normalized, external_url

    def _save_queens_registration_link(
            self, guild_id, game, member_id, name, normalized_name,
            external_url, linked_by):
        """Write the shared link, then claim results in every LinkedIn game."""
        games = self._linkedin_games()
        previous_link = cf_common.user_db.get_minigame_player_link(
            guild_id, game.link_key, member_id)
        if previous_link is not None:
            for linked_game in games:
                self._migrate_legacy_queens_results_to_external(
                    guild_id, linked_game)
                self._delete_queens_materialized_results_for_link(
                    guild_id, linked_game, previous_link)
        cf_common.user_db.set_minigame_player_link(
            guild_id, game.link_key, member_id, name, normalized_name,
            external_url, time.time(), linked_by)
        claimed = 0
        for linked_game in games:
            self._migrate_legacy_queens_results_to_external(
                guild_id, linked_game)
            claimed += self._claim_queens_unresolved_results(
                guild_id, linked_game, member_id, normalized_name)
            self._recompute_minigame_ratings(
                guild_id, linked_game, sync_results=False)
        return claimed

    def _cmd_queens_register_link(
            self, ctx, game, member, name_text, *, anonymous=False):
        name, normalized, external_url = self._prepare_queens_registration_link(
            ctx.guild, game, member, name_text, anonymous=anonymous)
        return self._save_queens_registration_link(
            ctx.guild.id, game, member.id, name, normalized, external_url,
            ctx.author.id)

    async def _cmd_queens_set(
            self, ctx, game, member, name_text, anonymous=False):
        """Moderator overwrite path; unlike ``register``, replacement is allowed."""
        self._require_enabled(ctx.guild.id, game)
        self._cmd_queens_register_link(
            ctx, game, member, name_text, anonymous=anonymous)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        message = (
            f'`{display_name}` is registered for '
            f'{self._linkedin_games_label()} as '
            f'`{_queens_public_link_name(link)}`.')
        if cf_common.user_db.is_minigame_opted_out(
                ctx.guild.id, game.name, member.id):
            message += (
                f' Their {game.display_name} rating opt-out remains active, '
                'so new results are stored unrated.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _cmd_queens_register(
            self, ctx, game, member, name_text, anonymous=False):
        self._require_enabled(ctx.guild.id, game)
        existing = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, member.id)
        if existing is not None:
            display_name = self._queens_public_user_name(
                ctx.guild, member.id, {str(member.id): existing})
            raise MinigameCogError(
                f'`{display_name}` is already registered for '
                f'{self._linkedin_games_label()}. Run '
                f'`;{game.name} unregister` before registering again.')

        self._ensure_queens_registration_allowed(
            ctx.guild.id, game, ctx.author.id, member.id,
            self._queens_public_user_name(ctx.guild, member.id))
        rating_opted_out = cf_common.user_db.is_minigame_opted_out(
            ctx.guild.id, game.name, member.id)
        claimed = self._cmd_queens_register_link(
            ctx, game, member, name_text, anonymous=anonymous)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        registered_name = _queens_public_link_name(link)
        if anonymous and getattr(
                ctx, 'reveal_queens_anonymous_name', False):
            registered_name = link.external_name
        lines = [
            f'`{display_name}` is registered for '
            f'{self._linkedin_games_label()} as `{registered_name}`.',
        ]
        if rating_opted_out:
            lines.append(
                f'Your {game.display_name} rating opt-out remains active. '
                f'Run `;{game.name} optin` before your next result if you '
                'want it to affect ratings.')
        elif claimed:
            lines.append(
                f'Claimed {claimed} stored {self._linkedin_short_name(game)} '
                'result(s) and recomputed ratings.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

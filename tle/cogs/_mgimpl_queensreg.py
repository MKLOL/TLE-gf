"""Direct Queens registration helpers. (Minigames cog impl mixin.)"""

import time

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import QUEENS_GAME, normalize_queens_name
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
            self, guild, member, name, normalized_name, *, anonymous=False):
        public_name = _QUEENS_ANONYMOUS_LABEL if anonymous else name
        existing = cf_common.user_db.get_minigame_player_link_by_name(
            guild.id, QUEENS_GAME.name, normalized_name)
        if existing is not None and str(existing.user_id) != str(member.id):
            if anonymous or _is_queens_link_anonymous(existing):
                raise MinigameCogError(
                    'That Queens name is already taken.')
            existing_label = self._queens_public_user_name(
                guild, existing.user_id, {str(existing.user_id): existing})
            raise MinigameCogError(
                f'Queens name `{public_name}` is already linked to '
                f'{existing_label}.')

    def _prepare_queens_registration_link(
            self, guild, member, name_text, *, anonymous=False):
        self._ensure_not_minigame_banned(
            guild.id, QUEENS_GAME, member.id, _safe_member_name(member))
        name = _clean_queens_linkedin_name(name_text)
        normalized = normalize_queens_name(name)
        self._ensure_queens_link_available(
            guild, member, name, normalized, anonymous=anonymous)
        external_url = (
            _QUEENS_ANONYMOUS_LINK_MARKER if anonymous else None)
        return name, normalized, external_url

    def _save_queens_registration_link(
            self, guild_id, member_id, name, normalized_name, external_url,
            linked_by):
        previous_link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member_id)
        if previous_link is not None:
            self._migrate_legacy_queens_results_to_external(guild_id)
            self._delete_queens_materialized_results_for_link(
                guild_id, previous_link)
        cf_common.user_db.set_minigame_player_link(
            guild_id, QUEENS_GAME.name, member_id, name, normalized_name,
            external_url, time.time(), linked_by)
        self._migrate_legacy_queens_results_to_external(guild_id)
        claimed = self._claim_queens_unresolved_results(
            guild_id, member_id, normalized_name)
        self._recompute_minigame_ratings(
            guild_id, QUEENS_GAME, sync_results=False)
        return claimed

    def _cmd_queens_register_link(
            self, ctx, member, name_text, *, anonymous=False):
        name, normalized, external_url = self._prepare_queens_registration_link(
            ctx.guild, member, name_text, anonymous=anonymous)
        return self._save_queens_registration_link(
            ctx.guild.id, member.id, name, normalized, external_url,
            ctx.author.id)

    async def _cmd_queens_set(
            self, ctx, member, name_text, anonymous=False):
        """Moderator overwrite path; unlike ``register``, replacement is allowed."""
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._cmd_queens_register_link(
            ctx, member, name_text, anonymous=anonymous)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        message = (
            f'`{display_name}` is registered for '
            f'{QUEENS_GAME.display_name} as '
            f'`{_queens_public_link_name(link)}`.')
        if cf_common.user_db.is_minigame_opted_out(
                ctx.guild.id, QUEENS_GAME.name, member.id):
            message += (
                ' Their rating opt-out remains active, so new results are '
                'stored unrated.')
        await ctx.send(embed=discord_common.embed_success(message))

    async def _cmd_queens_register(
            self, ctx, member, name_text, anonymous=False):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        existing = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if existing is not None:
            display_name = self._queens_public_user_name(
                ctx.guild, member.id, {str(member.id): existing})
            raise MinigameCogError(
                f'`{display_name}` is already registered for '
                f'{QUEENS_GAME.display_name}. Run `;queens unregister` '
                'before registering again.')

        self._ensure_queens_registration_allowed(
            ctx.guild.id, ctx.author.id, member.id,
            self._queens_public_user_name(ctx.guild, member.id))
        rating_opted_out = cf_common.user_db.is_minigame_opted_out(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        claimed = self._cmd_queens_register_link(
            ctx, member, name_text, anonymous=anonymous)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        registered_name = _queens_public_link_name(link)
        if anonymous and getattr(
                ctx, 'reveal_queens_anonymous_name', False):
            registered_name = link.external_name
        lines = [
            f'`{display_name}` is registered for {QUEENS_GAME.display_name} '
            f'as `{registered_name}`.',
        ]
        if rating_opted_out:
            lines.append(
                'Your rating opt-out remains active. Run `;queens optin` '
                'before your next result if you want it to affect ratings.')
        elif claimed:
            lines.append(
                f'Claimed {claimed} stored Queens result(s) and recomputed '
                'ratings.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

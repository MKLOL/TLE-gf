"""Lifecycle and shared minigame mod/config helpers."""

import asyncio
import json

from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common

from tle.cogs._minigame_akari import (
    AKARI_GAME,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, CaseInsensitiveMember, _mg,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_ADMINS_KEY, _split_queens_anonymous_flag,
)

# Extra per-guild Akari command admins (mirrors Queens' delegated-admin tier).
_AKARI_ADMINS_KEY = 'akari_admin_user_ids'


class ImplCoreMixin:
    async def cog_load(self):
        # ;akari and ;queens are canonical top-level groups; mirror them under
        # ;mg so the nested command paths keep working. Same object in both
        # all_commands dicts -> identical callback dispatch, no parent mutation.
        # Defensive guard: the test harness stubs commands.group, so the
        # group objects don't expose all_commands/get_command — skip in that case.
        if not hasattr(self.minigames, 'all_commands'):
            return
        for group in (self.akari, self.queens):
            if not hasattr(group, 'aliases'):
                continue
            for key in (group.name, *group.aliases):
                if self.minigames.all_commands.get(key) is None:
                    self.minigames.all_commands[key] = group

    async def cog_unload(self):
        import_tasks = list(self._import_tasks.values())
        for task in import_tasks:
            task.cancel()
        if import_tasks:
            await asyncio.gather(*import_tasks, return_exceptions=True)

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _is_enabled(guild_id, feature_flag):
        return cf_common.user_db.get_guild_config(guild_id, feature_flag) == '1'

    @staticmethod
    def _get_channel(guild_id, game_name):
        return cf_common.user_db.get_minigame_channel(guild_id, game_name)

    def _game_for_channel(self, message):
        """Return the GameDef whose configured channel matches, or None."""
        for game in self.GAMES.values():
            if game.manual_ingest_only:
                continue
            if not self._is_enabled(message.guild.id, game.feature_flag):
                continue
            channel_id = self._get_channel(message.guild.id, game.name)
            if channel_id is not None and str(message.channel.id) == str(channel_id):
                return game
        return None

    @staticmethod
    def _require_enabled(guild_id, game):
        if cf_common.user_db.get_guild_config(guild_id, game.feature_flag) != '1':
            raise MinigameCogError(
                f'{game.display_name} is not enabled. '
                f'An admin can enable it with `;meta config enable {game.feature_flag}`.'
            )

    async def _resolve_member(self, ctx, member_text):
        try:
            return await CaseInsensitiveMember().convert(ctx, member_text)
        except commands.BadArgument as exc:
            raise MinigameCogError(str(exc)) from exc

    @staticmethod
    def _resolve_registrar_target(ctx, member):
        """Validate that ``ctx.author`` may (un)register ``member``.

        Anyone can (un)register themselves; only mods/admins can act on someone
        else.  Passing your own member object is treated the same as omitting
        it.  Returns the resolved target.
        """
        if member is None or member.id == ctx.author.id:
            return ctx.author
        is_mod = any(r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
                     for r in ctx.author.roles)
        if not is_mod:
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                f'can register or unregister other users.')
        return member

    @staticmethod
    def _mod_role_error_message():
        return (
            f'You need the `{constants.TLE_ADMIN}` or '
            f'`{constants.TLE_MODERATOR}` role or Queens admin access.')

    @staticmethod
    def _has_server_mod_role(member):
        allowed = {constants.TLE_ADMIN, constants.TLE_MODERATOR}
        return any(r.name in allowed for r in getattr(member, 'roles', []))

    @staticmethod
    def _guild_admin_ids(guild_id, config_key):
        if cf_common.user_db is None:
            return set()
        raw = cf_common.user_db.get_guild_config(guild_id, config_key)
        if not raw:
            return set()
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return set()
        if not isinstance(data, list):
            return set()
        return {
            str(user_id)
            for user_id in data
            if str(user_id).strip()
        }

    @staticmethod
    def _set_guild_admin_ids(guild_id, config_key, user_ids):
        user_ids = sorted(
            {str(user_id) for user_id in user_ids},
            key=_mg().Minigames._user_id_sort_key)
        if user_ids:
            cf_common.user_db.set_guild_config(
                guild_id, config_key, json.dumps(user_ids))
        else:
            cf_common.user_db.delete_guild_config(guild_id, config_key)

    @staticmethod
    def _queens_admin_ids(guild_id):
        return _mg().Minigames._guild_admin_ids(guild_id, _QUEENS_ADMINS_KEY)

    @staticmethod
    def _set_queens_admin_ids(guild_id, user_ids):
        _mg().Minigames._set_guild_admin_ids(
            guild_id, _QUEENS_ADMINS_KEY, user_ids)

    @staticmethod
    def _akari_admin_ids(guild_id):
        return _mg().Minigames._guild_admin_ids(guild_id, _AKARI_ADMINS_KEY)

    @staticmethod
    def _set_akari_admin_ids(guild_id, user_ids):
        _mg().Minigames._set_guild_admin_ids(
            guild_id, _AKARI_ADMINS_KEY, user_ids)

    @staticmethod
    def _user_id_sort_key(user_id):
        try:
            return 0, int(user_id)
        except (TypeError, ValueError):
            return 1, str(user_id)

    def _has_queens_mod_access(self, guild_id, member):
        return (
            self._has_server_mod_role(member)
            or str(getattr(member, 'id', None)) in self._queens_admin_ids(guild_id)
        )

    @staticmethod
    def _akari_mod_role_error_message():
        return (
            f'You need the `{constants.TLE_ADMIN}` or '
            f'`{constants.TLE_MODERATOR}` role or Akari admin access.')

    def _has_akari_mod_access(self, guild_id, member):
        return (
            self._has_server_mod_role(member)
            or str(getattr(member, 'id', None)) in self._akari_admin_ids(guild_id)
        )

    def _resolve_queens_registrar_target(self, ctx, member):
        if member is None or member.id == ctx.author.id:
            return ctx.author
        if not self._has_queens_mod_access(ctx.guild.id, ctx.author):
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                'or Queens admins can register or unregister other users.')
        return member

    @staticmethod
    def _minigame_banned_user_ids(guild_id, game):
        return {
            str(row.user_id)
            for row in cf_common.user_db.get_minigame_bans(guild_id, game.name)
        }

    @staticmethod
    def _minigame_opted_out_user_ids(guild_id, game):
        return {
            str(row.user_id)
            for row in cf_common.user_db.get_minigame_optouts(
                guild_id, game.name)
        }

    def _minigame_hidden_user_ids(self, guild_id, game):
        """Users who must never appear in rankings: the self-opted-out.

        Bans are deliberately NOT part of this set — like Akari's, they are
        forward-only: they gate new ingestion/imports (with a notice) but a
        banned player's existing results stay in the rating pool.  Only the
        sticky self opt-out hides stored rows.
        """
        return self._minigame_opted_out_user_ids(guild_id, game)

    def _filter_minigame_banned_rows(self, guild_id, game, rows):
        # Akari's opt-out lives in its own tables and is applied via the
        # registrants filter at display time; generic opt-outs are for manual
        # minigames such as Queens.  (Despite the historical name, this
        # filters *hidden* users — bans are forward-only and never drop rows.)
        if game.name == AKARI_GAME.name:
            return rows
        hidden = self._minigame_hidden_user_ids(guild_id, game)
        if not hidden:
            return rows
        return [row for row in rows if str(row.user_id) not in hidden]

    def _ensure_queens_registration_allowed(self, guild_id, actor_id,
                                            target_id, target_label):
        """Gate ordinary Queens registration against a rating opt-out.

        Registration only controls the LinkedIn identity link and never changes
        the independent rating choice. An opted-out user may link themselves,
        while moderators can update their link through ``queens set``.
        """
        if str(actor_id) == str(target_id):
            return
        if cf_common.user_db.is_minigame_opted_out(
                guild_id, QUEENS_GAME.name, target_id):
            raise MinigameCogError(
                f'`{target_label}` opted out of {QUEENS_GAME.display_name} '
                'ratings. Use `;queens set` to update their LinkedIn link '
                'without changing that rating choice.')

    def _sync_minigame_results_for_read(self, guild_id, game):
        if game.name == QUEENS_GAME.name:
            self._sync_queens_materialized_results(
                guild_id, migrate_legacy=False)

    @staticmethod
    def _ensure_not_minigame_banned(guild_id, game, user_id, member_name):
        if cf_common.user_db.is_minigame_banned(guild_id, game.name, user_id):
            raise MinigameCogError(
                f'`{member_name}` is banned from {game.display_name}.')

    async def _resolve_queens_registration_args(self, ctx, first, rest):
        if first is None:
            raise MinigameCogError(
                'Usage: `;queens register [+username DiscordUser] '
                'LinkedIn Name [+anon]`.')
        first = str(first).strip()
        rest = (rest or '').strip()
        target = ctx.author
        linkedin = first if not rest else f'{first} {rest}'

        if first.casefold() == '+username':
            tokens = rest.split(maxsplit=1)
            if len(tokens) < 2:
                raise MinigameCogError(
                    'Usage: `;queens register +username DiscordUser '
                    'LinkedIn Name [+anon]`.')
            target = await self._resolve_member(ctx, tokens[0])
            target = self._resolve_queens_registrar_target(ctx, target)
            linkedin = tokens[1]
        linkedin, anonymous = _split_queens_anonymous_flag(linkedin)
        if not linkedin:
            raise MinigameCogError('A LinkedIn display name is required.')
        return target, linkedin, anonymous

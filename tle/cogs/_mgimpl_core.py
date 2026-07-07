"""Lifecycle, scheduled Queens update, and shared mod/config helpers. (Minigames cog impl mixin; see minigames.py)."""

import asyncio
import datetime as dt
import json
import logging
from zoneinfo import ZoneInfo

from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks

from tle.cogs._minigame_akari import (
    AKARI_GAME,
)
from tle.cogs._minigame_queens import (
    QUEENS_GAME,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, CaseInsensitiveMember, _mg, _ScheduledCtx,
)
from tle.cogs._minigame_queens_cog import (
    _QUEENS_CONNECTION_ACCOUNT_KEY, _QUEENS_DEFAULT_CONNECTION_ACCOUNT,
    _QUEENS_ADMINS_KEY, _QUEENS_DAILY_UPDATE_LAST_PREFIX,
    _QUEENS_DAILY_UPDATE_CHECK_INTERVAL, _QUEENS_DAILY_UPDATE_PRECISE_WINDOW,
    _QUEENS_DAILY_UPDATE_TZ,
    _queens_daily_update_target_datetime,
    _split_queens_anonymous_flag,
)

logger = logging.getLogger(__name__)

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
        connect_tasks = list(self._queens_connect_tasks.values())
        for task in connect_tasks:
            task.cancel()
        if connect_tasks:
            await asyncio.gather(*connect_tasks, return_exceptions=True)
        update_timers = list(self._queens_update_timers.values())
        for task in update_timers:
            task.cancel()
        if update_timers:
            await asyncio.gather(*update_timers, return_exceptions=True)

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._queens_daily_update_check.start()

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

    # ── Scheduled Queens update ─────────────────────────────────────────

    @tasks.task_spec(name='QueensDailyUpdateCheck',
                     waiter=tasks.Waiter.fixed_delay(
                         _QUEENS_DAILY_UPDATE_CHECK_INTERVAL))
    async def _queens_daily_update_check(self, _):
        if cf_common.user_db is None:
            return
        now = dt.datetime.now(ZoneInfo(_QUEENS_DAILY_UPDATE_TZ))
        today = now.strftime('%Y-%m-%d')
        for guild in self.bot.guilds:
            try:
                await self._check_queens_daily_update_guild(guild, now, today)
            except Exception:
                logger.warning(
                    'Queens daily update check failed for guild=%s',
                    getattr(guild, 'id', None), exc_info=True)

    async def _check_queens_daily_update_guild(self, guild, now, today):
        if not self._is_enabled(guild.id, QUEENS_GAME.feature_flag):
            return
        channel_id = self._get_channel(guild.id, QUEENS_GAME.name)
        if channel_id is None:
            return
        kvs_key = f'{_QUEENS_DAILY_UPDATE_LAST_PREFIX}{guild.id}'
        target = _queens_daily_update_target_datetime(now)
        if cf_common.user_db.kvs_get(kvs_key) == today:
            next_target = target + dt.timedelta(days=1)
            seconds_until_next = (next_target - now).total_seconds()
            if 0 < seconds_until_next <= _QUEENS_DAILY_UPDATE_PRECISE_WINDOW:
                self._schedule_queens_daily_update_timer(
                    guild, seconds_until_next)
            return

        seconds_until = (target - now).total_seconds()
        pending = self._queens_update_timers.get(guild.id)
        if seconds_until <= 0:
            if pending is not None and not pending.done():
                return
            if await self._send_queens_daily_update(guild):
                cf_common.user_db.kvs_set(kvs_key, today)
        elif seconds_until <= _QUEENS_DAILY_UPDATE_PRECISE_WINDOW:
            self._schedule_queens_daily_update_timer(guild, seconds_until)

    def _schedule_queens_daily_update_timer(self, guild, delay):
        pending = self._queens_update_timers.get(guild.id)
        if pending is None or pending.done():
            logger.info(
                'Scheduling precise Queens daily update for guild=%s in %.0fs',
                guild.id, delay)
            self._queens_update_timers[guild.id] = asyncio.create_task(
                self._precise_queens_daily_update(guild, delay))

    async def _precise_queens_daily_update(self, guild, delay):
        try:
            await asyncio.sleep(delay)
            current_today = dt.datetime.now(
                ZoneInfo(_QUEENS_DAILY_UPDATE_TZ)).strftime('%Y-%m-%d')
            kvs_key = f'{_QUEENS_DAILY_UPDATE_LAST_PREFIX}{guild.id}'
            if cf_common.user_db.kvs_get(kvs_key) == current_today:
                return
            if await self._send_queens_daily_update(guild):
                cf_common.user_db.kvs_set(kvs_key, current_today)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                'Precise Queens daily update failed for guild=%s',
                guild.id, exc_info=True)
        finally:
            self._queens_update_timers.pop(guild.id, None)

    async def _send_queens_daily_update(self, guild):
        channel_id = self._get_channel(guild.id, QUEENS_GAME.name)
        if channel_id is None:
            return False
        try:
            channel = await self._resolve_channel(int(channel_id))
        except Exception:
            logger.warning(
                'Queens daily update channel missing for guild=%s channel=%s',
                guild.id, channel_id, exc_info=True)
            return False

        ctx = _ScheduledCtx(self.bot, guild, channel)
        try:
            await self._cmd_queens_play(
                ctx, import_results=False, send_notice=False)
            await self._cmd_queens_update(ctx, results_day='yesterday')
            return True
        except MinigameCogError as exc:
            message = str(exc)
            if 'rate-limited' in message:
                logger.info(
                    'Queens daily update deferred by rate limit for guild=%s: %s',
                    guild.id, message)
                return False
            try:
                await channel.send(embed=discord_common.embed_alert(message))
            except Exception:
                logger.warning(
                    'Failed to send Queens daily update error for guild=%s',
                    guild.id, exc_info=True)
            return True

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
        """Gate Queens (re-)registration against a sticky self opt-out.

        A user who ran ``;queens unregister`` is hidden until *they themselves*
        register again.  When the actor is the target, registering expresses
        that intent, so we lift the opt-out and proceed.  When anyone else
        (a mod, ``+username``, an import) tries to register an opted-out user,
        we refuse so they cannot be re-surfaced against their will.
        """
        if str(actor_id) == str(target_id):
            cf_common.user_db.clear_minigame_optout(
                guild_id, QUEENS_GAME.name, target_id)
            return
        if cf_common.user_db.is_minigame_opted_out(
                guild_id, QUEENS_GAME.name, target_id):
            raise MinigameCogError(
                f'`{target_label}` opted out of {QUEENS_GAME.display_name} '
                'rankings. Only they can rejoin by running '
                '`;queens register` themselves.')

    def _sync_minigame_results_for_read(self, guild_id, game):
        if game.name == QUEENS_GAME.name:
            self._sync_queens_materialized_results(
                guild_id, migrate_legacy=False)

    @staticmethod
    def _ensure_not_minigame_banned(guild_id, game, user_id, member_name):
        if cf_common.user_db.is_minigame_banned(guild_id, game.name, user_id):
            raise MinigameCogError(
                f'`{member_name}` is banned from {game.display_name}.')

    @staticmethod
    def _get_queens_connection_account(guild_id):
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)
        if raw is None:
            return dict(_QUEENS_DEFAULT_CONNECTION_ACCOUNT)
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return {'name': raw, 'url': None}
        name = data.get('name')
        if not name:
            return None
        return {'name': name, 'url': data.get('url')}

    @staticmethod
    def _set_queens_connection_account(guild_id, name, url):
        cf_common.user_db.set_guild_config(
            guild_id,
            _QUEENS_CONNECTION_ACCOUNT_KEY,
            json.dumps({'name': name, 'url': url}, sort_keys=True),
        )

    @staticmethod
    def _clear_queens_connection_account(guild_id):
        cf_common.user_db.delete_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)

    def _queens_connection_instruction(self, guild_id):
        account = self._get_queens_connection_account(guild_id)
        if account is None:
            return (
                'Ask a moderator to set the LinkedIn account to connect with '
                'using `;queens connection set LinkedIn Name profile_url`.'
            )
        if account.get('url'):
            account_text = f'[this LinkedIn account]({account["url"]})'
        else:
            account_text = 'the configured LinkedIn account'
        return (
            'To join the rating system, send a LinkedIn connection request '
            f'to {account_text}. If you are already connected but not '
            'registered, disconnect on LinkedIn first, then send a new request.'
        )

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


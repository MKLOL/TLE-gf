"""Market lifecycle + bet execution + scheduler/settlement engine for the
betting cog.

Plain mixin (not a ``commands.Cog``); mixed into ``Betting``. Holds the heavy
implementation logic so the command file stays small. The ``bet`` command
callbacks in ``betting.py`` are thin wrappers over these methods.
"""
import asyncio
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import football_data
from tle.util import odds_api
from tle.cogs._betting_helpers import (
    is_remove_amount, normalize_event, parse_amount, parse_settle_arg,
    payout_amount, resolve_pick, _api_key, _event_fixture_key,
    _football_data_key, _no_mentions, _same_match_market_event,
)

logger = logging.getLogger(__name__)

_POOL_REFRESH_DELAY = 5
# Keep a settled market's betting thread open this long so members can keep
# talking about the finished game, then lock it. Was locked immediately.
_THREAD_LOCK_DELAY = 12 * 3600
# If a thread still can't be locked this long past its deadline it is gone
# (deleted, or auto-archived out of cache after ~24h idle) and unreachable —
# give up and mark it locked so it stops re-appearing on the pending-lock sweep.
_LOCK_GIVE_UP_GRACE = 24 * 3600


class BettingCogError(commands.CommandError):
    pass


class BetEngineMixin:
    # ── Odds cache ─────────────────────────────────────────────────────

    async def _ensure_wc_events(self, max_age):
        """Return World Cup odds events, refetching only if the cache is older
        than max_age. Raises BettingCogError if no key / fetch fails."""
        now = time.time()
        if (self._wc_events is not None and self._wc_fetched_at is not None
                and now - self._wc_fetched_at <= max_age):
            return self._wc_events
        api_key = _api_key()
        if not api_key:
            raise BettingCogError(
                'Live odds are not configured (no `ODDS_API_KEY`). An admin can '
                'still settle markets manually with `;bet settle`.')
        try:
            events = await odds_api.fetch_h2h(
                api_key, [odds_api.WORLD_CUP_SPORT_KEY])
        except odds_api.OddsApiError as e:
            logger.warning('World Cup odds fetch failed: %s', e)
            raise BettingCogError(f'Could not fetch World Cup odds: {e}')
        fd_matches = await self._ensure_fd_matches(max_age)
        self._wc_events = [
            normalize_event(event, knockout=self._event_knockout(event, fd_matches))
            for event in events]
        self._wc_fetched_at = now
        return self._wc_events

    async def _ensure_fd_matches(self, max_age):
        """Return the football-data World Cup fixture list (which carries each
        match's tournament ``stage``), refetching only if the cache is stale.

        Best-effort: with no token or on a fetch failure it returns the last
        good list (or ``[]``), so a football-data outage degrades to "no stage
        info" rather than breaking odds. Callers fail safe to a 1X2 market."""
        now = time.time()
        if (self._fd_matches is not None and self._fd_fetched_at is not None
                and now - self._fd_fetched_at <= max_age):
            return self._fd_matches
        token = _football_data_key()
        if not token:
            return self._fd_matches or []
        try:
            matches = await football_data.fetch_wc_matches(token)
        except football_data.FootballDataError as e:
            logger.warning('stage lookup: football-data fetch failed: %s', e)
            return self._fd_matches or []
        self._fd_matches = matches
        self._fd_fetched_at = now
        return matches

    def _event_knockout(self, event, fd_matches):
        """Whether a market for this fixture should be a 2-way 'to advance'
        market, decided from the authoritative competition stage rather than a
        hardcoded date. Unknown stage / no data → False (offer a draw)."""
        stage = football_data.find_match_stage(
            event.get('home_team'), event.get('away_team'),
            event.get('commence_time'), fd_matches)
        if stage is not None:
            return football_data.is_knockout_stage(stage)
        # No stage from a name match: football-data often lists a knockout slot
        # as a nameless placeholder (null vs null) until both feeder games
        # finish, so a clearly-knockout tie (e.g. Portugal vs Spain) would fall
        # back to a draw for days. The group phase strictly precedes knockout,
        # so treat any fixture after the last group kickoff as knockout.
        return football_data.is_after_group_stage(
            event.get('commence_time'), fd_matches)

    # ── Notify-role validation ─────────────────────────────────────────

    def _member_has_role(self, member, role_id):
        return any(str(getattr(role, 'id', None)) == str(role_id)
                   for role in getattr(member, 'roles', []) or [])

    def _bot_can_ping_role(self, ctx, role):
        if getattr(role, 'mentionable', True):
            return True
        me = getattr(ctx.guild, 'me', None)
        perms = getattr(me, 'guild_permissions', None)
        return (getattr(perms, 'administrator', False)
                or getattr(perms, 'mention_everyone', False))

    def _validate_notify_role(self, ctx, role):
        if hasattr(role, 'is_default') and role.is_default():
            raise BettingCogError('Configure a normal role, not `@everyone`.')
        if getattr(role, 'managed', False):
            raise BettingCogError('Managed roles cannot be used for notifications.')
        is_assignable = getattr(role, 'is_assignable', None)
        if callable(is_assignable) and not is_assignable():
            raise BettingCogError(
                'I cannot assign that role. Put my bot role above it and give '
                'me Manage Roles.')
        perms = getattr(role, 'permissions', None)
        if getattr(perms, 'value', 0):
            raise BettingCogError(
                'The notification role must have no server permissions.')
        if not self._bot_can_ping_role(ctx, role):
            raise BettingCogError(
                'That role is not mentionable. Make it mentionable or give me '
                'Mention Everyone so market-open pings work.')

    # ── Market lookup ──────────────────────────────────────────────────

    def _open_markets_for_channel(self, guild_id, channel_id):
        return [
            market for market in cf_common.user_db.bet_markets_open(guild_id)
            if str(market.channel_id) == str(channel_id)
        ]

    def _market_accepts_bets(self, market):
        """True if the market can still take a wager — open, not locked early,
        and pre-kickoff. Mirrors the closed-check in ``_execute_bet``."""
        return not market.bets_closed and time.time() < market.commence_time

    def _find_market(self, ctx, *, require_unambiguous=False, bettable_only=False):
        """The open market relevant to where the command was run: the betting
        thread if we're in one, else the channel's market.

        ``bettable_only`` narrows an ambiguous channel to markets still taking
        bets — a locked or kicked-off market isn't a real target for a wager,
        so if exactly one market can still take bets it's unambiguous.
        """
        m = cf_common.user_db.bet_market_get_active_by_thread(
            ctx.guild.id, ctx.channel.id)
        if m is not None:
            return m
        if require_unambiguous:
            candidates = self._open_markets_for_channel(ctx.guild.id, ctx.channel.id)
            if len(candidates) > 1 and bettable_only:
                live = [m for m in candidates if self._market_accepts_bets(m)]
                if len(live) == 1:
                    return live[0]
            if len(candidates) > 1:
                raise BettingCogError(
                    'Multiple betting markets are open here. Run this command in '
                    'the match thread so the target is unambiguous.')
            return candidates[0] if candidates else None
        return cf_common.user_db.bet_market_get_active(ctx.guild.id, ctx.channel.id)

    def _find_duplicate_match(self, guild_id, event):
        by_key = cf_common.user_db.bet_market_get_open_for_fixture(
            guild_id, _event_fixture_key(event))
        if by_key is not None:
            return by_key
        for market in cf_common.user_db.bet_markets_open(guild_id):
            if _same_match_market_event(market, event):
                return market
        return None

    def _market_place_ref(self, market):
        if market is None:
            return 'that match'
        if market.thread_id:
            return f'<#{market.thread_id}>'
        return f'<#{market.channel_id}>'

    def _parse_result(self, market, text):
        """Resolve a result for settle/correct: home/draw/away alias, a
        scoreline (2-1 → scores + outcome), or a team name. Returns
        (outcome, home_score, away_score) or None."""
        parsed = parse_settle_arg(text)
        if parsed is not None:
            return parsed
        pick = resolve_pick(text, market.home_team, market.away_team)
        if pick is not None:
            return (pick, None, None)
        return None

    # ── Market creation (shared by manual + auto) ──────────────────────

    def _create_market(self, guild_id, channel_id, event):
        if self._find_duplicate_match(guild_id, event) is not None:
            logger.warning(
                'skipping duplicate bet market for %s vs %s in guild %s '
                '(provider event_id=%s)',
                event.get('home_team'), event.get('away_team'), guild_id,
                event.get('event_id'))
            return None
        o = event['odds']
        creator = (self.bot.user.id if self.bot and self.bot.user else '0')
        return cf_common.user_db.bet_market_create(
            guild_id, channel_id, event['event_id'], event['sport_key'],
            event['home_team'], event['away_team'], event['commence_time'],
            o['home'], o['draw'], o['away'], creator, time.time())

    async def _create_thread(self, market_id, msg, market):
        """Create the betting thread off the announcement message and post the
        intro. Returns the thread, or None if creation failed."""
        try:
            thread = await msg.create_thread(name=self._thread_name(market),
                                             auto_archive_duration=1440)
        except (discord.HTTPException, AttributeError) as e:
            logger.warning('thread create failed for market %s: %s', market_id, e)
            return None
        cf_common.user_db.bet_market_set_thread(market_id, thread.id)
        try:
            intro = await thread.send(embed=self._thread_intro_embed(market))
            if getattr(intro, 'id', None) is not None:
                cf_common.user_db.bet_market_set_thread_intro(market_id, intro.id)
        except discord.HTTPException:
            pass
        return thread

    def _schedule_pool_refresh(self, market_id):
        if not self.bot:
            return
        existing = self._pool_refresh_timers.pop(market_id, None)
        if existing is not None and not existing.done():
            existing.cancel()
        self._pool_refresh_timers[market_id] = asyncio.create_task(
            self._pool_refresh_timer(market_id))

    async def _pool_refresh_timer(self, market_id):
        try:
            await asyncio.sleep(_POOL_REFRESH_DELAY)
            await self._refresh_pool_message(market_id)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning('bet pool refresh failed for market %s', market_id,
                           exc_info=True)
        finally:
            self._pool_refresh_timers.pop(market_id, None)

    async def _refresh_pool_message(self, market_id):
        if cf_common.user_db is None or not self.bot:
            return
        market = cf_common.user_db.bet_market_get(market_id)
        if market is None or not market.thread_id:
            return
        intro_id = getattr(market, 'thread_intro_id', None)
        if not intro_id:
            return
        thread = self.bot.get_channel(int(market.thread_id))
        if thread is None:
            return
        try:
            msg = await thread.fetch_message(int(intro_id))
            await msg.edit(embed=self._thread_intro_embed(market))
        except (discord.HTTPException, AttributeError, KeyError, ValueError):
            logger.warning('could not refresh bet pool for market %s', market_id)

    async def _delete_message(self, msg):
        try:
            await msg.delete()
        except (discord.HTTPException, AttributeError):
            pass

    # ── Placing bets ───────────────────────────────────────────────────

    async def _execute_bet(self, guild_id, market, user, pick, amount_str,
                           *, actor_id=None):
        """Core bet placement. ``user`` is the bettor whose wallet is charged;
        ``actor_id`` (default: the bettor) records who placed it in the wallet
        audit log so an admin betting on someone's behalf is attributable.

        Returns (status, data):
          'closed'       — kickoff passed
          'invalid'      — amount didn't parse / below minimum
          'insufficient' — not enough balance (data={'balance': N})
          'ok'           — placed (data has stake/odds/label/potential/balance)
          'removed'      — removed one pick (data has stake/label/balance)
          'unchanged'    — same pick already had the requested stake
        """
        if not self._market_accepts_bets(market):
            return ('closed', None)
        if not self._pick_allowed(market, pick):
            return ('invalid_pick', None)
        label = self._pick_label(market, pick)
        if is_remove_amount(amount_str):
            ok, reason, new_balance, refunded = cf_common.user_db.bet_remove_wager(
                guild_id, market.market_id, user.id, pick, time.time(),
                actor_id=actor_id)
            if not ok:
                if reason == 'closed':
                    return ('closed', None)
                return ('missing', {'label': label})
            return ('removed', {
                'stake': refunded, 'pick': pick, 'label': label,
                'balance': new_balance})
        balance = cf_common.user_db.bet_ensure_wallet(
            guild_id, user.id, self._bet_start_balance(guild_id))
        existing = cf_common.user_db.bet_get_wager(market.market_id, user.id, pick)
        available = balance + (existing.stake if existing else 0)
        stake = parse_amount(amount_str, available, constants.BET_MIN_STAKE)
        if stake is None:
            return ('invalid', None)
        if stake > available:
            return ('insufficient', {'balance': available})
        odds = self._pick_odds(market, pick)
        if odds is None:
            return ('invalid_pick', None)
        ok, reason, new_balance = cf_common.user_db.bet_place(
            guild_id, market.market_id, user.id, pick, stake,
            time.time(), self._bet_start_balance(guild_id), actor_id=actor_id)
        if ok and reason == 'unchanged':
            return ('unchanged', {
                'stake': stake, 'odds': odds, 'pick': pick, 'label': label,
                'potential': payout_amount(stake, odds), 'balance': new_balance})
        if not ok:
            if reason == 'closed':
                return ('closed', None)
            if reason == 'invalid':
                return ('invalid', None)
            return ('insufficient', {'balance': available})
        return ('ok', {
            'stake': stake, 'odds': odds, 'pick': pick,
            'label': label,
            'potential': payout_amount(stake, odds), 'balance': new_balance})

    async def _react(self, message, emoji):
        try:
            await message.add_reaction(emoji)
        except (discord.HTTPException, AttributeError):
            pass

    # ── Settle / archive ───────────────────────────────────────────────

    async def _do_settle(self, market, outcome, home_score, away_score, *, source):
        if not self._pick_allowed(market, outcome):
            raise BettingCogError('That result is not available for this market.')
        outcome_rows = cf_common.user_db.bet_settle(
            market.guild_id, market.market_id, outcome, home_score, away_score,
            time.time())
        if outcome_rows is None:
            # Already settled/cancelled (e.g. mod settled while the poller was
            # mid-fetch). The status guard paid nobody twice — just bow out.
            logger.info('market %s already terminal; skipping settle',
                        market.market_id)
            return
        embed = self._settlement_embed(market, outcome, home_score, away_score,
                                       outcome_rows, source)
        # The final result is the market's second user-facing message, posted
        # only in the parent betting channel. Winner mentions in the embed don't
        # ping, but pin that down explicitly.
        channel = self.bot.get_channel(int(market.channel_id)) if self.bot else None
        if channel is not None:
            try:
                await channel.send(embed=embed, allowed_mentions=_no_mentions())
            except discord.HTTPException:
                logger.warning('could not post settlement to %s',
                               market.channel_id)
        # Don't lock the thread now — members keep using it to talk about the
        # game after full time. Arm a timer to lock it 12h later instead.
        self._schedule_thread_lock(market)
        logger.info('Settled bet market %s (%s) source=%s winners=%d',
                    market.market_id, outcome, source,
                    sum(1 for r in outcome_rows if r[4] > 0))

    def _schedule_thread_lock(self, market, *, delay=None):
        """Arm a timer to lock a settled market's thread after ``delay`` seconds
        (default 12h), leaving it open for post-game chat until then. Skips if a
        live lock timer already exists (idempotent across catch-up sweeps)."""
        if market is None or not market.thread_id or not self.bot:
            return
        market_id = market.market_id
        existing = self._lock_timers.get(market_id)
        if existing is not None and not existing.done():
            return
        if delay is None:
            delay = _THREAD_LOCK_DELAY
        self._lock_timers[market_id] = asyncio.create_task(
            self._lock_timer(market_id, max(0.0, delay)))

    async def _lock_timer(self, market_id, delay):
        try:
            await asyncio.sleep(delay)
            market = (cf_common.user_db.bet_market_get(market_id)
                      if cf_common.user_db is not None else None)
            if market is not None and not await self._archive_thread(market):
                self._give_up_lock_if_stale(market)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning('thread lock timer failed for market %s', market_id,
                           exc_info=True)
        finally:
            self._lock_timers.pop(market_id, None)

    def _give_up_lock_if_stale(self, market):
        """A settled market whose thread we still can't lock long past its
        deadline is unreachable (deleted / archived out of cache); mark it
        locked so the pending-lock sweep stops re-attempting it forever."""
        if market.settled_at is None:
            return
        horizon = market.settled_at + _THREAD_LOCK_DELAY + _LOCK_GIVE_UP_GRACE
        if time.time() > horizon:
            logger.warning('giving up locking market %s thread (unreachable)',
                           market.market_id)
            cf_common.user_db.bet_market_mark_thread_locked(market.market_id)

    async def _arm_lock_timers(self):
        """Restore/catch up the 12h thread-lock timers for settled markets whose
        thread isn't locked yet — the in-memory timers are lost on restart. Each
        market is (re)scheduled for its remaining grace time; one already past
        its deadline gets a zero delay and fires immediately. Routing through
        ``_schedule_thread_lock`` keeps it deduplicated against a live timer and
        off the startup path (fire-and-forget), rather than blocking here."""
        if cf_common.user_db is None or not self.bot:
            return
        now = time.time()
        for market in cf_common.user_db.bet_markets_pending_lock():
            deadline = (market.settled_at or now) + _THREAD_LOCK_DELAY
            self._schedule_thread_lock(market, delay=deadline - now)

    async def _archive_thread(self, market):
        """Lock (archive) the market's betting thread. Returns True once Discord
        accepts the edit, False if the thread is gone or the edit failed — the
        settle path's delayed-lock sweep retries on False. (The cancel /
        remediation callers ignore the result and lock best-effort, as before.)"""
        if not market.thread_id or not self.bot:
            return False
        thread = self.bot.get_channel(int(market.thread_id))
        if thread is None:
            return False
        try:
            await thread.edit(archived=True, locked=True)
        except (discord.HTTPException, AttributeError):
            return False
        cf_common.user_db.bet_market_mark_thread_locked(market.market_id)
        return True

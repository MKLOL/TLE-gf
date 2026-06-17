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
    is_remove_amount, normalize_event, outcome_from_score, parse_amount,
    parse_settle_arg, payout_amount, resolve_pick,
    _api_key, _event_fixture_key, _football_data_key, _no_mentions,
    _same_match_market_event,
)

logger = logging.getLogger(__name__)

_POOL_REFRESH_DELAY = 5


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
        self._wc_events = [normalize_event(event) for event in events]
        self._wc_fetched_at = now
        return self._wc_events

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

    def _find_market(self, ctx, *, require_unambiguous=False):
        """The open market relevant to where the command was run: the betting
        thread if we're in one, else the channel's market."""
        m = cf_common.user_db.bet_market_get_active_by_thread(
            ctx.guild.id, ctx.channel.id)
        if m is not None:
            return m
        if require_unambiguous:
            candidates = self._open_markets_for_channel(ctx.guild.id, ctx.channel.id)
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

    async def _execute_bet(self, guild_id, market, user, pick, amount_str):
        """Core bet placement. Returns (status, data):
          'closed'       — kickoff passed
          'invalid'      — amount didn't parse / below minimum
          'insufficient' — not enough balance (data={'balance': N})
          'ok'           — placed (data has stake/odds/label/potential/balance)
          'removed'      — removed one pick (data has stake/label/balance)
          'unchanged'    — same pick already had the requested stake
        """
        if time.time() >= market.commence_time or market.bets_closed:
            return ('closed', None)
        if not self._pick_allowed(market, pick):
            return ('invalid_pick', None)
        label = self._pick_label(market, pick)
        if is_remove_amount(amount_str):
            ok, reason, new_balance, refunded = cf_common.user_db.bet_remove_wager(
                guild_id, market.market_id, user.id, pick, time.time())
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
            time.time(), self._bet_start_balance(guild_id))
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
        await self._archive_thread(market)
        logger.info('Settled bet market %s (%s) source=%s winners=%d',
                    market.market_id, outcome, source,
                    sum(1 for r in outcome_rows if r[4] > 0))

    async def _archive_thread(self, market):
        if not market.thread_id or not self.bot:
            return
        thread = self.bot.get_channel(int(market.thread_id))
        if thread is None:
            return
        try:
            await thread.edit(archived=True, locked=True)
        except (discord.HTTPException, AttributeError):
            pass

    async def _settle_pending(self):
        """Settle finished markets. Primary source is football-data.org (free,
        so we settle promptly at full time, any time after kickoff). The Odds
        API scores endpoint (credits) is a fallback for markets still unsettled
        after the buffer — e.g. if football-data isn't configured or can't
        match the fixture."""
        await self._settle_via_football_data()
        await self._settle_via_odds_api()

    async def _settle_via_football_data(self):
        token = _football_data_key()
        if not token:
            return
        # Any market past kickoff is eligible — football-data tells us whether
        # the game has actually FINISHED, so no fixed buffer is needed.
        markets = cf_common.user_db.bet_markets_pending_settlement(time.time())
        if not markets:
            return
        try:
            fd_matches = await football_data.fetch_wc_matches(token)
        except football_data.FootballDataError as e:
            logger.warning('football-data fetch failed: %s', e)
            return
        for m in markets:
            result = football_data.find_match_result(
                m.home_team, m.away_team, m.commence_time, fd_matches)
            if result is None:
                continue
            outcome = None
            if not self._market_allows_draw(m):
                outcome = result.get('winner')
            await self._settle_market_with_score(
                m, result['home_score'], result['away_score'], outcome=outcome)

    async def _settle_via_odds_api(self):
        api_key = _api_key()
        if not api_key:
            return
        cutoff = time.time() - constants.BET_SETTLE_BUFFER_SECONDS
        markets = cf_common.user_db.bet_markets_pending_settlement(cutoff)
        if not markets:
            return
        by_sport = {}
        for m in markets:
            by_sport.setdefault(m.sport_key, []).append(m)
        for sport_key, sport_markets in by_sport.items():
            event_ids = [m.event_id for m in sport_markets]
            try:
                scores = await odds_api.fetch_scores(
                    api_key, sport_key, event_ids=event_ids)
            except odds_api.OddsApiError as e:
                logger.warning('score fetch failed for %s: %s', sport_key, e)
                continue
            score_by_id = {s['event_id']: s for s in scores}
            for m in sport_markets:
                s = score_by_id.get(m.event_id)
                if not s or not s['completed'] or s['home_score'] is None:
                    continue
                await self._settle_market_with_score(
                    m, s['home_score'], s['away_score'])

    async def _settle_market_with_score(self, market, home_score, away_score,
                                        *, outcome=None):
        # Re-read in case a mod (or the other source) settled it already.
        fresh = cf_common.user_db.bet_market_get(market.market_id)
        if fresh is None or fresh.status != 'open':
            return
        outcome = outcome or outcome_from_score(home_score, away_score)
        if not self._pick_allowed(fresh, outcome):
            logger.warning('result %s is not valid for market %s; leaving pending',
                           outcome, market.market_id)
            return
        try:
            await self._do_settle(fresh, outcome, home_score, away_score,
                                  source='auto')
        except Exception:
            logger.warning('failed to settle market %s', market.market_id,
                           exc_info=True)

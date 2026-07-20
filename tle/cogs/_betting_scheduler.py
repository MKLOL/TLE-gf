"""Auto-open scheduler + market-open/close timers for the betting cog.

Plain mixin (not a ``commands.Cog``); mixed into ``Betting``. Drives the
precise per-fixture open timers and the kickoff-close timers, plus the
auto-open flow shared by the safety net and ``;prediction here``.
"""
import asyncio
import logging
import time

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.cogs._betting_helpers import (
    seconds_until_open, _api_key, _event_fixture_key, _is_archived,
)
from tle.cogs._betting_engine import BettingCogError

logger = logging.getLogger(__name__)

_CHANNEL_CONFIG_KEY = 'bet_channel'
_PAUSED_CONFIG_KEY = 'bet_paused'
_SCHEDULE_TTL = 6 * 3600


class BetSchedulerMixin:
    # ── Engine: precise per-fixture open timers + coarse safety net ────

    def _configured_guilds(self):
        """{guild_id: channel_id} for guilds that ran `;prediction here` and are
        not paused. (Pause stops auto-OPENING; settlement still runs.)"""
        out = {}
        if not self.bot:
            return out
        for guild in self.bot.guilds:
            if _is_archived(guild.id):
                continue
            if cf_common.user_db.get_guild_config(
                    guild.id, _PAUSED_CONFIG_KEY) == '1':
                continue
            channel_id = cf_common.user_db.get_guild_config(
                guild.id, _CHANNEL_CONFIG_KEY)
            if channel_id:
                out[guild.id] = channel_id
        return out

    async def _refresh_schedule(self):
        """Discover the fixture list (cached schedule, cheap) and, for each
        upcoming game, either arm a precise open timer (kickoff − 6h still in
        the future) or open it now (already inside the window — restart / missed
        timer catch-up). This is idempotent and safe to call often."""
        if not _api_key():
            return
        if not self._configured_guilds():
            return
        try:
            events = await self._ensure_wc_events(_SCHEDULE_TTL)
        except BettingCogError:
            return
        now = time.time()
        lead = constants.BET_OPEN_LEAD_SECONDS
        for event in events:
            if event['commence_time'] <= now:
                continue  # already kicked off — never open / thread
            if seconds_until_open(event['commence_time'], lead, now) > 0:
                self._schedule_open(event)
            else:
                # Inside the 6h window already — open (or attach a thread) now.
                await self._fire_open(_event_fixture_key(event))

    def _schedule_open(self, event):
        """Arm a precise asyncio timer to open this fixture at kickoff − lead.
        Skips if a live timer already exists (avoid churn on every refresh)."""
        fixture_key = _event_fixture_key(event)
        existing = self._open_timers.get(fixture_key)
        if existing is not None and not existing.done():
            return
        delay = seconds_until_open(
            event['commence_time'], constants.BET_OPEN_LEAD_SECONDS, time.time())
        self._open_timers[fixture_key] = asyncio.create_task(
            self._open_timer(fixture_key, delay))

    async def _open_timer(self, fixture_key, delay):
        """Sleep until the exact open moment, then open the market."""
        try:
            await asyncio.sleep(delay)
            await self._fire_open(fixture_key)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning('open timer failed for %s', fixture_key, exc_info=True)
        finally:
            self._open_timers.pop(fixture_key, None)

    async def _arm_close_timers(self):
        """Arm or catch up kickoff-close timers for every open market."""
        if cf_common.user_db is None or not self.bot:
            return
        now = time.time()
        for guild in self.bot.guilds:
            if _is_archived(guild.id):
                continue
            for market in cf_common.user_db.bet_markets_open(guild.id):
                if market.bets_closed:
                    continue
                if market.commence_time <= now:
                    await self._fire_close(market.market_id)
                else:
                    self._schedule_close(market)

    def _schedule_close(self, market):
        if market is None or market.bets_closed:
            return
        existing = self._close_timers.get(market.market_id)
        if existing is not None and not existing.done():
            return
        delay = max(0.0, market.commence_time - time.time())
        self._close_timers[market.market_id] = asyncio.create_task(
            self._close_timer(market.market_id, delay))

    async def _close_timer(self, market_id, delay):
        try:
            await asyncio.sleep(delay)
            await self._fire_close(market_id)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning('close timer failed for market %s', market_id,
                           exc_info=True)
        finally:
            self._close_timers.pop(market_id, None)

    async def _fire_close(self, market_id):
        if cf_common.user_db is None:
            return
        market = cf_common.user_db.bet_market_get(market_id)
        if market is None or market.status != 'open':
            return
        if time.time() < market.commence_time and not market.bets_closed:
            self._schedule_close(market)
            return
        changed = cf_common.user_db.bet_market_close_betting(market_id)
        if not changed:
            return
        fresh = cf_common.user_db.bet_market_get(market_id)
        await self._announce_betting_closed(fresh, automatic=True)

    async def _announce_betting_closed(self, market, *, automatic):
        if market is None or not self.bot:
            return
        await self._edit_market_message(market)

    async def _edit_market_message(self, market):
        if not market.message_id or not self.bot:
            return
        channel = self.bot.get_channel(int(market.channel_id))
        if channel is None:
            return
        try:
            msg = await channel.fetch_message(int(market.message_id))
            await msg.edit(embed=self._market_embed(market))
        except (discord.HTTPException, AttributeError, KeyError):
            logger.warning('could not edit betting announcement for market %s',
                           market.market_id)

    def _find_event_by_fixture(self, events, fixture_key):
        matches = [e for e in events if _event_fixture_key(e) == fixture_key]
        if not matches:
            return None
        matches.sort(key=lambda e: e['commence_time'])
        return matches[0]

    async def _fire_open(self, fixture_key):
        """Open this fixture's market in every configured guild that lacks one,
        freezing fresh odds read right now. Also (re)attaches a thread to a
        market that lost one. Idempotent; fetches odds only if a market is
        actually missing."""
        if cf_common.user_db is None:
            return
        configured = self._configured_guilds()
        if not configured:
            return
        now = time.time()
        needs_open = False
        for guild_id in configured:
            market = cf_common.user_db.bet_market_get_open_for_fixture(
                guild_id, fixture_key)
            if market is None:
                needs_open = True
            elif not market.thread_id and market.commence_time > now:
                # Market exists but lost its thread — no odds fetch needed.
                await self._ensure_thread(market)
        if not needs_open:
            return
        try:
            events = await self._ensure_wc_events(0)  # fresh odds, frozen at open
        except BettingCogError:
            return
        event = self._find_event_by_fixture(events, fixture_key)
        if event is None or event['commence_time'] <= time.time():
            # Vanished from the feed, or kicked off while we fetched — don't
            # open a market/thread for a game you can't bet on.
            return
        for guild_id, channel_id in configured.items():
            if cf_common.user_db.bet_market_get_open_for_fixture(
                    guild_id, fixture_key) is not None:
                continue
            try:
                await self._open_market_auto(guild_id, channel_id, event)
            except Exception:
                logger.warning('auto-open failed for %s in guild %s',
                               fixture_key, guild_id, exc_info=True)

    async def _open_market_auto(self, guild_id, channel_id, event):
        """Open + announce + thread a market headlessly. Returns the new
        market_id on success, or None if it could not be posted (channel gone,
        duplicate, or the announcement failed) — callers that void-then-reopen
        rely on this to tell a real repost from a no-op."""
        channel = self.bot.get_channel(int(channel_id)) if self.bot else None
        if channel is None:
            logger.warning('configured bet channel %s missing for guild %s',
                           channel_id, guild_id)
            return None
        market_id = self._create_market(guild_id, channel_id, event)
        if market_id is None:
            logger.info('Auto-open skipped duplicate fixture %s (%s vs %s) '
                        'in guild %s',
                        event.get('event_id'), event.get('home_team'),
                        event.get('away_team'), guild_id)
            return None
        # Several games can kick off at the same time in one channel; ping the
        # notify role only on the first market opened for that kickoff so members
        # aren't tagged once per simultaneous game.
        suppress_mention = cf_common.user_db.bet_market_has_earlier_open_at_kickoff(
            guild_id, channel_id, event['commence_time'], market_id)
        try:
            msg = await channel.send(
                **self._open_announcement_kwargs(
                    guild_id, event, suppress_mention=suppress_mention))
        except discord.HTTPException:
            logger.warning('failed to post auto market for %s in guild %s',
                           event.get('event_id'), guild_id, exc_info=True)
            cf_common.user_db.bet_void(guild_id, market_id, time.time())
            return None
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
        market = cf_common.user_db.bet_market_get(market_id)
        await self._create_thread(market_id, msg, market)
        self._schedule_close(market)
        logger.info('Auto-opened market %s (%s vs %s) in guild %s',
                    market_id, event['home_team'], event['away_team'], guild_id)
        return market_id

    async def _ensure_thread(self, market):
        if not market.message_id or not self.bot:
            return
        channel = self.bot.get_channel(int(market.channel_id))
        if channel is None:
            return
        try:
            msg = await channel.fetch_message(int(market.message_id))
        except (discord.HTTPException, AttributeError):
            return
        await self._create_thread(market.market_id, msg, market)


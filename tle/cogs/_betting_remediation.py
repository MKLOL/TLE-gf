"""One-time, idempotent remediation for markets a past bug shaped wrongly.

Earlier code decided group-vs-knockout from a hardcoded date, so late group
games (kicking off after that date) were opened as 2-way "to advance" markets
with no draw. This mixin — run once per guild on startup — voids any such
mislabelled market for the affected fixtures (refunding every stake) and
re-opens it as a correct 1X2 market with fresh odds, now that the stage is read
from football-data. Guarded by a guild-config flag so it runs only once.

Safety: it acts ONLY on a market that (a) is one of the named fixtures, (b)
currently has NO draw, and (c) has not kicked off. A correctly-posted 1X2 game
— including one already in progress — fails (b) and is never touched. Odds for
the replacement market are validated BEFORE the void, so stakes are never
refunded into a market that then cannot be recreated.
"""
import logging
import time

from tle.util import codeforces_common as cf_common
from tle.cogs._betting_helpers import _norm_team, _same_match_market_event

logger = logging.getLogger(__name__)

# Bump the suffix to schedule a future one-time remediation pass.
_REFIXTURE_FLAG = 'bet_refixture_draw_v1'

# Fixtures opened as no-draw markets by the date bug. Matched order-insensitively
# on normalised team names, so home/away orientation and spelling don't matter.
_REFIXTURE_PAIRS = (
    ('Algeria', 'Austria'),
    ('Jordan', 'Argentina'),
)


def _norm_pair(home, away):
    return frozenset((_norm_team(home), _norm_team(away)))


_REFIXTURE_TARGETS = frozenset(_norm_pair(a, b) for a, b in _REFIXTURE_PAIRS)


class BetRemediationMixin:
    async def _run_draw_refixture(self):
        """Replace each affected no-draw market with a fresh 1X2 one, once per
        guild. The flag is set only when the guild is fully resolved, so a
        transient odds/Discord failure simply retries on the next restart."""
        if cf_common.user_db is None or self.bot is None:
            return
        for guild in list(getattr(self.bot, 'guilds', []) or []):
            gid = getattr(guild, 'id', None)
            if gid is None:
                continue
            try:
                if cf_common.user_db.get_guild_config(gid, _REFIXTURE_FLAG):
                    continue
                if await self._refixture_guild(gid):
                    cf_common.user_db.set_guild_config(gid, _REFIXTURE_FLAG, '1')
            except Exception:
                logger.warning('draw refixture failed for guild %s', gid,
                               exc_info=True)

    async def _refixture_guild(self, guild_id):
        """Return True when every affected fixture in this guild is resolved."""
        bad = [m for m in cf_common.user_db.bet_markets_open(guild_id)
               if _norm_pair(m.home_team, m.away_team) in _REFIXTURE_TARGETS
               and not self._market_allows_draw(m)]
        if not bad:
            return True  # nothing mislabelled here (fresh DB, or already fixed)
        resolved = True
        for market in bad:
            resolved = await self._void_and_reopen(guild_id, market) and resolved
        return resolved

    async def _void_and_reopen(self, guild_id, market):
        if market.commence_time <= time.time():
            logger.warning('refixture: %s vs %s already kicked off; leaving '
                           'as-is', market.home_team, market.away_team)
            return True  # can't reopen a started game; never void it either
        event = await self._fresh_event_for_market(market)
        if event is None:
            return False  # no usable odds yet — retry next restart, no void
        # Both preconditions for the repost (fresh odds AND a live channel) are
        # checked BEFORE the irreversible void, so a refund never happens unless
        # the replacement market can actually be posted.
        if self.bot.get_channel(int(market.channel_id)) is None:
            logger.warning('refixture: channel %s for %s vs %s is gone; '
                           'skipping (no void) — will retry next restart',
                           market.channel_id, market.home_team, market.away_team)
            return False
        refunds = cf_common.user_db.bet_void(
            guild_id, market.market_id, time.time())
        if refunds is None:
            return True  # already settled/cancelled by someone else
        total = sum(stake for _, stake in refunds)
        logger.info('refixture: voided no-draw market %s (%s vs %s) — refunded '
                    '%s coin across %s bet(s)', market.market_id,
                    market.home_team, market.away_team, total, len(refunds))
        await self._archive_thread(market)
        new_id = await self._open_market_auto(guild_id, market.channel_id, event)
        if new_id is None:
            # Refund already issued; the scheduler's open timer / safety net
            # will reopen this fixture as 1X2. Don't stamp the flag so a clean
            # restart can also retry. Log honestly rather than claim success.
            logger.error('refixture: voided %s vs %s but the repost failed; '
                         'stakes are refunded — reopen via the scheduler or '
                         '`;bet open`', market.home_team, market.away_team)
            return False
        logger.info('refixture: reopened %s vs %s as 1X2 (market %s) in '
                    'channel %s', market.home_team, market.away_team, new_id,
                    market.channel_id)
        return True

    async def _fresh_event_for_market(self, market):
        """The current odds event for this fixture, or None if odds are
        unavailable or it has kicked off in the feed."""
        try:
            events = await self._ensure_wc_events(0)  # force-fresh, frozen at open
        except Exception as e:
            logger.warning('refixture: odds unavailable for %s vs %s, will '
                           'retry: %s', market.home_team, market.away_team, e)
            return None
        for event in events:
            if (_same_match_market_event(market, event)
                    and event['commence_time'] > time.time()):
                return event
        logger.warning('refixture: no fresh pre-kickoff odds for %s vs %s, '
                       'will retry', market.home_team, market.away_team)
        return None

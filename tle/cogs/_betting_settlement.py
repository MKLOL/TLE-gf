"""Auto-settlement pollers for the betting cog."""
import logging
import time

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import football_data
from tle.util import odds_api
from tle.cogs._betting_helpers import (
    fd_settle_outcome, outcome_from_score, _api_key, _football_data_key,
    _is_archived,
)

logger = logging.getLogger(__name__)

_BEYOND_REGULATION_PREFIX = 'bet_beyond_regulation_v1:'
_ODDS_FALLBACK_LAST_KEY = 'bet_odds_fallback_last'
_ODDS_FALLBACK_INTERVAL = 5 * 60


def _beyond_regulation_key(market_id):
    return f'{_BEYOND_REGULATION_PREFIX}{market_id}'


def _mark_beyond_regulation(market_id):
    cf_common.user_db.kvs_set(_beyond_regulation_key(market_id), '1')


def _clear_beyond_regulation(market_id):
    cf_common.user_db.kvs_delete(_beyond_regulation_key(market_id))


def _is_beyond_regulation(market_id):
    return cf_common.user_db.kvs_get(_beyond_regulation_key(market_id)) is not None


def _odds_fallback_due(now):
    raw = cf_common.user_db.kvs_get(_ODDS_FALLBACK_LAST_KEY)
    try:
        last = float(raw)
    except (TypeError, ValueError):
        return True
    return now - last >= _ODDS_FALLBACK_INTERVAL


class BetSettlementMixin:
    async def _settle_pending(self):
        """Settle finished markets.

        football-data.org is the primary source and is polled frequently. The
        Odds API scores endpoint is paid, so it is throttled separately and used
        only as a fallback for markets football-data did not identify as
        beyond-regulation.
        """
        await self._settle_via_football_data()
        await self._settle_via_odds_api()

    async def _settle_via_football_data(self):
        token = _football_data_key()
        if not token:
            return
        markets = [m for m in
                   cf_common.user_db.bet_markets_pending_settlement(time.time())
                   if not _is_archived(m.guild_id)]
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
            outcome = fd_settle_outcome(result)
            beyond = result.get('duration') in ('EXTRA_TIME', 'PENALTY_SHOOTOUT')
            if outcome is None:
                if beyond:
                    self._fd_pending_confirm[m.market_id] = None
                    _mark_beyond_regulation(m.market_id)
                else:
                    self._fd_pending_confirm.pop(m.market_id, None)
                continue
            if beyond:
                _mark_beyond_regulation(m.market_id)
                key = (outcome, result['home_score'], result['away_score'])
                if self._fd_pending_confirm.get(m.market_id) != key:
                    self._fd_pending_confirm[m.market_id] = key
                    logger.info('bet market %s: holding %s result (%s) one poll '
                                'to confirm', m.market_id,
                                result.get('duration'), outcome)
                    continue
            settled = await self._settle_market_with_score(
                m, result['home_score'], result['away_score'], outcome=outcome)
            if settled:
                self._fd_pending_confirm.pop(m.market_id, None)
                _clear_beyond_regulation(m.market_id)

    async def _settle_via_odds_api(self):
        api_key = _api_key()
        if not api_key:
            return
        now = time.time()
        if not _odds_fallback_due(now):
            return
        cutoff = now - constants.BET_SETTLE_BUFFER_SECONDS
        markets = cf_common.user_db.bet_markets_pending_settlement(cutoff)
        markets = [m for m in markets
                   if not _is_archived(m.guild_id)
                   and m.market_id not in self._fd_pending_confirm
                   and not _is_beyond_regulation(m.market_id)]
        if not markets:
            return
        cf_common.user_db.kvs_set(_ODDS_FALLBACK_LAST_KEY, str(now))
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
        fresh = cf_common.user_db.bet_market_get(market.market_id)
        if fresh is None or fresh.status != 'open':
            return True
        outcome = outcome or outcome_from_score(home_score, away_score)
        if not self._pick_allowed(fresh, outcome):
            logger.warning('result %s is not valid for market %s; leaving pending',
                           outcome, market.market_id)
            return False
        try:
            await self._do_settle(fresh, outcome, home_score, away_score,
                                  source='auto')
        except Exception:
            logger.warning('failed to settle market %s', market.market_id,
                           exc_info=True)
            return False
        return True

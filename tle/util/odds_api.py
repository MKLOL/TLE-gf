"""Thin async client for The Odds API (the-odds-api.com).

The betting cog needs three things from the API: a list of soccer
competitions in season, upcoming matches with 1X2 (h2h) odds, and final
scores to auto-settle on. Network I/O lives here; the parsing of raw payloads
is split into pure functions so it can be unit-tested without a key or a
socket.

Free tier is ~500 requests/month, so callers should fetch sparingly and
cache — one request per sport key per refresh.
"""
import logging
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)
_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)

BASE_URL = 'https://api.the-odds-api.com/v4'

# This bot is World Cup–only. The Odds API key for the 2026 tournament is
# 'soccer_fifa_world_cup' (verified live: active, "FIFA World Cup 2026", h2h
# 1X2 odds across ~16 bookmakers). NOTE: 'soccer_fifa_world_cup_winner' is a
# different (outright winner) market — do not use it for match betting.
WORLD_CUP_SPORT_KEY = 'soccer_fifa_world_cup'

# One region keeps an odds request at (1 market × 1 region) = 1 credit.
# 'eu' aggregates a broad set of European bookmakers.
DEFAULT_REGIONS = 'eu'


class OddsApiError(Exception):
    """Raised on a non-200 response or a transport failure."""


def iso_to_unix(iso):
    """Parse an ISO-8601 timestamp (e.g. '2026-06-15T18:00:00Z') to a unix
    timestamp. Naive timestamps are treated as UTC."""
    s = iso.strip().replace('Z', '+00:00')
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def parse_h2h_event(raw):
    """Normalize one raw odds event into a dict, or None if it lacks a full
    1X2 line. Decimal prices are averaged across bookmakers for stability.

    Returns: {event_id, sport_key, home_team, away_team, commence_time(unix),
              odds: {home, draw, away}}
    """
    event_id = raw.get('id')
    home = raw.get('home_team')
    away = raw.get('away_team')
    commence = raw.get('commence_time')
    if not event_id or not home or not away or not commence:
        return None
    prices = {'home': [], 'draw': [], 'away': []}
    for bookmaker in raw.get('bookmakers', []):
        for market in bookmaker.get('markets', []):
            if market.get('key') != 'h2h':
                continue
            for outcome in market.get('outcomes', []):
                name = outcome.get('name')
                price = outcome.get('price')
                if price is None or name is None:
                    continue
                if name == home:
                    prices['home'].append(price)
                elif name == away:
                    prices['away'].append(price)
                elif name.lower() == 'draw':
                    prices['draw'].append(price)
    odds = {}
    for key, values in prices.items():
        if not values:
            return None  # need home/draw/away to form a 1X2 market
        odds[key] = round(sum(values) / len(values), 2)
    return {
        'event_id': event_id,
        'sport_key': raw.get('sport_key'),
        'home_team': home,
        'away_team': away,
        'commence_time': iso_to_unix(commence),
        'odds': odds,
    }


def parse_score_event(raw):
    """Normalize a raw scores event into
    {event_id, completed, home_score, away_score}.

    home_score/away_score are None until the game is completed with a numeric
    score for both named teams.
    """
    event_id = raw.get('id')
    if not raw.get('completed'):
        return {'event_id': event_id, 'completed': False,
                'home_score': None, 'away_score': None}
    home = raw.get('home_team')
    away = raw.get('away_team')
    scores = {s.get('name'): s.get('score') for s in (raw.get('scores') or [])}
    try:
        home_score = int(scores[home])
        away_score = int(scores[away])
    except (KeyError, TypeError, ValueError):
        return {'event_id': event_id, 'completed': True,
                'home_score': None, 'away_score': None}
    return {'event_id': event_id, 'completed': True,
            'home_score': home_score, 'away_score': away_score}


async def _get_json(session, url, params):
    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise OddsApiError(f'HTTP {resp.status}: {body[:200]}')
            return await resp.json()
    except _AIOHTTP_CLIENT_ERROR as e:
        raise OddsApiError(f'request failed: {e}') from e


async def fetch_h2h(api_key, sport_keys, *, regions=DEFAULT_REGIONS,
                    session=None, base_url=BASE_URL):
    """Fetch upcoming 1X2 odds across the given sport keys.

    Returns a flat list of normalized events (see parse_h2h_event). A failure
    on one sport key is logged and skipped rather than aborting the whole
    fetch.
    """
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    events = []
    failures = []
    successful = 0
    try:
        for key in sport_keys:
            url = f'{base_url}/sports/{key}/odds'
            params = {'apiKey': api_key, 'regions': regions,
                      'markets': 'h2h', 'oddsFormat': 'decimal'}
            try:
                raw = await _get_json(session, url, params)
            except OddsApiError as e:
                logger.warning('odds fetch failed for %s: %s', key, e)
                failures.append((key, e))
                continue
            successful += 1
            for ev in raw or []:
                parsed = parse_h2h_event(ev)
                if parsed:
                    events.append(parsed)
        if successful == 0 and failures:
            detail = '; '.join(f'{key}: {err}' for key, err in failures)
            raise OddsApiError(detail)
    finally:
        if own:
            await session.close()
    return events


async def fetch_scores(api_key, sport_key, *, days_from=1, event_ids=None,
                       session=None, base_url=BASE_URL):
    """Fetch recent/live scores for one sport key. Returns a list of
    normalized score dicts (see parse_score_event).

    A scores request with ``daysFrom`` set costs 2 credits (vs 1 without, but
    that omits completed games, which is exactly what we need). days_from=1 is
    the smallest window that still includes completed games and covers our ~3h
    settle buffer. The request is batched per sport key, and the poller only
    calls it when a market is actually past kickoff, so cost stays bounded.
    """
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        url = f'{base_url}/sports/{sport_key}/scores'
        params = {'apiKey': api_key, 'daysFrom': str(days_from)}
        if event_ids:
            params['eventIds'] = ','.join(event_ids)
        raw = await _get_json(session, url, params)
        return [parse_score_event(ev) for ev in (raw or [])]
    finally:
        if own:
            await session.close()

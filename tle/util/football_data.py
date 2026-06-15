"""Thin async client for football-data.org — used only to read World Cup
results, so settlement is free and frequent (the free tier is rate-limited,
~10 req/min, not credit-limited). The Odds API stays the source of odds.

football-data.org has its own match ids and (sometimes) team spellings, so a
result is linked back to an Odds-API event by normalized team names + kickoff
date. Network I/O is isolated here; parsing/matching are pure for testing.
"""
import logging
import unicodedata
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.football-data.org/v4'
WORLD_CUP_COMPETITION = 'WC'


class FootballDataError(Exception):
    """Raised on a non-200 response or transport failure."""


# Equivalence groups for team names that differ between The Odds API and
# football-data.org. Each name (normalized) maps to a canonical token so the
# two providers' spellings compare equal. Unknown names just use their own
# normalized form (which already matches for the large majority of teams).
_EQUIV = [
    {'southkorea', 'korearepublic', 'korea'},
    {'unitedstates', 'usa', 'unitedstatesofamerica'},
    {'ivorycoast', 'cotedivoire'},
    {'czechia', 'czechrepublic'},
    {'capeverde', 'caboverde'},
    {'iran', 'iranislamicrepublic'},
    {'bosnia', 'bosniaherzegovina', 'bosniaandherzegovina'},
    {'drcongo', 'congodr', 'democraticrepublicofcongo'},
    {'turkey', 'turkiye'},
]
_CANON = {}
for _group in _EQUIV:
    _canon = sorted(_group)[0]
    for _name in _group:
        _CANON[_name] = _canon


def iso_to_unix(iso):
    """Parse an ISO-8601 timestamp to a unix timestamp; naive → UTC."""
    s = iso.strip().replace('Z', '+00:00')
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _norm(name):
    if not name:
        return ''
    decomposed = unicodedata.normalize('NFKD', name)
    stripped = ''.join(c for c in decomposed if not unicodedata.combining(c))
    return ''.join(c for c in stripped.lower() if c.isalnum())


def _canon_key(name):
    return _CANON.get(_norm(name), _norm(name))


def parse_match(raw):
    """Normalize a football-data match into
    {home, away, commence_time(unix), finished, home_score, away_score}.

    finished is True only when the game is over AND both full-time scores are
    present.
    """
    status = raw.get('status')
    ft = (raw.get('score') or {}).get('fullTime') or {}
    home_score = ft.get('home')
    away_score = ft.get('away')
    commence = raw.get('utcDate')
    finished = (status in ('FINISHED', 'AWARDED')
                and home_score is not None and away_score is not None)
    return {
        'home': (raw.get('homeTeam') or {}).get('name'),
        'away': (raw.get('awayTeam') or {}).get('name'),
        'commence_time': iso_to_unix(commence) if commence else None,
        'finished': finished,
        'home_score': home_score,
        'away_score': away_score,
    }


def find_result(home_team, away_team, commence_time, fd_matches,
                *, max_time_diff=86400):
    """Find a FINISHED football-data match for (home_team vs away_team) near
    commence_time and return (home_score, away_score) mapped to the given
    home/away orientation, or None.

    Pairing is order-insensitive (providers occasionally flip home/away); when
    flipped, the scores are swapped back so they line up with the supplied
    home_team/away_team. The date window tolerates provider time differences.
    """
    h, a = _canon_key(home_team), _canon_key(away_team)
    for m in fd_matches:
        if not m['finished']:
            continue
        mh, ma = _canon_key(m['home']), _canon_key(m['away'])
        if {mh, ma} != {h, a} or h == a:
            continue
        if (commence_time is not None and m['commence_time'] is not None
                and abs(commence_time - m['commence_time']) > max_time_diff):
            continue
        if mh == h and ma == a:
            return (m['home_score'], m['away_score'])
        return (m['away_score'], m['home_score'])  # provider flipped orientation
    return None


async def fetch_wc_matches(token, *, session=None, base_url=BASE_URL):
    """Fetch all World Cup matches (normalized). Free and rate-limited, so safe
    to poll frequently."""
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        url = f'{base_url}/competitions/{WORLD_CUP_COMPETITION}/matches'
        headers = {'X-Auth-Token': token}
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise FootballDataError(f'HTTP {resp.status}: {body[:200]}')
                data = await resp.json()
        except aiohttp.ClientError as e:
            raise FootballDataError(f'request failed: {e}') from e
        return [parse_match(m) for m in (data.get('matches') or [])]
    finally:
        if own:
            await session.close()

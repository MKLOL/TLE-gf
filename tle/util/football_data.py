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
_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)

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


def _score_value(node, side):
    if not node:
        return None
    # Current v4 docs use home/away; older examples use homeTeam/awayTeam.
    return node.get(side, node.get(f'{side}Team'))


def _winner(raw_winner):
    return {'HOME_TEAM': 'home', 'AWAY_TEAM': 'away', 'DRAW': 'draw'}.get(raw_winner)


def parse_match(raw):
    """Normalize a football-data match into
    {home, away, commence_time(unix), finished, home_score, away_score, winner}.

    finished is True only when the game is over AND both full-time scores are
    present.
    """
    status = raw.get('status')
    score = raw.get('score') or {}
    ft = score.get('fullTime') or {}
    home_score = _score_value(ft, 'home')
    away_score = _score_value(ft, 'away')
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
        'winner': _winner(score.get('winner')),
        'duration': score.get('duration'),
        'stage': raw.get('stage'),
    }


def is_knockout_stage(stage):
    """True when a football-data ``stage`` denotes a knockout round.

    Defined as "any known stage that is not the group phase", so a new
    knockout label (e.g. ``LAST_32`` for the 48-team 2026 format) is treated as
    knockout with no code change. The one load-bearing assumption is that the
    group phase is spelled exactly ``GROUP_STAGE`` — football-data v4 uses that
    literal for the World Cup (``LEAGUE_STAGE`` is the separate UCL value). A
    missing/unknown stage returns False, so the caller fails safe to a 1X2
    market that still offers a draw — the conservative direction, since a draw
    can always be settled but cannot be un-stripped from a group market."""
    return bool(stage) and stage != 'GROUP_STAGE'


def find_match_stage(home_team, away_team, commence_time, fd_matches,
                     *, max_time_diff=86400):
    """Return the tournament ``stage`` (e.g. 'GROUP_STAGE', 'LAST_16') for the
    fixture matching (home vs away) near commence_time, or None when no fixture
    matches.

    Order-insensitive and date-windowed, mirroring :func:`find_match_result`,
    but it does NOT require the match to have finished — it runs at market-open
    time to decide whether a market is 1X2 (group) or to-advance (knockout).
    """
    h, a = _canon_key(home_team), _canon_key(away_team)
    if h == a:
        return None
    for m in fd_matches:
        mh, ma = _canon_key(m['home']), _canon_key(m['away'])
        if {mh, ma} != {h, a}:
            continue
        if (commence_time is not None and m['commence_time'] is not None
                and abs(commence_time - m['commence_time']) > max_time_diff):
            continue
        return m.get('stage')
    return None


def find_result(home_team, away_team, commence_time, fd_matches,
                *, max_time_diff=86400):
    """Find a FINISHED football-data match for (home_team vs away_team) near
    commence_time and return (home_score, away_score) mapped to the given
    home/away orientation, or None.

    Pairing is order-insensitive (providers occasionally flip home/away); when
    flipped, the scores are swapped back so they line up with the supplied
    home_team/away_team. The date window tolerates provider time differences.
    """
    result = find_match_result(
        home_team, away_team, commence_time, fd_matches,
        max_time_diff=max_time_diff)
    if result is None:
        return None
    return (result['home_score'], result['away_score'])


def find_match_result(home_team, away_team, commence_time, fd_matches,
                      *, max_time_diff=86400):
    """Find the FINISHED football-data match for this fixture and return a dict
    mapped to the supplied home/away orientation, including the winner.

    When several finished fixtures share the same team pair inside the window
    (it shouldn't happen within one tournament, but a stale or duplicated feed
    entry can do it), the one whose kickoff is NEAREST the expected commence
    time wins — never just the first in feed order — so we always settle on the
    intended game rather than some other meeting of the two teams.
    """
    h, a = _canon_key(home_team), _canon_key(away_team)
    if h == a:
        return None
    best, best_diff = None, None
    for m in fd_matches:
        if not m['finished']:
            continue
        mh, ma = _canon_key(m['home']), _canon_key(m['away'])
        if {mh, ma} != {h, a}:
            continue
        if (commence_time is not None and m['commence_time'] is not None):
            diff = abs(commence_time - m['commence_time'])
            if diff > max_time_diff:
                continue
        else:
            diff = float('inf')  # no time to compare on — accept, lowest priority
        if best_diff is None or diff < best_diff:
            best, best_diff = m, diff
    if best is None:
        return None
    mh, ma = _canon_key(best['home']), _canon_key(best['away'])
    if mh == h and ma == a:
        return dict(best)
    winner = best.get('winner')
    if winner == 'home':
        winner = 'away'
    elif winner == 'away':
        winner = 'home'
    return {
        **best,
        'home': best['away'],
        'away': best['home'],
        'home_score': best['away_score'],
        'away_score': best['home_score'],
        'winner': winner,
    }


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
        except _AIOHTTP_CLIENT_ERROR as e:
            raise FootballDataError(f'request failed: {e}') from e
        return [parse_match(m) for m in (data.get('matches') or [])]
    finally:
        if own:
            await session.close()

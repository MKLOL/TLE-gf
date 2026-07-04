"""Pure, unit-tested helpers for the World Cup betting minigame.

These are module-level functions with no cog/DB state. They are re-exported
from ``tle.cogs.betting`` so existing imports (and tests) keep working.
"""
import unicodedata
from datetime import datetime, timezone

import discord

from tle import constants
from tle.util import discord_common
from tle.util import ranking
from tle.util.db.user_db_conn import bet_fixture_key

_COIN = '🪙'

_PICK_ALIASES = {
    'home': 'home', 'h': 'home', '1': 'home',
    'draw': 'draw', 'd': 'draw', 'x': 'draw', 'tie': 'draw',
    'away': 'away', 'a': 'away', '2': 'away',
}
_AMOUNT_WORDS = ('all', 'max', 'allin', 'all-in', 'everything')
_DIRECT_PICKS = ('home', 'draw', 'away')
# Provider event ids can drift. Treat the same team pair near the same kickoff
# as the same market so the 15-minute safety net cannot open a duplicate thread.
_DUPLICATE_MATCH_WINDOW = 6 * 3600


# ── Pure helpers (unit-tested) ─────────────────────────────────────────────

def outcome_from_score(home, away):
    """Map a final scoreline to the 1X2 outcome string."""
    if home > away:
        return 'home'
    if away > home:
        return 'away'
    return 'draw'


def _score_leader(home, away):
    if home is None or away is None or home == away:
        return None
    return 'home' if home > away else 'away'


def _scores_level(home, away):
    return home is not None and away is not None and home == away


def _shootout_fulltime_winner(result):
    winner = _score_leader(result.get('home_score'), result.get('away_score'))
    if winner is None:
        return None
    if not _scores_level(
            result.get('regular_home_score'), result.get('regular_away_score')):
        return None
    return winner


def fd_settle_outcome(result):
    """The outcome a football-data result supports for auto-settlement, or None
    when we must NOT settle it yet.

    football-data's ``winner`` is authoritative — it already reflects extra time
    and penalties — so it is trusted over the scoreline. But the feed's
    beyond-regulation data has proven unreliable, so a ``PENALTY_SHOOTOUT`` is
    accepted only when independent evidence agrees: a clear ``penalties`` tally
    and/or a decisive ``fullTime`` score with level ``regularTime``. If
    ``winner`` is present, it must agree too. Inconsistent readings are left for
    a manual ``;bet settle``. A plain in-regulation game with no published
    winner falls back to the scoreline.
    """
    winner = result.get('winner')
    duration = result.get('duration')
    if duration == 'PENALTY_SHOOTOUT':
        pens = result.get('penalties') or {}
        evidence = [
            side for side in (
                _score_leader(pens.get('home'), pens.get('away')),
                _shootout_fulltime_winner(result))
            if side is not None]
        if not evidence or len(set(evidence)) > 1:
            return None
        settled_winner = evidence[0]
        if winner is not None and winner != settled_winner:
            return None
        return settled_winner
    if winner in ('home', 'away', 'draw'):
        return winner
    if duration == 'EXTRA_TIME':
        return None  # beyond regulation, no decisive winner yet — don't guess
    return outcome_from_score(result.get('home_score'), result.get('away_score'))


def pick_is_negative(pick):
    return isinstance(pick, str) and pick.startswith('not_')


def positive_pick(pick):
    return pick[4:] if pick_is_negative(pick) else pick


def pick_wins(pick, result):
    base = positive_pick(pick)
    return base != result if pick_is_negative(pick) else base == result


def payout_amount(stake, odds):
    """Gross return on a winning stake at decimal odds (rounded to a point)."""
    return int(round(stake * odds))


def is_due(commence_time, now, lead):
    """True if a game with this kickoff is inside the auto-open window: not yet
    started, and within `lead` seconds of kickoff."""
    return 0 < commence_time - now <= lead


def seconds_until_open(commence_time, lead, now):
    """Seconds from now until a fixture's market should open (kickoff − lead),
    floored at 0 (already inside the window → open now)."""
    return max(0.0, (commence_time - lead) - now)


def normalize_pick(text):
    """Resolve a pick token (home/draw/away and common aliases) or None.
    Does NOT know team names — see resolve_pick for that."""
    if text is None:
        return None
    return _PICK_ALIASES.get(text.strip().lower())


def _norm_team(name):
    """Fold a team name to a comparison key: strip accents, lowercase, keep
    only alphanumerics. 'Cape Verde' → 'capeverde', 'Côte d\\'Ivoire' →
    'cotedivoire'."""
    if not name:
        return ''
    decomposed = unicodedata.normalize('NFKD', name)
    stripped = ''.join(c for c in decomposed if not unicodedata.combining(c))
    return ''.join(c for c in stripped.lower() if c.isalnum())


def resolve_pick(text, home_team, away_team):
    """Resolve a pick against a specific match: an outcome alias
    (home/draw/away/1/x/2/tie…) OR a team name ('Spain', 'cape verde'). Returns
    'home'/'draw'/'away' or None. Exact normalized name match, falling back to
    an unambiguous prefix (≥3 chars) so 'cape' resolves to 'Cape Verde'."""
    if text is None:
        return None
    base = _PICK_ALIASES.get(text.strip().lower())
    if base is not None:
        return base
    key = _norm_team(text)
    if not key:
        return None
    home_key, away_key = _norm_team(home_team), _norm_team(away_team)
    if key == home_key:
        return 'home'
    if key == away_key:
        return 'away'
    if len(key) >= 3:
        home_pre = home_key.startswith(key)
        away_pre = away_key.startswith(key)
        if home_pre and not away_pre:
            return 'home'
        if away_pre and not home_pre:
            return 'away'
    return None


def resolve_bet_pick(text, home_team, away_team, *, allow_draw=True):
    """Resolve a wager pick, including 'not <pick/team>' bets."""
    if text is None:
        return None
    raw = text.strip()
    negated = False
    lower = raw.lower()
    for prefix in ('not ', 'no '):
        if lower.startswith(prefix):
            negated = True
            raw = raw[len(prefix):].strip()
            break
    pick = resolve_pick(raw, home_team, away_team)
    if pick is None:
        return None
    if pick == 'draw' and not allow_draw:
        return None
    return f'not_{pick}' if negated else pick


def extract_bet_tokens(content):
    """Cheap, market-agnostic split of a possible thread bet into
    (pick_text, amount_str), or None. Accepts '<pick…> <amount>' or
    '<amount> <pick…>' where amount is a single number/percent/'all' token and
    pick is 1–3 words (a team name or an outcome alias). The pick is resolved
    to an outcome later, against the market, via resolve_bet_pick — keeping this
    off the DB for ordinary chatter."""
    if not content:
        return None
    tokens = content.strip().split()
    if not 2 <= len(tokens) <= 5:
        return None
    if _looks_like_amount(tokens[-1]):
        pick = ' '.join(tokens[:-1])
        return (pick, tokens[-1]) if _pick_token_count_ok(pick) else None
    if _looks_like_amount(tokens[0]):
        pick = ' '.join(tokens[1:])
        return (pick, tokens[0]) if _pick_token_count_ok(pick) else None
    return None


def _pick_token_count_ok(pick_text):
    words = pick_text.strip().split()
    if not words:
        return False
    if words[0].lower() in ('not', 'no'):
        return len(words) <= 4
    return len(words) <= 3


def _looks_like_amount(token):
    t = token.strip().lower()
    if t in _AMOUNT_WORDS:
        return True
    if t.endswith('%'):
        t = t[:-1]
    try:
        float(t)
        return True
    except ValueError:
        return False


def parse_amount(text, balance, min_stake=1):
    """Parse a stake from user text against a balance. Supports a whole
    number, a percentage of balance (`50%`), or `all`/`max`. Returns the stake
    (>= min_stake) or None if unparseable / below the minimum.

    Does NOT enforce stake <= balance for plain numbers — the caller reports
    that separately so the user hears 'you only have N', not 'invalid'.
    """
    if text is None:
        return None
    t = text.strip().lower()
    if t in _AMOUNT_WORDS:
        return balance if balance >= min_stake else None
    if t.endswith('%'):
        try:
            pct = float(t[:-1])
        except ValueError:
            return None
        if not 0 < pct <= 100:
            return None
        amount = int(balance * pct / 100)
        return amount if amount >= min_stake else None
    try:
        amount = int(t)
    except ValueError:
        return None
    return amount if amount >= min_stake else None


def is_remove_amount(text):
    """A zero stake removes the user's wager for that pick."""
    if text is None:
        return False
    return text.strip() == '0'


def _normalize_probabilities_from_odds(odds):
    implied = {}
    for pick in _DIRECT_PICKS:
        value = odds.get(pick)
        implied[pick] = (1.0 / value) if value and value > 1 else 0.0
    total = sum(implied.values())
    if total <= 0:
        return None
    return {pick: implied[pick] / total for pick in _DIRECT_PICKS}


def _odds_from_probability(probability):
    return round(1.0 / probability, 4) if probability > 0 else 0.0


def normalized_market_odds(odds, *, knockout=False):
    """Convert provider odds into no-vig/fair decimal odds.

    Group-stage markets remain 1X2. Knockout markets are two-outcome
    "to advance" markets: draw probability is redistributed between the two
    teams according to their non-draw win probabilities.
    """
    probabilities = _normalize_probabilities_from_odds(odds)
    if probabilities is None:
        return dict(odds)
    if knockout:
        decisive = probabilities['home'] + probabilities['away']
        if decisive <= 0:
            return dict(odds)
        draw = probabilities['draw']
        probabilities = {
            'home': probabilities['home'] + draw * probabilities['home'] / decisive,
            'draw': 0.0,
            'away': probabilities['away'] + draw * probabilities['away'] / decisive,
        }
    return {pick: _odds_from_probability(probabilities[pick])
            for pick in _DIRECT_PICKS}


def normalize_event(event, *, knockout=False):
    """Normalise a raw odds event into market-ready form.

    ``knockout`` is decided by the caller from the authoritative competition
    stage (football-data), NOT from the kickoff date — a knockout match folds
    the draw into the two teams to form a 2-way 'to advance' market, while a
    group match stays 1X2. Defaults to False so an unknown stage fails safe to
    a market that still offers a draw.
    """
    out = dict(event)
    out['odds'] = normalized_market_odds(event['odds'], knockout=knockout)
    out['market_type'] = 'advance' if not _odds_allow_draw(out['odds']) else 'result'
    return out


def _odds_allow_draw(odds):
    return (odds.get('draw') or 0) > 1


def _event_fixture_key(event):
    return bet_fixture_key(
        event.get('sport_key'), event.get('home_team'), event.get('away_team'),
        event.get('commence_time'))


def _same_match_market_event(market, event, *, window=_DUPLICATE_MATCH_WINDOW):
    """True when an open DB market and odds event look like the same fixture.

    This intentionally does not depend on provider event_id. The Odds API can
    relist an event under a new id; deduping by team pair + nearby kickoff keeps
    the safety-net poller from opening a second thread for the same match.
    """
    try:
        if abs(float(market.commence_time) - float(event['commence_time'])) > window:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    market_key = getattr(market, 'fixture_key', None)
    event_key = _event_fixture_key(event)
    if market_key and market_key == event_key:
        return True
    market_pair = {_norm_team(market.home_team), _norm_team(market.away_team)}
    event_pair = {_norm_team(event.get('home_team')), _norm_team(event.get('away_team'))}
    return '' not in market_pair and market_pair == event_pair


def parse_settle_arg(text):
    """Parse a manual-settle argument into (result, home_score, away_score).

    Accepts a pick word ('home'/'draw'/'away' + aliases) → scores None, or a
    scoreline ('2-1', '2:1') → result derived. Returns None if unparseable.
    """
    if text is None:
        return None
    t = text.strip().lower()
    pick = normalize_pick(t)
    if pick is not None:
        return (pick, None, None)
    for sep in ('-', ':', '–'):
        if sep in t:
            a, _, b = t.partition(sep)
            try:
                home, away = int(a.strip()), int(b.strip())
            except ValueError:
                return None
            if home < 0 or away < 0:
                return None
            return (outcome_from_score(home, away), home, away)
    return None


def rank_line(rows, user_id, value_attr, label, unit=_COIN):
    """Build the 'Your rank: #N — V unit' line shown above a leaderboard.
    `rows` is the leaderboard order; matches user_id as TEXT or int. Uses
    standard competition ranking so users tied on `value_attr` share a rank."""
    uid = str(user_id)
    ranks = ranking.competition_ranks([getattr(row, value_attr) for row in rows])
    for rank, row in zip(ranks, rows):
        if str(row.user_id) == uid:
            value = getattr(row, value_attr)
            return f'Your rank: **#{rank}** — {value} {unit}'
    return f"You're not on the {label} board yet."


def _utc_today():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _bot_prefix():
    return getattr(discord_common, '_BOT_PREFIX', ';')


def unknown_subcommand_token(ctx):
    """The first token after the group name that isn't a registered subcommand
    (so ``;bet junk`` -> ``'junk'``), or None for a bare ``;bet`` or a real
    subcommand/alias.

    Why parse the raw message instead of reading ``ctx.subcommand_passed``:
    discord.py sets ``subcommand_passed`` on the group but then resets it to
    None in ``Command.invoke`` *before* a group's own callback runs (the
    ``invoke_without_command`` fall-through). So inside the ``bet`` callback it
    is always None — we recover the attempted subcommand from the content.
    """
    content = getattr(getattr(ctx, 'message', None), 'content', '') or ''
    prefix = getattr(ctx, 'prefix', '') or ''
    if prefix and content.startswith(prefix):
        content = content[len(prefix):]
    content = content.lstrip()
    invoked = getattr(ctx, 'invoked_with', '') or ''
    if invoked and content[:len(invoked)].lower() == invoked.lower():
        content = content[len(invoked):]
    parts = content.split()
    if not parts:
        return None
    token = parts[0]
    group = getattr(ctx, 'command', None)
    known = getattr(group, 'all_commands', None) or {}
    if token.lower() in {name.lower() for name in known}:
        return None
    return token


def _no_mentions():
    allowed = getattr(discord, 'AllowedMentions', None)
    return allowed.none() if allowed is not None and hasattr(allowed, 'none') else None


def _role_mentions():
    allowed = getattr(discord, 'AllowedMentions', None)
    if allowed is None:
        return None
    try:
        return allowed(everyone=False, users=False, roles=True, replied_user=False)
    except TypeError:
        return None


def _api_key():
    return getattr(constants, 'ODDS_API_KEY', None)


def _football_data_key():
    return getattr(constants, 'FOOTBALL_DATA_API_KEY', None)


def _short_error(error, limit=180):
    text = str(error) or error.__class__.__name__
    text = text.replace('`', "'")
    if len(text) > limit:
        return text[:limit - 3] + '...'
    return text

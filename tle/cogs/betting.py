"""World Cup soccer betting minigame.

Fully automated and World Cup–only. An admin points the bot at a channel with
`;prediction here`; from then on the bot, on its own, ~2 hours before each
World Cup kickoff:
  1. reads the live 1X2 odds from The Odds API and **freezes** them,
  2. posts the market in the configured channel and opens a **thread**,
  3. members bet by replying in the thread (`home 100`, `away all`, `draw 25%`).
At kickoff betting closes; at full time the bot reads the final score and
auto-settles, paying winners stake × odds. Everyone starts at 1000 coins and
claims +100/day with `;bet daily`.

Commands (group `;bet`, alias `;prediction`):
  ;prediction here          set this channel for auto-opened markets       (admin)
  ;bet matches [query]      list upcoming World Cup matches with odds
  ;bet open <n|event_id>    manually open a market early                    (admin)
  ;bet home|draw|away <amt> stake on an outcome (also: reply in the thread)
  ;bet me                  show your betting summary
  ;bet balance [@user]      show a wallet balance
  ;bet daily                claim the daily allowance
  ;bet transfer @from @to <amt> move coins between users                 (admin)
  ;beg @user [amount]       ask someone to give you betting coins
  ;bet notify               toggle the configured notification role
  ;bet notifyrole @role     set role pinged when markets open             (admin)
  ;bet leaderboard [profit] richest wallets / net profit
  ;bet mybet                show your bet on the active market
  ;bet withdraw             remove all your bets on the active match
  ;bet pending              list markets stuck open past kickoff
  ;bet settle <home|draw|away|2-1>  settle the active market manually       (admin)
  ;bet cancel               cancel the active market, refund stakes         (admin)
"""
import asyncio
import logging
import time
from datetime import datetime, timezone

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import football_data
from tle.util import odds_api
from tle.util import paginator
from tle.util import tasks
from tle.util.db.user_db_conn import bet_fixture_key

logger = logging.getLogger(__name__)

_COIN = '🪙'
_LB_PER_PAGE = 15
_MATCH_LIST_LIMIT = 15
# Manual `;bet matches` reuses a fetch no older than this.
_MATCH_CACHE_MAX_AGE = 10 * 60
# Each fixture gets a precise asyncio timer that opens its market at exactly
# kickoff − BET_OPEN_LEAD_SECONDS (never late), mirroring rpoll's per-poll
# expiry timers. The safety-net task is only a coarse backstop: it re-discovers
# the schedule (to arm timers for new fixtures) and catches anything a missed
# timer / restart left in-window. So opening precision comes from the timers,
# NOT this interval.
_SAFETY_NET_INTERVAL = 15 * 60
# Auto-settle poller cadence. Results come from football-data.org (free), so we
# can poll often; only hits the network when a market is actually past kickoff.
_SETTLE_INTERVAL = 5 * 60
# How stale the cached schedule may be before the safety net refetches it (to
# arm timers for newly-listed fixtures). Fresh odds at the open moment are
# guaranteed separately — the timer fetches with max_age 0 when it fires.
_SCHEDULE_TTL = 6 * 3600
# Provider event ids can drift. Treat the same team pair near the same kickoff
# as the same market so the 15-minute safety net cannot open a duplicate thread.
_DUPLICATE_MATCH_WINDOW = 6 * 3600
# Coalesce rapid thread bets into one first-message edit.
_POOL_REFRESH_DELAY = 5
_BEG_TIMEOUT = 60

_PICK_ALIASES = {
    'home': 'home', 'h': 'home', '1': 'home',
    'draw': 'draw', 'd': 'draw', 'x': 'draw', 'tie': 'draw',
    'away': 'away', 'a': 'away', '2': 'away',
}
_AMOUNT_WORDS = ('all', 'max', 'allin', 'all-in', 'everything')
_DIRECT_PICKS = ('home', 'draw', 'away')
_KNOCKOUT_START_TS = datetime(2026, 6, 28, tzinfo=timezone.utc).timestamp()

_CHANNEL_CONFIG_KEY = 'bet_channel'
_PAUSED_CONFIG_KEY = 'bet_paused'
_NOTIFY_ROLE_CONFIG_KEY = 'bet_notify_role'


class BettingCogError(commands.CommandError):
    pass


# ── Pure helpers (unit-tested) ─────────────────────────────────────────────

def outcome_from_score(home, away):
    """Map a final scoreline to the 1X2 outcome string."""
    if home > away:
        return 'home'
    if away > home:
        return 'away'
    return 'draw'


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
    import unicodedata
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


def _event_is_knockout(event):
    return (event.get('commence_time') or 0) >= _KNOCKOUT_START_TS


def normalize_event(event):
    out = dict(event)
    out['odds'] = normalized_market_odds(
        event['odds'], knockout=_event_is_knockout(event))
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
    `rows` is the leaderboard order; matches user_id as TEXT or int."""
    uid = str(user_id)
    for i, row in enumerate(rows):
        if str(row.user_id) == uid:
            value = getattr(row, value_attr)
            return f'Your rank: **#{i + 1}** — {value} {unit}'
    return f"You're not on the {label} board yet."


def _utc_today():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _bot_prefix():
    return getattr(discord_common, '_BOT_PREFIX', ';')


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


def _user_mentions():
    allowed = getattr(discord, 'AllowedMentions', None)
    if allowed is None:
        return None
    try:
        return allowed(everyone=False, users=True, roles=False, replied_user=False)
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


# ── Cog ────────────────────────────────────────────────────────────────────

class Betting(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # channel_id -> events shown by the last `;bet matches` (for `;bet open <n>`)
        self._match_cache = {}
        # Shared cache of the last World Cup odds fetch (schedule + frozen-able
        # odds), reused by the scheduler, open timers and `;bet matches`.
        self._wc_events = None
        self._wc_fetched_at = None
        # fixture_key -> asyncio.Task: precise per-fixture "open at kickoff − 2h"
        # timers. Provider event ids can drift, so timers use canonical fixtures.
        self._open_timers = {}
        # market_id -> asyncio.Task: edit/announce exactly when betting closes.
        self._close_timers = {}
        # market_id -> asyncio.Task: coalesced thread intro pool refresh.
        self._pool_refresh_timers = {}

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        # user_db is set in the bot's on_ready handler, which may run after cog
        # listeners — wait briefly (as rpoll does) before arming timers.
        for _ in range(30):
            if cf_common.user_db is not None:
                break
            await asyncio.sleep(1)
        if cf_common.user_db is None:
            logger.warning('betting: user_db still None after waiting; skipping')
            return
        await self._refresh_schedule()   # arm open timers + catch in-window games
        await self._arm_close_timers()   # restore close timers after restart
        self._safety_net_task.start()
        self._settle_task.start()

    async def cog_unload(self):
        await self._safety_net_task.stop()
        await self._settle_task.stop()
        for task in list(self._open_timers.values()):
            if not task.done():
                task.cancel()
        self._open_timers.clear()
        for task in list(self._close_timers.values()):
            if not task.done():
                task.cancel()
        self._close_timers.clear()
        for task in list(self._pool_refresh_timers.values()):
            if not task.done():
                task.cancel()
        self._pool_refresh_timers.clear()

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

    def _pick_label(self, market, pick):
        base = positive_pick(pick)
        label = {'home': market.home_team, 'draw': 'Draw',
                 'away': market.away_team}[base]
        return f'Not {label}' if pick_is_negative(pick) else label

    def _pick_odds(self, market, pick):
        base = positive_pick(pick)
        odds = {'home': market.odds_home, 'draw': market.odds_draw,
                'away': market.odds_away}[base]
        if not pick_is_negative(pick):
            return odds
        if odds <= 1:
            return None
        base_probability = 1.0 / odds
        if base_probability >= 1:
            return None
        return 1.0 / (1.0 - base_probability)

    def _market_allows_draw(self, market):
        return (getattr(market, 'odds_draw', 0) or 0) > 1

    def _pick_allowed(self, market, pick):
        base = positive_pick(pick)
        if base == 'draw':
            return self._market_allows_draw(market)
        return base in ('home', 'away')

    def _not_odds_line(self, market):
        picks = ['home']
        if self._market_allows_draw(market):
            picks.append('draw')
        picks.append('away')
        parts = []
        for pick in picks:
            neg = f'not_{pick}'
            odds = self._pick_odds(market, neg)
            if odds is not None:
                parts.append(f'{self._pick_label(market, neg)} — **{odds:.2f}**')
        return ' · '.join(parts)

    def _pool_summary(self, market):
        pool = cf_common.user_db.bet_pool(market.market_id)
        if not pool:
            return None
        return ' · '.join(
            f'{self._pick_label(market, p.pick)}: {p.cnt} ({p.total} {_COIN})'
            for p in pool)

    def _add_pool_field(self, embed, market):
        summary = self._pool_summary(market)
        if summary:
            embed.add_field(name='Action so far', value=summary, inline=False)
        return embed

    def _configured_notify_role_id(self, guild_id):
        if cf_common.user_db is None:
            return None
        role_id = cf_common.user_db.get_guild_config(
            guild_id, _NOTIFY_ROLE_CONFIG_KEY)
        if role_id is None:
            return None
        try:
            int(role_id)
        except (TypeError, ValueError):
            return None
        return str(role_id)

    def _configured_notify_role(self, guild):
        role_id = self._configured_notify_role_id(guild.id)
        if role_id is None or not hasattr(guild, 'get_role'):
            return None
        return guild.get_role(int(role_id))

    def _notify_role_mention(self, guild_id):
        role_id = self._configured_notify_role_id(guild_id)
        return f'<@&{role_id}>' if role_id is not None else None

    def _open_announcement_kwargs(self, guild_id, event):
        kwargs = {'embed': self._open_announce_embed(event)}
        mention = self._notify_role_mention(guild_id)
        if mention is not None:
            kwargs['content'] = mention
            kwargs['allowed_mentions'] = _role_mentions()
        return kwargs

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

    # ── Embeds ─────────────────────────────────────────────────────────

    def _market_embed(self, market, *, current_channel_id=None):
        kickoff = int(market.commence_time)
        now = time.time()
        open_now = (market.status == 'open' and now < market.commence_time
                    and not market.bets_closed)
        if self._market_allows_draw(market):
            lines = [
                f'**1** · {market.home_team} win — **{market.odds_home:.2f}**',
                f'**X** · Draw — **{market.odds_draw:.2f}**',
                f'**2** · {market.away_team} win — **{market.odds_away:.2f}**',
            ]
        else:
            lines = [
                f'**1** · {market.home_team} advances — **{market.odds_home:.2f}**',
                f'**2** · {market.away_team} advances — **{market.odds_away:.2f}**',
            ]
        not_line = self._not_odds_line(market)
        if not_line:
            lines.extend(['', f'Not bets: {not_line}'])
        lines.extend(['', f'Kickoff: <t:{kickoff}:F> (<t:{kickoff}:R>)'])
        if open_now:
            if market.thread_id:
                if str(current_channel_id) == str(market.thread_id):
                    lines.append('\nReply in this thread to bet — '
                                 'betting closes at kickoff.')
                else:
                    lines.append(
                        f'\nBetting thread: <#{market.thread_id}> — '
                        'betting closes at kickoff.')
            else:
                lines.append('\nBet with `;bet home/draw/away <amount>` — '
                             'betting closes at kickoff.')
            color = 0x2ecc71
        elif market.status == 'open':
            if now < market.commence_time:
                lines.append('\n🔒 **Betting ended** — awaiting kickoff.')
            else:
                lines.append('\n🔒 **Betting ended** — awaiting result.')
            color = 0xf1c40f
        else:
            color = 0x95a5a6
        suffix = ' — who advances?' if not self._market_allows_draw(market) else ''
        embed = discord.Embed(
            title=f'⚽ {market.home_team} vs {market.away_team}{suffix}',
            description='\n'.join(lines), color=color)
        return self._add_pool_field(embed, market)

    def _thread_intro_embed(self, market):
        kickoff = int(market.commence_time)
        if self._market_allows_draw(market):
            odds_line = (f'Odds (fair/no-vig, frozen): **1** {market.odds_home:.2f} · '
                         f'**X** {market.odds_draw:.2f} · **2** {market.odds_away:.2f}')
            examples = (
                f'• `{market.home_team} 100` — back {market.home_team}\n'
                '• `draw 50` (or `tie`) — back a draw\n'
                f'• `{market.away_team} all` (also `25%`)\n'
                f'• `not {market.away_team} 25` — back {market.away_team} not winning')
        else:
            odds_line = (f'Odds to advance (fair/no-vig, frozen): '
                         f'**1** {market.odds_home:.2f} · '
                         f'**2** {market.odds_away:.2f}')
            examples = (
                f'• `{market.home_team} 100` — back {market.home_team} to advance\n'
                f'• `{market.away_team} all` — back {market.away_team} to advance\n'
                f'• `not {market.home_team} 25` — back {market.home_team} not advancing')
        not_line = self._not_odds_line(market)
        if not_line:
            odds_line += f'\nNot bets: {not_line}'
        pick_hint = 'home/draw/away' if self._market_allows_draw(market) else 'home/away'
        desc = (
            'Reply in this thread to bet — use the **country name** or '
            f'{pick_hint}:\n'
            f'{examples}\n'
            '\n'
            f'{odds_line}\n'
            f'Returns = stake × odds. Re-bet a pick to change it; use `0` '
            f'to remove that pick.\n'
            f'Kickoff: <t:{kickoff}:F> (<t:{kickoff}:R>)\n'
            '⏱️ **Betting closes at kickoff.**')
        embed = discord.Embed(title='🎟️ Place your bets', description=desc,
                              color=0x2ecc71)
        return self._add_pool_field(embed, market)

    def _thread_name(self, market):
        name = f'⚽ {market.home_team} vs {market.away_team} — bets'
        return name[:100]

    def _open_announce_embed(self, event):
        """The 'betting open' announcement, built from a raw odds event so it
        can be posted BEFORE the market row exists (send-first, so a failed
        send never orphans a market)."""
        o = event['odds']
        kickoff = int(event['commence_time'])
        if _odds_allow_draw(o):
            lines = [
                f'**1** · {event["home_team"]} win — **{o["home"]:.2f}**',
                f'**X** · Draw — **{o["draw"]:.2f}**',
                f'**2** · {event["away_team"]} win — **{o["away"]:.2f}**',
            ]
        else:
            lines = [
                f'**1** · {event["home_team"]} advances — **{o["home"]:.2f}**',
                f'**2** · {event["away_team"]} advances — **{o["away"]:.2f}**',
            ]
        lines.extend([
            '',
            # <t:..:R> renders as a live countdown on the client ("in 53 minutes").
            f'Kickoff: <t:{kickoff}:F> (<t:{kickoff}:R>)',
            '⏱️ **Betting closes at kickoff.**',
            '\n👇 **Place your bets in the thread below.**',
        ])
        suffix = ' — who advances?' if not _odds_allow_draw(o) else ''
        return discord.Embed(
            title=f'⚽ {event["home_team"]} vs {event["away_team"]}{suffix}',
            description='\n'.join(lines), color=0x2ecc71)

    # ── Group ──────────────────────────────────────────────────────────

    @commands.group(name='bet',
                    aliases=['betting', 'prediction', 'pred', 'wager'],
                    brief='World Cup betting', invoke_without_command=True)
    async def bet(self, ctx):
        """Show the active market here and your balance."""
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, ctx.author.id, constants.BET_START_BALANCE)
        market = self._find_market(ctx)
        if market is None:
            configured = cf_common.user_db.get_guild_config(
                ctx.guild.id, _CHANNEL_CONFIG_KEY)
            hint = ('Markets auto-open ~2h before each World Cup kickoff'
                    if configured else
                    'An admin can run `;prediction here` to start auto-opening '
                    'World Cup markets in a channel')
            await ctx.send(embed=discord_common.embed_neutral(
                f'No open market here. You have **{balance}** {_COIN}.\n'
                f'{hint}. See `;help bet`.'))
            return
        embed = self._market_embed(market, current_channel_id=ctx.channel.id)
        embed.set_footer(text=f'Your balance: {balance} coins')
        await ctx.send(embed=embed)

    @bet.command(name='here',
                 brief='Set this channel for auto-opened World Cup markets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        """Designate this channel as where the bot auto-posts markets."""
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _CHANNEL_CONFIG_KEY, str(ctx.channel.id))
        note = ('' if _api_key() else
                '\n⚠️ No `ODDS_API_KEY` is set, so nothing will auto-open until '
                'one is configured.')
        await ctx.send(embed=discord_common.embed_success(
            f'World Cup markets will auto-open in {ctx.channel.mention} ~2h '
            f'before each kickoff, with a thread for bets.{note}'))
        # Arm timers now (and open anything already inside the 2h window) so we
        # don't wait for the next safety-net sweep.
        if _api_key():
            try:
                await self._refresh_schedule()
            except Exception:
                logger.warning('schedule refresh after `;prediction here` '
                               'failed', exc_info=True)

    @bet.command(name='notifyrole', aliases=['pingrole'],
                 brief='Set the role pinged when a market opens (admin)',
                 usage='[@role]')
    @commands.has_role(constants.TLE_ADMIN)
    async def notifyrole(self, ctx, role: discord.Role = None):
        if role is None:
            role_id = self._configured_notify_role_id(ctx.guild.id)
            if role_id is None:
                await ctx.send(embed=discord_common.embed_neutral(
                    'No betting notification role is configured.'))
            else:
                await ctx.send(embed=discord_common.embed_neutral(
                    f'Betting notification role: <@&{role_id}>.'))
            return
        self._validate_notify_role(ctx, role)
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _NOTIFY_ROLE_CONFIG_KEY, str(role.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Betting markets will ping {role.mention} when they open.'),
            allowed_mentions=_no_mentions())

    @bet.command(name='clearnotifyrole', aliases=['notifyroleoff', 'pingroleoff'],
                 brief='Stop pinging a role when markets open (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def clearnotifyrole(self, ctx):
        cf_common.user_db.delete_guild_config(ctx.guild.id, _NOTIFY_ROLE_CONFIG_KEY)
        await ctx.send(embed=discord_common.embed_success(
            'Betting notification role cleared.'))

    @bet.command(name='notify', aliases=['notifications'],
                 brief='Toggle betting notifications for yourself')
    async def notify(self, ctx):
        role_id = self._configured_notify_role_id(ctx.guild.id)
        if role_id is None:
            raise BettingCogError(
                'No betting notification role is configured yet.')
        role = self._configured_notify_role(ctx.guild)
        if role is None:
            raise BettingCogError(
                'The configured betting notification role no longer exists.')
        try:
            if self._member_has_role(ctx.author, role_id):
                await ctx.author.remove_roles(role, reason='Betting notifications off')
                await ctx.send(embed=discord_common.embed_success(
                    f'Removed {role.mention} from you.'),
                    allowed_mentions=_no_mentions())
            else:
                await ctx.author.add_roles(role, reason='Betting notifications on')
                await ctx.send(embed=discord_common.embed_success(
                    f'Added {role.mention} to you.'),
                    allowed_mentions=_no_mentions())
        except (discord.Forbidden, discord.HTTPException, AttributeError):
            raise BettingCogError(
                'I could not update that role. Check my role permissions.')

    @bet.command(name='check',
                 brief='Check betting API keys without exposing secrets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def check(self, ctx):
        """Verify that the betting API keys are configured and usable."""
        lines = ['Betting API check:']

        api_key = _api_key()
        if not api_key:
            lines.append('❌ `ODDS_API_KEY` is not set.')
        else:
            try:
                sports = await odds_api.fetch_sports(api_key)
            except odds_api.OddsApiError as e:
                lines.append(f'❌ `ODDS_API_KEY` failed: `{_short_error(e)}`')
            else:
                wc = next((s for s in sports or []
                           if s.get('key') == odds_api.WORLD_CUP_SPORT_KEY), None)
                if wc is None:
                    lines.append(
                        f'⚠️ `ODDS_API_KEY` works, but '
                        f'`{odds_api.WORLD_CUP_SPORT_KEY}` is not listed as active.')
                else:
                    title = wc.get('title') or odds_api.WORLD_CUP_SPORT_KEY
                    lines.append(f'✅ `ODDS_API_KEY` works; `{title}` is active.')

        fd_key = _football_data_key()
        if not fd_key:
            lines.append('❌ `FOOTBALL_DATA_API_KEY` is not set.')
        else:
            try:
                matches = await football_data.fetch_wc_matches(fd_key)
            except football_data.FootballDataError as e:
                lines.append(
                    f'❌ `FOOTBALL_DATA_API_KEY` failed: `{_short_error(e)}`')
            else:
                lines.append(
                    f'✅ `FOOTBALL_DATA_API_KEY` works; '
                    f'{len(matches)} World Cup match(es) returned.')

        lines.append('\nOdds check uses The Odds API `/sports` endpoint '
                     '(documented quota-free).')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    # ── Matches / manual open ──────────────────────────────────────────

    @bet.command(name='matches', aliases=['games', 'fixtures'],
                 brief='List upcoming World Cup matches with odds',
                 usage='[query]')
    async def matches(self, ctx, *, query: str = None):
        """List upcoming World Cup matches (optionally filtered by team)."""
        async with ctx.typing():
            events = await self._ensure_wc_events(_MATCH_CACHE_MAX_AGE)

        now = time.time()
        events = [e for e in events if e['commence_time'] > now]
        if query:
            q = query.strip().lower()
            events = [e for e in events
                      if q in e['home_team'].lower() or q in e['away_team'].lower()]
        events.sort(key=lambda e: e['commence_time'])
        if not events:
            raise BettingCogError(
                'No upcoming World Cup matches with odds found'
                + (f' for “{query}”.' if query else '.'))

        events = events[:_MATCH_LIST_LIMIT]
        self._match_cache[ctx.channel.id] = events
        lines = []
        for i, e in enumerate(events, 1):
            o = e['odds']
            ko = int(e['commence_time'])
            if _odds_allow_draw(o):
                odds_line = (f'1 **{o["home"]:.2f}** · X **{o["draw"]:.2f}** · '
                             f'2 **{o["away"]:.2f}**')
            else:
                odds_line = (f'to advance: 1 **{o["home"]:.2f}** · '
                             f'2 **{o["away"]:.2f}**')
            lines.append(
                f'**{i}.** {e["home_team"]} vs {e["away_team"]} — <t:{ko}:R>\n'
                f'    {odds_line}')
        embed = discord.Embed(title='⚽ Upcoming World Cup matches',
                              description='\n'.join(lines), color=0x3498db)
        embed.set_footer(text='Auto-opens ~2h before kickoff · '
                              'admins: ;bet open <number> to open early')
        await ctx.send(embed=embed)

    @bet.command(name='open', brief='Manually open a market early (admin)',
                 usage='<number from ;bet matches | event_id>')
    @commands.has_role(constants.TLE_ADMIN)
    async def open_market(self, ctx, *, ref: str):
        """Open betting on a match from the last `;bet matches` list, early."""
        if cf_common.user_db.bet_market_get_active(ctx.guild.id, ctx.channel.id):
            raise BettingCogError(
                'A market is already open in this channel. Settle or '
                '`;bet cancel` it first.')

        events = self._match_cache.get(ctx.channel.id)
        if not events:
            raise BettingCogError('Run `;bet matches` first, then '
                                  '`;bet open <number>`.')
        ref = ref.strip()
        if ref.isdigit() and 1 <= int(ref) <= len(events):
            event = events[int(ref) - 1]
        else:
            event = next((e for e in events if e['event_id'] == ref), None)
        if event is None:
            raise BettingCogError(
                f'`{discord.utils.escape_markdown(ref)}` is not in the current '
                'list. Run `;bet matches` again and use the row number.')

        if event['commence_time'] <= time.time():
            raise BettingCogError('That match has already kicked off.')
        if cf_common.user_db.bet_market_exists_open_for_event(
                ctx.guild.id, event['event_id']):
            raise BettingCogError('There is already an open market on that match.')
        duplicate = self._find_duplicate_match(ctx.guild.id, event)
        if duplicate is not None:
            raise BettingCogError(
                'There is already an open market on that match: '
                f'{self._market_place_ref(duplicate)}.')

        market_id = self._create_market(ctx.guild.id, ctx.channel.id, event)
        if market_id is None:
            raise BettingCogError('There is already an open market on that match.')
        try:
            msg = await ctx.send(
                **self._open_announcement_kwargs(ctx.guild.id, event))
        except discord.HTTPException:
            cf_common.user_db.bet_void(ctx.guild.id, market_id, time.time())
            raise
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
        market = cf_common.user_db.bet_market_get(market_id)
        thread = await self._create_thread(market_id, msg, market)
        if thread is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Could not create a betting thread (missing "Create Public '
                'Threads" permission?). Bets can still be placed here with '
                '`;bet home/draw/away <amount>`.'))
        self._schedule_close(market)
        logger.info('Manually opened market %s (%s vs %s) in guild %s',
                    market_id, event['home_team'], event['away_team'], ctx.guild.id)

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
            guild_id, user.id, constants.BET_START_BALANCE)
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
            time.time(), constants.BET_START_BALANCE)
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

    async def _place(self, ctx, pick, amount_str):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError(
                'No open market here. Bets are placed in the match thread the '
                'bot opens ~2h before kickoff.')
        if not self._pick_allowed(market, pick):
            raise BettingCogError('That outcome is not available for this market.')
        status, data = await self._execute_bet(
            ctx.guild.id, market, ctx.author, pick, amount_str)
        if status == 'closed':
            raise BettingCogError('Betting is closed — kickoff has passed.')
        if status == 'invalid':
            raise BettingCogError(
                f'Invalid amount. Use a whole number (min {constants.BET_MIN_STAKE}), '
                'a percentage like `50%`, or `all`.')
        if status == 'invalid_pick':
            raise BettingCogError('That outcome is not available for this market.')
        if status == 'insufficient':
            raise BettingCogError(
                f'You only have **{data["balance"]}** {_COIN}. Try `;bet daily`.')
        if status == 'missing':
            raise BettingCogError(
                f'You do not have a bet on **{data["label"]}** to remove.')
        if status == 'removed':
            self._schedule_pool_refresh(market.market_id)
            await ctx.send(embed=discord_common.embed_success(
                f'Removed bet on **{data["label"]}** and refunded '
                f'**{data["stake"]}** {_COIN}.\n'
                f'Balance: **{data["balance"]}** {_COIN}.'))
            return
        if status == 'unchanged':
            await ctx.send(embed=discord_common.embed_neutral(
                f'Bet unchanged: **{data["stake"]}** {_COIN} on '
                f'**{data["label"]}** @ **{data["odds"]:.2f}**.\n'
                f'Balance: **{data["balance"]}** {_COIN}.'))
            return
        self._schedule_pool_refresh(market.market_id)
        await ctx.send(embed=discord_common.embed_success(
            f'Bet placed: **{data["stake"]}** {_COIN} on **{data["label"]}** @ '
            f'**{data["odds"]:.2f}** — returns **{data["potential"]}** {_COIN} '
            f'if it hits.\nBalance: **{data["balance"]}** {_COIN}.'))

    @bet.command(name='home', aliases=['1'], brief='Bet on the home win',
                 usage='<amount | 50% | all | 0 to remove>')
    async def bet_home(self, ctx, amount: str):
        await self._place(ctx, 'home', amount)

    @bet.command(name='draw', aliases=['x', 'tie'], brief='Bet on a draw',
                 usage='<amount | 50% | all | 0 to remove>')
    async def bet_draw(self, ctx, amount: str):
        await self._place(ctx, 'draw', amount)

    @bet.command(name='away', aliases=['2'], brief='Bet on the away win',
                 usage='<amount | 50% | all | 0 to remove>')
    async def bet_away(self, ctx, amount: str):
        await self._place(ctx, 'away', amount)

    @bet.command(name='not', aliases=['no'], brief='Bet that an outcome will not happen',
                 usage='<team|home|draw|away> <amount | 50% | all | 0 to remove>')
    async def bet_not(self, ctx, *, text: str):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        tokens = extract_bet_tokens(text)
        if tokens is None:
            raise BettingCogError(
                'Use `;bet not <team|home|draw|away> <amount>`, for example '
                '`;bet not draw 100`.')
        pick_text, amount = tokens
        pick = resolve_bet_pick(
            f'not {pick_text}', market.home_team, market.away_team,
            allow_draw=self._market_allows_draw(market))
        if pick is None:
            raise BettingCogError('That outcome is not available for this market.')
        await self._place(ctx, pick, amount)

    @bet.command(name='mybet', aliases=['mybets'], brief='Show your active bet')
    async def mybet(self, ctx):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        wagers = cf_common.user_db.bet_get_wagers_for_user(
            market.market_id, ctx.author.id)
        if not wagers:
            await ctx.send(embed=discord_common.embed_neutral(
                "You haven't bet on this match yet."))
            return
        lines = []
        total_stake = 0
        for wager in wagers:
            label = self._pick_label(market, wager.pick)
            odds = self._pick_odds(market, wager.pick)  # frozen on the market
            potential = payout_amount(wager.stake, odds)
            total_stake += wager.stake
            lines.append(
                f'**{wager.stake}** {_COIN} on **{label}** @ '
                f'**{odds:.2f}** → returns **{potential}** {_COIN}')
        await ctx.send(embed=discord_common.embed_neutral(
            'Your bets:\n' + '\n'.join(lines)
            + f'\n\nTotal staked: **{total_stake}** {_COIN}.'))

    async def _withdraw_match(self, ctx):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        if time.time() >= market.commence_time or market.bets_closed:
            raise BettingCogError('Betting is closed — bets can no longer be withdrawn.')
        ok, reason, balance, refunded, count = cf_common.user_db.bet_remove_wagers_for_user(
            ctx.guild.id, market.market_id, ctx.author.id, time.time())
        if not ok:
            if reason == 'closed':
                raise BettingCogError(
                    'Betting is closed — bets can no longer be withdrawn.')
            if reason == 'missing':
                await ctx.send(embed=discord_common.embed_neutral(
                    "You don't have any bets on this match."))
                return
            raise BettingCogError('Could not withdraw bets from this match.')
        self._schedule_pool_refresh(market.market_id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed **{count}** bet(s) on **{market.home_team} vs '
            f'{market.away_team}** and refunded **{refunded}** {_COIN}.\n'
            f'Balance: **{balance}** {_COIN}.'))

    @bet.command(name='withdraw', aliases=['clear', 'removeall', 'unbet', 'cancelbets'],
                 brief='Remove all your bets on the active match')
    async def withdraw(self, ctx):
        await self._withdraw_match(ctx)

    # ── Thread bet listener ────────────────────────────────────────────

    async def _react(self, message, emoji):
        try:
            await message.add_reaction(emoji)
        except (discord.HTTPException, AttributeError):
            pass

    @commands.Cog.listener()
    async def on_message(self, message):
        """Treat a plain `pick amount` message inside a betting thread as a
        bet. Cheap pre-filters keep this off the DB for ordinary chatter."""
        if message.author.bot or message.guild is None:
            return
        content = message.content or ''
        if content.startswith(_bot_prefix()):
            return  # a command — let the command system handle it
        tokens = extract_bet_tokens(content)
        if tokens is None:
            return
        if cf_common.user_db is None:
            return  # startup window — DB not initialized yet
        market = cf_common.user_db.bet_market_get_active_by_thread(
            message.guild.id, message.channel.id)
        if market is None:
            return  # not a betting thread — ignored on purpose
        pick_text, amount_str = tokens
        pick = resolve_bet_pick(
            pick_text, market.home_team, market.away_team,
            allow_draw=self._market_allows_draw(market))
        if pick is None:
            return  # not a recognizable team/outcome — ignore (avoid chat noise)
        try:
            status, data = await self._execute_bet(
                message.guild.id, market, message.author, pick, amount_str)
        except Exception:
            logger.warning('thread bet failed in market %s', market.market_id,
                           exc_info=True)
            return
        if status in ('ok', 'removed', 'unchanged'):
            await self._react(message, '✅')
            if status != 'unchanged':
                self._schedule_pool_refresh(market.market_id)
        elif status == 'closed':
            await self._react(message, '🔒')
        elif status == 'insufficient':
            await self._react(message, '❌')
        else:  # invalid
            await self._react(message, '❓')

    # ── Wallet ─────────────────────────────────────────────────────────

    @bet.command(name='balance', aliases=['bal', 'wallet'], brief='Show a balance',
                 usage='[@user]')
    async def balance(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        bal = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, target.id, constants.BET_START_BALANCE)
        who = 'You have' if target == ctx.author else \
            f'{discord.utils.escape_markdown(target.display_name)} has'
        await ctx.send(embed=discord_common.embed_neutral(
            f'{who} **{bal}** {_COIN}.'))

    @bet.command(name='me', aliases=['profile', 'summary'],
                 brief='Show your betting summary')
    async def me(self, ctx):
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, ctx.author.id, constants.BET_START_BALANCE)
        wallet = cf_common.user_db.bet_wallet_get(ctx.guild.id, ctx.author.id)
        wallet_rank = rank_line(
            cf_common.user_db.bet_balance_leaderboard(ctx.guild.id),
            ctx.author.id, 'balance', 'wallet')
        profit_rank = rank_line(
            cf_common.user_db.bet_profit_leaderboard(ctx.guild.id),
            ctx.author.id, 'profit', 'profit')
        daily = 'claimed today' if wallet and wallet.last_daily == _utc_today() \
            else 'available'
        name = discord.utils.escape_markdown(ctx.author.display_name)
        embed = discord.Embed(
            title=f'Betting — {name}',
            description=(
                f'Balance: **{balance}** {_COIN}\n'
                f'{wallet_rank}\n'
                f'{profit_rank}\n'
                f'Daily: **{daily}**'),
            color=0x3498db)

        active = cf_common.user_db.bet_active_wagers_for_user(
            ctx.guild.id, ctx.author.id, 5)
        if active:
            lines = []
            for row in active:
                odds = self._pick_odds(row, row.pick)
                potential = payout_amount(row.stake, odds) if odds is not None else 0
                odds_text = f'{odds:.2f}' if odds is not None else '?'
                ref = f'<#{row.thread_id}>' if row.thread_id else f'<#{row.channel_id}>'
                locked = ' locked' if row.bets_closed else ''
                lines.append(
                    f'{ref} **{row.home_team} vs {row.away_team}**{locked}: '
                    f'{row.stake} {_COIN} on **{self._pick_label(row, row.pick)}** '
                    f'@ {odds_text} → {potential} {_COIN}')
            embed.add_field(name='Active bets', value='\n'.join(lines),
                            inline=False)
        else:
            embed.add_field(name='Active bets', value='No active bets.',
                            inline=False)

        history = cf_common.user_db.bet_wallet_history(ctx.guild.id, ctx.author.id, 5)
        if history:
            embed.add_field(
                name='Recent wallet activity',
                value='\n'.join(self._wallet_txn_line(row) for row in history),
                inline=False)
        await ctx.send(embed=embed, allowed_mentions=_no_mentions())

    @bet.command(name='daily', aliases=['claim'], brief='Claim the daily allowance')
    async def daily(self, ctx):
        granted, balance, reason = cf_common.user_db.bet_claim_daily(
            ctx.guild.id, ctx.author.id, _utc_today(),
            constants.BET_DAILY_AMOUNT, constants.BET_START_BALANCE)
        if granted:
            await ctx.send(embed=discord_common.embed_success(
                f'Claimed **+{constants.BET_DAILY_AMOUNT}** {_COIN}. '
                f'Balance: **{balance}** {_COIN}. Come back tomorrow!'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'Already claimed today. Balance: **{balance}** {_COIN}. '
                'Resets at 00:00 UTC.'))

    @commands.command(name='beg', brief='Ask someone for betting coins',
                      usage='@user [amount]')
    async def beg(self, ctx, donor: discord.Member, *, suggested: str = None):
        if donor.id == ctx.author.id:
            raise BettingCogError('You cannot beg yourself.')
        if getattr(donor, 'bot', False):
            raise BettingCogError('You cannot beg bots for coins.')
        if self.bot is None:
            raise BettingCogError('Begging is not available right now.')

        donor_balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, donor.id, constants.BET_START_BALANCE)
        suggestion = ''
        if suggested:
            suggested_amount = parse_amount(suggested, donor_balance, 1)
            if suggested_amount is None:
                raise BettingCogError(
                    'Invalid suggested amount. Use a positive whole number, '
                    'a percentage like `50%`, or `all`.')
            suggestion = (
                f'\nSuggested amount: **{suggested_amount}** {_COIN}. '
                'You can still choose a different amount.')

        requester = discord.utils.escape_markdown(ctx.author.display_name)
        donor_name = discord.utils.escape_markdown(donor.display_name)
        await ctx.send(
            content=donor.mention,
            embed=discord_common.embed_neutral(
                f'`{requester}` is begging `{donor_name}` for betting coins.'
                f'{suggestion}\n'
                f'{donor.mention}, reply in this channel with an amount to give '
                f'(`100`, `50%`, or `all`), or `no` to decline. '
                f'This expires in {_BEG_TIMEOUT}s.'),
            allowed_mentions=_user_mentions())

        end_time = asyncio.get_running_loop().time() + _BEG_TIMEOUT

        def check(message):
            return (
                getattr(message, 'guild', None) is not None
                and str(message.guild.id) == str(ctx.guild.id)
                and str(message.channel.id) == str(ctx.channel.id)
                and str(message.author.id) == str(donor.id)
                and not getattr(message.author, 'bot', False)
            )

        while True:
            timeout = end_time - asyncio.get_running_loop().time()
            if timeout <= 0:
                await ctx.send(embed=discord_common.embed_neutral(
                    f'Beg request expired. `{donor_name}` did not respond.'))
                return
            try:
                message = await self.bot.wait_for(
                    'message', timeout=timeout, check=check)
            except asyncio.TimeoutError:
                await ctx.send(embed=discord_common.embed_neutral(
                    f'Beg request expired. `{donor_name}` did not respond.'))
                return

            text = (message.content or '').strip()
            if text.startswith(_bot_prefix()):
                continue
            lowered = text.lower()
            if lowered in {'no', 'n', 'decline', 'deny', 'cancel', '0'}:
                await ctx.send(embed=discord_common.embed_neutral(
                    f'`{donor_name}` declined the beg request.'))
                return

            donor_balance = cf_common.user_db.bet_ensure_wallet(
                ctx.guild.id, donor.id, constants.BET_START_BALANCE)
            amount = parse_amount(text, donor_balance, 1)
            if amount is None:
                await ctx.send(embed=discord_common.embed_alert(
                    f'Invalid amount. `{donor_name}`, reply with a positive whole '
                    'number, a percentage like `50%`, `all`, or `no`.'))
                continue
            ok, reason, donor_balance, requester_balance = cf_common.user_db.bet_transfer(
                ctx.guild.id, donor.id, ctx.author.id, amount,
                constants.BET_START_BALANCE, actor_id=donor.id)
            if not ok:
                if reason == 'insufficient':
                    await ctx.send(embed=discord_common.embed_alert(
                        f'Insufficient balance. `{donor_name}` has '
                        f'**{donor_balance}** {_COIN}.'))
                    continue
                raise BettingCogError('Beg transfer failed.')

            await ctx.send(embed=discord_common.embed_success(
                f'`{donor_name}` gave `{requester}` **{amount}** {_COIN}.\n'
                f'`{donor_name}`: **{donor_balance}** {_COIN}. '
                f'`{requester}`: **{requester_balance}** {_COIN}.'))
            return

    @bet.command(name='transfer', aliases=['send', 'pay'],
                 brief='Move coins from one user to another (admin)',
                 usage='@from @to <amount|all|percent>')
    @commands.has_role(constants.TLE_ADMIN)
    async def transfer(self, ctx, from_member: discord.Member,
                       to_member: discord.Member, amount: str):
        if from_member.id == to_member.id:
            raise BettingCogError('Source and destination must be different users.')
        if getattr(from_member, 'bot', False) or getattr(to_member, 'bot', False):
            raise BettingCogError('You cannot transfer coins to or from a bot.')
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, from_member.id, constants.BET_START_BALANCE)
        amount_value = parse_amount(amount, balance, 1)
        if amount_value is None:
            raise BettingCogError(
                'Invalid amount. Use a positive whole number, a percentage like '
                '`50%`, or `all`.')
        ok, reason, sender_balance, receiver_balance = cf_common.user_db.bet_transfer(
            ctx.guild.id, from_member.id, to_member.id, amount_value,
            constants.BET_START_BALANCE, actor_id=ctx.author.id)
        if not ok:
            if reason == 'insufficient':
                raise BettingCogError(
                    f'Insufficient balance. Source has **{sender_balance}** {_COIN}.')
            if reason == 'self':
                raise BettingCogError(
                    'Source and destination must be different users.')
            raise BettingCogError('Transfer failed.')
        source = discord.utils.escape_markdown(from_member.display_name)
        target = discord.utils.escape_markdown(to_member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Moved **{amount_value}** {_COIN} from `{source}` to `{target}`. '
            f'`{source}`: **{sender_balance}** {_COIN}. '
            f'`{target}`: **{receiver_balance}** {_COIN}.'))

    def _wallet_txn_line(self, row):
        labels = {
            'init': 'wallet opened',
            'daily': 'daily claim',
            'wager_refund': 're-bet refund',
            'wager_stake': 'wager',
            'payout': 'payout',
            'resettle_delta': 'correction',
            'void_refund': 'void refund',
            'admin_grant': 'admin grant',
            'admin_take': 'admin take',
            'admin_setbalance': 'admin set balance',
            'mod_grant': 'mod grant',
            'mod_take': 'mod take',
            'mod_setbalance': 'mod set balance',
            'transfer_out': 'transfer sent',
            'transfer_in': 'transfer received',
            'adjust': 'adjustment',
            'setbalance': 'set balance',
        }
        sign = '+' if row.amount > 0 else ''
        actor = ''
        if row.action == 'transfer_out':
            if row.actor_id and str(row.actor_id) != str(row.user_id):
                actor = f' by <@{row.actor_id}>'
        elif row.action == 'transfer_in':
            if row.actor_id and row.note and str(row.actor_id) != str(row.note):
                actor = f' by <@{row.actor_id}>'
        elif row.actor_id and str(row.actor_id) != str(row.user_id):
            actor = f' by <@{row.actor_id}>'
        market = f' · market #{row.market_id}' if row.market_id is not None else ''
        if row.action == 'transfer_out' and row.note:
            note = f' · to <@{row.note}>'
        elif row.action == 'transfer_in' and row.note:
            note = f' · from <@{row.note}>'
        else:
            note = f' · {discord.utils.escape_markdown(str(row.note))}' if row.note else ''
        label = labels.get(row.action, row.action.replace('_', ' '))
        return (f'<t:{int(row.created_at)}:R> — **{sign}{row.amount}** {_COIN} '
                f'({label}{actor}{market}{note}) → **{row.balance_after}**')

    @bet.command(name='history', aliases=['walletlog', 'ledger'],
                 brief='Show wallet audit history', usage='[@user]')
    async def history(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        rows = cf_common.user_db.bet_wallet_history(ctx.guild.id, target.id, 15)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral(
                'No wallet history yet.'))
            return
        name = discord.utils.escape_markdown(target.display_name)
        embed = discord.Embed(
            title=f'Wallet history — {name}',
            description='\n'.join(self._wallet_txn_line(row) for row in rows),
            color=0x3498db)
        await ctx.send(embed=embed, allowed_mentions=_no_mentions())

    # ── Leaderboard ────────────────────────────────────────────────────

    @bet.command(name='leaderboard', aliases=['lb', 'board', 'top'],
                 brief='Wallet leaderboard (add `profit` for net profit)',
                 usage='[profit]')
    async def leaderboard(self, ctx, mode: str = None):
        profit = mode is not None and mode.strip().lower() in ('profit', 'net')
        if profit:
            rows = cf_common.user_db.bet_profit_leaderboard(ctx.guild.id)
            title = '💰 Betting profit'
            value_attr = 'profit'

            def fmt(row):
                sign = '+' if row.profit >= 0 else ''
                return f'{sign}{row.profit} {_COIN} ({row.wins}/{row.bets} won)'
        else:
            rows = cf_common.user_db.bet_balance_leaderboard(ctx.guild.id)
            title = '🏆 Richest wallets'
            value_attr = 'balance'

            def fmt(row):
                return f'{row.balance} {_COIN}'

        if not rows:
            raise BettingCogError('No bettors yet. Markets auto-open before '
                                  'each World Cup kickoff — `;bet matches`.')

        personal = rank_line(rows, ctx.author.id, value_attr,
                             'profit' if profit else 'wallet')
        chunks = paginator.chunkify(rows, _LB_PER_PAGE)
        pages = []
        for page_idx, chunk in enumerate(chunks):
            lines = []
            for i, row in enumerate(chunk):
                rank = page_idx * _LB_PER_PAGE + i + 1
                member = ctx.guild.get_member(int(row.user_id))
                name = member.mention if member is not None else f'`{row.user_id}`'
                lines.append(f'**#{rank}** {name} — {fmt(row)}')
            embed = discord.Embed(title=title, description='\n'.join(lines),
                                  color=0xf1c40f)
            pages.append((personal, embed))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    # ── Settle / cancel / pending ──────────────────────────────────────

    @bet.command(name='settle', brief='Settle the active market manually (admin)',
                 usage='<home|draw|away|2-1>')
    @commands.has_role(constants.TLE_ADMIN)
    async def settle(self, ctx, *, result: str):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here to settle.')
        parsed = self._parse_result(market, result)
        if parsed is None:
            raise BettingCogError(
                'Give the result as `home`, `draw`, `away`, a scoreline like '
                '`2-1`, or the winning team name.')
        outcome, home_score, away_score = parsed
        await self._do_settle(market, outcome, home_score, away_score,
                             source='manual')

    @bet.command(name='cancel', aliases=['void'],
                 brief='Cancel the active market and refund (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def cancel(self, ctx):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here to cancel.')
        refunds = cf_common.user_db.bet_void(
            market.guild_id, market.market_id, time.time())
        if refunds is None:
            raise BettingCogError('That market was just settled or cancelled.')
        total = sum(stake for _, stake in refunds)
        await ctx.send(embed=discord_common.embed_success(
            f'Market on **{market.home_team} vs {market.away_team}** cancelled. '
            f'Refunded **{total}** {_COIN} across **{len(refunds)}** bet(s).'))
        await self._archive_thread(market)
        logger.info('Cancelled bet market %s in guild %s (%s refunds)',
                    market.market_id, ctx.guild.id, len(refunds))

    @bet.command(name='pending', aliases=['stuck'],
                 brief='List open markets past kickoff awaiting a result')
    async def pending(self, ctx):
        """Show markets that have kicked off but not yet settled — e.g. a
        fixture the scores API never reported as completed. Stakes stay
        escrowed until an admin settles (`;bet settle`) or cancels (`;bet cancel`).
        """
        now = time.time()
        markets = [m for m in cf_common.user_db.bet_markets_open(ctx.guild.id)
                   if m.commence_time <= now]
        if not markets:
            await ctx.send(embed=discord_common.embed_neutral(
                'No markets are stuck — every open market is still pre-kickoff.'))
            return
        lines = []
        for m in markets:
            ch = f'<#{m.thread_id}>' if m.thread_id else f'<#{m.channel_id}>'
            lines.append(
                f'• **{m.home_team} vs {m.away_team}** — kicked off '
                f'<t:{int(m.commence_time)}:R> · {ch}')
        embed = discord.Embed(
            title='⏳ Markets awaiting a result',
            description='\n'.join(lines)
            + '\n\nAn admin can `;bet settle <home|draw|away|2-1>` or `;bet cancel` '
            'in each market\'s channel/thread.',
            color=0xf1c40f)
        await ctx.send(embed=embed,
                       allowed_mentions=_no_mentions())

    @bet.command(name='correct', aliases=['fix', 'resettle'],
                 brief='Fix a wrongly-settled result (admin)',
                 usage='<home|draw|away|2-1|team>')
    @commands.has_role(constants.TLE_ADMIN)
    async def correct(self, ctx, *, result: str):
        """Re-settle the most recently settled market here with the corrected
        result, reversing the wrong payouts and applying the right ones."""
        market = (cf_common.user_db.bet_market_get_latest_settled_by_thread(
                      ctx.guild.id, ctx.channel.id)
                  or cf_common.user_db.bet_market_get_latest_settled_by_channel(
                      ctx.guild.id, ctx.channel.id))
        if market is None:
            raise BettingCogError(
                'No settled market here to correct. Run this in the match\'s '
                'thread or channel.')
        parsed = self._parse_result(market, result)
        if parsed is None:
            raise BettingCogError(
                'Give the corrected result as `home`/`draw`/`away`, a scoreline '
                'like `2-1`, or the winning team name.')
        outcome, home_score, away_score = parsed
        if not self._pick_allowed(market, outcome):
            raise BettingCogError('That result is not available for this market.')
        rows = cf_common.user_db.bet_resettle(
            market.guild_id, market.market_id, outcome, home_score, away_score,
            time.time())
        if rows is None:
            raise BettingCogError('That market is no longer in a settled state.')
        label = self._pick_label(market, outcome)
        adjusted = [r for r in rows if r[5] != 0]
        head = (f'{market.home_team} {home_score}–{away_score} {market.away_team}'
                if home_score is not None else f'winner: **{label}**')
        lines = [f'Corrected result: {head}']
        if adjusted:
            lines.append('')
            for user_id, pick, stake, odds, new_pay, delta in adjusted:
                sign = '+' if delta > 0 else ''
                lines.append(f'<@{user_id}> **{sign}{delta}** {_COIN}')
        else:
            lines.append('\nNo payouts changed.')
        embed = discord.Embed(
            title=f'🔧 Correction — {market.home_team} vs {market.away_team}',
            description='\n'.join(lines), color=0xe67e22)
        await ctx.send(embed=embed,
                       allowed_mentions=_no_mentions())
        logger.info('Corrected market %s → %s by %s',
                    market.market_id, outcome, ctx.author.id)

    @bet.command(name='grant', brief='Give a user coins (admin)',
                 usage='@user <amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def grant(self, ctx, member: discord.Member, amount: int):
        if amount <= 0:
            raise BettingCogError('Amount must be a positive whole number.')
        new = cf_common.user_db.bet_adjust_balance(
            ctx.guild.id, member.id, amount, constants.BET_START_BALANCE,
            actor_id=ctx.author.id, action='admin_grant')
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Gave **{amount}** {_COIN} to `{name}`. New balance: **{new}** {_COIN}.'))

    @bet.command(name='take', brief='Remove coins from a user (admin)',
                 usage='@user <amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def take(self, ctx, member: discord.Member, amount: int):
        if amount <= 0:
            raise BettingCogError('Amount must be a positive whole number.')
        new = cf_common.user_db.bet_adjust_balance(
            ctx.guild.id, member.id, -amount, constants.BET_START_BALANCE,
            actor_id=ctx.author.id, action='admin_take')
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Took **{amount}** {_COIN} from `{name}`. New balance: **{new}** {_COIN}.'))

    @bet.command(name='setbalance', aliases=['setbal'],
                 brief='Set a user\'s balance (admin)', usage='@user <amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def setbalance(self, ctx, member: discord.Member, amount: int):
        if amount < 0:
            raise BettingCogError('Balance cannot be negative.')
        new = cf_common.user_db.bet_set_balance(
            ctx.guild.id, member.id, amount, constants.BET_START_BALANCE,
            actor_id=ctx.author.id, action='admin_setbalance')
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Set `{name}`\'s balance to **{new}** {_COIN}.'))

    @bet.command(name='pause', brief='Stop auto-opening new markets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def pause(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '1')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **paused** — no new markets will open. Existing markets '
            'still settle. `;bet resume` to re-enable.'))

    @bet.command(name='resume', aliases=['unpause'],
                 brief='Resume auto-opening markets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def resume(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '0')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **resumed** — markets will open ~2h before kickoff again.'))

    @bet.command(name='book', brief='Show all bets on the active market')
    async def book(self, ctx):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        wagers = cf_common.user_db.bet_get_wagers(market.market_id)
        if not wagers:
            await ctx.send(embed=discord_common.embed_neutral('No bets placed yet.'))
            return
        by_pick = {}
        for w in wagers:
            by_pick.setdefault(w.pick, []).append(w)
        lines = []
        pick_order = ['home']
        if self._market_allows_draw(market):
            pick_order.append('draw')
        pick_order.append('away')
        pick_order += [f'not_{pick}' for pick in pick_order]
        for pick in pick_order:
            ws = by_pick.get(pick) or []
            if not ws:
                continue
            odds = self._pick_odds(market, pick)
            total = sum(w.stake for w in ws)
            lines.append(f'__{self._pick_label(market, pick)} @ {odds:.2f}__ — '
                         f'{len(ws)} bet(s), {total} {_COIN} staked')
            for w in sorted(ws, key=lambda x: x.stake, reverse=True)[:15]:
                lines.append(f'• <@{w.user_id}> {w.stake} → '
                             f'{payout_amount(w.stake, odds)} {_COIN}')
        embed = discord.Embed(
            title=f'📒 Book — {market.home_team} vs {market.away_team}',
            description='\n'.join(lines), color=0x3498db)
        await ctx.send(embed=embed,
                       allowed_mentions=_no_mentions())

    @bet.command(name='odds', brief='Re-line a market before any bets (admin)',
                 usage='<home> <draw> <away>')
    @commands.has_role(constants.TLE_ADMIN)
    async def setodds(self, ctx, home: float, draw: float, away: float):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        if cf_common.user_db.bet_market_count_wagers(market.market_id) > 0:
            raise BettingCogError(
                'Bets are already placed — re-lining would be unfair to them. '
                '`;bet cancel` to refund and reopen instead.')
        if self._market_allows_draw(market):
            if not (home > 1 and draw > 1 and away > 1):
                raise BettingCogError('Odds must be decimal and greater than 1.0.')
            fair = normalized_market_odds(
                {'home': home, 'draw': draw, 'away': away}, knockout=False)
        else:
            if not (home > 1 and away > 1):
                raise BettingCogError('Home/away odds must be decimal and greater than 1.0.')
            fair = normalized_market_odds(
                {'home': home, 'draw': 0.0, 'away': away}, knockout=True)
        cf_common.user_db.bet_market_set_odds(
            market.market_id, fair['home'], fair['draw'], fair['away'])
        await ctx.send(embed=discord_common.embed_success(
            f'Odds re-lined: **1** {fair["home"]:.2f} · '
            + (f'**X** {fair["draw"]:.2f} · ' if fair['draw'] > 1 else '')
            + f'**2** {fair["away"]:.2f}.'))

    @bet.command(name='close', brief='Close betting early on the active market (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def close(self, ctx):
        market = self._find_market(ctx, require_unambiguous=True)
        if market is None:
            raise BettingCogError('No open market here.')
        if cf_common.user_db.bet_market_close_betting(market.market_id):
            fresh = cf_common.user_db.bet_market_get(market.market_id)
            await self._announce_betting_closed(fresh, automatic=False)
            await ctx.send(embed=discord_common.embed_success(
                'Betting **closed early** — no more bets. The market still '
                'settles at full time.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'Betting was already closed on this market.'))

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

    def _settlement_embed(self, market, outcome, home_score, away_score,
                          outcome_rows, source):
        label = self._pick_label(market, outcome)
        if home_score is not None:
            headline = (f'{market.home_team} **{home_score}–{away_score}** '
                        f'{market.away_team}')
        else:
            headline = f'Result: **{label}**'
        bettor_results = []
        total_staked = 0
        total_paid = 0
        for user_id, pick, stake, odds, pay in outcome_rows:
            stake = int(stake or 0)
            pay = int(pay or 0)
            net = pay - stake
            total_staked += stake
            total_paid += pay
            bettor_results.append((user_id, pick, stake, odds, pay, net))
        bettor_results.sort(key=lambda r: (r[5], r[4]), reverse=True)
        lines = [headline, '']
        if bettor_results:
            lines.append(f'**Winning pick: {label}**')
            if total_paid == 0:
                lines.append(f'Nobody backed **{label}**.')
            lines.append('')
            lines.append('**Bettor results (net):**')
            for user_id, pick, stake, odds, pay, net in bettor_results[:20]:
                sign = '+' if net > 0 else ''
                pick_label = self._pick_label(market, pick)
                odds_text = f' @ {odds:.2f}' if odds is not None else ''
                paid_text = f', paid {pay}' if pay else ''
                lines.append(
                    f'<@{user_id}> **{sign}{net}** {_COIN} — {pick_label} '
                    f'(staked {stake}{paid_text}{odds_text})')
            if len(bettor_results) > 20:
                lines.append(f'…and {len(bettor_results) - 20} more.')
            player_net = total_paid - total_staked
            net_sign = '+' if player_net > 0 else ''
            lines.append(
                f'\nTotal staked: **{total_staked}** {_COIN} · '
                f'paid out: **{total_paid}** {_COIN} · '
                f'player net: **{net_sign}{player_net}** {_COIN}.')
        else:
            lines.append(f'**Winning pick: {label}**')
            lines.append('No bets were placed.')
        tag = 'auto-settled from final score' if source == 'auto' \
            else 'settled by a moderator'
        embed = discord.Embed(
            title=f'✅ {market.home_team} vs {market.away_team} — final',
            description='\n'.join(lines), color=0x2ecc71)
        embed.set_footer(text=tag)
        return embed

    # ── Engine: precise per-fixture open timers + coarse safety net ────

    @tasks.task_spec(name='BetSafetyNet',
                     waiter=tasks.Waiter.fixed_delay(_SAFETY_NET_INTERVAL))
    async def _safety_net_task(self, _):
        # Backstop only: arm timers for newly-listed fixtures and catch any
        # game a missed timer / restart left inside the window. The on-time
        # opening itself is done by the per-fixture timers, not this sweep.
        if cf_common.user_db is None:
            return
        try:
            await self._refresh_schedule()
        except Exception:
            logger.warning('bet schedule refresh failed', exc_info=True)
        try:
            await self._arm_close_timers()
        except Exception:
            logger.warning('bet close timer refresh failed', exc_info=True)

    @tasks.task_spec(name='BetSettle',
                     waiter=tasks.Waiter.fixed_delay(_SETTLE_INTERVAL))
    async def _settle_task(self, _):
        if cf_common.user_db is None:
            return
        try:
            await self._settle_pending()
        except Exception:
            logger.warning('bet auto-settle pass failed', exc_info=True)

    def _configured_guilds(self):
        """{guild_id: channel_id} for guilds that ran `;prediction here` and are
        not paused. (Pause stops auto-OPENING; settlement still runs.)"""
        out = {}
        if not self.bot:
            return out
        for guild in self.bot.guilds:
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
        upcoming game, either arm a precise open timer (kickoff − 2h still in
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
                # Inside the 2h window already — open (or attach a thread) now.
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
        channel = self.bot.get_channel(int(channel_id)) if self.bot else None
        if channel is None:
            logger.warning('configured bet channel %s missing for guild %s',
                           channel_id, guild_id)
            return
        market_id = self._create_market(guild_id, channel_id, event)
        if market_id is None:
            logger.info('Auto-open skipped duplicate fixture %s (%s vs %s) '
                        'in guild %s',
                        event.get('event_id'), event.get('home_team'),
                        event.get('away_team'), guild_id)
            return
        try:
            msg = await channel.send(
                **self._open_announcement_kwargs(guild_id, event))
        except discord.HTTPException:
            logger.warning('failed to post auto market for %s in guild %s',
                           event.get('event_id'), guild_id, exc_info=True)
            cf_common.user_db.bet_void(guild_id, market_id, time.time())
            return
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
        market = cf_common.user_db.bet_market_get(market_id)
        await self._create_thread(market_id, msg, market)
        self._schedule_close(market)
        logger.info('Auto-opened market %s (%s vs %s) in guild %s',
                    market_id, event['home_team'], event['away_team'], guild_id)

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

    @discord_common.send_error_if(BettingCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Betting(bot))

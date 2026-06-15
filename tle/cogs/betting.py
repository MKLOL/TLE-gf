"""World Cup soccer betting minigame.

Fully automated and World Cup–only. A mod points the bot at a channel with
`;prediction here`; from then on the bot, on its own, ~2 hours before each
World Cup kickoff:
  1. reads the live 1X2 odds from The Odds API and **freezes** them,
  2. posts the market in the configured channel and opens a **thread**,
  3. members bet by replying in the thread (`home 100`, `away all`, `draw 25%`).
At kickoff betting closes; at full time the bot reads the final score and
auto-settles, paying winners stake × odds. Everyone starts at 1000 coins and
claims +100/day with `;bet daily`.

Commands (group `;bet`, alias `;prediction`):
  ;prediction here          set this channel for auto-opened markets       (mod)
  ;bet matches [query]      list upcoming World Cup matches with odds
  ;bet open <n|event_id>    manually open a market early                    (mod)
  ;bet home|draw|away <amt> stake on an outcome (also: reply in the thread)
  ;bet balance [@user]      show a wallet balance
  ;bet daily                claim the daily allowance
  ;bet leaderboard [profit] richest wallets / net profit
  ;bet mybet                show your bet on the active market
  ;bet pending              list markets stuck open past kickoff           (mod-ish)
  ;bet settle <home|draw|away|2-1>  settle the active market manually       (mod)
  ;bet cancel               cancel the active market, refund stakes         (mod)
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

_PICK_ALIASES = {
    'home': 'home', 'h': 'home', '1': 'home',
    'draw': 'draw', 'd': 'draw', 'x': 'draw', 'tie': 'draw',
    'away': 'away', 'a': 'away', '2': 'away',
}
_AMOUNT_WORDS = ('all', 'max', 'allin', 'all-in', 'everything')

_CHANNEL_CONFIG_KEY = 'bet_channel'
_PAUSED_CONFIG_KEY = 'bet_paused'


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


def extract_bet_tokens(content):
    """Cheap, market-agnostic split of a possible thread bet into
    (pick_text, amount_str), or None. Accepts '<pick…> <amount>' or
    '<amount> <pick…>' where amount is a single number/percent/'all' token and
    pick is 1–3 words (a team name or an outcome alias). The pick is resolved
    to an outcome later, against the market, via resolve_pick — keeping this
    off the DB for ordinary chatter."""
    if not content:
        return None
    tokens = content.strip().split()
    if not 2 <= len(tokens) <= 4:
        return None
    if _looks_like_amount(tokens[-1]):
        return (' '.join(tokens[:-1]), tokens[-1])
    if _looks_like_amount(tokens[0]):
        return (' '.join(tokens[1:]), tokens[0])
    return None


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


def _api_key():
    return getattr(constants, 'ODDS_API_KEY', None)


def _football_data_key():
    return getattr(constants, 'FOOTBALL_DATA_API_KEY', None)


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
        # event_id -> asyncio.Task: precise per-fixture "open at kickoff − 2h"
        # timers (see rpoll's _scheduled_timers).
        self._open_timers = {}

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
        self._safety_net_task.start()
        self._settle_task.start()

    async def cog_unload(self):
        await self._safety_net_task.stop()
        await self._settle_task.stop()
        for task in list(self._open_timers.values()):
            if not task.done():
                task.cancel()
        self._open_timers.clear()

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
                'Live odds are not configured (no `ODDS_API_KEY`). A mod can '
                'still settle markets manually with `;bet settle`.')
        try:
            events = await odds_api.fetch_h2h(
                api_key, [odds_api.WORLD_CUP_SPORT_KEY])
        except odds_api.OddsApiError as e:
            logger.warning('World Cup odds fetch failed: %s', e)
            raise BettingCogError(f'Could not fetch World Cup odds: {e}')
        self._wc_events = events
        self._wc_fetched_at = now
        return events

    def _pick_label(self, market, pick):
        return {'home': market.home_team, 'draw': 'Draw',
                'away': market.away_team}[pick]

    def _pick_odds(self, market, pick):
        return {'home': market.odds_home, 'draw': market.odds_draw,
                'away': market.odds_away}[pick]

    def _find_market(self, ctx):
        """The open market relevant to where the command was run: the betting
        thread if we're in one, else the channel's market."""
        m = cf_common.user_db.bet_market_get_active_by_thread(
            ctx.guild.id, ctx.channel.id)
        if m is not None:
            return m
        return cf_common.user_db.bet_market_get_active(ctx.guild.id, ctx.channel.id)

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

    def _market_embed(self, market):
        kickoff = int(market.commence_time)
        open_now = market.status == 'open' and time.time() < market.commence_time
        lines = [
            f'**1** · {market.home_team} win — **{market.odds_home:.2f}**',
            f'**X** · Draw — **{market.odds_draw:.2f}**',
            f'**2** · {market.away_team} win — **{market.odds_away:.2f}**',
            '',
            f'Kickoff: <t:{kickoff}:F> (<t:{kickoff}:R>)',
        ]
        if open_now:
            lines.append('\n👇 **Place your bets in the thread below** — '
                         'betting closes at kickoff.')
            color = 0x2ecc71
        elif market.status == 'open':
            lines.append('\n⏳ Kickoff passed — betting closed, awaiting result.')
            color = 0xf1c40f
        else:
            color = 0x95a5a6
        embed = discord.Embed(
            title=f'⚽ {market.home_team} vs {market.away_team}',
            description='\n'.join(lines), color=color)
        pool = cf_common.user_db.bet_pool(market.market_id)
        if pool:
            summary = ' · '.join(
                f'{self._pick_label(market, p.pick)}: {p.cnt} ({p.total} {_COIN})'
                for p in pool)
            embed.add_field(name='Action so far', value=summary, inline=False)
        return embed

    def _thread_intro_embed(self, market):
        kickoff = int(market.commence_time)
        desc = (
            'Reply in this thread to bet — use the **country name** or '
            'home/draw/away:\n'
            f'• `{market.home_team} 100` — back {market.home_team}\n'
            '• `draw 50` (or `tie`) — back a draw\n'
            f'• `{market.away_team} all` — back {market.away_team} (also `25%`)\n\n'
            f'Odds (frozen): **1** {market.odds_home:.2f} · '
            f'**X** {market.odds_draw:.2f} · **2** {market.odds_away:.2f}\n'
            f'Returns = stake × odds. Re-bet before kickoff to change it.\n'
            f'⏱️ **Betting closes <t:{kickoff}:R>**')
        return discord.Embed(title='🎟️ Place your bets', description=desc,
                             color=0x2ecc71)

    def _thread_name(self, market):
        name = f'⚽ {market.home_team} vs {market.away_team} — bets'
        return name[:100]

    def _open_announce_embed(self, event):
        """The 'betting open' announcement, built from a raw odds event so it
        can be posted BEFORE the market row exists (send-first, so a failed
        send never orphans a market)."""
        o = event['odds']
        kickoff = int(event['commence_time'])
        lines = [
            f'**1** · {event["home_team"]} win — **{o["home"]:.2f}**',
            f'**X** · Draw — **{o["draw"]:.2f}**',
            f'**2** · {event["away_team"]} win — **{o["away"]:.2f}**',
            '',
            f'Kickoff: <t:{kickoff}:F>',
            # <t:..:R> renders as a live countdown on the client ("in 53 minutes").
            f'⏱️ **Betting closes <t:{kickoff}:R>**',
            '\n👇 **Place your bets in the thread below.**',
        ]
        return discord.Embed(
            title=f'⚽ {event["home_team"]} vs {event["away_team"]}',
            description='\n'.join(lines), color=0x2ecc71)

    # ── Group ──────────────────────────────────────────────────────────

    @commands.group(name='bet', aliases=['betting', 'prediction', 'pred'],
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
                    'A mod can run `;prediction here` to start auto-opening '
                    'World Cup markets in a channel')
            await ctx.send(embed=discord_common.embed_neutral(
                f'No open market here. You have **{balance}** {_COIN}.\n'
                f'{hint}. See `;help bet`.'))
            return
        embed = self._market_embed(market)
        embed.set_footer(text=f'Your balance: {balance} coins')
        await ctx.send(embed=embed)

    @bet.command(name='here',
                 brief='Set this channel for auto-opened World Cup markets (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
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
            lines.append(
                f'**{i}.** {e["home_team"]} vs {e["away_team"]} — <t:{ko}:R>\n'
                f'    1 **{o["home"]:.2f}** · X **{o["draw"]:.2f}** · '
                f'2 **{o["away"]:.2f}**')
        embed = discord.Embed(title='⚽ Upcoming World Cup matches',
                              description='\n'.join(lines), color=0x3498db)
        embed.set_footer(text='Auto-opens ~2h before kickoff · '
                              'mods: ;bet open <number> to open early')
        await ctx.send(embed=embed)

    @bet.command(name='open', brief='Manually open a market early (mod)',
                 usage='<number from ;bet matches | event_id>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
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

        # Send-first: only persist the market once the announcement lands, so a
        # failed send never leaves an orphan market with no Discord presence.
        msg = await ctx.send(embed=self._open_announce_embed(event))
        market_id = self._create_market(ctx.guild.id, ctx.channel.id, event)
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
        market = cf_common.user_db.bet_market_get(market_id)
        thread = await self._create_thread(market_id, msg, market)
        if thread is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Could not create a betting thread (missing "Create Public '
                'Threads" permission?). Bets can still be placed here with '
                '`;bet home/draw/away <amount>`.'))
        logger.info('Manually opened market %s (%s vs %s) in guild %s',
                    market_id, event['home_team'], event['away_team'], ctx.guild.id)

    # ── Market creation (shared by manual + auto) ──────────────────────

    def _create_market(self, guild_id, channel_id, event):
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
            await thread.send(embed=self._thread_intro_embed(market))
        except discord.HTTPException:
            pass
        return thread

    # ── Placing bets ───────────────────────────────────────────────────

    async def _execute_bet(self, guild_id, market, user, pick, amount_str):
        """Core bet placement. Returns (status, data):
          'closed'       — kickoff passed
          'invalid'      — amount didn't parse / below minimum
          'insufficient' — not enough balance (data={'balance': N})
          'ok'           — placed (data has stake/odds/label/potential/balance)
        """
        if time.time() >= market.commence_time or market.bets_closed:
            return ('closed', None)
        balance = cf_common.user_db.bet_ensure_wallet(
            guild_id, user.id, constants.BET_START_BALANCE)
        stake = parse_amount(amount_str, balance, constants.BET_MIN_STAKE)
        if stake is None:
            return ('invalid', None)
        if stake > balance:
            return ('insufficient', {'balance': balance})
        odds = self._pick_odds(market, pick)
        ok, reason, new_balance = cf_common.user_db.bet_place(
            guild_id, market.market_id, user.id, pick, stake,
            time.time(), constants.BET_START_BALANCE)
        if not ok:
            return ('insufficient', {'balance': balance})
        return ('ok', {
            'stake': stake, 'odds': odds, 'pick': pick,
            'label': self._pick_label(market, pick),
            'potential': payout_amount(stake, odds), 'balance': new_balance})

    async def _place(self, ctx, pick, amount_str):
        market = self._find_market(ctx)
        if market is None:
            raise BettingCogError(
                'No open market here. Bets are placed in the match thread the '
                'bot opens ~2h before kickoff.')
        status, data = await self._execute_bet(
            ctx.guild.id, market, ctx.author, pick, amount_str)
        if status == 'closed':
            raise BettingCogError('Betting is closed — kickoff has passed.')
        if status == 'invalid':
            raise BettingCogError(
                f'Invalid amount. Use a whole number (min {constants.BET_MIN_STAKE}), '
                'a percentage like `50%`, or `all`.')
        if status == 'insufficient':
            raise BettingCogError(
                f'You only have **{data["balance"]}** {_COIN}. Try `;bet daily`.')
        await ctx.send(embed=discord_common.embed_success(
            f'Bet placed: **{data["stake"]}** {_COIN} on **{data["label"]}** @ '
            f'**{data["odds"]:.2f}** — returns **{data["potential"]}** {_COIN} '
            f'if it hits.\nBalance: **{data["balance"]}** {_COIN}.'))

    @bet.command(name='home', aliases=['1'], brief='Bet on the home win',
                 usage='<amount | 50% | all>')
    async def bet_home(self, ctx, amount: str):
        await self._place(ctx, 'home', amount)

    @bet.command(name='draw', aliases=['x', 'tie'], brief='Bet on a draw',
                 usage='<amount | 50% | all>')
    async def bet_draw(self, ctx, amount: str):
        await self._place(ctx, 'draw', amount)

    @bet.command(name='away', aliases=['2'], brief='Bet on the away win',
                 usage='<amount | 50% | all>')
    async def bet_away(self, ctx, amount: str):
        await self._place(ctx, 'away', amount)

    @bet.command(name='mybet', aliases=['mybets'], brief='Show your active bet')
    async def mybet(self, ctx):
        market = self._find_market(ctx)
        if market is None:
            raise BettingCogError('No open market here.')
        wager = cf_common.user_db.bet_get_wager(market.market_id, ctx.author.id)
        if wager is None:
            await ctx.send(embed=discord_common.embed_neutral(
                "You haven't bet on this match yet."))
            return
        label = self._pick_label(market, wager.pick)
        odds = self._pick_odds(market, wager.pick)  # frozen on the market
        potential = payout_amount(wager.stake, odds)
        await ctx.send(embed=discord_common.embed_neutral(
            f'Your bet: **{wager.stake}** {_COIN} on **{label}** @ '
            f'**{odds:.2f}** → returns **{potential}** {_COIN}.'))

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
        if content.startswith(discord_common._BOT_PREFIX):
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
        pick = resolve_pick(pick_text, market.home_team, market.away_team)
        if pick is None:
            return  # not a recognizable team/outcome — ignore (avoid chat noise)
        try:
            status, data = await self._execute_bet(
                message.guild.id, market, message.author, pick, amount_str)
        except Exception:
            logger.warning('thread bet failed in market %s', market.market_id,
                           exc_info=True)
            return
        if status == 'ok':
            await self._react(message, '✅')
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

    # ── Settle / cancel / pending (mod) ────────────────────────────────

    @bet.command(name='settle', brief='Settle the active market manually (mod)',
                 usage='<home|draw|away|2-1>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def settle(self, ctx, *, result: str):
        market = self._find_market(ctx)
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
                 brief='Cancel the active market and refund (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def cancel(self, ctx):
        market = self._find_market(ctx)
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
        escrowed until a mod settles (`;bet settle`) or cancels (`;bet cancel`).
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
            + '\n\nA mod can `;bet settle <home|draw|away|2-1>` or `;bet cancel` '
            'in each market\'s channel/thread.',
            color=0xf1c40f)
        await ctx.send(embed=embed,
                       allowed_mentions=discord.AllowedMentions.none())

    @bet.command(name='correct', aliases=['fix', 'resettle'],
                 brief='Fix a wrongly-settled result (mod)',
                 usage='<home|draw|away|2-1|team>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
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
                       allowed_mentions=discord.AllowedMentions.none())
        logger.info('Corrected market %s → %s by %s',
                    market.market_id, outcome, ctx.author.id)

    @bet.command(name='grant', brief='Give a user coins (mod)',
                 usage='@user <amount>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def grant(self, ctx, member: discord.Member, amount: int):
        if amount <= 0:
            raise BettingCogError('Amount must be a positive whole number.')
        new = cf_common.user_db.bet_adjust_balance(
            ctx.guild.id, member.id, amount, constants.BET_START_BALANCE)
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Gave **{amount}** {_COIN} to `{name}`. New balance: **{new}** {_COIN}.'))

    @bet.command(name='take', brief='Remove coins from a user (mod)',
                 usage='@user <amount>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def take(self, ctx, member: discord.Member, amount: int):
        if amount <= 0:
            raise BettingCogError('Amount must be a positive whole number.')
        new = cf_common.user_db.bet_adjust_balance(
            ctx.guild.id, member.id, -amount, constants.BET_START_BALANCE)
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Took **{amount}** {_COIN} from `{name}`. New balance: **{new}** {_COIN}.'))

    @bet.command(name='setbalance', aliases=['setbal'],
                 brief='Set a user\'s balance (mod)', usage='@user <amount>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def setbalance(self, ctx, member: discord.Member, amount: int):
        if amount < 0:
            raise BettingCogError('Balance cannot be negative.')
        new = cf_common.user_db.bet_set_balance(
            ctx.guild.id, member.id, amount, constants.BET_START_BALANCE)
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Set `{name}`\'s balance to **{new}** {_COIN}.'))

    @bet.command(name='pause', brief='Stop auto-opening new markets (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def pause(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '1')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **paused** — no new markets will open. Existing markets '
            'still settle. `;bet resume` to re-enable.'))

    @bet.command(name='resume', aliases=['unpause'],
                 brief='Resume auto-opening markets (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def resume(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '0')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **resumed** — markets will open ~2h before kickoff again.'))

    @bet.command(name='book', brief='Show all bets on the active market (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def book(self, ctx):
        market = self._find_market(ctx)
        if market is None:
            raise BettingCogError('No open market here.')
        wagers = cf_common.user_db.bet_get_wagers(market.market_id)
        if not wagers:
            await ctx.send(embed=discord_common.embed_neutral('No bets placed yet.'))
            return
        by_pick = {'home': [], 'draw': [], 'away': []}
        for w in wagers:
            by_pick.setdefault(w.pick, []).append(w)
        lines = []
        for pick in ('home', 'draw', 'away'):
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
                       allowed_mentions=discord.AllowedMentions.none())

    @bet.command(name='odds', brief='Re-line a market before any bets (mod)',
                 usage='<home> <draw> <away>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def setodds(self, ctx, home: float, draw: float, away: float):
        market = self._find_market(ctx)
        if market is None:
            raise BettingCogError('No open market here.')
        if cf_common.user_db.bet_market_count_wagers(market.market_id) > 0:
            raise BettingCogError(
                'Bets are already placed — re-lining would be unfair to them. '
                '`;bet cancel` to refund and reopen instead.')
        if not (home > 1 and draw > 1 and away > 1):
            raise BettingCogError('Odds must be decimal and greater than 1.0.')
        cf_common.user_db.bet_market_set_odds(market.market_id, home, draw, away)
        await ctx.send(embed=discord_common.embed_success(
            f'Odds re-lined: **1** {home:.2f} · **X** {draw:.2f} · '
            f'**2** {away:.2f}.'))

    @bet.command(name='close', brief='Close betting early on the active market (mod)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def close(self, ctx):
        market = self._find_market(ctx)
        if market is None:
            raise BettingCogError('No open market here.')
        if cf_common.user_db.bet_market_close_betting(market.market_id):
            await ctx.send(embed=discord_common.embed_success(
                'Betting **closed early** — no more bets. The market still '
                'settles at full time.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'Betting was already closed on this market.'))

    async def _do_settle(self, market, outcome, home_score, away_score, *, source):
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
        # Announce in the parent channel for visibility, and the thread (where
        # bettors are watching), then archive the thread. Winner mentions in
        # the embed don't ping, but pin that down explicitly.
        for cid in self._announce_targets(market):
            channel = self.bot.get_channel(int(cid)) if self.bot else None
            if channel is not None:
                try:
                    await channel.send(
                        embed=embed,
                        allowed_mentions=discord.AllowedMentions.none())
                except discord.HTTPException:
                    logger.warning('could not post settlement to %s', cid)
        await self._archive_thread(market)
        logger.info('Settled bet market %s (%s) source=%s winners=%d',
                    market.market_id, outcome, source,
                    sum(1 for r in outcome_rows if r[4] > 0))

    def _announce_targets(self, market):
        targets = [market.channel_id]
        if market.thread_id and market.thread_id != market.channel_id:
            targets.append(market.thread_id)
        return targets

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
        winners = sorted((r for r in outcome_rows if r[4] > 0),
                         key=lambda r: r[4], reverse=True)
        lines = [headline, '']
        if winners:
            lines.append(f'**Winning pick: {label}**')
            for user_id, pick, stake, odds, pay in winners[:20]:
                lines.append(f'🏆 <@{user_id}> +**{pay}** {_COIN} '
                             f'(staked {stake} @ {odds:.2f})')
            if len(winners) > 20:
                lines.append(f'…and {len(winners) - 20} more.')
            total_paid = sum(r[4] for r in winners)
            lines.append(f'\nTotal paid out: **{total_paid}** {_COIN}.')
        else:
            if outcome_rows:
                lines.append(f'Nobody backed **{label}** — the house keeps '
                             f'**{sum(r[2] for r in outcome_rows)}** {_COIN}. 😈')
            else:
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
                self._schedule_open(event['event_id'], event['commence_time'])
            else:
                # Inside the 2h window already — open (or attach a thread) now.
                await self._fire_open(event['event_id'])

    def _schedule_open(self, event_id, commence_time):
        """Arm a precise asyncio timer to open this fixture at kickoff − lead.
        Skips if a live timer already exists (avoid churn on every refresh)."""
        existing = self._open_timers.get(event_id)
        if existing is not None and not existing.done():
            return
        delay = seconds_until_open(
            commence_time, constants.BET_OPEN_LEAD_SECONDS, time.time())
        self._open_timers[event_id] = asyncio.create_task(
            self._open_timer(event_id, delay))

    async def _open_timer(self, event_id, delay):
        """Sleep until the exact open moment, then open the market."""
        try:
            await asyncio.sleep(delay)
            await self._fire_open(event_id)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning('open timer failed for %s', event_id, exc_info=True)
        finally:
            self._open_timers.pop(event_id, None)

    async def _fire_open(self, event_id):
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
            market = cf_common.user_db.bet_market_get_open_for_event(
                guild_id, event_id)
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
        event = next((e for e in events if e['event_id'] == event_id), None)
        if event is None or event['commence_time'] <= time.time():
            # Vanished from the feed, or kicked off while we fetched — don't
            # open a market/thread for a game you can't bet on.
            return
        for guild_id, channel_id in configured.items():
            if cf_common.user_db.bet_market_get_open_for_event(
                    guild_id, event_id) is not None:
                continue
            try:
                await self._open_market_auto(guild_id, channel_id, event)
            except Exception:
                logger.warning('auto-open failed for %s in guild %s',
                               event_id, guild_id, exc_info=True)

    async def _open_market_auto(self, guild_id, channel_id, event):
        channel = self.bot.get_channel(int(channel_id)) if self.bot else None
        if channel is None:
            logger.warning('configured bet channel %s missing for guild %s',
                           channel_id, guild_id)
            return
        # Send-first: a failed announcement send must not orphan a market row
        # (which the watcher would then never recover). Persist only on success;
        # the next tick re-opens cleanly if this send fails.
        try:
            msg = await channel.send(embed=self._open_announce_embed(event))
        except discord.HTTPException:
            logger.warning('failed to post auto market for %s in guild %s',
                           event.get('event_id'), guild_id, exc_info=True)
            return
        market_id = self._create_market(guild_id, channel_id, event)
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
        market = cf_common.user_db.bet_market_get(market_id)
        await self._create_thread(market_id, msg, market)
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
            score = football_data.find_result(
                m.home_team, m.away_team, m.commence_time, fd_matches)
            if score is None:
                continue
            await self._settle_market_with_score(m, score[0], score[1])

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

    async def _settle_market_with_score(self, market, home_score, away_score):
        # Re-read in case a mod (or the other source) settled it already.
        fresh = cf_common.user_db.bet_market_get(market.market_id)
        if fresh is None or fresh.status != 'open':
            return
        outcome = outcome_from_score(home_score, away_score)
        try:
            await self._do_settle(market, outcome, home_score, away_score,
                                  source='auto')
        except Exception:
            logger.warning('failed to settle market %s', market.market_id,
                           exc_info=True)

    @discord_common.send_error_if(BettingCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Betting(bot))

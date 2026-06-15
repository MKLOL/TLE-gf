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
import logging
import time
from datetime import datetime, timezone

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import odds_api
from tle.util import paginator
from tle.util import tasks

logger = logging.getLogger(__name__)

_COIN = '🪙'
_LB_PER_PAGE = 15
_MATCH_LIST_LIMIT = 15
# Manual `;bet matches` reuses a fetch no older than this.
_MATCH_CACHE_MAX_AGE = 10 * 60
# Engine tick cadence (auto-open watcher + auto-settle poller share one task).
_ENGINE_INTERVAL = 15 * 60
# Adaptive odds-fetch freshness: refresh every tick when a game is approaching
# its open window, else just keep the schedule loosely fresh. Keeps API credits
# down — most of the day there is no game within the lead window.
_WC_TTL_ACTIVE = 15 * 60
_WC_TTL_IDLE = 3 * 3600

_PICK_ALIASES = {
    'home': 'home', 'h': 'home', '1': 'home',
    'draw': 'draw', 'd': 'draw', 'x': 'draw', 'tie': 'draw',
    'away': 'away', 'a': 'away', '2': 'away',
}
_AMOUNT_WORDS = ('all', 'max', 'allin', 'all-in', 'everything')

_CHANNEL_CONFIG_KEY = 'bet_channel'


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


def normalize_pick(text):
    """Resolve a pick token (home/draw/away and common aliases) or None."""
    if text is None:
        return None
    return _PICK_ALIASES.get(text.strip().lower())


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


def parse_bet_message(content):
    """Parse a free-form thread bet like 'home 100', '100 away', 'x 50%',
    'draw all' into (pick, amount_str), or None if it isn't clearly a bet.

    Strict: exactly two whitespace-separated tokens — one a pick token, the
    other a stake token — so ordinary chat in the thread is left alone.
    """
    if not content:
        return None
    tokens = content.strip().split()
    if len(tokens) != 2:
        return None
    first, second = tokens
    pick = normalize_pick(first)
    if pick is not None and _looks_like_amount(second):
        return (pick, second)
    pick = normalize_pick(second)
    if pick is not None and _looks_like_amount(first):
        return (pick, first)
    return None


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


def _is_mod(member):
    roles = getattr(member, 'roles', None) or []
    return any(r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
               for r in roles)


def _api_key():
    return getattr(constants, 'ODDS_API_KEY', None)


# ── Cog ────────────────────────────────────────────────────────────────────

class Betting(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # channel_id -> events shown by the last `;bet matches` (for `;bet open <n>`)
        self._match_cache = {}
        # Shared cache of the last World Cup odds fetch (schedule + frozen-able
        # odds), reused by the watcher and `;bet matches`.
        self._wc_events = None
        self._wc_fetched_at = None

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._engine_task.start()

    async def cog_unload(self):
        await self._engine_task.stop()

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
            'Reply in this thread to bet:\n'
            '• `home 100` — back the home win\n'
            '• `draw 50` — back a draw\n'
            '• `away all` — back the away win (also `25%`)\n\n'
            f'Odds (frozen): **1** {market.odds_home:.2f} · '
            f'**X** {market.odds_draw:.2f} · **2** {market.odds_away:.2f}\n'
            f'Returns = stake × odds. Re-bet before kickoff to change it.\n'
            f'Betting closes at kickoff (<t:{kickoff}:R>).')
        return discord.Embed(title='🎟️ Place your bets', description=desc,
                             color=0x2ecc71)

    def _thread_name(self, market):
        name = f'⚽ {market.home_team} vs {market.away_team} — bets'
        return name[:100]

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

        market_id = self._create_market(ctx.guild.id, ctx.channel.id, event)
        market = cf_common.user_db.bet_market_get(market_id)
        msg = await ctx.send(embed=self._market_embed(market))
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
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
        if time.time() >= market.commence_time:
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
            guild_id, market.market_id, user.id, pick, stake, odds,
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
        potential = payout_amount(wager.stake, wager.odds)
        await ctx.send(embed=discord_common.embed_neutral(
            f'Your bet: **{wager.stake}** {_COIN} on **{label}** @ '
            f'**{wager.odds:.2f}** → returns **{potential}** {_COIN}.'))

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
        parsed = parse_bet_message(content)
        if parsed is None:
            return
        market = cf_common.user_db.bet_market_get_active_by_thread(
            message.guild.id, message.channel.id)
        if market is None:
            return  # not a betting thread — ignored on purpose
        pick, amount_str = parsed
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
        parsed = parse_settle_arg(result)
        if parsed is None:
            raise BettingCogError(
                'Give the result as `home`, `draw`, `away`, or a scoreline '
                'like `2-1`.')
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

    # ── Engine: auto-open watcher + auto-settle poller ─────────────────

    @tasks.task_spec(name='BetEngine',
                     waiter=tasks.Waiter.fixed_delay(_ENGINE_INTERVAL))
    async def _engine_task(self, _):
        try:
            await self._watch_pending()
        except Exception:
            logger.warning('bet auto-open pass failed', exc_info=True)
        try:
            await self._settle_pending()
        except Exception:
            logger.warning('bet auto-settle pass failed', exc_info=True)

    def _configured_guilds(self):
        """{guild_id: channel_id} for guilds that ran `;prediction here`."""
        out = {}
        if not self.bot:
            return out
        for guild in self.bot.guilds:
            channel_id = cf_common.user_db.get_guild_config(
                guild.id, _CHANNEL_CONFIG_KEY)
            if channel_id:
                out[guild.id] = channel_id
        return out

    def _watch_max_age(self, now, configured):
        """How stale the cached odds may be before the watcher refetches.

        Forces a fresh fetch (0) when a configured guild has a game inside the
        open window with no market yet — we need current odds to freeze. Else
        refreshes often only while a game is approaching, and rarely otherwise.
        """
        if not self._wc_events:
            return 0
        lead = constants.BET_OPEN_LEAD_SECONDS
        for e in self._wc_events:
            if is_due(e['commence_time'], now, lead):
                for guild_id in configured:
                    if cf_common.user_db.bet_market_get_open_for_event(
                            guild_id, e['event_id']) is None:
                        return 0
        upcoming = [e['commence_time'] - now for e in self._wc_events
                    if e['commence_time'] > now]
        if upcoming and min(upcoming) <= lead + _ENGINE_INTERVAL:
            return _WC_TTL_ACTIVE
        return _WC_TTL_IDLE

    async def _watch_pending(self):
        """Auto-open markets for World Cup games entering the 2h window."""
        if not _api_key():
            return
        configured = self._configured_guilds()
        if not configured:
            return
        now = time.time()
        try:
            events = await self._ensure_wc_events(
                self._watch_max_age(now, configured))
        except BettingCogError:
            return
        lead = constants.BET_OPEN_LEAD_SECONDS
        due = [e for e in events if is_due(e['commence_time'], now, lead)]
        for guild_id, channel_id in configured.items():
            for event in due:
                try:
                    await self._auto_open_or_thread(guild_id, channel_id, event, now)
                except Exception:
                    logger.warning('auto-open failed for %s in guild %s',
                                   event.get('event_id'), guild_id, exc_info=True)

    async def _auto_open_or_thread(self, guild_id, channel_id, event, now):
        market = cf_common.user_db.bet_market_get_open_for_event(
            guild_id, event['event_id'])
        if market is None:
            await self._open_market_auto(guild_id, channel_id, event)
        elif not market.thread_id and market.commence_time > now:
            # Market exists but lost its thread (e.g. earlier perms hiccup) —
            # attach one so people can still bet, as long as it's pre-kickoff.
            await self._ensure_thread(market)

    async def _open_market_auto(self, guild_id, channel_id, event):
        channel = self.bot.get_channel(int(channel_id)) if self.bot else None
        if channel is None:
            logger.warning('configured bet channel %s missing for guild %s',
                           channel_id, guild_id)
            return
        market_id = self._create_market(guild_id, channel_id, event)
        market = cf_common.user_db.bet_market_get(market_id)
        try:
            msg = await channel.send(embed=self._market_embed(market))
        except discord.HTTPException:
            logger.warning('failed to post auto market %s', market_id,
                           exc_info=True)
            return
        cf_common.user_db.bet_market_set_message(market_id, msg.id)
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
                # Re-read in case a mod settled it manually since the work-list.
                fresh = cf_common.user_db.bet_market_get(m.market_id)
                if fresh is None or fresh.status != 'open':
                    continue
                outcome = outcome_from_score(s['home_score'], s['away_score'])
                try:
                    await self._do_settle(m, outcome, s['home_score'],
                                          s['away_score'], source='auto')
                except Exception:
                    logger.warning('failed to settle market %s', m.market_id,
                                   exc_info=True)

    @discord_common.send_error_if(BettingCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Betting(bot))

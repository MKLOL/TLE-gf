"""World Cup soccer betting minigame.

Fully automated and World Cup–only. An admin points the bot at a channel with
`;prediction here`; from then on the bot, on its own, ~6 hours before each
World Cup kickoff:
  1. reads the live 1X2 odds from The Odds API and **freezes** them,
  2. posts the market in the configured channel and opens a **thread**,
  3. members bet by replying in the thread (`home 100`, `away all`, `draw 25%`).
At kickoff betting closes; at full time the bot reads the final score and
auto-settles, paying winners stake × odds. Everyone starts at 1000 coins and
claims +100/day with `;bet daily`.

Core commands (group `;bet`, alias `;prediction`) shown in `;help bet`:
  ;bet home|draw|away <amt> stake on an outcome (also: reply in the thread)
  ;bet mybet / withdraw     show / remove your bets on the active market
  ;bet me                   show your betting summary
  ;bet balance [@user]      show a wallet balance
  ;bet daily                claim the daily allowance
  ;bet leaderboard [profit] richest wallets / net profit
  ;bet matches [query]      list upcoming World Cup matches with odds
  ;bet notify               toggle the configured notification role
  ;prediction here          set this channel for auto-opened markets       (admin)
  ;bet notifyrole [@role|off]  set/clear the ping role for open markets     (admin)
  ;bet settle <home|draw|away|2-1>  settle the active market manually       (admin)
  ;bet cancel               cancel the active market, refund stakes         (admin)
  ;bet grant @user <±amt>   give (or, negative, take) a user's coins        (admin)
  ;bet grantall <±amt>      grant/raise every wallet (negative reverts)     (admin)

To keep `;help bet` small, several niche/advanced subcommands are registered
with ``hidden=True`` — they still work and respond to `;help bet <name>`, they
just don't clutter the group listing: ``open``, ``not``, ``book``, ``pending``,
``correct``, ``setbalance``, ``transfer``, ``for``, ``history``, ``odds``,
``pause``, ``resume``, ``close`` and ``check``. ``grant`` absorbs the old ``take`` and
``grantall`` absorbs ``ungrantall`` via negative amounts; ``notifyrole off``
replaces the old ``clearnotifyrole``. ``here`` hides itself from the listing
once a channel is configured (see ``_bet_channel_is_set``).

The implementation is split across helper modules to keep every file under 500
lines: pure helpers in ``_betting_helpers``, presentation in ``_betting_format``,
market/bet/settle engine in ``_betting_engine``, the open/close scheduler in
``_betting_scheduler``, and the heavier subcommand bodies in
``_betting_commands`` / ``_betting_wallet_cmds``. This file keeps the cog
itself: the ``bet`` group and all ``@bet.command`` callbacks in one class body
(as discord.py requires), the message listener and the background task hooks.
"""
import asyncio
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks

from tle.cogs._betting_commands import BetCommandImplMixin
from tle.cogs._betting_engine import BetEngineMixin, BettingCogError
from tle.cogs._betting_format import BetFormatMixin
from tle.cogs._betting_remediation import BetRemediationMixin
from tle.cogs._betting_scheduler import BetSchedulerMixin
from tle.cogs._betting_wallet_cmds import BetWalletCmdImplMixin
# Re-export the pure helpers so `from tle.cogs.betting import <helper>` keeps
# working for callers and tests.
from tle.cogs._betting_helpers import (  # noqa: F401
    extract_bet_tokens, is_due, is_remove_amount, normalize_event,
    normalize_pick, normalized_market_odds, outcome_from_score, parse_amount,
    parse_settle_arg, payout_amount, pick_is_negative, pick_wins, positive_pick,
    rank_line, resolve_bet_pick, resolve_pick, seconds_until_open,
    unknown_subcommand_token,
    _COIN, _api_key, _bot_prefix, _football_data_key, _no_mentions,
    _role_mentions, _short_error, _utc_today,
)

logger = logging.getLogger(__name__)

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

_CHANNEL_CONFIG_KEY = 'bet_channel'
_PAUSED_CONFIG_KEY = 'bet_paused'
_NOTIFY_ROLE_CONFIG_KEY = 'bet_notify_role'


def _bet_channel_is_set(ctx):
    """Help predicate: hide `;bet here` from `;help bet` once a channel is
    already configured (setup is done — only an unconfigured guild needs it)."""
    try:
        return bool(ctx and ctx.guild and cf_common.user_db is not None
                    and cf_common.user_db.get_guild_config(
                        ctx.guild.id, _CHANNEL_CONFIG_KEY))
    except Exception:
        return False


# ── Cog ────────────────────────────────────────────────────────────────────

class Betting(BetWalletCmdImplMixin, BetCommandImplMixin, BetFormatMixin,
              BetEngineMixin, BetSchedulerMixin, BetRemediationMixin,
              commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # channel_id -> events shown by the last `;bet matches` (for `;bet open <n>`)
        self._match_cache = {}
        # Shared cache of the last World Cup odds fetch (schedule + frozen-able
        # odds), reused by the scheduler, open timers and `;bet matches`.
        self._wc_events = None
        self._wc_fetched_at = None
        # Cache of the football-data fixture list, which carries each match's
        # tournament stage (group vs knockout) — used to shape new markets.
        self._fd_matches = None
        self._fd_fetched_at = None
        # fixture_key -> asyncio.Task: precise per-fixture "open at kickoff − 6h"
        # timers. Provider event ids can drift, so timers use canonical fixtures.
        self._open_timers = {}
        # market_id -> asyncio.Task: edit/announce exactly when betting closes.
        self._close_timers = {}
        # market_id -> asyncio.Task: coalesced thread intro pool refresh.
        self._pool_refresh_timers = {}
        # market_id -> (outcome, home_score, away_score) or None: the
        # beyond-regulation football-data result seen last poll. Such games settle
        # once two consecutive polls agree, filtering the feed's transient
        # (sometimes wrong) shootout readings. In-memory: a restart just re-waits
        # one round, which is safe.
        self._fd_pending_confirm = {}

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
        await self._run_draw_refixture()  # one-time: fix mislabelled no-draw markets
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

    # ── Group ──────────────────────────────────────────────────────────

    @commands.group(name='bet',
                    aliases=['betting', 'prediction', 'pred', 'wager'],
                    brief='World Cup betting', invoke_without_command=True)
    async def bet(self, ctx):
        """Show the active market here and your balance."""
        # `invoke_without_command=True` routes `;bet <unknown>` here too. NOTE:
        # discord.py wipes `ctx.subcommand_passed` before this callback runs, so
        # we recover the attempted subcommand from the raw message instead. Any
        # leftover token means `;bet <unknown>` — error rather than silently
        # acting like a bare `;bet`.
        attempted = unknown_subcommand_token(ctx)
        if attempted:
            raise BettingCogError(
                f'`{discord.utils.escape_markdown(attempted)}` isn\'t a `;bet` '
                'command. See `;help bet` for the full list.')
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, ctx.author.id, self._bet_start_balance(ctx.guild.id))
        market = self._find_market(ctx)
        if market is None:
            configured = cf_common.user_db.get_guild_config(
                ctx.guild.id, _CHANNEL_CONFIG_KEY)
            hint = ('Markets auto-open ~6h before each World Cup kickoff'
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
                 brief='Set this channel for auto-opened World Cup markets (admin)',
                 extras={'help_hidden_when': _bet_channel_is_set})
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        """Designate this channel as where the bot auto-posts markets."""
        await self._cmd_here(ctx)

    @bet.command(name='notifyrole', aliases=['pingrole'],
                 brief='Set/clear the role pinged when a market opens (admin)',
                 usage='[@role | off]')
    @commands.has_role(constants.TLE_ADMIN)
    async def notifyrole(self, ctx, role: str = None):
        await self._cmd_notifyrole(ctx, role)

    @bet.command(name='notify', aliases=['notifications'],
                 brief='Toggle betting notifications for yourself')
    async def notify(self, ctx):
        await self._cmd_notify(ctx)

    @bet.command(name='check', hidden=True,
                 brief='Check betting API keys without exposing secrets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def check(self, ctx):
        """Verify that the betting API keys are configured and usable."""
        await self._cmd_check(ctx)

    # ── Matches / manual open ──────────────────────────────────────────

    @bet.command(name='matches', aliases=['games', 'fixtures'],
                 brief='List upcoming World Cup matches with odds',
                 usage='[query]')
    async def matches(self, ctx, *, query: str = None):
        """List upcoming World Cup matches (optionally filtered by team)."""
        await self._cmd_matches(ctx, query)

    @bet.command(name='open', hidden=True,
                 brief='Manually open a market early (admin)',
                 usage='<number from ;bet matches | event_id>')
    @commands.has_role(constants.TLE_ADMIN)
    async def open_market(self, ctx, *, ref: str):
        """Open betting on a match from the last `;bet matches` list, early."""
        await self._cmd_open_market(ctx, ref)

    # ── Placing bets ───────────────────────────────────────────────────

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

    @bet.command(name='not', aliases=['no'], hidden=True,
                 brief='Bet that an outcome will not happen',
                 usage='<team|home|draw|away> <amount | 50% | all | 0 to remove>')
    async def bet_not(self, ctx, *, text: str):
        await self._cmd_bet_not(ctx, text)

    @bet.command(name='mybet', aliases=['mybets'], brief='Show your active bet')
    async def mybet(self, ctx):
        await self._cmd_mybet(ctx)

    @bet.command(name='withdraw', aliases=['clear', 'removeall', 'unbet', 'cancelbets'],
                 brief='Remove all your bets on the active match')
    async def withdraw(self, ctx):
        await self._withdraw_match(ctx)

    @bet.command(name='for', aliases=['forcebet', 'betfor', 'placefor'], hidden=True,
                 brief='Place a bet on behalf of a user (admin)',
                 usage='@user <home|draw|away|team> <amount | 50% | all | 0 to remove>')
    @commands.has_role(constants.TLE_ADMIN)
    async def bet_for(self, ctx, member: discord.Member, *, text: str):
        """Place (or, with `0`, remove) a bet for another member — for when
        they're away but have told you what they want to wager. Spends their
        own wallet; you're recorded as the actor in the wallet history."""
        await self._cmd_place_for(ctx, member, text)

    # ── Thread bet listener ────────────────────────────────────────────

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
        await self._cmd_balance(ctx, member)

    @bet.command(name='me', aliases=['profile', 'summary'],
                 brief='Show your betting summary')
    async def me(self, ctx):
        await self._cmd_me(ctx)

    @bet.command(name='daily', aliases=['claim'], brief='Claim the daily allowance')
    async def daily(self, ctx):
        await self._cmd_daily(ctx)

    @bet.command(name='transfer', aliases=['send', 'pay'], hidden=True,
                 brief='Move coins from one user to another (admin)',
                 usage='@from @to <amount|all|percent>')
    @commands.has_role(constants.TLE_ADMIN)
    async def transfer(self, ctx, from_member: discord.Member,
                       to_member: discord.Member, amount: str):
        await self._cmd_transfer(ctx, from_member, to_member, amount)

    @bet.command(name='history', aliases=['walletlog', 'ledger'], hidden=True,
                 brief='Show wallet audit history', usage='[@user]')
    async def history(self, ctx, member: discord.Member = None):
        await self._cmd_history(ctx, member)

    # ── Leaderboard ────────────────────────────────────────────────────

    @bet.command(name='leaderboard', aliases=['lb', 'board', 'top'],
                 brief='Wallet leaderboard (add `profit` for net profit)',
                 usage='[profit]')
    async def leaderboard(self, ctx, mode: str = None):
        await self._cmd_leaderboard(ctx, mode)

    # ── Settle / cancel / pending / correct ────────────────────────────

    @bet.command(name='settle', brief='Settle the active market manually (admin)',
                 usage='<home|draw|away|2-1>')
    @commands.has_role(constants.TLE_ADMIN)
    async def settle(self, ctx, *, result: str):
        await self._cmd_settle(ctx, result)

    @bet.command(name='cancel', aliases=['void'],
                 brief='Cancel the active market and refund (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def cancel(self, ctx):
        await self._cmd_cancel(ctx)

    @bet.command(name='pending', aliases=['stuck'], hidden=True,
                 brief='List open markets past kickoff awaiting a result')
    async def pending(self, ctx):
        """Show markets that have kicked off but not yet settled — e.g. a
        fixture the scores API never reported as completed. Stakes stay
        escrowed until an admin settles (`;bet settle`) or cancels (`;bet cancel`).
        """
        await self._cmd_pending(ctx)

    @bet.command(name='correct', aliases=['fix', 'resettle'], hidden=True,
                 brief='Fix a wrongly-settled result (admin)',
                 usage='<home|draw|away|2-1|team>')
    @commands.has_role(constants.TLE_ADMIN)
    async def correct(self, ctx, *, result: str):
        """Re-settle the most recently settled market here with the corrected
        result, reversing the wrong payouts and applying the right ones."""
        await self._cmd_correct(ctx, result)

    @bet.command(name='grant',
                 brief='Give a user coins; a negative amount takes (admin)',
                 usage='@user <amount | -amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def grant(self, ctx, member: discord.Member, amount: int):
        await self._cmd_grant(ctx, member, amount)

    @bet.command(name='setbalance', aliases=['setbal'], hidden=True,
                 brief='Set a user\'s balance (admin)', usage='@user <amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def setbalance(self, ctx, member: discord.Member, amount: int):
        await self._cmd_setbalance(ctx, member, amount)

    @bet.command(name='grantall', aliases=['granteveryone', 'allgrant'],
                 brief='Give every wallet coins; a negative amount reverts (admin)',
                 usage='<amount | -amount>')
    @commands.has_role(constants.TLE_ADMIN)
    async def grantall(self, ctx, amount: int):
        await self._cmd_grant_all(ctx, amount)

    @bet.command(name='pause', hidden=True,
                 brief='Stop auto-opening new markets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def pause(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '1')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **paused** — no new markets will open. Existing markets '
            'still settle. `;bet resume` to re-enable.'))

    @bet.command(name='resume', aliases=['unpause'], hidden=True,
                 brief='Resume auto-opening markets (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def resume(self, ctx):
        cf_common.user_db.set_guild_config(ctx.guild.id, _PAUSED_CONFIG_KEY, '0')
        await ctx.send(embed=discord_common.embed_success(
            'Auto-open **resumed** — markets will open ~6h before kickoff again.'))

    @bet.command(name='book', hidden=True,
                 brief='Show all bets on the active market')
    async def book(self, ctx):
        await self._cmd_book(ctx)

    @bet.command(name='odds', hidden=True,
                 brief='Re-line a market before any bets (admin)',
                 usage='<home> <draw> <away>')
    @commands.has_role(constants.TLE_ADMIN)
    async def setodds(self, ctx, home: float, draw: float, away: float):
        await self._cmd_setodds(ctx, home, draw, away)

    @bet.command(name='close', hidden=True,
                 brief='Close betting early on the active market (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def close(self, ctx):
        await self._cmd_close(ctx)

    # ── Background tasks ───────────────────────────────────────────────

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

    @discord_common.send_error_if(BettingCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Betting(bot))

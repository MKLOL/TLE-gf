"""Implementation bodies for the wallet-facing ``;bet`` subcommands.

Plain mixin (not a ``commands.Cog``); mixed into ``Betting``. Holds the
bet-placing, withdraw and leaderboard flows. The command callbacks live in
``betting.py`` and delegate here.
"""
import logging
import time

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import ranking
from tle.cogs._betting_engine import BettingCogError
from tle.cogs._betting_helpers import (
    extract_bet_tokens, parse_amount, payout_amount, rank_line,
    resolve_bet_pick, _COIN, _bot_prefix, _no_mentions,
    _utc_today,
)

logger = logging.getLogger(__name__)

_LB_PER_PAGE = 15


class BetWalletCmdImplMixin:
    # ── Placing / withdrawing bets ─────────────────────────────────────

    async def _place(self, ctx, pick, amount_str):
        market = self._find_market(ctx, require_unambiguous=True, bettable_only=True)
        if market is None:
            raise BettingCogError(
                'No open market here. Bets are placed in the match thread the '
                'bot opens ~6h before kickoff.')
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

    async def _cmd_bet_not(self, ctx, text):
        market = self._find_market(ctx, require_unambiguous=True, bettable_only=True)
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

    async def _cmd_place_for(self, ctx, member, text):
        """Admin: place (or, with `0`, remove) a bet on behalf of another
        member — for when they're away but have said what they want to wager.
        Spends the member's own wallet exactly as if they'd bet themselves; the
        admin is recorded as the actor in the wallet audit log."""
        if getattr(member, 'bot', False):
            raise BettingCogError('You cannot place a bet for a bot.')
        market = self._find_market(ctx, require_unambiguous=True, bettable_only=True)
        if market is None:
            raise BettingCogError(
                'No open market here. Run this in the match thread the bot opens '
                '~6h before kickoff.')
        tokens = extract_bet_tokens(text)
        if tokens is None:
            raise BettingCogError(
                'Use `;bet for @user <home|draw|away|team> <amount>`, e.g. '
                '`;bet for @alice home 100` (or `0` to remove their bet).')
        pick_text, amount = tokens
        pick = resolve_bet_pick(
            pick_text, market.home_team, market.away_team,
            allow_draw=self._market_allows_draw(market))
        if pick is None or not self._pick_allowed(market, pick):
            raise BettingCogError('That outcome is not available for this market.')
        status, data = await self._execute_bet(
            ctx.guild.id, market, member, pick, amount, actor_id=ctx.author.id)
        name = discord.utils.escape_markdown(member.display_name)
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
                f'`{name}` only has **{data["balance"]}** {_COIN}.')
        if status == 'missing':
            raise BettingCogError(
                f'`{name}` has no bet on **{data["label"]}** to remove.')
        if status == 'removed':
            self._schedule_pool_refresh(market.market_id)
            await ctx.send(embed=discord_common.embed_success(
                f'Removed `{name}`\'s bet on **{data["label"]}** and refunded '
                f'**{data["stake"]}** {_COIN}.\n'
                f'`{name}`\'s balance: **{data["balance"]}** {_COIN}.'))
            return
        if status == 'unchanged':
            await ctx.send(embed=discord_common.embed_neutral(
                f'`{name}` already has **{data["stake"]}** {_COIN} on '
                f'**{data["label"]}** @ **{data["odds"]:.2f}**.\n'
                f'`{name}`\'s balance: **{data["balance"]}** {_COIN}.'))
            return
        self._schedule_pool_refresh(market.market_id)
        await ctx.send(embed=discord_common.embed_success(
            f'Placed **{data["stake"]}** {_COIN} for `{name}` on '
            f'**{data["label"]}** @ **{data["odds"]:.2f}** — returns '
            f'**{data["potential"]}** {_COIN} if it hits.\n'
            f'`{name}`\'s balance: **{data["balance"]}** {_COIN}.'))

    async def _cmd_mybet(self, ctx):
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
        market = self._find_market(ctx, require_unambiguous=True, bettable_only=True)
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

    # ── Leaderboard ────────────────────────────────────────────────────

    async def _cmd_leaderboard(self, ctx, mode):
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
        # Standard competition ranking so users tied on the leaderboard value
        # share a rank instead of being split by the secondary sort.
        ranked = ranking.rank_items(rows, lambda r: getattr(r, value_attr))
        chunks = paginator.chunkify(ranked, _LB_PER_PAGE)
        pages = []
        for chunk in chunks:
            lines = []
            for rank, row in chunk:
                member = ctx.guild.get_member(int(row.user_id))
                name = member.mention if member is not None else f'`{row.user_id}`'
                lines.append(f'**#{rank}** {name} — {fmt(row)}')
            embed = discord.Embed(title=title, description='\n'.join(lines),
                                  color=0xf1c40f)
            pages.append((personal, embed))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    # ── Wallet accounts ────────────────────────────────────────────────

    def _bet_start_balance(self, guild_id):
        """The balance a first-time wallet in this guild is seeded with: the
        base start balance plus any ``;bet grantall`` bonus the guild has
        accumulated. Used everywhere a wallet may be lazily created so a member
        who joins the economy after a grantall starts at the raised amount."""
        return (constants.BET_START_BALANCE
                + cf_common.user_db.bet_get_start_bonus(guild_id))

    async def _cmd_balance(self, ctx, member):
        target = member or ctx.author
        bal = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, target.id, self._bet_start_balance(ctx.guild.id))
        who = 'You have' if target == ctx.author else \
            f'{discord.utils.escape_markdown(target.display_name)} has'
        await ctx.send(embed=discord_common.embed_neutral(
            f'{who} **{bal}** {_COIN}.'))

    async def _cmd_me(self, ctx):
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, ctx.author.id, self._bet_start_balance(ctx.guild.id))
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

    async def _cmd_daily(self, ctx):
        granted, balance, reason = cf_common.user_db.bet_claim_daily(
            ctx.guild.id, ctx.author.id, _utc_today(),
            constants.BET_DAILY_AMOUNT, self._bet_start_balance(ctx.guild.id))
        if granted:
            await ctx.send(embed=discord_common.embed_success(
                f'Claimed **+{constants.BET_DAILY_AMOUNT}** {_COIN}. '
                f'Balance: **{balance}** {_COIN}. Come back tomorrow!'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'Already claimed today. Balance: **{balance}** {_COIN}. '
                'Resets at 00:00 UTC.'))

    async def _cmd_transfer(self, ctx, from_member, to_member, amount):
        if from_member.id == to_member.id:
            raise BettingCogError('Source and destination must be different users.')
        if getattr(from_member, 'bot', False) or getattr(to_member, 'bot', False):
            raise BettingCogError('You cannot transfer coins to or from a bot.')
        balance = cf_common.user_db.bet_ensure_wallet(
            ctx.guild.id, from_member.id, self._bet_start_balance(ctx.guild.id))
        amount_value = parse_amount(amount, balance, 1)
        if amount_value is None:
            raise BettingCogError(
                'Invalid amount. Use a positive whole number, a percentage like '
                '`50%`, or `all`.')
        ok, reason, sender_balance, receiver_balance = cf_common.user_db.bet_transfer(
            ctx.guild.id, from_member.id, to_member.id, amount_value,
            self._bet_start_balance(ctx.guild.id), actor_id=ctx.author.id)
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

    async def _cmd_history(self, ctx, member):
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

    async def _cmd_grant(self, ctx, member, amount):
        """Give a member coins, or take them with a negative amount."""
        if amount == 0:
            raise BettingCogError(
                'Amount must be a non-zero whole number (negative takes coins).')
        new = cf_common.user_db.bet_adjust_balance(
            ctx.guild.id, member.id, amount, self._bet_start_balance(ctx.guild.id),
            actor_id=ctx.author.id,
            action='admin_grant' if amount > 0 else 'admin_take')
        name = discord.utils.escape_markdown(member.display_name)
        if amount > 0:
            await ctx.send(embed=discord_common.embed_success(
                f'Gave **{amount}** {_COIN} to `{name}`. '
                f'New balance: **{new}** {_COIN}.'))
        else:
            await ctx.send(embed=discord_common.embed_success(
                f'Took **{-amount}** {_COIN} from `{name}`. '
                f'New balance: **{new}** {_COIN}.'))

    async def _cmd_setbalance(self, ctx, member, amount):
        if amount < 0:
            raise BettingCogError('Balance cannot be negative.')
        new = cf_common.user_db.bet_set_balance(
            ctx.guild.id, member.id, amount, self._bet_start_balance(ctx.guild.id),
            actor_id=ctx.author.id, action='admin_setbalance')
        name = discord.utils.escape_markdown(member.display_name)
        await ctx.send(embed=discord_common.embed_success(
            f'Set `{name}`\'s balance to **{new}** {_COIN}.'))

    async def _cmd_grant_all(self, ctx, amount):
        """Shift every existing wallet by `amount` coins and move the guild's
        starting balance by the same, so members who join later start higher
        too. A negative amount claws the grant back (floored at 0)."""
        if amount == 0:
            raise BettingCogError(
                'Amount must be a non-zero whole number (negative reverts a grant).')
        changed, new_bonus = cf_common.user_db.bet_grant_all(
            ctx.guild.id, amount, actor_id=ctx.author.id)
        start = constants.BET_START_BALANCE + new_bonus
        prefix = _bot_prefix()
        if amount < 0:
            tail = (f'New members now start with **{start}** {_COIN}'
                    + (f' (base {constants.BET_START_BALANCE} + {new_bonus} '
                       'bonus).' if new_bonus else ', back to the base.'))
            await ctx.send(embed=discord_common.embed_success(
                f'Clawed back **{-amount}** {_COIN} from **{changed}** wallet(s). '
                f'{tail}'))
        else:
            await ctx.send(embed=discord_common.embed_success(
                f'Granted **+{amount}** {_COIN} to **{changed}** wallet(s). '
                f'New members now start with **{start}** {_COIN} '
                f'(base {constants.BET_START_BALANCE} + {new_bonus} bonus). '
                f'Undo with `{prefix}bet grantall -{amount}`.'))

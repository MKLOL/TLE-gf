"""Embed/label/notify-role formatting helpers for the betting cog.

Plain mixin (not a ``commands.Cog``); mixed into ``Betting`` so these read-only
presentation methods live off the command file.
"""
import time

import discord

from tle.util import codeforces_common as cf_common
from tle.cogs._betting_helpers import (
    _COIN, pick_is_negative, positive_pick, _odds_allow_draw, _role_mentions,
)

_NOTIFY_ROLE_CONFIG_KEY = 'bet_notify_role'


class BetFormatMixin:
    # ── Pick labels / odds ─────────────────────────────────────────────

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

    # ── Pool summary ───────────────────────────────────────────────────

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

    # ── Notify role ────────────────────────────────────────────────────

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
            'grantall': 'grant-all',
            'grantall_revert': 'grant-all revert',
            'mod_grant': 'mod grant',
            'mod_take': 'mod take',
            'mod_setbalance': 'mod set balance',
            'transfer_out': 'transfer sent',
            'transfer_in': 'transfer received',
            'steal_success': 'steal success',
            'steal_victim': 'stolen from',
            'steal_caught': 'caught stealing',
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
        elif row.action in ('steal_success', 'steal_caught') and row.note:
            note = f' · target <@{row.note}>'
        elif row.action == 'steal_victim' and row.note:
            note = f' · thief <@{row.note}>'
        else:
            note = f' · {discord.utils.escape_markdown(str(row.note))}' if row.note else ''
        label = labels.get(row.action, row.action.replace('_', ' '))
        return (f'<t:{int(row.created_at)}:R> — **{sign}{row.amount}** {_COIN} '
                f'({label}{actor}{market}{note}) → **{row.balance_after}**')

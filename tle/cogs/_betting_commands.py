"""Implementation bodies for the heavier ``;bet`` subcommands.

Plain mixin (not a ``commands.Cog``); mixed into ``Betting``. The command
*callbacks* (the ``@bet.command``-decorated functions) live in ``betting.py``
and stay in one class body; they delegate to these methods so each file stays
under 500 lines.
"""
import logging
import time

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import football_data
from tle.util import odds_api
from tle.cogs._betting_engine import BettingCogError
from tle.cogs._betting_helpers import (
    normalized_market_odds, payout_amount, _COIN, _api_key,
    _football_data_key, _no_mentions, _short_error,
)

logger = logging.getLogger(__name__)

_MATCH_LIST_LIMIT = 15
_MATCH_CACHE_MAX_AGE = 10 * 60

_CHANNEL_CONFIG_KEY = 'bet_channel'
_NOTIFY_ROLE_CONFIG_KEY = 'bet_notify_role'


class BetCommandImplMixin:
    # ── Setup / config ─────────────────────────────────────────────────

    async def _cmd_here(self, ctx):
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

    async def _cmd_notifyrole(self, ctx, role):
        # `role` arrives as a raw string from the command (or None). `off`/`none`
        # /`clear` removes the configured role; no arg shows the current one;
        # anything else resolves to a Role. Tests pass a Role object directly,
        # so a non-str value is taken as an already-resolved role.
        if isinstance(role, str):
            token = role.strip()
            if token.lower() in ('off', 'none', 'clear', 'remove'):
                cf_common.user_db.delete_guild_config(
                    ctx.guild.id, _NOTIFY_ROLE_CONFIG_KEY)
                await ctx.send(embed=discord_common.embed_success(
                    'Betting notification role cleared.'))
                return
            try:
                role = await commands.RoleConverter().convert(ctx, token)
            except commands.BadArgument:
                raise BettingCogError(
                    f'No role matching “{discord.utils.escape_markdown(token)}”. '
                    'Use `;bet notifyrole @role` to set one or '
                    '`;bet notifyrole off` to clear it.')
        if role is None:
            role_id = self._configured_notify_role_id(ctx.guild.id)
            if role_id is None:
                await ctx.send(embed=discord_common.embed_neutral(
                    'No betting notification role is configured. Set one with '
                    '`;bet notifyrole @role`.'))
            else:
                await ctx.send(embed=discord_common.embed_neutral(
                    f'Betting notification role: <@&{role_id}>. '
                    'Clear it with `;bet notifyrole off`.'))
            return
        self._validate_notify_role(ctx, role)
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _NOTIFY_ROLE_CONFIG_KEY, str(role.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Betting markets will ping {role.mention} when they open.'),
            allowed_mentions=_no_mentions())

    async def _cmd_notify(self, ctx):
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

    async def _cmd_check(self, ctx):
        lines = ['Betting API check:']
        api_key = _api_key()
        if not api_key:
            lines.append('❌ `ODDS_API_KEY` is not set.')
        else:
            try:
                sports, quota = await odds_api.fetch_sports(
                    api_key, with_quota=True)
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
                remaining = quota.get('remaining')
                if remaining is not None:
                    used = quota.get('used')
                    usage = f' ({used} used)' if used is not None else ''
                    lines.append(
                        f'📊 Odds API: **{remaining}** request(s) remaining '
                        f'this month{usage}.')

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
                     '(quota-free); the remaining count is read from its '
                     'response headers at no cost.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    # ── Matches / manual open ──────────────────────────────────────────

    async def _cmd_matches(self, ctx, query):
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
            if (o.get('draw') or 0) > 1:
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

    async def _cmd_open_market(self, ctx, ref):
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
        suppress_mention = cf_common.user_db.bet_market_has_earlier_open_at_kickoff(
            ctx.guild.id, ctx.channel.id, event['commence_time'], market_id)
        try:
            msg = await ctx.send(
                **self._open_announcement_kwargs(
                    ctx.guild.id, event, suppress_mention=suppress_mention))
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


    # ── Settle / cancel / pending / correct ────────────────────────────

    async def _cmd_settle(self, ctx, result):
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

    async def _cmd_cancel(self, ctx):
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

    async def _cmd_pending(self, ctx):
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
        await ctx.send(embed=embed, allowed_mentions=_no_mentions())

    async def _cmd_correct(self, ctx, result):
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
        await ctx.send(embed=embed, allowed_mentions=_no_mentions())
        logger.info('Corrected market %s → %s by %s',
                    market.market_id, outcome, ctx.author.id)

    # ── Book / odds / close ────────────────────────────────────────────

    async def _cmd_book(self, ctx):
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
        await ctx.send(embed=embed, allowed_mentions=_no_mentions())

    async def _cmd_setodds(self, ctx, home, draw, away):
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

    async def _cmd_close(self, ctx):
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

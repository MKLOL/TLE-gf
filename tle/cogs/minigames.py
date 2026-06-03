import asyncio
import datetime as dt
import html
import io
import logging
import time
from typing import Optional

import cairo
import discord
import gi
from discord import app_commands
from discord.ext import commands
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util import table

from tle.cogs._minigame_common import (
    compute_vs, compute_vs_matchups, compute_streak, compute_longest_streak, compute_top,
    pick_best_results, format_duration, normalize_puzzle_date, parse_date_args,
    resolve_scoring, strip_codeblock,
)
from tle.cogs._minigame_akari import AKARI_GAME, expected_puzzle_number
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_stats import (
    plot_akari_performance, plot_akari_rating,
    plot_akari_stats, plot_guessgame_stats,
)
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError
from tle.util.akari_rating import compute_ratings, rank_for_rating

logger = logging.getLogger(__name__)

_IMPORT_BATCH_SIZE = 500
_IMPORT_RATE_DELAY = 0.5
_AKARI_IMAGE_MAX_ROWS = 40
_AKARI_IMAGE_WIDTH = 900
_AKARI_IMAGE_MARGIN = 20
_AKARI_IMAGE_ROW_HEIGHT = 36
_AKARI_IMAGE_HEADER_SPACING = 1.25
_AKARI_IMAGE_COLUMN_MARGIN = 10
_AKARI_IMAGE_COLS = (54, 300, 260, 150, 96)
_AKARI_IMAGE_FONTS = [
    'Noto Sans',
    'Noto Sans CJK JP',
    'Noto Sans CJK SC',
    'Noto Sans CJK TC',
    'Noto Sans CJK HK',
    'Noto Sans CJK KR',
    # Keep this in sync with the Cairo/Pango renderers in handles/training.
    # extra/fonts.conf rejects Noto Color Emoji on old Cairo; fonts-color.conf
    # allows it only after startup verifies a compatible Cairo runtime.
    'Noto Color Emoji',
    'Noto Emoji',
]
_DISCORD_GRAY = (.212, .244, .247)
_TABLE_ROW_COLORS = ((0.95, 0.95, 0.95), (0.9, 0.9, 0.9))
_BLACK = (0, 0, 0)
_SMOKE_WHITE = (250, 250, 250)


class MinigameCogError(commands.CommandError):
    pass


class ChannelOrThread(commands.Converter):
    """Converter that finds text channels, threads, and archived threads.

    discord.py's built-in converters only search the guild cache, so
    archived threads (not in cache) can't be found by name or ID.
    This falls back to bot.fetch_channel() for IDs and mentions.
    """

    async def convert(self, ctx, argument):
        # Try the built-in converters first (handles mentions, cached channels/threads)
        for converter in (commands.TextChannelConverter, commands.ThreadConverter):
            try:
                return await converter().convert(ctx, argument)
            except commands.BadArgument:
                continue

        # Fall back to fetch_channel for raw IDs (handles archived threads)
        try:
            channel_id = int(argument.strip('<#>'))
        except ValueError:
            raise commands.BadArgument(f'Channel or thread "{argument}" not found.')
        try:
            return await ctx.bot.fetch_channel(channel_id)
        except discord.NotFound:
            raise commands.BadArgument(f'Channel or thread "{argument}" not found.')
        except discord.Forbidden:
            raise commands.BadArgument(f'I don\'t have access to channel "{argument}".')


class CaseInsensitiveMember(commands.MemberConverter):
    """MemberConverter with a case-insensitive fallback on name/display_name."""

    async def convert(self, ctx, argument):
        try:
            return await super().convert(ctx, argument)
        except commands.BadArgument:
            pass
        lowered = argument.lower()
        for member in ctx.guild.members:
            if member.name.lower() == lowered or member.display_name.lower() == lowered:
                return member
        raise commands.BadArgument(f'Member "{argument}" not found.')


def _safe_member_name(member):
    return discord.utils.escape_mentions(member.display_name)


# ── Slash command helpers ──────────────────────────────────────────────

_TIMEFRAME_CHOICES = [
    app_commands.Choice(name='This week', value='week'),
    app_commands.Choice(name='This month', value='month'),
    app_commands.Choice(name='This year', value='year'),
]

_MODE_CHOICES = [
    app_commands.Choice(name='Raw (time only)', value='raw'),
    app_commands.Choice(name='All puzzles', value='all'),
]


class _FollowupChannel:
    """Channel-like wrapper that sends via interaction followups.

    Lets code that reads ``ctx.channel.id`` / ``.mention`` or calls
    ``ctx.channel.send()`` (e.g. the paginator) work unchanged.
    """

    def __init__(self, interaction):
        self._interaction = interaction
        self.id = interaction.channel_id
        self.mention = f'<#{interaction.channel_id}>'

    async def send(self, content=None, *, embed=None, view=None,
                   delete_after=None, **kw):
        return await self._interaction.followup.send(
            content, embed=embed, view=view, wait=True, **kw)


class _SlashCtx:
    """Adapter that wraps a *deferred* ``Interaction`` to look like ``commands.Context``.

    Create this **after** calling ``interaction.response.defer()`` so that
    ``followup.send()`` works immediately.
    """

    def __init__(self, interaction):
        self.interaction = interaction
        self.guild = interaction.guild
        self.author = interaction.user
        self.channel = _FollowupChannel(interaction)
        self.bot = interaction.client
        self.message = type('_Msg', (), {'id': 0})()

    async def send(self, content=None, *, embed=None, **kw):
        return await self.interaction.followup.send(
            content, embed=embed, wait=True, **kw)

    async def send_help(self, command=None):
        pass


def _safe_user_name(guild, user_id):
    member = guild.get_member(int(user_id))
    if member is not None:
        return _safe_member_name(member)
    return f'user `{user_id}`'


def _safe_cf_handle(guild, user_id):
    if cf_common.user_db is None:
        return '-'
    handle = cf_common.user_db.get_handle(user_id, guild.id)
    return handle or '-'


def _format_score(score):
    return f'{score:.3f}'.rstrip('0').rstrip('.')


def _maybe_parse_puzzle_selector(arg):
    """Resolve a single ``;akari stats`` argument into a puzzle/day selector.

    Returns ``('puzzle', n)``, ``('day', date)``, or ``None`` (the caller then
    treats ``arg`` as a member/filter).

    An explicit ``#N`` or ``p=N`` prefix always means puzzle number ``N``. This
    is the unambiguous way to look up a puzzle whose number collides with a bare
    date format -- e.g. ``#1000`` once daily puzzle numbers reach four digits,
    since a bare ``1000`` parses as the year 1000. Bare numbers keep their
    historical meaning: length 4/6/8 digit strings are dates (year / month-year
    / day-month-year), anything else is a puzzle number.
    """
    if not arg:
        return None
    explicit = None
    if arg.startswith('#'):
        explicit = arg[1:]
    elif arg[:2].lower() == 'p=':
        explicit = arg[2:]
    if explicit is not None:
        return ('puzzle', int(explicit)) if explicit.isdigit() else None
    try:
        day_start = int(cf_common.parse_date(arg))
    except (cf_common.ParamParseError, ValueError, OverflowError):
        if arg.isdigit():
            return ('puzzle', int(arg))
        return None
    day = dt.datetime.fromtimestamp(day_start).date()
    return ('day', day)


def _format_akari_result_status(row):
    if row.is_perfect:
        return 'perfect'
    return f'{int(row.accuracy)}%'


def _sort_akari_puzzle_results(rows):
    return sorted(
        rows,
        key=lambda row: (
            -int(bool(row.is_perfect)),
            -int(getattr(row, 'accuracy', 0)),
            int(getattr(row, 'time_seconds', 0)),
            int(getattr(row, 'message_id', 0)),
        ),
    )


def _akari_puzzle_table_rows(guild, rows):
    return [
        (
            index,
            _safe_user_name(guild, row.user_id),
            _safe_cf_handle(guild, row.user_id),
            _format_akari_result_status(row),
            format_duration(row.time_seconds),
        )
        for index, row in enumerate(_sort_akari_puzzle_results(rows), start=1)
    ]


def _format_akari_puzzle_table(guild, rows):
    style = table.Style('{:>}  {:<}  {:<}  {:<}  {:>}')
    t = table.Table(style)
    t += table.Header('#', 'Name', 'Handle', 'Result', 'Time')
    t += table.Line()

    for row in _akari_puzzle_table_rows(guild, rows):
        t += table.Data(*row)
    return str(t)


def _get_akari_puzzle_table_image(table_rows, *, title=None, footer=None,
                                  header=('#', 'Name', 'Handle', 'Result', 'Time'),
                                  row_colors=None):
    title_height = _AKARI_IMAGE_ROW_HEIGHT if title is not None else 0
    footer_height = _AKARI_IMAGE_ROW_HEIGHT if footer is not None else 0
    height = int(
        (len(table_rows) + _AKARI_IMAGE_HEADER_SPACING) * _AKARI_IMAGE_ROW_HEIGHT
        + title_height + footer_height + 2 * _AKARI_IMAGE_MARGIN
    )

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, _AKARI_IMAGE_WIDTH, height)
    context = cairo.Context(surface)
    context.set_source_rgb(*_DISCORD_GRAY)
    context.rectangle(0, 0, _AKARI_IMAGE_WIDTH, height)
    context.fill()

    layout = PangoCairo.create_layout(context)
    layout.set_font_description(
        Pango.font_description_from_string(','.join(_AKARI_IMAGE_FONTS) + ' 18'))
    layout.set_ellipsize(Pango.EllipsizeMode.END)

    def draw_bg(y, color):
        context.set_source_rgb(*color)
        context.rectangle(0, y, _AKARI_IMAGE_WIDTH, _AKARI_IMAGE_ROW_HEIGHT)
        context.fill()

    def draw_cell(text, width, *, align=Pango.Alignment.LEFT, bold=False):
        text = html.escape(str(text))
        if bold:
            text = f'<b>{text}</b>'
        layout.set_width(max(1, int((width - _AKARI_IMAGE_COLUMN_MARGIN) * Pango.SCALE)))
        layout.set_alignment(align)
        layout.set_markup(text, -1)
        PangoCairo.show_layout(context, layout)
        context.rel_move_to(width, 0)

    def draw_line(text, y, color, *, bold=False):
        context.set_source_rgb(*(component / 255 for component in color))
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        draw_cell(
            text,
            _AKARI_IMAGE_WIDTH - 2 * _AKARI_IMAGE_MARGIN,
            bold=bold,
        )

    def draw_row(row, y, color, *, bold=False):
        context.set_source_rgb(*(component / 255 for component in color))
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        draw_cell(row[0], _AKARI_IMAGE_COLS[0], align=Pango.Alignment.RIGHT, bold=bold)
        draw_cell(row[1], _AKARI_IMAGE_COLS[1], bold=bold)
        draw_cell(row[2], _AKARI_IMAGE_COLS[2], bold=bold)
        draw_cell(row[3], _AKARI_IMAGE_COLS[3], bold=bold)
        draw_cell(row[4], _AKARI_IMAGE_COLS[4], align=Pango.Alignment.RIGHT, bold=bold)

    y = _AKARI_IMAGE_MARGIN
    if title is not None:
        draw_line(title, y, _SMOKE_WHITE, bold=True)
        y += _AKARI_IMAGE_ROW_HEIGHT

    draw_row(header, y, _SMOKE_WHITE, bold=True)
    y += int(_AKARI_IMAGE_ROW_HEIGHT * _AKARI_IMAGE_HEADER_SPACING)

    for i, row in enumerate(table_rows):
        draw_bg(y, _TABLE_ROW_COLORS[i % 2])
        # row_colors (when provided) gives the per-row text colour as a 0–255
        # RGB tuple; otherwise everything stays black like the puzzle tables.
        text_color = row_colors[i] if row_colors is not None else _BLACK
        draw_row(row, y, text_color)
        y += _AKARI_IMAGE_ROW_HEIGHT

    if footer is not None:
        draw_line(footer, y, _SMOKE_WHITE)

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename='akari-results.png')


def _get_akari_puzzle_table_image_file(guild, rows, title):
    rows = _sort_akari_puzzle_results(rows)
    displayed_rows = _akari_puzzle_table_rows(guild, rows[:_AKARI_IMAGE_MAX_ROWS])
    footer = None
    if len(rows) > len(displayed_rows):
        footer = f'Showing top {len(displayed_rows)} of {len(rows)} results'
    return _get_akari_puzzle_table_image(displayed_rows, title=title, footer=footer)


def _akari_rating_table_rows(guild, rating_rows, registrants, *, mark_registered=True):
    """Build display rows (#, Name[✓], Handle, Rating · Rank, Games) for the leaderboard.

    ``rating`` is rounded only here for display, and the rank abbreviation
    (N/P/S/E/CM/…) is appended so scanners see the tier without a separate
    column.  When ``mark_registered`` is True, a ``✓`` after the name marks
    users who opted in via ``;mg akari register``; pass False on a registered-only
    view (the marker is redundant when every row is opted in).
    """
    rows = []
    for index, row in enumerate(rating_rows, start=1):
        name = _safe_user_name(guild, row.user_id)
        if mark_registered and row.user_id in registrants:
            name = f'{name} \N{CHECK MARK}'
        rating = round(row.rating)
        rank = rank_for_rating(rating)
        rows.append((
            index,
            name,
            _safe_cf_handle(guild, row.user_id),
            f'{rating} · {rank.title_abbr}',
            str(row.games),
        ))
    return rows


def _akari_row_text_color(rating):
    """Per-row text colour for the rating leaderboard image.

    Uses the rank's ``color_embed`` (the darker integer variant) so the text
    stays legible on the light-gray alternating row backgrounds — the pastel
    ``color_graph`` shades are tuned for plot fills and would wash out here.
    """
    embed = rank_for_rating(round(rating)).color_embed
    return ((embed >> 16) & 0xFF, (embed >> 8) & 0xFF, embed & 0xFF)


def _get_akari_rating_table_image_file(guild, rating_rows, registrants,
                                       *, title='Daily Akari Ratings',
                                       mark_registered=True):
    displayed = rating_rows[:_AKARI_IMAGE_MAX_ROWS]
    table_rows = _akari_rating_table_rows(
        guild, displayed, registrants, mark_registered=mark_registered)
    row_colors = [_akari_row_text_color(row.rating) for row in displayed]
    footer = None
    if len(rating_rows) > len(table_rows):
        footer = f'Showing top {len(table_rows)} of {len(rating_rows)} rated players'
    return _get_akari_puzzle_table_image(
        table_rows, title=title, footer=footer,
        header=('#', 'Name', 'Handle', 'Rating', 'Games'),
        row_colors=row_colors)


# Same per-page count as ``;handles updates`` — embed descriptions cap at 4096
# chars so 15 contest lines (~80 chars each) leave plenty of headroom.
_AKARI_HISTORY_PER_PAGE = 15


def _format_akari_history_line(point):
    """One CF-style line of ``;mg akari history`` for one contest day.

    ``**#446** · 2026-06-03 · 🌟 1:34 · 1234 ─ **+12** → 1246 (CM) · perf 1289``

    The horizontal bar / right-arrow combo mirrors ``;handles updates``
    (handles.py:884-889), the canonical CF rating-change format in this
    codebase. Solo days are filtered out by the caller, so ``performance`` is
    guaranteed to be non-None here.
    """
    new_rating = round(point.rating)
    old_rating = round(point.rating - point.delta)
    delta = round(point.delta)
    rank_abbr = rank_for_rating(new_rating).title_abbr
    if point.is_perfect:
        result_str = f'\N{GLOWING STAR} {format_duration(point.time_seconds)}'
    else:
        result_str = f'{point.accuracy}% {format_duration(point.time_seconds)}'
    date_str = normalize_puzzle_date(point.puzzle_date).isoformat()
    return (
        f'**#{point.puzzle_number}** \N{MIDDLE DOT} {date_str} '
        f'\N{MIDDLE DOT} {result_str} '
        f'\N{MIDDLE DOT} {old_rating} \N{HORIZONTAL BAR} **{delta:+}** '
        f'\N{LONG RIGHTWARDS ARROW} {new_rating} ({rank_abbr}) '
        f'\N{MIDDLE DOT} perf {round(point.performance)}'
    )


def _format_akari_ban_line(guild, row):
    """One line of ``;mg akari bans``: who's banned, when, by whom, why.

    Example:
        ``• **Alice** \N{MIDDLE DOT} banned 2026-06-03 by **mod1** \N{MIDDLE DOT} spamming``
    """
    target = _safe_user_name(guild, row.user_id)
    banner = _safe_user_name(guild, row.banned_by)
    date_str = dt.datetime.fromtimestamp(row.banned_at).date().isoformat()
    reason_part = f' \N{MIDDLE DOT} {row.reason}' if row.reason else ''
    return (f'\N{BULLET} **{target}** \N{MIDDLE DOT} banned {date_str} '
            f'by **{banner}**{reason_part}')


class Minigames(commands.Cog):
    GAMES = {
        'akari': AKARI_GAME,
        'guessgame': GUESSGAME_GAME,
    }

    def __init__(self, bot):
        self.bot = bot
        self._import_tasks = {}   # (guild_id, game_name) -> asyncio.Task
        self._import_status = {}  # (guild_id, game_name) -> dict

    async def cog_unload(self):
        tasks = list(self._import_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _is_enabled(guild_id, feature_flag):
        return cf_common.user_db.get_guild_config(guild_id, feature_flag) == '1'

    @staticmethod
    def _get_channel(guild_id, game_name):
        return cf_common.user_db.get_minigame_channel(guild_id, game_name)

    def _game_for_channel(self, message):
        """Return the GameDef whose configured channel matches, or None."""
        for game in self.GAMES.values():
            if not self._is_enabled(message.guild.id, game.feature_flag):
                continue
            channel_id = self._get_channel(message.guild.id, game.name)
            if channel_id is not None and str(message.channel.id) == str(channel_id):
                return game
        return None

    @staticmethod
    def _require_enabled(guild_id, game):
        if cf_common.user_db.get_guild_config(guild_id, game.feature_flag) != '1':
            raise MinigameCogError(
                f'{game.display_name} is not enabled. '
                f'An admin can enable it with `;meta config enable {game.feature_flag}`.'
            )

    async def _resolve_member(self, ctx, member_text):
        try:
            return await CaseInsensitiveMember().convert(ctx, member_text)
        except commands.BadArgument as exc:
            raise MinigameCogError(str(exc)) from exc

    @staticmethod
    def _resolve_registrar_target(ctx, member):
        """Validate that ``ctx.author`` may (un)register ``member``.

        Anyone can (un)register themselves; only mods/admins can act on someone
        else.  Passing your own member object is treated the same as omitting
        it.  Returns the resolved target.
        """
        if member is None or member.id == ctx.author.id:
            return ctx.author
        is_mod = any(r.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
                     for r in ctx.author.roles)
        if not is_mod:
            raise MinigameCogError(
                f'Only `{constants.TLE_ADMIN}` / `{constants.TLE_MODERATOR}` '
                f'can register or unregister other users.')
        return member

    # ── Rating ──────────────────────────────────────────────────────────

    def _recompute_akari_ratings(self, guild_id):
        """Replay all Akari results and overwrite the persisted rating snapshot.

        Pure function of the result tables, so this is always correct after any
        edit/delete/import.  Synchronous and free of ``await`` points, so it runs
        atomically with respect to the event loop (no lock needed).  Only fired
        when an Akari result actually changed, and once (not per row) after an
        import, so the brief CPU cost stays off the hot path.  Never raises — a
        rating failure must not break ingestion.
        """
        try:
            rows = cf_common.user_db.get_minigame_results_for_guild(
                guild_id, AKARI_GAME.name)
            max_puzzle = (expected_puzzle_number(dt.date.today())
                          + constants.AKARI_MAX_PUZZLE_LOOKAHEAD)
            states = compute_ratings(rows, max_puzzle=max_puzzle)
            cf_common.user_db.replace_akari_ratings(
                guild_id, states.values(), time.time())
        except Exception:
            logger.error('Failed to recompute Akari ratings for guild %s',
                         guild_id, exc_info=True)

    @staticmethod
    def _active_ranking_rows(rows):
        """Keep only recently-active players for the ranking.

        Hides anyone who hasn't played in the last
        ``AKARI_RANKING_MAX_INACTIVE_DAYS`` days, plus any stale future/garbage
        ``last_puzzle`` (e.g. a troll number lingering until the next recompute).
        """
        current = expected_puzzle_number(dt.date.today())
        cutoff = constants.AKARI_RANKING_MAX_INACTIVE_DAYS
        lookahead = constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        return [
            row for row in rows
            if -lookahead <= current - int(row.last_puzzle) <= cutoff
        ]

    # ── Listeners ───────────────────────────────────────────────────────

    async def _ingest_message(self, message, game):
        results = game.parse(strip_codeblock(message.content))
        if not results:
            return 0

        puzzle_date_fallback = message.created_at.date()

        saved = 0
        for parsed in results:
            existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
                message.guild.id, game.name, message.author.id, parsed.puzzle_number
            )
            if existing is not None and str(existing.message_id) != str(message.id):
                logger.info(
                    '%s result ignored (duplicate): guild=%s msg=%s user=%s puzzle=%s first_msg=%s',
                    game.display_name, message.guild.id, message.id,
                    message.author.id, parsed.puzzle_number, existing.message_id,
                )
                continue

            puzzle_date = parsed.puzzle_date or puzzle_date_fallback

            cf_common.user_db.save_minigame_result(
                message.id, message.guild.id, game.name, message.channel.id,
                message.author.id, parsed.puzzle_number,
                puzzle_date.isoformat(), parsed.accuracy,
                parsed.time_seconds, parsed.is_perfect, message.content,
            )
            saved += 1
        return saved

    @staticmethod
    def _is_akari_banned(guild_id, user_id, game):
        """True iff this is an Akari message from a banned user.

        Banning is akari-only — other games (e.g. GuessGame) don't have a
        banlist and pass through.  Used to short-circuit ingest at every entry
        point: live messages, edits, history import, and reparse.
        """
        return (game.name == AKARI_GAME.name
                and cf_common.user_db.is_akari_banned(guild_id, user_id))

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(message)
        if game is not None:
            try:
                if self._is_akari_banned(message.guild.id, message.author.id, game):
                    return  # silently drop — no raw store, no ingest
                # Save raw content for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, message.guild.id, message.channel.id,
                    message.author.id, message.created_at.isoformat(),
                    message.content,
                )
                saved = await self._ingest_message(message, game)
                if saved and game.name == AKARI_GAME.name:
                    self._recompute_akari_ratings(message.guild.id)
            except Exception:
                logger.error('Error ingesting message %s', message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        if after.guild is None or after.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(after)
        if game is None:
            return
        if self._is_akari_banned(after.guild.id, after.author.id, game):
            return  # silent drop — leave pre-ban data untouched
        try:
            # Update raw content so future reparse uses the edited version
            cf_common.user_db.update_raw_message(after.id, after.content)
            # Delete all existing live results for this message, then re-ingest.
            # Handles the case where an edit removes some results from a multi-result message.
            changed = cf_common.user_db.delete_minigame_result(after.id)
            results = game.parse(strip_codeblock(after.content))
            if results:
                changed += await self._ingest_message(after, game)
            else:
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
            if changed and game.name == AKARI_GAME.name:
                self._recompute_akari_ratings(after.guild.id)
        except Exception:
            logger.error('Error handling message edit %s', after.id, exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        try:
            deleted = cf_common.user_db.delete_minigame_result(payload.message_id)
            deleted += cf_common.user_db.delete_imported_minigame_result(payload.message_id)
            cf_common.user_db.delete_raw_message(payload.message_id)
            # Game isn't known from a raw delete payload; a removed result row may
            # have been an Akari one, so refresh ratings whenever any row went.
            if deleted:
                self._recompute_akari_ratings(payload.guild_id)
        except Exception:
            logger.error('Error handling message delete %s', payload.message_id, exc_info=True)

    # ── Import ──────────────────────────────────────────────────────────

    _KVS_IMPORT_PREFIX = 'mg_import_reply:'

    async def _resolve_channel(self, channel_id):
        """Get a channel from cache, falling back to fetch_channel for threads."""
        ch = self.bot.get_channel(channel_id)
        if ch is not None:
            return ch
        return await self.bot.fetch_channel(channel_id)

    async def _notify_import_complete(self, guild_id, game, status):
        """Reply to the original import command message with the final result."""
        kvs_key = f'{self._KVS_IMPORT_PREFIX}{guild_id}:{game.name}'
        try:
            reply_info = cf_common.user_db.kvs_get(kvs_key)
            if reply_info is None:
                return
            cf_common.user_db.kvs_delete(kvs_key)
            reply_channel_id, reply_message_id = reply_info.split(':')
            reply_channel = await self._resolve_channel(int(reply_channel_id))
            reply_message = await reply_channel.fetch_message(int(reply_message_id))

            state = status['state']
            skipped = status.get('skipped', [])
            lines = [
                f'**{game.display_name} import {state}.**',
                f'Messages scanned: **{status["scanned"]}**',
                f'Results imported: **{status["done"]}**',
            ]
            if skipped:
                lines.append(f'Detected but unparseable: **{len(skipped)}**')
            if status.get('error'):
                lines.append(f'Error: `{status["error"]}`')

            embed_fn = discord_common.embed_success if state == 'done' else discord_common.embed_alert
            await reply_message.reply(embed=embed_fn('\n'.join(lines)))
        except BaseException:
            logger.warning('Failed to send import completion reply for guild=%s game=%s',
                           guild_id, game.name, exc_info=True)
            # Clean up KVS key even on CancelledError
            try:
                cf_common.user_db.kvs_delete(kvs_key)
            except Exception:
                pass

    async def _run_import(self, guild_id, channel_id, game):
        key = (guild_id, game.name)
        status = self._import_status[key]
        try:
            try:
                channel = await self._resolve_channel(channel_id)
            except discord.NotFound:
                raise MinigameCogError(f'Channel `{channel_id}` is not available.')

            uncommitted = 0
            async for message in channel.history(oldest_first=True, limit=None):
                status['scanned'] += 1
                if message.author.bot or not message.content:
                    continue

                if self._is_akari_banned(guild_id, message.author.id, game):
                    continue  # skip banned users entirely (no raw, no result)

                # Save every non-bot message for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, guild_id, channel_id, message.author.id,
                    message.created_at.isoformat(), message.content,
                    commit=False,
                )
                uncommitted += 1

                cleaned = strip_codeblock(message.content)
                results = game.parse(cleaned)
                if not results:
                    if game.detect and game.detect.search(cleaned):
                        status['skipped'].append(str(message.id))
                        logger.warning(
                            '%s import: detected but unparseable msg=%s user=%s content=%r',
                            game.display_name, message.id, message.author.id,
                            message.content[:200],
                        )
                else:
                    puzzle_date_fallback = message.created_at.date()
                    for parsed in results:
                        puzzle_date = parsed.puzzle_date or puzzle_date_fallback
                        cf_common.user_db.save_imported_minigame_result(
                            message.id, guild_id, game.name, channel_id,
                            message.author.id, parsed.puzzle_number,
                            puzzle_date.isoformat(), parsed.accuracy,
                            parsed.time_seconds, parsed.is_perfect,
                            message.content, commit=False,
                        )
                        status['done'] += 1
                    status['latest_message_id'] = str(message.id)

                if uncommitted >= _IMPORT_BATCH_SIZE:
                    cf_common.user_db.conn.commit()
                    logger.info(
                        '%s import progress: guild=%s channel=%s scanned=%d imported=%d latest_msg=%s',
                        game.display_name, guild_id, channel_id,
                        status['scanned'], status['done'], status['latest_message_id'],
                    )
                    uncommitted = 0
                    await asyncio.sleep(_IMPORT_RATE_DELAY)

            if uncommitted > 0:
                cf_common.user_db.conn.commit()

            status['state'] = 'done'
            logger.info(
                '%s import complete: guild=%s channel=%s scanned=%d imported=%d',
                game.display_name, guild_id, channel_id,
                status['scanned'], status['done'],
            )
        except asyncio.CancelledError:
            status['state'] = 'cancelled'
            cf_common.user_db.conn.rollback()
            logger.info('%s import cancelled: guild=%s scanned=%d imported=%d',
                        game.display_name, guild_id, status['scanned'], status['done'])
            raise
        except RetryExhaustedError as exc:
            status['state'] = 'failed'
            status['error'] = f'Discord API retries exhausted: {exc.last_exception}'
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed (retries exhausted): guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        except Exception as exc:
            status['state'] = 'failed'
            status['error'] = str(exc)
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed: guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        finally:
            self._import_tasks.pop(key, None)
            # Recompute once after the whole import (committed batches persist even
            # on cancel/fail), rather than per imported row.
            if game.name == AKARI_GAME.name:
                self._recompute_akari_ratings(guild_id)
            await self._notify_import_complete(guild_id, game, status)

    # ── Shared command implementations ──────────────────────────────────

    async def _cmd_here(self, ctx, game):
        cf_common.user_db.set_minigame_channel(ctx.guild.id, game.name, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel set to {ctx.channel.mention}'
        ))

    async def _cmd_clear(self, ctx, game):
        cf_common.user_db.clear_minigame_channel(ctx.guild.id, game.name)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel cleared.'
        ))

    async def _cmd_show(self, ctx, game):
        enabled = self._is_enabled(ctx.guild.id, game.feature_flag)
        channel_id = self._get_channel(ctx.guild.id, game.name)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            f'channel: {channel}',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {game.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @staticmethod
    def _guessgame_puzzle_url(puzzle_number):
        return f'https://guessthe.game/p/{int(puzzle_number)}'

    @staticmethod
    def _format_guessgame_result(row):
        if row is None:
            return 'no result'

        accuracy = int(getattr(row, 'accuracy', 0))
        yellow_pos = int(getattr(row, 'time_seconds', 7))
        if accuracy > 0:
            green_pos = 7 - accuracy
            if green_pos == 1:
                return 'perfect'
            return f'green {green_pos}'
        if yellow_pos < 7:
            return f'yellow {yellow_pos}'
        return 'no green'

    def _make_guessgame_vs_pages(self, ctx, game, member1, member2, stats, matchups, scoring_name):
        title_suffix = f' ({scoring_name.title()})' if scoring_name else ''
        summary_lines = [
            f'`{_safe_member_name(member1)}`: **{_format_score(stats["score1"])}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{_format_score(stats["score2"])}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ]

        pages = []
        per_page = 10
        ordered_matchups = list(reversed(matchups))
        for chunk in paginator.chunkify(ordered_matchups, per_page):
            embed = discord.Embed(
                title=f'{game.display_name} Head to Head{title_suffix}',
                description='\n'.join(summary_lines),
                color=discord_common.random_cf_color(),
            )

            col1 = []
            col2 = []
            for matchup in chunk:
                row1 = matchup['row1']
                row2 = matchup['row2']
                puzzle_number = int(
                    row1.puzzle_number if row1 is not None else row2.puzzle_number
                )
                puzzle_link = f'[#{puzzle_number}]({self._guessgame_puzzle_url(puzzle_number)})'
                col1.append(
                    f'{puzzle_link} {self._format_guessgame_result(row1)}'
                    f' · {_format_score(matchup["score1"])} pts'
                )
                col2.append(
                    f'{puzzle_link} {self._format_guessgame_result(row2)}'
                    f' · {_format_score(matchup["score2"])} pts'
                )

            embed.add_field(
                name=_safe_member_name(member1),
                value='\n'.join(col1),
                inline=True,
            )
            embed.add_field(
                name=_safe_member_name(member2),
                value='\n'.join(col2),
                inline=True,
            )
            pages.append((None, embed))

        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_vs(self, ctx, game, member1, member2, *args):
        self._require_enabled(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        stats = compute_vs(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        title_suffix = f' ({scoring_name.title()})' if scoring_name else ''
        description = '\n'.join([
            f'`{_safe_member_name(member1)}`: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ])
        embed = discord.Embed(
            title=f'{game.display_name} Head to Head{title_suffix}',
            description=description,
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_guessgame_matchups(self, ctx, member1, member2, *args):
        game = GUESSGAME_GAME
        self._require_enabled(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        stats = compute_vs(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        matchups = compute_vs_matchups(
            rows1, rows2,
            score_fn=scoring.score_matchup,
            missing_is_loss=(
                scoring.missing_is_loss
                if scoring.missing_is_loss is not None
                else game.missing_is_loss
            ),
            best_result_sort_key_fn=scoring.best_result_sort_key,
            group_key_fn=scoring.result_group_key,
            missing_result=(
                scoring.missing_result
                if scoring.missing_result is not None
                else game.missing_result
            ),
        )
        self._make_guessgame_vs_pages(
            ctx, game, member1, member2, stats, matchups, scoring_name)

    async def _cmd_streak(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        filter_args = list(args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except MinigameCogError:
                member = ctx.author

        try:
            dlo, dhi, plo, phi = parse_date_args(filter_args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
        streak = compute_streak(rows)
        longest = compute_longest_streak(rows)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        best = pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        embed = discord.Embed(
            title=f'{game.display_name} Streak',
            description='\n'.join([
                f'`{_safe_member_name(member)}`: **{streak}** consecutive perfect day(s)',
                f'Longest streak: **{longest}** day(s)',
                f'Latest result: **{latest_status}** in **{format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_top(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        try:
            args, scoring_name, scoring = resolve_scoring(game, args)
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, game.name, dlo, dhi, plo, phi)
        winners = compute_top(
            rows,
            is_eligible=scoring.is_eligible_winner,
            best_result_sort_key_fn=scoring.best_result_sort_key,
            winner_result_sort_key_fn=scoring.winner_result_sort_key,
            group_key_fn=scoring.result_group_key,
        )
        if not winners:
            raise MinigameCogError(
                f'No {game.display_name} winners found for this range.')

        title_suffix = f' ({scoring_name.title()})' if scoring_name else ''
        pages = []
        per_page = 10
        for page_idx, chunk in enumerate(paginator.chunkify(winners, per_page)):
            lines = []
            for i, (user_id, wins) in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                name = _safe_user_name(ctx.guild, user_id)
                lines.append(f'**#{rank}** `{name}` — **{wins}** wins')
            embed = discord.Embed(
                title=f'{game.display_name} Winners{title_suffix}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_remove(self, ctx, game, member, puzzle_id):
        rc = cf_common.user_db.delete_minigame_result_for_user_puzzle(
            ctx.guild.id, game.name, member.id, puzzle_id)
        if not rc:
            raise MinigameCogError(
                f'No {game.display_name} result found for '
                f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.')
        if game.name == AKARI_GAME.name:
            self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {game.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.'))

    async def _cmd_akari_ratings(self, ctx):
        """Mod-gated guild leaderboard — registered, recently-active players only."""
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        rows = cf_common.user_db.get_akari_ratings(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} ratings yet. They appear once '
                f'players post results.')
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        registered = [r for r in rows if r.user_id in registrants]
        if not registered:
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players yet. '
                f'Players opt in with `;mg akari register`.')
        active = self._active_ranking_rows(registered)
        if not active:
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players active in '
                f'the last {constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days.')
        # All shown users are registered, so the ✓ marker is redundant noise.
        discord_file = _get_akari_rating_table_image_file(
            ctx.guild, active, registrants, mark_registered=False)
        await ctx.send(file=discord_file)

    def _akari_user_history(self, guild_id, user_id):
        """Replay the guild's results and return one user's per-day history.

        Shared by the rating and performance graphs — the replay is the same;
        each caller picks the field it needs off the :class:`HistoryPoint`s.
        """
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        max_puzzle = (expected_puzzle_number(dt.date.today())
                      + constants.AKARI_MAX_PUZZLE_LOOKAHEAD)
        histories = {}
        compute_ratings(result_rows, max_puzzle=max_puzzle, histories=histories)
        return histories.get(str(user_id), [])

    async def _cmd_akari_rating(self, ctx, member, *, require_registered=True):
        """Per-user rating graph (``;plot rating`` style).

        ``require_registered=True`` (the default, public-facing path) refuses
        to show the rating of users who haven't opted in via ``;mg akari register``.
        The ``rating debug`` subcommand passes False so admins can inspect any
        shadow-rated player.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered and not cf_common.user_db.is_akari_registered(
                ctx.guild.id, member.id):
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has not opted in to '
                f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')
        row = cf_common.user_db.get_akari_rating(ctx.guild.id, member.id)
        if row is None:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} rating for '
                f'`{_safe_member_name(member)}` yet.')

        history = self._akari_user_history(ctx.guild.id, member.id)
        if not history:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has no rated '
                f'{AKARI_GAME.display_name} days to plot yet.')

        rating = round(row.rating)
        rank = rank_for_rating(rating)
        peak_rank = rank_for_rating(round(row.peak))
        # Last contest day's delta and performance (skip solo-day Nones).
        # row.last_delta on the snapshot is overwritten by daily decay steps and
        # rounds to +0 for most users — use the history to find their last
        # actual contest instead, matching how Performance is computed below.
        last_contest = next((h for h in reversed(history)
                             if h.performance is not None), None)
        last_change_str = (f'{last_contest.delta:+.0f}'
                           if last_contest is not None else '—')
        last_perf_str = (f'{round(last_contest.performance)} '
                         f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                         if last_contest is not None else '—')
        discord_file = plot_akari_rating(history, _safe_member_name(member))
        embed = discord.Embed(
            title=f'{AKARI_GAME.display_name} rating — {_safe_member_name(member)}',
            color=rank.color_embed,
        )
        embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
        embed.add_field(name='Peak', value=f'{round(row.peak)} ({peak_rank.title_abbr})')
        embed.add_field(name='Games', value=str(row.games))
        embed.add_field(name='Last change', value=last_change_str)
        embed.add_field(name='Last performance', value=last_perf_str)
        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_performance(self, ctx, member, *, require_registered=True):
        """Per-user performance graph.

        Performance is the rating that, given the day's field, would seed the
        player at exactly their actual rank — i.e. their "rating-equivalent
        finish" for that contest, independent of their incoming rating.  Solo
        days have no field and are dropped from the plot.

        ``require_registered=True`` (the default, public-facing path) refuses
        to show performance for users who haven't opted in via ``;mg akari register``.
        The ``performance debug`` subcommand passes False so admins can inspect
        any shadow-rated player.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered and not cf_common.user_db.is_akari_registered(
                ctx.guild.id, member.id):
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has not opted in to '
                f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')
        row = cf_common.user_db.get_akari_rating(ctx.guild.id, member.id)
        if row is None:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} rating for '
                f'`{_safe_member_name(member)}` yet.')

        history = self._akari_user_history(ctx.guild.id, member.id)
        contest_history = [h for h in history if h.performance is not None]
        if not contest_history:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has no contested '
                f'{AKARI_GAME.display_name} days to plot performance for yet.')

        discord_file = plot_akari_performance(
            history, _safe_member_name(member), round(row.rating))
        last_perf = contest_history[-1].performance
        last_rank = rank_for_rating(round(last_perf))
        best_perf = max(h.performance for h in contest_history)
        best_rank = rank_for_rating(round(best_perf))
        embed = discord.Embed(
            title=f'{AKARI_GAME.display_name} performance — {_safe_member_name(member)}',
            color=last_rank.color_embed,
        )
        embed.add_field(name='Last performance',
                        value=f'{round(last_perf)} ({last_rank.title_abbr})')
        embed.add_field(name='Best performance',
                        value=f'{round(best_perf)} ({best_rank.title_abbr})')
        embed.add_field(name='Contests', value=str(len(contest_history)))
        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_ratings_debug(self, ctx):
        """Admin view: leaderboard image including shadow-rated (unopted-in) users.

        Same image as ``;mg akari ratings`` but without the registration filter —
        so admins can see everyone's rating, with a ``✓`` marking opted-in users.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        rows = cf_common.user_db.get_akari_ratings(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} ratings yet. They appear once '
                f'players post results.')
        active = self._active_ranking_rows(rows)
        if not active:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} players active in the last '
                f'{constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days.')
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        discord_file = _get_akari_rating_table_image_file(
            ctx.guild, active, registrants,
            title='Daily Akari Ratings (all)', mark_registered=True)
        await ctx.send(file=discord_file)

    async def _cmd_akari_history(self, ctx, member, *, require_registered=True):
        """Per-user paginated rating delta history (``;handles updates`` style).

        One line per contest the user played, newest first.  Solo days (single
        player) are skipped — they have no field, no contest delta, and don't
        appear on the rating graph either.  Decay days never had their own
        history points to begin with; their net effect surfaces in the next
        played day's rating.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered and not cf_common.user_db.is_akari_registered(
                ctx.guild.id, member.id):
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has not opted in to '
                f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')

        history = self._akari_user_history(ctx.guild.id, member.id)
        contest_history = [h for h in history if h.performance is not None]
        if not contest_history:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has no contested '
                f'{AKARI_GAME.display_name} days yet.')

        lines = [_format_akari_history_line(h) for h in reversed(contest_history)]
        title = (f'{AKARI_GAME.display_name} rating history — '
                 f'{_safe_member_name(member)} ({len(contest_history)} contests)')
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            embed = discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    _STATS_PLOTTERS = {
        'akari': plot_akari_stats,
        'guessgame': plot_guessgame_stats,
    }

    async def _cmd_stats(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        if game.name == 'akari' and len(args) == 1:
            selector = _maybe_parse_puzzle_selector(args[0])
            if selector is not None:
                selector_type, selector_value = selector
                if selector_type == 'puzzle':
                    rows = cf_common.user_db.get_minigame_results_for_guild(
                        ctx.guild.id, game.name, plo=selector_value, phi=selector_value + 1)
                    title = f'{game.display_name} #{selector_value} Results'
                else:
                    day_start = dt.datetime.combine(selector_value, dt.time.min).timestamp()
                    day_end = day_start + 24 * 60 * 60
                    rows = cf_common.user_db.get_minigame_results_for_guild(
                        ctx.guild.id, game.name, dlo=day_start, dhi=day_end)
                    title = f'{game.display_name} {selector_value.isoformat()} Results'

                if not rows:
                    raise MinigameCogError(f'No {game.display_name} results found for `{args[0]}`.')

                discord_file = _get_akari_puzzle_table_image_file(
                    ctx.guild, rows, title)
                await ctx.send(file=discord_file)
                return

        filter_args = list(args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except MinigameCogError:
                member = ctx.author

        try:
            dlo, dhi, plo, phi = parse_date_args(filter_args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        plotter = self._STATS_PLOTTERS.get(game.name)
        if plotter is None:
            raise MinigameCogError(f'Stats are not available for {game.display_name}.')

        discord_file = plotter(rows, _safe_member_name(member))
        await ctx.send(file=discord_file)

    async def _cmd_import_start(self, ctx, game, channel=None):
        key = (ctx.guild.id, game.name)
        if key in self._import_tasks:
            task = self._import_tasks[key]
            if not task.done():
                raise MinigameCogError(
                    f'A {game.display_name} import is already running.')

        configured_channel_id = self._get_channel(ctx.guild.id, game.name)
        if channel is None and configured_channel_id is not None:
            try:
                channel = await self._resolve_channel(int(configured_channel_id))
            except discord.NotFound:
                pass
        channel = channel or ctx.channel

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name, channel_id=channel.id)
        self._import_status[key] = {
            'state': 'running',
            'channel_id': channel.id,
            'scanned': 0,
            'done': 0,
            'skipped': [],
            'error': None,
            'latest_message_id': None,
            'cleared': deleted,
            'started_at': dt.datetime.now(),
        }
        task = asyncio.create_task(self._run_import(ctx.guild.id, channel.id, game))
        self._import_tasks[key] = task

        # Save reply target so the background task can reply when done
        kvs_key = f'{self._KVS_IMPORT_PREFIX}{ctx.guild.id}:{game.name}'
        cf_common.user_db.kvs_set(kvs_key, f'{ctx.channel.id}:{ctx.message.id}')

        logger.info(
            '%s import started: guild=%s channel=%s cleared=%d',
            game.display_name, ctx.guild.id, channel.id, deleted,
        )
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import started for {channel.mention}. '
            f'Cleared {deleted} imported row(s) first.'))

    async def _cmd_import_status(self, ctx, game):
        key = (ctx.guild.id, game.name)
        status = self._import_status.get(key)
        if status is None:
            raise MinigameCogError(
                f'No {game.display_name} import has been started.')

        elapsed = dt.datetime.now() - status['started_at']
        elapsed_str = str(elapsed).split('.')[0]  # drop microseconds
        lines = [
            f'state: `{status["state"]}`',
            f'channel: <#{status["channel_id"]}>',
            f'messages scanned: **{status["scanned"]}**',
            f'results imported: **{status["done"]}**',
            f'elapsed: `{elapsed_str}`',
        ]
        if status['latest_message_id'] is not None:
            lines.append(f'latest message: `{status["latest_message_id"]}`')
        skipped = status.get('skipped', [])
        if skipped:
            lines.append(f'detected but unparseable: **{len(skipped)}** '
                         f'(IDs: {", ".join(skipped[:10])}{"…" if len(skipped) > 10 else ""})')
        if status['error']:
            lines.append(f'error: `{status["error"]}`')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_import_cancel(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is None or task.done():
            raise MinigameCogError(
                f'No {game.display_name} import is currently running.')
        task.cancel()
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import cancelled.'))

    async def _cmd_import_clear(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is not None and not task.done():
            raise MinigameCogError(
                f'Cancel the running {game.display_name} import before clearing it.')

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        self._import_status.pop(key, None)
        if game.name == AKARI_GAME.name:
            self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Deleted {deleted} imported {game.display_name} row(s). '
            f'Raw messages preserved for reparse.'))

    async def _cmd_reparse(self, ctx, game):
        raw_messages = cf_common.user_db.get_raw_messages_for_guild(ctx.guild.id)
        if not raw_messages:
            raise MinigameCogError(
                f'No raw messages stored. Run an import first to populate them.')

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        parsed_count = 0
        skipped = []

        for row in raw_messages:
            if self._is_akari_banned(row.guild_id, row.user_id, game):
                continue  # banned users' raw rows stay in the store but produce no results
            cleaned = strip_codeblock(row.raw_content)
            results = game.parse(cleaned)
            if not results:
                if game.detect and game.detect.search(cleaned):
                    skipped.append(row.message_id)
                continue
            puzzle_date_fallback = dt.date.fromisoformat(row.created_at[:10])
            for parsed in results:
                puzzle_date = parsed.puzzle_date or puzzle_date_fallback
                cf_common.user_db.save_imported_minigame_result(
                    row.message_id, row.guild_id, game.name, row.channel_id,
                    row.user_id, parsed.puzzle_number,
                    puzzle_date.isoformat(), parsed.accuracy,
                    parsed.time_seconds, parsed.is_perfect,
                    row.raw_content, commit=False,
                )
                parsed_count += 1
        cf_common.user_db.conn.commit()

        if game.name == AKARI_GAME.name:
            self._recompute_akari_ratings(ctx.guild.id)

        lines = [
            f'raw messages scanned: **{len(raw_messages)}**',
            f'previous imported rows cleared: **{deleted}**',
            f'results parsed: **{parsed_count}**',
        ]
        if skipped:
            lines.append(
                f'detected but unparseable: **{len(skipped)}** '
                f'(IDs: {", ".join(skipped[:10])}{"…" if len(skipped) > 10 else ""})')
        logger.info(
            '%s reparse: guild=%s raw=%d cleared=%d parsed=%d skipped=%d',
            game.display_name, ctx.guild.id, len(raw_messages), deleted,
            parsed_count, len(skipped),
        )
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    # ── Command tree: ;minigames ────────────────────────────────────────

    @commands.group(name='minigames', aliases=['mg'], brief='Daily puzzle minigame commands',
                    invoke_without_command=True)
    async def minigames(self, ctx):
        """Daily puzzle minigame commands."""
        await ctx.send_help(ctx.command)

    # ── Akari commands: ;minigames akari … ──────────────────────────────

    @minigames.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
                     invoke_without_command=True)
    async def akari(self, ctx):
        """Daily Akari commands."""
        await ctx.send_help(ctx.command)

    @akari.command(name='here', brief='Set the Daily Akari channel to the current channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_here(self, ctx):
        await self._cmd_here(ctx, AKARI_GAME)

    @akari.command(name='clear', brief='Clear the Daily Akari channel')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_clear(self, ctx):
        await self._cmd_clear(ctx, AKARI_GAME)

    @akari.command(name='show', brief='Show Daily Akari settings')
    async def akari_show(self, ctx):
        await self._cmd_show(ctx, AKARI_GAME)

    @akari.command(name='register', brief='Opt in to Daily Akari ratings',
                   usage='[@user (mods only)]')
    async def akari_register(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        added = cf_common.user_db.register_akari_user(
            ctx.guild.id, target.id, time.time())
        who = ('You are' if target.id == ctx.author.id
               else f'`{_safe_member_name(target)}` is')
        msg = (f'{who} now registered for {AKARI_GAME.display_name} ratings.'
               if added else
               f'{who} already registered for {AKARI_GAME.display_name} ratings.')
        await ctx.send(embed=discord_common.embed_success(msg))

    @akari.command(name='unregister', brief='Opt out of Daily Akari ratings',
                   usage='[@user (mods only)]')
    async def akari_unregister(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        removed = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, target.id)
        if removed:
            who = ('You are' if target.id == ctx.author.id
                   else f'`{_safe_member_name(target)}` is')
            msg = (f'{who} no longer registered for {AKARI_GAME.display_name} ratings. '
                   f'Results are still recorded.')
        else:
            msg = ('You were not registered.' if target.id == ctx.author.id
                   else f'`{_safe_member_name(target)}` was not registered.')
        await ctx.send(embed=discord_common.embed_success(msg))

    @akari.command(name='ban',
                   brief='(Mod) Block a user from Akari ingestion',
                   usage='@user [reason...]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ban(self, ctx, member: CaseInsensitiveMember, *,
                        reason: str = None):
        added = cf_common.user_db.ban_akari_user(
            ctx.guild.id, member.id, time.time(), ctx.author.id, reason)
        if not added:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is already banned from '
                f'{AKARI_GAME.display_name}.')
        # Auto-unregister so the rating display state stays consistent.
        unregistered = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, member.id)
        lines = [f'`{_safe_member_name(member)}` is now banned from '
                 f'{AKARI_GAME.display_name} ingestion. New results from '
                 f'them will be dropped silently.']
        if unregistered:
            lines.append('Also removed from registrants.')
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @akari.command(name='unban',
                   brief='(Mod) Lift an Akari ingestion ban',
                   usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_unban(self, ctx, member: CaseInsensitiveMember):
        removed = cf_common.user_db.unban_akari_user(ctx.guild.id, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{AKARI_GAME.display_name}. They are not auto-registered — '
            f'they need to run `;mg akari register` again.'))

    @akari.command(name='bans',
                   brief='(Mod) List Akari ingestion bans')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_bans(self, ctx):
        rows = cf_common.user_db.get_akari_bans(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No active {AKARI_GAME.display_name} bans.')
        lines = [_format_akari_ban_line(ctx.guild, row) for row in rows]
        title = f'{AKARI_GAME.display_name} bans ({len(rows)})'
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            embed = discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    @akari.command(name='vs', brief='Head-to-head comparison',
                   usage='@user1 @user2 [filters...] [raw|all]')
    async def akari_vs(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, AKARI_GAME, member1, member2, *args)

    @akari.command(name='streak', brief='Show current perfect streak',
                   usage='[@user] [filters...]')
    async def akari_streak(self, ctx, *args):
        await self._cmd_streak(ctx, AKARI_GAME, *args)

    @akari.command(name='top', brief='Show winners leaderboard',
                   usage='[filters...] [raw|all]')
    async def akari_top(self, ctx, *args):
        await self._cmd_top(ctx, AKARI_GAME, *args)

    @akari.command(name='stats', brief='Show personal stats with graphs',
                   usage='[@user] [filters...] | [day | puzzle_id | #puzzle_id]')
    async def akari_stats(self, ctx, *args):
        await self._cmd_stats(ctx, AKARI_GAME, *args)

    @akari.command(name='remove', brief='Remove a user result for a puzzle',
                   usage='@user puzzle_id')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, AKARI_GAME, member, puzzle_id)

    @akari.group(name='import', brief='Manage imported history',
                 invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import(self, ctx):
        await ctx.send_help(ctx.command)

    @akari_import.command(name='start', brief='Rebuild imported history')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, AKARI_GAME, channel)

    @akari_import.command(name='status', brief='Show import status')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_status(self, ctx):
        await self._cmd_import_status(ctx, AKARI_GAME)

    @akari_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, AKARI_GAME)

    @akari_import.command(name='clear', brief='Delete imported history')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, AKARI_GAME)

    @akari.command(name='reparse', brief='Reparse all stored raw messages')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_reparse(self, ctx):
        await self._cmd_reparse(ctx, AKARI_GAME)

    @akari.group(name='ratings', brief='(Mod) Show Akari rating leaderboard',
                 invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings(self, ctx):
        await self._cmd_akari_ratings(ctx)

    @akari.group(name='rating',
                 brief='(Mod) Show one registered user\'s Akari rating graph',
                 usage='[@user]', invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_rating(self, ctx, member: CaseInsensitiveMember = None):
        if member is None:
            member = ctx.author
        await self._cmd_akari_rating(ctx, member)

    @akari_rating.command(name='debug',
                          brief='(Mod) Rating graph for any user (incl. shadow-rated)',
                          usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_rating_debug(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_akari_rating(ctx, member, require_registered=False)

    @akari.group(name='performance', aliases=['perf'],
                 brief='(Mod) Show one registered user\'s Akari performance graph',
                 usage='[@user]', invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_performance(self, ctx, member: CaseInsensitiveMember = None):
        if member is None:
            member = ctx.author
        await self._cmd_akari_performance(ctx, member)

    @akari_performance.command(name='debug',
                               brief='(Mod) Performance graph for any user (incl. shadow-rated)',
                               usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_performance_debug(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_akari_performance(ctx, member, require_registered=False)

    @akari.group(name='history',
                 brief='(Mod) Paginated rating delta log for one registered user',
                 usage='[@user]', invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_history(self, ctx, member: CaseInsensitiveMember = None):
        if member is None:
            member = ctx.author
        await self._cmd_akari_history(ctx, member)

    @akari_history.command(name='debug',
                           brief='(Mod) Rating delta log for any user (incl. shadow-rated)',
                           usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_history_debug(self, ctx, member: CaseInsensitiveMember):
        await self._cmd_akari_history(ctx, member, require_registered=False)

    @akari_ratings.command(name='recompute', brief='(Mod) Rebuild the rating snapshot')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_recompute(self, ctx):
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{AKARI_GAME.display_name} ratings recomputed.'))

    @akari_ratings.command(name='debug', aliases=['all'],
                           brief='(Mod) Leaderboard incl. shadow-rated (unopted-in) users')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_debug(self, ctx):
        await self._cmd_akari_ratings_debug(ctx)

    # ── GuessGame commands: ;minigames guessgame … ──────────────────────

    @minigames.group(name='guessgame', aliases=['gg'], brief='GuessThe.Game commands',
                     invoke_without_command=True)
    async def guessgame(self, ctx):
        """GuessThe.Game commands."""
        await ctx.send_help(ctx.command)

    @guessgame.command(name='here', brief='Set the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_here(self, ctx):
        await self._cmd_here(ctx, GUESSGAME_GAME)

    @guessgame.command(name='clear', brief='Clear the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_clear(self, ctx):
        await self._cmd_clear(ctx, GUESSGAME_GAME)

    @guessgame.command(name='show', brief='Show GuessGame settings')
    async def gg_show(self, ctx):
        await self._cmd_show(ctx, GUESSGAME_GAME)

    @guessgame.command(name='vs', brief='Head-to-head comparison',
                       usage='@user1 @user2 [p>=N] [p<N] [filters...]')
    async def gg_vs(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, GUESSGAME_GAME, member1, member2, *args)

    @guessgame.command(name='results', aliases=['matchups'], brief='Show per-puzzle side-by-side results',
                       usage='@user1 @user2 [p>=N] [p<N] [filters...]')
    async def gg_results(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_guessgame_matchups(ctx, member1, member2, *args)

    @guessgame.command(name='streak', brief='Show current win streak',
                       usage='[@user] [filters...]')
    async def gg_streak(self, ctx, *args):
        await self._cmd_streak(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='top', brief='Show winners leaderboard',
                       usage='[p>=N] [p<N] [filters...]')
    async def gg_top(self, ctx, *args):
        await self._cmd_top(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='stats', brief='Show personal stats with graphs',
                       usage='[@user] [filters...]')
    async def gg_stats(self, ctx, *args):
        await self._cmd_stats(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='remove', brief='Remove a user result for a puzzle',
                       usage='@user puzzle_id')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, GUESSGAME_GAME, member, puzzle_id)

    @guessgame.group(name='import', brief='Manage imported history',
                     invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import(self, ctx):
        await ctx.send_help(ctx.command)

    @gg_import.command(name='start', brief='Rebuild imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, GUESSGAME_GAME, channel)

    @gg_import.command(name='status', brief='Show import status')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_status(self, ctx):
        await self._cmd_import_status(ctx, GUESSGAME_GAME)

    @gg_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, GUESSGAME_GAME)

    @gg_import.command(name='clear', brief='Delete imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, GUESSGAME_GAME)

    @guessgame.command(name='reparse', brief='Reparse all stored raw messages')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_reparse(self, ctx):
        await self._cmd_reparse(ctx, GUESSGAME_GAME)

    # ── Slash commands: /akari ─────────────────────────────────────────

    akari_slash = app_commands.Group(
        name='akari', description='Daily Akari commands', guild_only=True)

    def _has_mod_role(self, interaction):
        allowed = {constants.TLE_ADMIN, constants.TLE_MODERATOR}
        return any(r.name in allowed for r in interaction.user.roles)

    async def _slash_send_error(self, interaction, error):
        try:
            await interaction.followup.send(
                embed=discord_common.embed_alert(str(error)))
        except Exception:
            logger.warning('Failed to send slash error response', exc_info=True)

    @akari_slash.command(name='show', description='Show Daily Akari settings')
    async def slash_akari_show(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_show(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='vs', description='Head-to-head comparison')
    @app_commands.describe(
        member1='First player', member2='Second player',
        timeframe='Time period filter', mode='Scoring mode')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_vs(
        self, interaction: discord.Interaction,
        member1: discord.Member, member2: discord.Member,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        args = []
        if timeframe:
            args.append(timeframe.value)
        if mode:
            args.append(mode.value)
        try:
            await self._cmd_vs(
                _SlashCtx(interaction), AKARI_GAME, member1, member2, *args)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='streak', description='Show current perfect streak')
    @app_commands.describe(member='Player to check', timeframe='Time period filter')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_streak(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        args = []
        if timeframe:
            args.append(timeframe.value)
        try:
            await self._cmd_streak(ctx, AKARI_GAME, *args)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='top', description='Show winners leaderboard')
    @app_commands.describe(timeframe='Time period filter', mode='Scoring mode')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_top(
        self, interaction: discord.Interaction,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        args = []
        if timeframe:
            args.append(timeframe.value)
        if mode:
            args.append(mode.value)
        try:
            await self._cmd_top(_SlashCtx(interaction), AKARI_GAME, *args)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='stats', description='Show personal stats with graphs')
    @app_commands.describe(member='Player to check', timeframe='Time period filter')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_stats(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        args = []
        if timeframe:
            args.append(timeframe.value)
        try:
            await self._cmd_stats(ctx, AKARI_GAME, *args)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)

    @akari_slash.command(name='ratings', description='(Mod) Show Akari rating leaderboard')
    async def slash_akari_ratings(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_akari_ratings(_SlashCtx(interaction))
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='rating', description="(Mod) Show one user's Akari rating graph")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        target = member or interaction.user
        try:
            await self._cmd_akari_rating(_SlashCtx(interaction), target)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='performance', description="(Mod) Show one user's Akari performance graph")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        target = member or interaction.user
        try:
            await self._cmd_akari_performance(_SlashCtx(interaction), target)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='history', description="(Mod) Show one user's Akari rating delta log")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        target = member or interaction.user
        try:
            await self._cmd_akari_history(_SlashCtx(interaction), target)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='here', description='Set the Daily Akari channel')
    async def slash_akari_here(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_here(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='clear', description='Clear the Daily Akari channel')
    async def slash_akari_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_clear(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='remove', description='Remove a user result')
    @app_commands.describe(member='Player', puzzle_id='Puzzle number')
    async def slash_akari_remove(
        self, interaction: discord.Interaction,
        member: discord.Member, puzzle_id: int,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_remove(
                _SlashCtx(interaction), AKARI_GAME, member, puzzle_id)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='reparse', description='Reparse all stored raw messages')
    async def slash_akari_reparse(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_reparse(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='import-start', description='Rebuild imported history')
    @app_commands.describe(channel='Channel to import from')
    async def slash_akari_import_start(
        self, interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        ctx = _SlashCtx(interaction)
        try:
            original = await interaction.original_response()
            ctx.message = original
            await self._cmd_import_start(ctx, AKARI_GAME, channel)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='import-status', description='Show import status')
    async def slash_akari_import_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_status(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='import-cancel', description='Cancel a running import')
    async def slash_akari_import_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_cancel(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='import-clear', description='Delete imported history')
    async def slash_akari_import_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_clear(_SlashCtx(interaction), AKARI_GAME)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    # ── Error handler ───────────────────────────────────────────────────

    @discord_common.send_error_if(MinigameCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Minigames(bot))

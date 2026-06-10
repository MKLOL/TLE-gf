import asyncio
import datetime as dt
import hashlib
import html
import io
import json
import logging
import os
import pathlib
import re
import statistics
import sys
import time
from collections import namedtuple
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
from tle.cogs._minigame_akari import (
    AKARI_GAME, expected_puzzle_number, looks_like_non_pro_akari, puzzle_date_for,
)
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens import (
    QUEENS_GAME, normalize_queens_name, parse_queens_leaderboard,
    parse_queens_time, queens_status_flags,
)
from tle.cogs._minigame_stats import (
    plot_akari_performance, plot_akari_rating,
    plot_akari_stats, plot_guessgame_stats,
)
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError
from tle.util.akari_rating import rank_for_rating
from tle.util.minigame_rating import compute_ratings

logger = logging.getLogger(__name__)

_IMPORT_BATCH_SIZE = 500
_IMPORT_RATE_DELAY = 0.5
_QUEENS_HISTORY_PER_PAGE = 15
_AKARI_IMAGE_MAX_ROWS = 40
_AKARI_IMAGE_WIDTH = 900
_AKARI_IMAGE_MARGIN = 20
_AKARI_IMAGE_ROW_HEIGHT = 36
_AKARI_IMAGE_HEADER_SPACING = 1.25
_AKARI_IMAGE_COLUMN_MARGIN = 10
# Table layouts share the same Cairo renderer.  Akari keeps separate Result
# and Time columns; Queens omits Result because the day leaderboard is ranked
# by time only.  Widths sum to ``_AKARI_IMAGE_WIDTH − 2 × MARGIN`` (860).
_AKARI_RATING_COLS = (54, 300, 260, 150, 96)
_AKARI_PUZZLE_COLS = (54, 300, 260, 150, 96)
_AKARI_PUZZLE_DELTA_COLS = (54, 316, 230, 90, 90, 80)
_QUEENS_RESULTS_COLS = (54, 360, 340, 106)
_QUEENS_RESULTS_DELTA_COLS = (54, 330, 320, 90, 66)


# Per-puzzle table annotation for one opted-in player: pre-puzzle rating and
# the day's delta (contest + transfer share).  Built from a single full-history
# replay so a stats request only costs one ``compute_ratings`` pass.
_PuzzlePlayerInfo = namedtuple('_PuzzlePlayerInfo', 'pre_rating delta')
_QueensResolvedEntry = namedtuple(
    '_QueensResolvedEntry',
    'user_id linkedin_name time_seconds no_hints no_mistakes',
)
_QueensImportPreview = namedtuple(
    '_QueensImportPreview',
    'puzzle_date puzzle_number resolved unresolved raw_content',
)
_QueensImportSaveResult = namedtuple(
    '_QueensImportSaveResult',
    'resolved unresolved',
)
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
_URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)
_QUEENS_CONNECTION_ACCOUNT_KEY = 'queens_connection_account'
_QUEENS_ANONYMOUS_LINK_MARKER = 'tle:queens:anonymous'
_QUEENS_ANONYMOUS_LABEL = 'Anonymous'
_QUEENS_ANONYMOUS_FLAGS = {'+anon', '+anonymous'}
_QUEENS_ANCHOR_DATE = dt.date(2026, 6, 8)
_QUEENS_ANCHOR_NUMBER = 769

# Scraper config — stored per-guild in guild_config.
#  - Discord user id of the importer (resolved from `;queens login` whoami)
#  - Optional override for the storage_state.json path
# Rate-limit bookkeeping for `;queens update` lives in kvs under
# `queens_update_throttle:{guild_id}`.
_QUEENS_IMPORTER_KEY = 'queens_importer_user'  # legacy — cleared on login
_QUEENS_LINKEDIN_NAME_KEY = 'queens_linkedin_name'  # display only
_QUEENS_STATE_PATH_KEY = 'queens_state_path'
_QUEENS_UPDATE_THROTTLE_PREFIX = 'queens_update_throttle:'
_QUEENS_UPDATE_THROTTLE_SECONDS = 60
_QUEENS_SCRAPER_TIMEOUT = 240  # seconds — playwright start + slow auto-play
_QUEENS_WHOAMI_TIMEOUT = 60    # seconds — quick /in/me/ visit only
# Bleeding-edge Ubuntu (26.04+) isn't in Playwright's platform support
# matrix yet, so ``playwright install chromium`` refuses with
# ``Playwright does not support chromium on ubuntuXX.04-x64``.  Overriding
# to ubuntu24.04-x64 forces the install AND the runtime browser lookup to
# use the LTS binary, whose glibc dependency is compatible with anything
# newer.  Harmless on Ubuntu 24.04 itself (the natural platform).  May not
# work on Ubuntu <22 — those hosts have an older glibc than the 24.04
# binary expects; admin would need to install older Playwright manually.
_QUEENS_PLAYWRIGHT_PLATFORM = 'ubuntu24.04-x64'
# Tolerate a state file up to ~256KiB.  Real Playwright state.json files for
# LinkedIn are ~10-30KiB; this gives generous headroom without inviting
# someone to upload a giant attachment.
_QUEENS_STATE_MAX_BYTES = 256 * 1024
# Backfill JSON files can be much larger (years of history × many
# players).  10 MiB covers any realistic LinkedIn export.
_QUEENS_BACKFILL_MAX_BYTES = 10 * 1024 * 1024
# tle/cogs/minigames.py → repo root → extra/queens_scrape.py
_QUEENS_SCRAPER_SCRIPT = (
    pathlib.Path(__file__).resolve().parent.parent.parent
    / 'extra' / 'queens_scrape.py'
)
_QUEENS_DEFAULT_STATE_PATH = (
    _QUEENS_SCRAPER_SCRIPT.parent / '.queens_state.json'
)


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
        # Discord interaction IDs are globally unique snowflakes, so they're
        # safe to use anywhere a per-invocation message_id is expected (e.g.
        # /akari add storing the row keyed on this id).  ``import-start``
        # overrides this with the real bot reply's id after deferring.
        self.message = type('_Msg', (), {'id': interaction.id})()

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


def _parse_queens_date(date_text):
    text = str(date_text).strip()
    formats = (
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%d%m%Y',
    )
    for fmt in formats:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise MinigameCogError(
        f'Could not parse Queens date `{date_text}`. Use `YYYY-MM-DD`.')


def _queens_puzzle_number_for_date(puzzle_date):
    puzzle_date = normalize_puzzle_date(puzzle_date)
    return _QUEENS_ANCHOR_NUMBER + (puzzle_date - _QUEENS_ANCHOR_DATE).days


def _queens_date_for_puzzle_number(puzzle_number):
    return _QUEENS_ANCHOR_DATE + dt.timedelta(
        days=int(puzzle_number) - _QUEENS_ANCHOR_NUMBER)


def _parse_queens_date_or_number(value):
    try:
        return _parse_queens_date(value)
    except MinigameCogError:
        text = str(value).strip()
        if text.startswith('#'):
            text = text[1:]
        if text.isdigit():
            return _queens_date_for_puzzle_number(int(text))
        raise


def _queens_puzzle_numbers_for_date(puzzle_date):
    puzzle_date = normalize_puzzle_date(puzzle_date)
    numbers = [_queens_puzzle_number_for_date(puzzle_date)]
    legacy_number = puzzle_date.toordinal()
    if legacy_number != numbers[0]:
        numbers.append(legacy_number)
    return numbers


def _queens_puzzle_date_text(puzzle_date):
    return normalize_puzzle_date(puzzle_date).isoformat()


def _queens_result_message_id(guild_id, puzzle_date, user_id):
    date_text = _queens_puzzle_date_text(puzzle_date)
    raw = f'{guild_id}:queens:{date_text}:{user_id}'.encode('utf-8')
    digest = hashlib.blake2b(raw, digest_size=8).digest()
    return str(int.from_bytes(digest, 'big') & ((1 << 63) - 1))


def _format_queens_date(row_or_date):
    value = getattr(row_or_date, 'puzzle_date', row_or_date)
    return normalize_puzzle_date(value).isoformat()


def _is_queens_link_anonymous(link):
    return (
        link is not None
        and getattr(link, 'external_url', None) == _QUEENS_ANONYMOUS_LINK_MARKER
    )


def _queens_public_link_name(link):
    if _is_queens_link_anonymous(link):
        return _QUEENS_ANONYMOUS_LABEL
    return getattr(link, 'external_name', '-')


def _split_queens_anonymous_flag(linkedin_text):
    tokens = str(linkedin_text or '').split()
    anonymous = any(
        token.casefold() in _QUEENS_ANONYMOUS_FLAGS
        for token in tokens)
    name_tokens = [
        token for token in tokens
        if token.casefold() not in _QUEENS_ANONYMOUS_FLAGS
    ]
    return ' '.join(name_tokens).strip(), anonymous


def _is_queens_anonymous_modal_request(first, rest):
    text = ' '.join(
        part for part in (str(first or '').strip(), str(rest or '').strip())
        if part)
    if not text:
        return False
    name, anonymous = _split_queens_anonymous_flag(text)
    return anonymous and not name


def _clean_queens_linkedin_name(text):
    if _URL_RE.search(text or ''):
        raise MinigameCogError(
            'Profile URLs are not needed. Use only the LinkedIn display name.')
    name = (text or '').strip()
    name = ' '.join(name.split())
    if not name:
        raise MinigameCogError('A LinkedIn display name is required.')
    return name


def _split_queens_connection_account_text(text):
    urls = _URL_RE.findall(text or '')
    if not urls:
        raise MinigameCogError(
            'A LinkedIn profile URL is required for the connection account.')
    name = _URL_RE.sub('', text or '').strip()
    name = ' '.join(name.split())
    if not name:
        raise MinigameCogError('A LinkedIn display name is required.')
    return name, urls[0]


def _format_queens_result(entry, *, name_override=None):
    """Format a single leaderboard entry as ``<name> — M:SS (badges)``.

    ``name_override`` short-circuits the entry's stored LinkedIn name —
    pass ``_queens_public_link_name(link)`` for resolved entries so an
    anonymously-registered user's real LinkedIn name never appears in
    a public embed.  When omitted, ``entry.linkedin_name`` is used (safe
    for unresolved entries — by definition, no Discord user is claiming
    that name yet, so there's no privacy expectation to honour).
    """
    badges = []
    if entry.no_hints:
        badges.append('no hints')
    if entry.no_mistakes:
        badges.append('no mistakes')
    suffix = f' ({", ".join(badges)})' if badges else ''
    name = entry.linkedin_name if name_override is None else name_override
    return f'{name} — {format_duration(entry.time_seconds)}{suffix}'


def _queens_best_results_by_date(rows):
    return pick_best_results(
        rows,
        sort_key_fn=QUEENS_GAME.best_result_sort_key,
        group_key_fn=QUEENS_GAME.result_group_key,
    )


def _queens_streak_info(rows):
    best = _queens_best_results_by_date(rows)
    if not best:
        return 0, 0, None

    latest_day = max(best)
    current = 0
    day = latest_day
    while day in best and best[day].is_perfect:
        current += 1
        day -= dt.timedelta(days=1)

    longest = 0
    run = 0
    previous_day = None
    for day in sorted(best):
        if best[day].is_perfect:
            is_consecutive = (
                previous_day is not None
                and day == previous_day + dt.timedelta(days=1)
            )
            run = (
                run + 1
                if is_consecutive
                else 1
            )
            longest = max(longest, run)
        else:
            run = 0
        previous_day = day

    return current, longest, best[latest_day]


def _legend_name_for(guild, member):
    """Pick a matplotlib-safe display name for the rating/perf graph legend.

    Prefers the user's CF handle (ASCII-only by CF's rules → no emoji → no
    matplotlib tofu boxes); falls back to their Discord display name when no
    handle is linked.  See the discussion of why matplotlib can't render emoji
    the way Pango can in the leaderboard image.
    """
    handle = _safe_cf_handle(guild, member.id)
    if handle != '-':
        return handle
    return _safe_member_name(member)


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
    """Accuracy cell for the per-puzzle table.

    Uses ``100%`` instead of the word ``perfect`` so the cell stays narrow;
    time lives in its own column next to it.
    """
    pct = 100 if row.is_perfect else int(row.accuracy)
    return f'{pct}%'


def _sort_akari_puzzle_results(rows, *, sort_key_fn=None):
    if sort_key_fn is not None:
        return sorted(rows, key=sort_key_fn)
    return sorted(
        rows,
        key=lambda row: (
            -int(bool(row.is_perfect)),
            -int(getattr(row, 'accuracy', 0)),
            int(getattr(row, 'time_seconds', 0)),
            int(getattr(row, 'message_id', 0)),
        ),
    )


def _akari_puzzle_table_rows(guild, rows, *, puzzle_info=None,
                             registrants=None, identity_fn=None,
                             sort_key_fn=None):
    """Build display rows for a per-puzzle table.

    When ``puzzle_info`` and ``registrants`` are both supplied, each opted-in
    user's name cell gets ``(<pre-rating> <tier>)`` appended and a signed delta
    cell (``+12`` / ``-8``) is included as the 5th column.  Unregistered users
    get the plain name and an empty delta (privacy: we don't surface their
    rating or its change).  Without ``puzzle_info`` the rows are 4-tuples so
    the un-annotated text/image paths stay unchanged.
    """
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    annotated = puzzle_info is not None and registrants is not None
    result = []
    for index, row in enumerate(
            _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn),
            start=1):
        name = _safe_user_name(guild, row.user_id)
        delta_cell = ''
        if (annotated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            r = round(info.pre_rating)
            name = f'{name} ({r} {rank_for_rating(r).title_abbr})'
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            index,
            name,
            identity_fn(guild, row),
            _format_akari_result_status(row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.append(delta_cell)
        result.append(tuple(cells))
    return result


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
                                  cols=_AKARI_PUZZLE_COLS,
                                  right_align_cols=None,
                                  row_colors=None,
                                  cell_colors=None,
                                  width=_AKARI_IMAGE_WIDTH,
                                  filename='akari-results.png'):
    title_height = _AKARI_IMAGE_ROW_HEIGHT if title is not None else 0
    footer_height = _AKARI_IMAGE_ROW_HEIGHT if footer is not None else 0
    height = int(
        (len(table_rows) + _AKARI_IMAGE_HEADER_SPACING) * _AKARI_IMAGE_ROW_HEIGHT
        + title_height + footer_height + 2 * _AKARI_IMAGE_MARGIN
    )

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    context = cairo.Context(surface)
    context.set_source_rgb(*_DISCORD_GRAY)
    context.rectangle(0, 0, width, height)
    context.fill()

    layout = PangoCairo.create_layout(context)
    layout.set_font_description(
        Pango.font_description_from_string(','.join(_AKARI_IMAGE_FONTS) + ' 18'))
    layout.set_ellipsize(Pango.EllipsizeMode.END)

    def draw_bg(y, color):
        context.set_source_rgb(*color)
        context.rectangle(0, y, width, _AKARI_IMAGE_ROW_HEIGHT)
        context.fill()

    def draw_cell(text, cell_width, *, align=Pango.Alignment.LEFT, bold=False):
        text = html.escape(str(text))
        if bold:
            text = f'<b>{text}</b>'
        layout.set_width(max(1, int((cell_width - _AKARI_IMAGE_COLUMN_MARGIN) * Pango.SCALE)))
        layout.set_alignment(align)
        layout.set_markup(text, -1)
        PangoCairo.show_layout(context, layout)
        context.rel_move_to(cell_width, 0)

    def draw_line(text, y, color, *, bold=False):
        context.set_source_rgb(*(component / 255 for component in color))
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        draw_cell(
            text,
            width - 2 * _AKARI_IMAGE_MARGIN,
            bold=bold,
        )

    if right_align_cols is None:
        # Default: rank (#) and the last column (Time / Games) right-align.
        right_set = {0, len(cols) - 1}
    else:
        right_set = set(right_align_cols)

    def draw_row(row, y, color, *, bold=False, per_cell=None):
        context.move_to(_AKARI_IMAGE_MARGIN, y)
        for i, (value, cell_width) in enumerate(zip(row, cols)):
            c = per_cell[i] if per_cell is not None else color
            context.set_source_rgb(*(component / 255 for component in c))
            align = (Pango.Alignment.RIGHT if i in right_set
                     else Pango.Alignment.LEFT)
            draw_cell(value, cell_width, align=align, bold=bold)

    y = _AKARI_IMAGE_MARGIN
    if title is not None:
        draw_line(title, y, _SMOKE_WHITE, bold=True)
        y += _AKARI_IMAGE_ROW_HEIGHT

    draw_row(header, y, _SMOKE_WHITE, bold=True)
    y += int(_AKARI_IMAGE_ROW_HEIGHT * _AKARI_IMAGE_HEADER_SPACING)

    for i, row in enumerate(table_rows):
        draw_bg(y, _TABLE_ROW_COLORS[i % 2])
        # row_colors (when provided) gives the per-row text colour as a 0–255
        # RGB tuple; cell_colors gives per-cell colours and overrides row_colors;
        # otherwise everything stays black like the puzzle tables.
        text_color = row_colors[i] if row_colors is not None else _BLACK
        per_cell = cell_colors[i] if cell_colors is not None else None
        draw_row(row, y, text_color, per_cell=per_cell)
        y += _AKARI_IMAGE_ROW_HEIGHT

    if footer is not None:
        draw_line(footer, y, _SMOKE_WHITE)

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    return discord.File(image_data, filename=filename)


def _get_akari_puzzle_table_image_file(guild, rows, title,
                                       *, puzzle_info=None, registrants=None,
                                       identity_label='Handle',
                                       identity_fn=None,
                                       sort_key_fn=None):
    rows = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    displayed = rows[:_AKARI_IMAGE_MAX_ROWS]
    displayed_rows = _akari_puzzle_table_rows(
        guild, displayed, puzzle_info=puzzle_info, registrants=registrants,
        identity_fn=identity_fn, sort_key_fn=sort_key_fn)
    annotated = puzzle_info is not None and registrants is not None
    row_colors = None
    if annotated:
        # Only opted-in users get a tier colour; the rest stay default-black.
        row_colors = [
            _akari_row_text_color(puzzle_info[row.user_id].pre_rating)
            if row.user_id in registrants and row.user_id in puzzle_info
            else _BLACK
            for row in displayed
        ]
    footer = None
    if len(rows) > len(displayed_rows):
        footer = f'Showing top {len(displayed_rows)} of {len(rows)} results'
    if annotated:
        header = ('#', 'Name', identity_label, 'Result', 'Time', '\N{INCREMENT}')
        cols = _AKARI_PUZZLE_DELTA_COLS
        # Time and Δ both carry numeric content — right-align them so values
        # line up at the column's right edge.
        right_align_cols = (0, 4, 5)
    else:
        header = ('#', 'Name', identity_label, 'Result', 'Time')
        cols = _AKARI_PUZZLE_COLS
        right_align_cols = None  # default — # and Time right
    return _get_akari_puzzle_table_image(
        displayed_rows, title=title, footer=footer,
        header=header, cols=cols,
        right_align_cols=right_align_cols, row_colors=row_colors)


def _queens_results_table_rows(guild, rows, *, puzzle_info=None,
                               registrants=None, identity_fn=None,
                               name_fn=None, sort_key_fn=None):
    if identity_fn is None:
        identity_fn = lambda _g, row: getattr(row, 'user_id', '-')
    if name_fn is None:
        name_fn = lambda g, row: _safe_user_name(g, row.user_id)
    annotated = puzzle_info is not None and registrants is not None
    result = []
    for index, row in enumerate(
            _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn),
            start=1):
        name = name_fn(guild, row)
        delta_cell = ''
        if (annotated
                and row.user_id in registrants
                and row.user_id in puzzle_info):
            info = puzzle_info[row.user_id]
            r = round(info.pre_rating)
            name = f'{name} ({r} {rank_for_rating(r).title_abbr})'
            delta_cell = f'{round(info.delta):+d}'
        cells = [
            index,
            name,
            identity_fn(guild, row),
            format_duration(row.time_seconds),
        ]
        if annotated:
            cells.append(delta_cell)
        result.append(tuple(cells))
    return result


def _get_queens_results_table_image_file(guild, rows, title,
                                         *, puzzle_info=None, registrants=None,
                                         identity_label='LinkedIn',
                                         identity_fn=None,
                                         name_fn=None,
                                         sort_key_fn=None):
    rows = _sort_akari_puzzle_results(rows, sort_key_fn=sort_key_fn)
    displayed = rows[:_AKARI_IMAGE_MAX_ROWS]
    displayed_rows = _queens_results_table_rows(
        guild, displayed, puzzle_info=puzzle_info, registrants=registrants,
        identity_fn=identity_fn, name_fn=name_fn, sort_key_fn=sort_key_fn)
    annotated = puzzle_info is not None and registrants is not None
    row_colors = None
    if annotated:
        row_colors = [
            _akari_row_text_color(puzzle_info[row.user_id].pre_rating)
            if row.user_id in registrants and row.user_id in puzzle_info
            else _BLACK
            for row in displayed
        ]
    footer = None
    if len(rows) > len(displayed_rows):
        footer = f'Showing top {len(displayed_rows)} of {len(rows)} results'
    if annotated:
        header = ('#', 'Name', identity_label, 'Time', '\N{INCREMENT}')
        cols = _QUEENS_RESULTS_DELTA_COLS
        right_align_cols = (0, 3, 4)
    else:
        header = ('#', 'Name', identity_label, 'Time')
        cols = _QUEENS_RESULTS_COLS
        right_align_cols = None
    return _get_akari_puzzle_table_image(
        displayed_rows, title=title, footer=footer,
        header=header, cols=cols,
        right_align_cols=right_align_cols, row_colors=row_colors,
        filename='queens-results.png')


def _akari_rating_table_rows(guild, rating_rows, registrants, *,
                             mark_registered=True, identity_fn=None,
                             name_fn=None):
    """Build display rows (#, Name[✓], Handle, Rating · Rank, Games) for the leaderboard.

    ``rating`` is rounded only here for display, and the rank abbreviation
    (N/P/S/E/CM/…) is appended so scanners see the tier without a separate
    column.  When ``mark_registered`` is True, a ``✓`` after the name marks
    users who opted in via ``;mg akari register``; pass False on a registered-only
    view (the marker is redundant when every row is opted in).
    """
    if identity_fn is None:
        identity_fn = lambda g, row: _safe_cf_handle(g, row.user_id)
    if name_fn is None:
        name_fn = lambda g, row: _safe_user_name(g, row.user_id)
    rows = []
    for index, row in enumerate(rating_rows, start=1):
        name = name_fn(guild, row)
        if mark_registered and row.user_id in registrants:
            name = f'{name} \N{CHECK MARK}'
        rating = round(row.rating)
        rank = rank_for_rating(rating)
        rows.append((
            index,
            name,
            identity_fn(guild, row),
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
                                       mark_registered=True,
                                       identity_label='Handle',
                                       identity_fn=None,
                                       name_fn=None):
    displayed = rating_rows[:_AKARI_IMAGE_MAX_ROWS]
    table_rows = _akari_rating_table_rows(
        guild, displayed, registrants, mark_registered=mark_registered,
        identity_fn=identity_fn, name_fn=name_fn)
    row_colors = [_akari_row_text_color(row.rating) for row in displayed]
    footer = None
    if len(rating_rows) > len(table_rows):
        footer = f'Showing top {len(table_rows)} of {len(rating_rows)} rated players'
    return _get_akari_puzzle_table_image(
        table_rows, title=title, footer=footer,
        header=('#', 'Name', identity_label, 'Rating', 'Games'),
        cols=_AKARI_RATING_COLS,
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


def _format_minigame_history_line(point):
    """One rating-history line for date-keyed minigames such as Queens."""
    new_rating = round(point.rating)
    old_rating = round(point.rating - point.delta)
    delta = round(point.delta)
    rank_abbr = rank_for_rating(new_rating).title_abbr
    result_str = format_duration(point.time_seconds)
    if point.is_perfect:
        result_str = f'{result_str} clean'
    date_str = normalize_puzzle_date(point.puzzle_date).isoformat()
    return (
        f'**{date_str}** \N{MIDDLE DOT} {result_str} '
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


class _QueensAnonymousRegisterModal(discord.ui.Modal):
    def __init__(self, cog):
        super().__init__(title='Register for Queens')
        self.cog = cog
        self.linkedin_name = discord.ui.TextInput(
            label='LinkedIn display name',
            placeholder='Name as it appears on the Queens leaderboard',
            required=True,
            max_length=100,
        )
        self.add_item(self.linkedin_name)

    async def on_submit(self, interaction):
        ctx = type('_QueensModalCtx', (), {
            'guild': interaction.guild,
            'author': interaction.user,
        })()
        try:
            claimed = self.cog._cmd_queens_register_link(
                ctx, interaction.user, self.linkedin_name.value,
                anonymous=True)
            link = cf_common.user_db.get_minigame_player_link(
                interaction.guild.id, QUEENS_GAME.name, interaction.user.id)
            lines = [
                f'You are registered for {QUEENS_GAME.display_name} as '
                f'`{_queens_public_link_name(link)}`.',
            ]
            if claimed:
                lines.append(
                    f'Claimed {claimed} stored Queens result(s) and '
                    'recomputed ratings.')
            lines.append(
                self.cog._queens_connection_instruction(interaction.guild.id))
            await interaction.response.send_message(
                embed=discord_common.embed_success('\n'.join(lines)),
                ephemeral=True)
        except MinigameCogError as exc:
            await interaction.response.send_message(
                embed=discord_common.embed_alert(str(exc)),
                ephemeral=True)


class _QueensAnonymousRegisterView(discord.ui.View):
    def __init__(self, cog, requester_id):
        super().__init__(timeout=300)
        self.cog = cog
        self.requester_id = int(requester_id)
        button = discord.ui.Button(
            label='Enter LinkedIn name',
            style=discord.ButtonStyle.primary,
        )
        button.callback = self._open_modal
        self.add_item(button)

    async def interaction_check(self, interaction):
        if int(interaction.user.id) == self.requester_id:
            return True
        await interaction.response.send_message(
            'Only the requester can use this registration prompt.',
            ephemeral=True)
        return False

    async def _open_modal(self, interaction):
        if not await self.interaction_check(interaction):
            return
        await interaction.response.send_modal(
            _QueensAnonymousRegisterModal(self.cog))


class Minigames(commands.Cog):
    GAMES = {
        'akari': AKARI_GAME,
        'guessgame': GUESSGAME_GAME,
        'queens': QUEENS_GAME,
    }

    def __init__(self, bot):
        self.bot = bot
        self._import_tasks = {}   # (guild_id, game_name) -> asyncio.Task
        self._import_status = {}  # (guild_id, game_name) -> dict
        self._queens_pending_imports = {}  # (guild_id, user_id) -> _QueensImportPreview

    async def cog_load(self):
        # ;akari and ;queens are canonical top-level groups; mirror them under
        # ;mg so the nested command paths keep working. Same object in both
        # all_commands dicts -> identical callback dispatch, no parent mutation.
        # Defensive guard: the test harness stubs commands.group, so the
        # group objects don't expose all_commands/get_command — skip in that case.
        if not hasattr(self.minigames, 'all_commands'):
            return
        for group in (self.akari, self.queens):
            if not hasattr(group, 'aliases'):
                continue
            for key in (group.name, *group.aliases):
                if self.minigames.all_commands.get(key) is None:
                    self.minigames.all_commands[key] = group

    async def cog_unload(self):
        import_tasks = list(self._import_tasks.values())
        for task in import_tasks:
            task.cancel()
        if import_tasks:
            await asyncio.gather(*import_tasks, return_exceptions=True)

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
            if game.manual_ingest_only:
                continue
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

    @staticmethod
    def _minigame_banned_user_ids(guild_id, game):
        return {
            str(row.user_id)
            for row in cf_common.user_db.get_minigame_bans(guild_id, game.name)
        }

    def _filter_minigame_banned_rows(self, guild_id, game, rows):
        # Akari has its own ban/opt-out/rating tables; generic bans are for
        # manual minigames such as Queens and must not affect legacy Akari data.
        if game.name == AKARI_GAME.name:
            return rows
        banned = self._minigame_banned_user_ids(guild_id, game)
        if not banned:
            return rows
        return [row for row in rows if str(row.user_id) not in banned]

    @staticmethod
    def _ensure_not_minigame_banned(guild_id, game, user_id, member_name):
        if cf_common.user_db.is_minigame_banned(guild_id, game.name, user_id):
            raise MinigameCogError(
                f'`{member_name}` is banned from {game.display_name}.')

    @staticmethod
    def _get_queens_connection_account(guild_id):
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return {'name': raw, 'url': None}
        name = data.get('name')
        if not name:
            return None
        return {'name': name, 'url': data.get('url')}

    @staticmethod
    def _set_queens_connection_account(guild_id, name, url):
        cf_common.user_db.set_guild_config(
            guild_id,
            _QUEENS_CONNECTION_ACCOUNT_KEY,
            json.dumps({'name': name, 'url': url}, sort_keys=True),
        )

    @staticmethod
    def _clear_queens_connection_account(guild_id):
        cf_common.user_db.delete_guild_config(
            guild_id, _QUEENS_CONNECTION_ACCOUNT_KEY)

    def _queens_connection_instruction(self, guild_id):
        account = self._get_queens_connection_account(guild_id)
        if account is None:
            return (
                'Ask a moderator to set the LinkedIn account to connect with '
                'using `;queens connection set LinkedIn Name profile_url`.'
            )
        if account.get('url'):
            account_text = f'[this]({account["url"]}) account'
        else:
            account_text = 'the configured account'
        return (
            f'In order to join the rating system, send a LinkedIn connection '
            f'request to {account_text} with the note "GF Queens" so your '
            'result appears on the leaderboard.'
        )

    async def _resolve_queens_registration_args(self, ctx, first, rest):
        if first is None:
            raise MinigameCogError(
                'Usage: `;queens register LinkedIn Name [+anon]`.')
        first = str(first).strip()
        rest = (rest or '').strip()
        target = ctx.author
        linkedin = first if not rest else f'{first} {rest}'

        looks_like_target = (
            first.startswith('<@')
            or first.isdigit()
        )
        if looks_like_target:
            if not rest:
                raise MinigameCogError('A LinkedIn display name is required.')
            target = await self._resolve_member(ctx, first)
            target = self._resolve_registrar_target(ctx, target)
            linkedin = rest
        linkedin, anonymous = _split_queens_anonymous_flag(linkedin)
        if not linkedin:
            raise MinigameCogError('A LinkedIn display name is required.')
        return target, linkedin, anonymous

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
            self._recompute_minigame_ratings(guild_id, AKARI_GAME)
        except Exception:
            logger.error('Failed to recompute Akari ratings for guild %s',
                         guild_id, exc_info=True)

    def _recompute_minigame_ratings(self, guild_id, game):
        try:
            rating = game.rating
            if rating is None:
                return
            rows = cf_common.user_db.get_minigame_results_for_guild(
                guild_id, game.name)
            rows = self._filter_minigame_banned_rows(guild_id, game, rows)
            kwargs = self._rating_compute_kwargs(game)
            states = compute_ratings(rows, **kwargs)
            if game.name == AKARI_GAME.name:
                cf_common.user_db.replace_akari_ratings(
                    guild_id, states.values(), time.time())
            else:
                cf_common.user_db.replace_minigame_ratings(
                    guild_id, game.name, states.values(), time.time())
        except Exception:
            logger.error('Failed to recompute %s ratings for guild %s',
                         game.name, guild_id, exc_info=True)

    @staticmethod
    def _rating_compute_kwargs(game):
        rating = game.rating
        if rating is None:
            return {}
        kwargs = {}
        for name in (
                'start_rating', 'damping', 'decay_base', 'decay_max',
                'decay_grace'):
            value = getattr(rating, name)
            if value is not None:
                kwargs[name] = value
        if rating.current_puzzle_number_fn is not None:
            current_puzzle = rating.current_puzzle_number_fn()
            kwargs['current_puzzle_number'] = current_puzzle
            if rating.max_puzzle_lookahead is not None:
                kwargs['max_puzzle'] = (
                    current_puzzle + rating.max_puzzle_lookahead)
        if rating.rank_fn is not None:
            kwargs['rank_fn'] = rating.rank_fn
        return kwargs

    def _minigame_rating_rows(self, guild_id, game, *, excluded_ids=None,
                              included_ids=None):
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        states = compute_ratings(rows, **self._rating_compute_kwargs(game))
        return sorted(
            states.values(),
            key=lambda s: (-s.rating, -s.games, int(s.user_id)),
        )

    def _minigame_user_data(self, guild_id, game, user_id, *,
                            include_decay=False, excluded_ids=None,
                            included_ids=None):
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        histories = {}
        states = compute_ratings(
            rows, histories=histories,
            include_decay_in_history=include_decay,
            **self._rating_compute_kwargs(game))
        key = str(user_id)
        return states.get(key), histories.get(key, [])

    def _minigame_user_history(self, guild_id, game, user_id, *,
                               include_decay=False, excluded_ids=None,
                               included_ids=None):
        state, history = self._minigame_user_data(
            guild_id, game, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids)
        del state
        return history

    def _minigame_puzzle_change_info(self, guild_id, game, puzzle_number, *,
                                     excluded_ids=None, included_ids=None):
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, game.name)
        rows = self._filter_minigame_banned_rows(guild_id, game, rows)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        histories = {}
        compute_ratings(
            rows, histories=histories,
            **self._rating_compute_kwargs(game))
        info = {}
        for user_id, points in histories.items():
            for point in points:
                if point.puzzle_number == puzzle_number:
                    info[user_id] = _PuzzlePlayerInfo(
                        pre_rating=point.rating - point.delta,
                        delta=point.delta,
                    )
                    break
        return info

    @staticmethod
    def _active_ranking_rows(rows, *, include_inactive=False):
        """Keep only recently-active players for the ranking.

        Hides anyone who hasn't played in the last
        ``AKARI_RANKING_MAX_INACTIVE_DAYS`` days, plus any stale future/garbage
        ``last_puzzle`` (e.g. a troll number lingering until the next recompute).
        With ``include_inactive=True`` the day-cutoff is dropped but the
        garbage-future filter still applies — those rows are never a real
        player.
        """
        current = expected_puzzle_number(dt.date.today())
        cutoff = constants.AKARI_RANKING_MAX_INACTIVE_DAYS
        lookahead = constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        if include_inactive:
            return [
                row for row in rows
                if -lookahead <= current - int(row.last_puzzle)
            ]
        return [
            row for row in rows
            if -lookahead <= current - int(row.last_puzzle) <= cutoff
        ]

    # ── Queens helpers ─────────────────────────────────────────────────

    def _cmd_queens_register_link(self, ctx, member, linkedin_text,
                                  anonymous=False):
        self._ensure_not_minigame_banned(
            ctx.guild.id, QUEENS_GAME, member.id, _safe_member_name(member))
        name = _clean_queens_linkedin_name(linkedin_text)
        normalized = normalize_queens_name(name)
        existing = cf_common.user_db.get_minigame_player_link_by_name(
            ctx.guild.id, QUEENS_GAME.name, normalized)
        if existing is not None and str(existing.user_id) != str(member.id):
            existing_label = self._queens_public_user_name(
                ctx.guild, existing.user_id, {str(existing.user_id): existing})
            raise MinigameCogError(
                f'LinkedIn name `{name}` is already linked to '
                f'{existing_label}.')
        external_url = _QUEENS_ANONYMOUS_LINK_MARKER if anonymous else None
        cf_common.user_db.set_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id, name, normalized,
            external_url, time.time(), ctx.author.id)
        claimed = self._claim_queens_unresolved_results(
            ctx.guild.id, member.id, normalized)
        if claimed:
            self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        return claimed

    def _claim_queens_unresolved_results(self, guild_id, user_id,
                                         normalized_name):
        rows = cf_common.user_db.get_minigame_unresolved_results_for_name(
            guild_id, QUEENS_GAME.name, normalized_name)
        if not rows:
            return 0
        for row in rows:
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
            for existing_number in _queens_puzzle_numbers_for_date(puzzle_date):
                cf_common.user_db.delete_minigame_result_for_user_puzzle(
                    guild_id, QUEENS_GAME.name, user_id, existing_number)
            cf_common.user_db.save_minigame_result(
                _queens_result_message_id(guild_id, puzzle_date, user_id),
                guild_id, QUEENS_GAME.name, row.channel_id, user_id,
                puzzle_number, row.puzzle_date, row.accuracy,
                row.time_seconds, row.is_perfect, row.raw_content)
        cf_common.user_db.delete_minigame_unresolved_results_for_name(
            guild_id, QUEENS_GAME.name, normalized_name)
        return len(rows)

    def _resolve_queens_leaderboard(self, ctx, leaderboard, *,
                                    skip_importer=False):
        """Resolve a parsed leaderboard into rated rows + unresolved names.

        ``skip_importer=True`` is the bot-driven mode used by ``;queens play``
        / ``;queens update``: no Discord user is treated as the importer, and
        the "You" row (the bot's own scraper-paced solve) is dropped on sight
        so it never enters the rating pool.  The default ``False`` is the
        manual ``;queens import`` paste path — a human ran the command, their
        Discord-side player_link supplies the "You" row's identity.
        """
        entries = parse_queens_leaderboard(leaderboard)
        if not entries:
            raise MinigameCogError('No LinkedIn Queens leaderboard rows found.')

        importer_link = None
        if not skip_importer:
            importer_link = cf_common.user_db.get_minigame_player_link(
                ctx.guild.id, QUEENS_GAME.name, ctx.author.id)
            if importer_link is None:
                raise MinigameCogError(
                    'Register the importer with `;queens register` before '
                    'importing LinkedIn Queens leaderboard results.')

        resolved = []
        unresolved = []
        seen_users = set()

        for entry in entries:
            normalized = normalize_queens_name(entry.linkedin_name)
            if entry.is_you:
                if skip_importer:
                    # Bot's own row — never imported.
                    continue
                link = importer_link
            else:
                link = cf_common.user_db.get_minigame_player_link_by_name(
                    ctx.guild.id, QUEENS_GAME.name, normalized)
                if link is None:
                    unresolved.append(_QueensResolvedEntry(
                        user_id=None,
                        linkedin_name=entry.linkedin_name,
                        time_seconds=entry.time_seconds,
                        no_hints=entry.no_hints,
                        no_mistakes=entry.no_mistakes,
                    ))
                    continue

            if cf_common.user_db.is_minigame_banned(
                    ctx.guild.id, QUEENS_GAME.name, link.user_id):
                continue
            if link.user_id in seen_users:
                continue
            seen_users.add(link.user_id)
            resolved.append(_QueensResolvedEntry(
                user_id=link.user_id,
                linkedin_name=link.external_name,
                time_seconds=entry.time_seconds,
                no_hints=entry.no_hints,
                no_mistakes=entry.no_mistakes,
            ))

        return resolved, unresolved

    def _make_queens_import_preview(self, ctx, date_text, leaderboard, *,
                                    skip_importer=False):
        puzzle_date = _parse_queens_date(date_text)
        puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
        resolved, unresolved = self._resolve_queens_leaderboard(
            ctx, leaderboard, skip_importer=skip_importer)
        if not resolved and not unresolved:
            raise MinigameCogError(
                'No leaderboard rows matched Queens players.')
        return _QueensImportPreview(
            puzzle_date=puzzle_date,
            puzzle_number=puzzle_number,
            resolved=resolved,
            unresolved=unresolved,
            raw_content=leaderboard,
        )

    def _format_queens_import_preview(self, ctx, preview):
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        lines = [
            f'{QUEENS_GAME.display_name} #{preview.puzzle_number} '
            f'import preview for {preview.puzzle_date.isoformat()}',
            '',
            'Registered:',
        ]
        if preview.resolved:
            for index, entry in enumerate(
                    sorted(preview.resolved, key=lambda e: e.time_seconds), start=1):
                discord_name = self._queens_public_user_name(
                    ctx.guild, entry.user_id, links_by_user)
                link = links_by_user.get(str(entry.user_id))
                li_display = (_queens_public_link_name(link)
                              if link else entry.linkedin_name)
                lines.append(
                    f'{index}. {discord_name} — '
                    f'{_format_queens_result(entry, name_override=li_display)}')
        else:
            lines.append('- none yet')
        if preview.unresolved:
            lines += ['', 'Stored unresolved LinkedIn names:']
            for entry in sorted(preview.unresolved, key=lambda e: e.time_seconds)[:20]:
                lines.append(f'- {_format_queens_result(entry)}')
            if len(preview.unresolved) > 20:
                lines.append(f'- ... and {len(preview.unresolved) - 20} more')
        lines += [
            '',
            'Run `;queens import confirm` to replace saved results for this date.',
        ]
        return '\n'.join(lines)

    def _filter_new_queens_entries(self, guild_id, preview):
        """Strip entries from ``preview`` that already have rows in the DB.

        Used by ``;queens update`` to keep the import additive — never
        overwrites a previously-saved row, only adds new ones.  Returns
        ``(new_resolved, new_unresolved)`` lists.
        """
        new_resolved = []
        for entry in preview.resolved:
            already_saved = False
            for puzzle_number in _queens_puzzle_numbers_for_date(
                    preview.puzzle_date):
                existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
                    guild_id, QUEENS_GAME.name, entry.user_id, puzzle_number)
                if existing is not None:
                    already_saved = True
                    break
            if not already_saved:
                new_resolved.append(entry)

        existing_unresolved_names = set()
        for puzzle_number in _queens_puzzle_numbers_for_date(
                preview.puzzle_date):
            for row in cf_common.user_db.get_minigame_unresolved_results_for_puzzle(
                    guild_id, QUEENS_GAME.name, puzzle_number):
                existing_unresolved_names.add(row.normalized_name)
        new_unresolved = [
            entry for entry in preview.unresolved
            if normalize_queens_name(entry.linkedin_name)
            not in existing_unresolved_names]

        return new_resolved, new_unresolved

    def _save_queens_import(self, ctx, preview, *, skip_wipe=False):
        if not skip_wipe:
            for puzzle_number in _queens_puzzle_numbers_for_date(preview.puzzle_date):
                cf_common.user_db.delete_minigame_results_for_puzzle(
                    ctx.guild.id, QUEENS_GAME.name, puzzle_number)
                cf_common.user_db.delete_minigame_unresolved_results_for_puzzle(
                    ctx.guild.id, QUEENS_GAME.name, puzzle_number)
        for entry in preview.resolved:
            cf_common.user_db.save_minigame_result(
                _queens_result_message_id(
                    ctx.guild.id, preview.puzzle_date, entry.user_id),
                ctx.guild.id, QUEENS_GAME.name, ctx.channel.id, entry.user_id,
                preview.puzzle_number,
                _queens_puzzle_date_text(preview.puzzle_date),
                100 if entry.no_mistakes else 0,
                entry.time_seconds,
                entry.no_hints and entry.no_mistakes,
                preview.raw_content,
            )
        for entry in preview.unresolved:
            cf_common.user_db.save_minigame_unresolved_result(
                ctx.guild.id,
                QUEENS_GAME.name,
                normalize_queens_name(entry.linkedin_name),
                entry.linkedin_name,
                ctx.channel.id,
                preview.puzzle_number,
                _queens_puzzle_date_text(preview.puzzle_date),
                100 if entry.no_mistakes else 0,
                entry.time_seconds,
                entry.no_hints and entry.no_mistakes,
                preview.raw_content,
            )
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        return _QueensImportSaveResult(
            resolved=len(preview.resolved),
            unresolved=len(preview.unresolved),
        )

    @staticmethod
    def _queens_links_by_user(guild_id):
        return {
            str(row.user_id): row
            for row in cf_common.user_db.get_minigame_player_links(
                guild_id, QUEENS_GAME.name)
        }

    def _queens_public_user_name(self, guild, user_id, links_by_user=None):
        del links_by_user
        return _safe_user_name(guild, user_id)

    def _queens_name_fn(self, links_by_user):
        return lambda guild, row: self._queens_public_user_name(
            guild, row.user_id, links_by_user)

    def _minigame_public_user_name(self, guild, game, user_id):
        if game.name == QUEENS_GAME.name:
            return self._queens_public_user_name(guild, user_id)
        return _safe_user_name(guild, user_id)

    def _require_queens_registered_member(self, guild_id, member):
        link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member.id)
        if link is None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not registered for '
                f'{QUEENS_GAME.display_name} (`;queens register LinkedIn Name`).')
        return link

    def _queens_rating_identity_fn(self, links_by_user):
        return lambda _guild, row: (
            _queens_public_link_name(links_by_user.get(str(row.user_id)))
            if str(row.user_id) in links_by_user
            else '-'
        )

    def _queens_legend_name(self, guild_id, member):
        link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member.id)
        if link is not None:
            return _queens_public_link_name(link)
        return _safe_member_name(member)

    async def _resolve_queens_linked_player(self, ctx, player_text):
        player_text = str(player_text or '').strip()
        if not player_text:
            raise MinigameCogError(
                'A Discord user or registered LinkedIn name is required.')

        try:
            member = await self._resolve_member(ctx, player_text)
        except MinigameCogError:
            member = None
        if member is not None:
            link = cf_common.user_db.get_minigame_player_link(
                ctx.guild.id, QUEENS_GAME.name, member.id)
            if link is None:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` is not registered for '
                    f'{QUEENS_GAME.display_name}.')
            return (
                str(member.id),
                self._queens_public_user_name(
                    ctx.guild, member.id, {str(member.id): link}),
                link,
            )

        name = _clean_queens_linkedin_name(player_text)
        link = cf_common.user_db.get_minigame_player_link_by_name(
            ctx.guild.id, QUEENS_GAME.name, normalize_queens_name(name))
        if link is None:
            raise MinigameCogError(
                f'Could not find a Discord user or registered LinkedIn name '
                f'for `{discord.utils.escape_mentions(player_text)}`.')
        label = self._queens_public_user_name(
            ctx.guild, link.user_id, {str(link.user_id): link})
        return str(link.user_id), label, link

    @staticmethod
    def _parse_queens_add_args(args):
        tokens = str(args or '').split()
        if len(tokens) < 3:
            raise MinigameCogError(
                'Usage: `;queens add <@user|LinkedIn Name> DATE/# time '
                '[status...]`.')
        for index in range(1, len(tokens) - 1):
            try:
                parsed_date = _parse_queens_date_or_number(tokens[index])
                parse_queens_time(tokens[index + 1])
            except (MinigameCogError, ValueError):
                continue
            player_text = ' '.join(tokens[:index]).strip()
            status = ' '.join(tokens[index + 2:]).strip()
            return (
                player_text,
                parsed_date,
                tokens[index + 1],
                status or 'No hints & no mistakes',
            )
        raise MinigameCogError(
            'Usage: `;queens add <@user|LinkedIn Name> DATE/# time [status...]`.')

    @staticmethod
    def _parse_queens_remove_args(args):
        tokens = str(args or '').split()
        if len(tokens) < 2:
            raise MinigameCogError(
                'Usage: `;queens remove <@user|LinkedIn Name> DATE/#`.')
        try:
            parsed_date = _parse_queens_date_or_number(tokens[-1])
        except MinigameCogError as exc:
            raise MinigameCogError(
                'Usage: `;queens remove <@user|LinkedIn Name> DATE/#`.') from exc
        player_text = ' '.join(tokens[:-1]).strip()
        return player_text, parsed_date

    async def _extract_queens_rating_filters(self, ctx, args):
        (remaining, include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._extract_akari_filters(ctx, args)
        if include_decay:
            raise MinigameCogError(
                f'{QUEENS_GAME.display_name} ratings do not use decay.')
        return remaining, excluded_ids, included_ids

    async def _parse_queens_rating_args(self, ctx, args, *,
                                        member_required=False):
        remaining, excluded_ids, included_ids = (
            await self._extract_queens_rating_filters(ctx, args))
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return members, excluded_ids, included_ids

    async def _cmd_queens_ratings(self, ctx, *, show_all=False,
                                  excluded_ids=None, included_ids=None):
        if excluded_ids or included_ids:
            rows = self._minigame_rating_rows(
                ctx.guild.id, QUEENS_GAME,
                excluded_ids=excluded_ids, included_ids=included_ids)
        else:
            rows = cf_common.user_db.get_minigame_ratings(
                ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} ratings yet.')
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        linked_ids = set(links_by_user)
        shown = rows if show_all else [row for row in rows if row.user_id in linked_ids]
        if not shown:
            raise MinigameCogError(
                f'No registered {QUEENS_GAME.display_name} players yet. '
                f'Players register with `;queens register LinkedIn Name`.')
        title = (f'{QUEENS_GAME.display_name} Ratings (all)'
                 if show_all else f'{QUEENS_GAME.display_name} Ratings')
        discord_file = _get_akari_rating_table_image_file(
            ctx.guild, shown, linked_ids,
            title=title,
            mark_registered=show_all,
            identity_label='LinkedIn',
            identity_fn=self._queens_rating_identity_fn(links_by_user),
            name_fn=self._queens_name_fn(links_by_user))
        await ctx.send(file=discord_file)

    async def _cmd_queens_rating(self, ctx, members, *,
                                 require_registered=True,
                                 excluded_ids=None, included_ids=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            for member in members:
                self._require_queens_registered_member(ctx.guild.id, member)

        filtered = bool(excluded_ids or included_ids)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._minigame_user_data(
                    ctx.guild.id, QUEENS_GAME, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids)
            else:
                row = cf_common.user_db.get_minigame_rating(
                    ctx.guild.id, QUEENS_GAME.name, member.id)
                history = self._minigame_user_history(
                    ctx.guild.id, QUEENS_GAME, member.id)
            if row is None:
                raise MinigameCogError(
                    f'No {QUEENS_GAME.display_name} rating for '
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` yet.')
            if not history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no rated '
                    f'{QUEENS_GAME.display_name} days to plot yet.')
            per_member.append((member, row, history))

        series = [
            (history, self._queens_legend_name(ctx.guild.id, member))
            for member, _row, history in per_member
        ]
        discord_file = plot_akari_rating(series)

        if len(per_member) == 1:
            member, row, history = per_member[0]
            display_name = self._queens_public_user_name(ctx.guild, member.id)
            rating = round(row.rating)
            rank = rank_for_rating(rating)
            peak_rank = rank_for_rating(round(row.peak))
            last_contest = next((h for h in reversed(history)
                                 if h.performance is not None), None)
            last_change_str = (f'{last_contest.delta:+.0f}'
                               if last_contest is not None else '—')
            last_perf_str = (
                f'{round(last_contest.performance)} '
                f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                if last_contest is not None else '—')
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} rating — '
                       f'{display_name}'),
                color=rank.color_embed,
            )
            embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
            embed.add_field(name='Peak', value=f'{round(row.peak)} ({peak_rank.title_abbr})')
            embed.add_field(name='Games', value=str(row.games))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            _top_member, top_row, _history = max(
                per_member, key=lambda t: t[1].rating)
            top_rank = rank_for_rating(round(top_row.rating))
            lines = [
                f'**{self._queens_public_user_name(ctx.guild, member.id)}**: '
                f'{round(row.rating)} '
                f'({rank_for_rating(round(row.rating)).title_abbr})'
                for member, row, _history in per_member
            ]
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} ratings — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_queens_performance(self, ctx, members, *,
                                      require_registered=True,
                                      excluded_ids=None, included_ids=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            for member in members:
                self._require_queens_registered_member(ctx.guild.id, member)

        filtered = bool(excluded_ids or included_ids)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._minigame_user_data(
                    ctx.guild.id, QUEENS_GAME, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids)
            else:
                row = cf_common.user_db.get_minigame_rating(
                    ctx.guild.id, QUEENS_GAME.name, member.id)
                history = self._minigame_user_history(
                    ctx.guild.id, QUEENS_GAME, member.id)
            if row is None:
                raise MinigameCogError(
                    f'No {QUEENS_GAME.display_name} rating for '
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` yet.')
            contest_history = [h for h in history if h.performance is not None]
            if not contest_history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no contested '
                    f'{QUEENS_GAME.display_name} days to plot performance for yet.')
            per_member.append((member, row, history, contest_history))

        series = [
            (history, self._queens_legend_name(ctx.guild.id, member),
             round(row.rating))
            for member, row, history, _contest_history in per_member
        ]
        discord_file = plot_akari_performance(series)

        if len(per_member) == 1:
            member, _row, _history, contest_history = per_member[0]
            display_name = self._queens_public_user_name(ctx.guild, member.id)
            last_perf = contest_history[-1].performance
            last_rank = rank_for_rating(round(last_perf))
            best_perf = max(h.performance for h in contest_history)
            best_rank = rank_for_rating(round(best_perf))
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} performance — '
                       f'{display_name}'),
                color=last_rank.color_embed,
            )
            embed.add_field(name='Last performance',
                            value=f'{round(last_perf)} ({last_rank.title_abbr})')
            embed.add_field(name='Best performance',
                            value=f'{round(best_perf)} ({best_rank.title_abbr})')
            embed.add_field(name='Contests', value=str(len(contest_history)))
        else:
            top_rank = rank_for_rating(round(max(
                contest_history[-1].performance
                for _member, _row, _history, contest_history in per_member)))
            lines = [
                f'**{self._queens_public_user_name(ctx.guild, member.id)}**: '
                f'last {round(contest_history[-1].performance)} '
                f'({rank_for_rating(round(contest_history[-1].performance)).title_abbr})'
                for member, _row, _history, contest_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{QUEENS_GAME.display_name} performance — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_queens_history(self, ctx, member, *,
                                  require_registered=True,
                                  excluded_ids=None, included_ids=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if require_registered:
            self._require_queens_registered_member(ctx.guild.id, member)

        history = self._minigame_user_history(
            ctx.guild.id, QUEENS_GAME, member.id,
            excluded_ids=excluded_ids, included_ids=included_ids)
        contest_history = [h for h in history if h.performance is not None]
        if not contest_history:
            raise MinigameCogError(
                f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no contested '
                f'{QUEENS_GAME.display_name} days yet.')

        lines = [_format_minigame_history_line(h)
                 for h in reversed(contest_history)]
        title = (f'{QUEENS_GAME.display_name} rating history — '
                 f'{self._queens_public_user_name(ctx.guild, member.id)} '
                 f'({len(contest_history)} contests)')
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

    async def _cmd_queens_show(self, ctx):
        enabled = self._is_enabled(ctx.guild.id, QUEENS_GAME.feature_flag)
        links = cf_common.user_db.get_minigame_player_links(
            ctx.guild.id, QUEENS_GAME.name)
        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, QUEENS_GAME.name)
        dates = {_format_queens_date(row) for row in rows}
        account = self._get_queens_connection_account(ctx.guild.id)
        account_text = 'not set'
        if account is not None:
            account_text = account['name']
            if account.get('url'):
                account_text += f' <{account["url"]}>'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            'ingest: manual leaderboard import',
            f'connection account: {account_text}',
            f'linked players: **{len(links)}**',
            f'results: **{len(rows)}** across **{len(dates)}** date(s)',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {QUEENS_GAME.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_queens_streak(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
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
            ctx.guild.id, QUEENS_GAME.name, member.id, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        display_name = self._queens_public_user_name(ctx.guild, member.id)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'`{display_name}`.')

        current, longest, latest = _queens_streak_info(rows)
        latest_status = (
            'no hints & no mistakes'
            if latest.is_perfect
            else 'not clean'
        )
        description = '\n'.join([
            f'`{display_name}`: **{current}** consecutive clean day(s)',
            f'Longest clean streak: **{longest}** day(s)',
            f'Latest result: **{_format_queens_date(latest)}**, **{format_duration(latest.time_seconds)}**, {latest_status}',
        ])
        await ctx.send(embed=discord.Embed(
            title=f'{QUEENS_GAME.display_name} Streak',
            description=description,
            color=discord_common.random_cf_color(),
        ))

    async def _cmd_queens_stats(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
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
            ctx.guild.id, QUEENS_GAME.name, member.id, dlo, dhi, plo, phi)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        best = _queens_best_results_by_date(rows)
        display_name = self._queens_public_user_name(ctx.guild, member.id)
        if not best:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'`{display_name}`.')

        results = [best[day] for day in sorted(best)]
        total = len(results)
        clean = [row for row in results if row.is_perfect]
        no_mistakes = [row for row in results if int(row.accuracy) == 100]
        times = [int(row.time_seconds) for row in results]
        current, longest, latest = _queens_streak_info(results)
        clean_rate = len(clean) / total * 100 if total else 0
        lines = [
            f'Player: `{display_name}`',
            f'Queens days: **{total}**',
            f'Clean: **{len(clean)}** ({clean_rate:.0f}%)',
            f'No mistakes: **{len(no_mistakes)}**',
            '',
            f'Best time: **{format_duration(min(times))}**',
            f'Average time: **{format_duration(sum(times) / len(times))}**',
            f'Median time: **{format_duration(statistics.median(times))}**',
            '',
            f'Current clean streak: **{current}**',
            f'Longest clean streak: **{longest}**',
            f'Latest: **{_format_queens_date(latest)}** in **{format_duration(latest.time_seconds)}**',
        ]
        await ctx.send(embed=discord.Embed(
            title=f'{QUEENS_GAME.display_name} Stats',
            description='\n'.join(lines),
            color=discord_common.random_cf_color(),
        ))

    async def _cmd_queens_stats_date(self, ctx, date_arg, *,
                                     show_all=False, excluded_ids=None,
                                     included_ids=None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        puzzle_date = _parse_queens_date_or_number(date_arg)
        puzzle_number = _queens_puzzle_number_for_date(puzzle_date)
        day_start = dt.datetime.combine(puzzle_date, dt.time.min).timestamp()
        day_end = dt.datetime.combine(
            puzzle_date + dt.timedelta(days=1), dt.time.min).timestamp()
        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, QUEENS_GAME.name, dlo=day_start, dhi=day_end)
        rows = self._filter_minigame_banned_rows(ctx.guild.id, QUEENS_GAME, rows)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'`{puzzle_date.isoformat()}`.')

        links_by_user = self._queens_links_by_user(ctx.guild.id)
        puzzle_numbers = {int(row.puzzle_number) for row in rows}
        puzzle_info = None
        registrants = None
        if len(puzzle_numbers) == 1:
            puzzle_info = self._minigame_puzzle_change_info(
                ctx.guild.id, QUEENS_GAME, next(iter(puzzle_numbers)),
                excluded_ids=excluded_ids, included_ids=included_ids)
            registrants = (
                set(puzzle_info.keys())
                if show_all
                else set(links_by_user)
            )
        discord_file = _get_queens_results_table_image_file(
            ctx.guild, rows,
            f'{QUEENS_GAME.display_name} #{puzzle_number} '
            f'{puzzle_date.isoformat()} Results',
            puzzle_info=puzzle_info,
            registrants=registrants,
            identity_label='LinkedIn',
            identity_fn=self._queens_rating_identity_fn(links_by_user),
            name_fn=self._queens_name_fn(links_by_user),
            sort_key_fn=lambda row: (
                int(getattr(row, 'time_seconds', 0)),
                int(getattr(row, 'message_id', 0)),
            ))
        await ctx.send(file=discord_file)

    # ── Queens scraper plumbing (used by ;queens play / update / login) ───

    @staticmethod
    def _queens_state_path(guild_id):
        """Resolve the storage_state.json path for this guild.

        Per-guild override stored in guild_config; falls back to the
        scraper's default (``extra/.queens_state.json`` next to the script).
        Returns a ``pathlib.Path``.
        """
        raw = cf_common.user_db.get_guild_config(
            guild_id, _QUEENS_STATE_PATH_KEY)
        if raw:
            return pathlib.Path(raw).expanduser()
        return _QUEENS_DEFAULT_STATE_PATH

    async def _run_queens_scraper(self, guild_id, *, auto_play):
        """Spawn the scraper's ``fetch`` subprocess.

        ``auto_play=True`` makes the scraper solve today's puzzle if the
        leaderboard isn't visible (used by ``;queens play``).
        ``auto_play=False`` only fetches what's currently visible (used by
        ``;queens update``).

        Returns ``(payload, error_message)``: exactly one is non-None.
        The payload is the parsed JSON dict including the ``status`` field
        (``ok`` / ``not_played`` / ``session_expired`` / ``error``).
        """
        if not _QUEENS_SCRAPER_SCRIPT.exists():
            return None, (
                f'Scraper script missing at `{_QUEENS_SCRAPER_SCRIPT}`.')
        state_path = self._queens_state_path(guild_id)
        cmd = [sys.executable, str(_QUEENS_SCRAPER_SCRIPT),
               '--state', str(state_path), 'fetch', '--json']
        if auto_play:
            cmd.append('--auto-play')
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8',
                     'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                        _QUEENS_PLAYWRIGHT_PLATFORM},
            )
        except FileNotFoundError as exc:
            return None, f'Could not launch scraper: {exc}'
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=_QUEENS_SCRAPER_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return None, (
                f'Scraper timed out after {_QUEENS_SCRAPER_TIMEOUT}s.')
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        if not stdout_text:
            tail = stderr_text or '(no output)'
            return None, f'Scraper produced no output. stderr: `{tail[-800:]}`'
        try:
            payload = json.loads(stdout_text.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return None, (
                f'Could not parse scraper output as JSON: {exc}. '
                f'Tail of stdout: ```{stdout_text[-800:]}```')
        if not isinstance(payload, dict):
            return None, f'Scraper JSON was not an object: `{payload!r}`'
        return payload, None

    async def _run_queens_whoami(self, guild_id):
        """Run the scraper's ``whoami`` subcommand.

        Returns ``(name, error_message)``: exactly one is non-None.
        Same JSON-status conventions as ``_run_queens_scraper``.
        """
        state_path = self._queens_state_path(guild_id)
        if not state_path.exists():
            return None, (
                f'No session file at `{state_path}`. '
                'Upload one with `;queens login` (attach state.json).')
        if not _QUEENS_SCRAPER_SCRIPT.exists():
            return None, f'Scraper script missing at `{_QUEENS_SCRAPER_SCRIPT}`.'
        cmd = [sys.executable, str(_QUEENS_SCRAPER_SCRIPT),
               '--state', str(state_path), 'whoami']
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8',
                     'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                        _QUEENS_PLAYWRIGHT_PLATFORM},
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=_QUEENS_WHOAMI_TIMEOUT)
        except (FileNotFoundError, asyncio.TimeoutError) as exc:
            return None, f'whoami failed: {exc}'
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        if not stdout_text:
            tail = stderr_text or '(no stderr either)'
            return None, f'whoami produced no output. stderr: ```{tail[-600:]}```'
        try:
            payload = json.loads(stdout_text.splitlines()[-1])
        except json.JSONDecodeError as exc:
            return None, f'whoami JSON parse error: {exc}'
        status = payload.get('status')
        if status == 'ok':
            return payload.get('name'), None
        if status == 'session_expired':
            return None, 'Session expired — upload a fresh state file.'
        return None, payload.get('error') or f'whoami status: {status}'

    @staticmethod
    def _queens_status_message(status):
        """Human-readable fallback for an unexpected scraper status string."""
        return {
            'session_expired': (
                'LinkedIn session has expired. A mod needs to run '
                '`;queens login` with a fresh state file.'),
            'not_played': (
                "The bot hasn't solved today's Queens puzzle yet. "
                'Ask a mod to run `;queens play`.'),
        }.get(status, f'Unexpected scraper status: `{status}`')

    async def _do_queens_import(self, ctx, payload, *, source_label):
        """Apply a scraper payload's ``raw_text`` to the DB additively.

        Used by both ``;queens play`` and ``;queens update``.  Neither
        wipes previously-saved rows; only entries that don't already
        have a row get inserted.  The bot's own ``You`` row is dropped
        on sight via ``skip_importer``.

        Posts a success embed listing every entry that was added (both
        resolved and unresolved).
        """
        raw_text = payload.get('raw_text') or ''
        today_iso = dt.datetime.now(dt.timezone.utc).date().isoformat()
        preview = self._make_queens_import_preview(
            ctx, today_iso, raw_text, skip_importer=True)

        new_resolved, new_unresolved = self._filter_new_queens_entries(
            ctx.guild.id, preview)
        if not new_resolved and not new_unresolved:
            await ctx.send(embed=discord_common.embed_neutral(
                f'{source_label} of {QUEENS_GAME.display_name} '
                f'#{preview.puzzle_number} {today_iso}:\n'
                'No new results since the last refresh.'))
            return

        preview = preview._replace(
            resolved=new_resolved, unresolved=new_unresolved)
        self._save_queens_import(ctx, preview, skip_wipe=True)

        await ctx.send(embed=self._format_queens_save_embed(
            ctx, preview, source_label, today_iso))

    def _format_queens_save_embed(self, ctx, preview, source_label, today_iso):
        """Build the success embed listing every entry that was added.

        Resolved entries use the *public* link name (``Anonymous`` for
        anonymously-registered users); unresolved entries show the raw
        scraped name (no Discord user is claiming it).
        """
        links_by_user = self._queens_links_by_user(ctx.guild.id)
        lines = [
            f'{source_label} of {QUEENS_GAME.display_name} '
            f'#{preview.puzzle_number} {today_iso}',
        ]
        if preview.resolved:
            lines.append('')
            lines.append(f'Added **{len(preview.resolved)}** result(s):')
            for index, entry in enumerate(
                    sorted(preview.resolved, key=lambda e: e.time_seconds),
                    start=1):
                discord_name = self._queens_public_user_name(
                    ctx.guild, entry.user_id, links_by_user)
                link = links_by_user.get(str(entry.user_id))
                li_display = (_queens_public_link_name(link)
                              if link else entry.linkedin_name)
                lines.append(
                    f'{index}. {discord_name} — '
                    f'{_format_queens_result(entry, name_override=li_display)}')
        if preview.unresolved:
            lines.append('')
            lines.append(
                f'Added **{len(preview.unresolved)}** unresolved '
                'LinkedIn name(s):')
            for entry in sorted(
                    preview.unresolved, key=lambda e: e.time_seconds)[:20]:
                lines.append(f'- {_format_queens_result(entry)}')
            if len(preview.unresolved) > 20:
                lines.append(
                    f'- ... and {len(preview.unresolved) - 20} more')
        return discord_common.embed_success('\n'.join(lines))

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

    async def _notify_non_pro_mode(self, message):
        """Reply to a non-pro Daily Akari submission asking the user to enable Pro Mode.

        Same best-effort pattern as :meth:`_notify_banned_submission` —
        a failed reply is logged and swallowed so the ingestion path can't be
        broken by a notice failure.
        """
        embed = discord_common.embed_alert(
            "Your result doesn't include accuracy. Please turn on "
            "Pro Mode \U0001f3af\U0001f31f in the settings and submit "
            "again for it to count.")
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify non-pro mode for message %s',
                           message.id, exc_info=True)

    async def _notify_banned_submission(self, message, game):
        """Reply to a banned user's parsable Akari post explaining the ban.

        Only called after we've confirmed the message *would have* produced a
        result — chat messages from banned users in the Akari channel stay
        silent so we don't spam unrelated conversation.  Best-effort: a failed
        reply (deleted message, missing perms) is logged and swallowed so the
        ingestion path can't be broken by a notice failure.
        """
        ban = cf_common.user_db.get_akari_ban(message.guild.id, message.author.id)
        reason = ban.reason if ban is not None else None
        body = f'You are banned from posting {game.display_name} results.'
        if reason:
            body += f'\nReason: {reason}'
        body += '\nAsk a moderator to lift the ban.'
        embed = discord_common.embed_alert(body)
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify banned user for message %s',
                           message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(message)
        if game is not None:
            try:
                cleaned = strip_codeblock(message.content)
                is_submission = bool(game.parse(cleaned)) or (
                    game.name == AKARI_GAME.name
                    and looks_like_non_pro_akari(message.content))
                if self._is_akari_banned(message.guild.id, message.author.id, game):
                    # Reply only if this post is a submission attempt — banned
                    # users chatting in the channel stay silent.
                    if is_submission:
                        await self._notify_banned_submission(message, game)
                    return  # never save/ingest for banned users
                # Save raw content for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, message.guild.id, message.channel.id,
                    message.author.id, message.created_at.isoformat(),
                    message.content,
                )
                # Non-pro mode submissions look like results but lack accuracy;
                # ask the user to enable Pro Mode and skip the ingest.
                if (game.name == AKARI_GAME.name
                        and looks_like_non_pro_akari(message.content)):
                    await self._notify_non_pro_mode(message)
                    return
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
        is_non_pro = (game.name == AKARI_GAME.name
                      and looks_like_non_pro_akari(after.content))
        if self._is_akari_banned(after.guild.id, after.author.id, game):
            try:
                if game.parse(strip_codeblock(after.content)) or is_non_pro:
                    await self._notify_banned_submission(after, game)
            except Exception:
                logger.warning('Failed to notify banned edit %s',
                               after.id, exc_info=True)
            return  # leave pre-ban data untouched
        try:
            # Update raw content so future reparse uses the edited version
            cf_common.user_db.update_raw_message(after.id, after.content)
            # An edit into a non-pro shape: drop any prior result for this
            # message and tell the user.  Same skip-the-ingest path on_message
            # uses for fresh non-pro posts.
            if is_non_pro:
                changed = cf_common.user_db.delete_minigame_result(after.id)
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
                await self._notify_non_pro_mode(after)
                if changed:
                    self._recompute_akari_ratings(after.guild.id)
                return
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
        rows1 = self._filter_minigame_banned_rows(ctx.guild.id, game, rows1)
        rows2 = self._filter_minigame_banned_rows(ctx.guild.id, game, rows2)
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
        name1 = self._minigame_public_user_name(ctx.guild, game, member1.id)
        name2 = self._minigame_public_user_name(ctx.guild, game, member2.id)
        description = '\n'.join([
            f'`{name1}`: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'`{name2}`: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
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
        rows = self._filter_minigame_banned_rows(ctx.guild.id, game, rows)
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
                name = self._minigame_public_user_name(ctx.guild, game, user_id)
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

    async def _cmd_akari_add(self, ctx, member, puzzle_number, result_text, time_text):
        """Mod-only: manually insert an Akari result for a (user, puzzle) pair.

        For backfilling missed posts or posts that landed in the wrong channel.
        The row goes into the live result table keyed on the command/interaction
        message id, so deleting the originating message removes the row (the
        same path the normal ingestion uses for edits/deletes).
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)

        # ── Parse result ───────────────────────────────────────────────
        cleaned = result_text.strip().lower().lstrip('\U0001f31f').strip()
        if cleaned in ('perfect', '\U0001f31f'):
            is_perfect, accuracy = True, 100
        else:
            cleaned = cleaned.rstrip('%').strip()
            try:
                n = int(cleaned)
            except ValueError:
                raise MinigameCogError(
                    f'Could not parse result `{result_text}` \N{EM DASH} '
                    f'expected `perfect` or `N%`.')
            if not 0 <= n <= 100:
                raise MinigameCogError(
                    f'Accuracy must be between 0 and 100, got `{n}`.')
            is_perfect = n == 100
            accuracy = n

        # ── Parse time (mirrors _minigame_akari._parse_time) ──────────
        try:
            parts = [int(p) for p in time_text.split(':')]
        except ValueError:
            raise MinigameCogError(f'Could not parse time `{time_text}`.')
        if len(parts) == 2:
            time_seconds = parts[0] * 60 + parts[1]
        elif len(parts) == 3:
            time_seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
        else:
            raise MinigameCogError(
                f'Time `{time_text}` must be `M:SS` or `H:MM:SS`.')
        if time_seconds < 0:
            raise MinigameCogError(f'Time must be non-negative.')

        # ── Validate puzzle number ─────────────────────────────────────
        today_puzzle = expected_puzzle_number(dt.date.today())
        if puzzle_number < 1 or puzzle_number > today_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD:
            raise MinigameCogError(
                f'Puzzle number `{puzzle_number}` is out of range '
                f'(today\'s puzzle is `{today_puzzle}`).')
        puzzle_date = puzzle_date_for(puzzle_number)

        existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
            ctx.guild.id, AKARI_GAME.name, member.id, puzzle_number)
        if existing is not None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` already has a result for '
                f'puzzle `{puzzle_number}`. Use `;mg akari remove` first.')

        result_label = 'perfect' if is_perfect else f'{accuracy}%'
        raw_content = (
            f'Daily Akari {puzzle_number}\n'
            f'{puzzle_date.isoformat()}\n'
            f'\U0001f3af {result_label} \U0001f553 {time_text}\n'
            f'[manually added by {ctx.author}]'
        )
        cf_common.user_db.save_minigame_result(
            ctx.message.id, ctx.guild.id, AKARI_GAME.name, ctx.channel.id,
            member.id, puzzle_number, puzzle_date.isoformat(),
            accuracy, time_seconds, is_perfect, raw_content)

        self._recompute_akari_ratings(ctx.guild.id)

        await ctx.send(embed=discord_common.embed_success(
            f'Added {AKARI_GAME.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_number}` '
            f'({puzzle_date.isoformat()}): **{result_label}** in '
            f'**{format_duration(time_seconds)}**.'))

    async def _cmd_akari_ratings(self, ctx, *, excluded_ids=None,
                                  included_ids=None, include_inactive=False):
        """Guild leaderboard — registered, recently-active players only.

        ``excluded_ids`` / ``included_ids`` run an ad-hoc replay with the
        chosen filter applied and render the result, leaving the persisted
        snapshot untouched so the cache stays canonical.

        ``include_inactive=True`` (the ``+inactive`` arg) skips the
        ``AKARI_RANKING_MAX_INACTIVE_DAYS`` cutoff so dormant players
        reappear on the board.  Garbage future puzzle numbers are still
        filtered out — they're never a real player, just a stale row.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if excluded_ids or included_ids:
            rows = self._akari_filtered_rating_rows(
                ctx.guild.id, excluded_ids=excluded_ids,
                included_ids=included_ids)
        else:
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
        shown = self._active_ranking_rows(
            registered, include_inactive=include_inactive)
        if not shown:
            if include_inactive:
                raise MinigameCogError(
                    f'No registered {AKARI_GAME.display_name} players yet.')
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players active in '
                f'the last {constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days. '
                f'Use `+inactive` to include dormant players.')
        # All shown users are registered, so the ✓ marker is redundant noise.
        title = ('Daily Akari Ratings (incl. inactive)'
                 if include_inactive else 'Daily Akari Ratings')
        discord_file = _get_akari_rating_table_image_file(
            ctx.guild, shown, registrants, title=title, mark_registered=False)
        await ctx.send(file=discord_file)

    def _akari_user_history(self, guild_id, user_id, *, include_decay=False,
                            excluded_ids=None, included_ids=None):
        """Replay the guild's results and return one user's per-day history.

        Shared by the rating and performance graphs — the replay is the same;
        each caller picks the field it needs off the :class:`HistoryPoint`s.
        ``include_decay=True`` additionally emits one entry per absent puzzle
        day for the rating graph's ``+decay`` mode.  ``excluded_ids`` and
        ``included_ids`` (sets of stringified user IDs) compose the include /
        exclude filter before the replay so the queried user's history
        reflects only the surviving field.
        """
        state, history = self._akari_user_data(
            guild_id, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids)
        del state  # this helper returns history only; callers needing both use _akari_user_data
        return history

    def _akari_user_data(self, guild_id, user_id, *, include_decay=False,
                          excluded_ids=None, included_ids=None):
        """One replay, two artefacts: ``(RatingState, [HistoryPoint])`` for one user.

        Used by rating / performance commands that show both an embed (needs
        the snapshot-shaped state) and a graph (needs the history).  Saves a
        second replay versus calling ``_akari_user_history`` separately.
        Returns ``(None, [])`` when the user has no rated days.
        """
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        histories = {}
        states = compute_ratings(
            result_rows, max_puzzle=max_puzzle, histories=histories,
            include_decay_in_history=include_decay,
            current_puzzle_number=current_puzzle)
        key = str(user_id)
        return states.get(key), histories.get(key, [])

    def _akari_filtered_rating_rows(self, guild_id, *, excluded_ids=None,
                                     included_ids=None):
        """Fresh leaderboard states with some users excluded/included — bypasses cache.

        Used by ``;mg akari ratings +exclude=...`` / ``+include=...`` so the
        persisted snapshot (the canonical rating store) stays untouched while
        we render an ad-hoc view.  Returns the same
        ``rating DESC, games DESC, user_id ASC`` order ``get_akari_ratings``
        produces, so the rest of the rendering path doesn't care which source
        it got.
        """
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        states = compute_ratings(
            rows, max_puzzle=max_puzzle,
            current_puzzle_number=current_puzzle)
        return sorted(
            states.values(),
            key=lambda s: (-s.rating, -s.games, int(s.user_id)),
        )

    def _akari_puzzle_change_info(self, guild_id, puzzle_number,
                                   *, excluded_ids=None, included_ids=None):
        """Map ``user_id -> _PuzzlePlayerInfo(pre_rating, delta)`` for puzzle N.

        Replays the full guild history once and pulls each user's HistoryPoint
        for the target puzzle; the pre-contest rating is the post-contest one
        minus the day's delta (so first-timers get the seed value, 1200).
        Used by ``;mg akari stats <puzzle>`` to colour each row by the
        player's pre-puzzle tier (post-puzzle would be circular) and to fill
        the Δ column with the day's signed change.  ``excluded_ids`` /
        ``included_ids`` apply the same include / exclude filter as the rest
        of the command surface so the surfaced pre-rating and delta reflect
        the chosen field.
        """
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        histories = {}
        compute_ratings(
            result_rows, max_puzzle=max_puzzle, histories=histories,
            current_puzzle_number=current_puzzle)
        info = {}
        for user_id, points in histories.items():
            for point in points:
                if point.puzzle_number == puzzle_number:
                    info[user_id] = _PuzzlePlayerInfo(
                        pre_rating=point.rating - point.delta,
                        delta=point.delta,
                    )
                    break
        return info

    async def _extract_akari_filters(self, ctx, args):
        """Pull akari-wide filter flags out of ``args``.

        Recognised flags:

        - ``+decay``: include decay days in history/graph output
        - ``+inactive``: keep players whose last puzzle is older than
          ``AKARI_RANKING_MAX_INACTIVE_DAYS`` (default behaviour hides them
          from the ratings leaderboard).  Only meaningful for commands that
          surface an active-only leaderboard; harmless elsewhere.
        - ``+exclude=user1,user2,...``: pretend the listed users never played;
          they drop out of result tables, leaderboards, and every other user's
          rating calculation
        - ``+include=user1,user2,...``: the inverse — *only* the listed users
          count; everyone else is dropped before the replay.  When both flags
          are supplied they compose: the universe shrinks to the include set
          first, then the exclude set is removed from it.

        Each comma-separated name is resolved via the usual case-insensitive
        member converter, so mentions / display names / raw IDs all work.

        Returns ``(remaining_args, include_decay, excluded_ids, included_ids,
        include_inactive)``.  Unknown flags pass through in ``remaining_args``;
        the caller decides whether they're a member, a puzzle selector, or an
        error.
        """
        remaining = []
        include_decay = False
        include_inactive = False
        excluded_ids = set()
        included_ids = set()
        for arg in args:
            if arg == '+decay':
                include_decay = True
            elif arg == '+inactive':
                include_inactive = True
            elif arg.startswith('+exclude=') or arg.startswith('+include='):
                positive = arg.startswith('+include=')
                payload = arg[len('+include=' if positive else '+exclude='):]
                target_set = included_ids if positive else excluded_ids
                for raw in payload.split(','):
                    name = raw.strip()
                    if not name:
                        continue
                    member = await self._resolve_member(ctx, name)
                    target_set.add(str(member.id))
            else:
                remaining.append(arg)
        return (remaining, include_decay, excluded_ids, included_ids,
                include_inactive)

    @staticmethod
    def _filter_akari_rows(rows, *, excluded_ids=None, included_ids=None):
        """Apply ``+include`` and ``+exclude`` filters to a result-row iterable.

        Include narrows first, exclude trims; composition is the natural
        intersection of the two sets minus the excluded ones.  Both arguments
        accept ``None`` / empty set for "no filter", and the function returns
        the input untouched in that case.
        """
        if included_ids:
            rows = [r for r in rows if str(r.user_id) in included_ids]
        if excluded_ids:
            rows = [r for r in rows if str(r.user_id) not in excluded_ids]
        return rows

    async def _parse_akari_rating_args(self, ctx, args, *, member_required=False):
        """Pull ``+decay`` / ``+inactive`` / ``+exclude=`` / ``+include=`` and
        zero-or-more members out of the args.

        Returns ``(members, include_decay, excluded_ids, included_ids,
        include_inactive)``.  Every remaining token is resolved via the
        case-insensitive member converter, so the rating / performance graphs
        can plot multiple users at once (``;mg akari rating @alice @bob``).
        An empty list falls back to ``[ctx.author]`` unless
        ``member_required=True`` (the ``debug`` subcommands), which then
        errors with a usage hint.
        """
        (remaining, include_decay, excluded_ids, included_ids,
         include_inactive) = await self._extract_akari_filters(ctx, args)
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return (members, include_decay, excluded_ids, included_ids,
                include_inactive)

    async def _cmd_akari_rating(self, ctx, members, *, require_registered=True,
                                include_decay=False, excluded_ids=None,
                                included_ids=None):
        """Per-user rating graph (``;plot rating`` style).

        ``members`` is a list of one-or-more members.  With a single member
        the embed keeps the rich layout (Rating / Peak / Games / Last change /
        Last performance); with multiple members the graph plots one line per
        user and the embed switches to a compact roster.

        ``require_registered=True`` (the default, public-facing path) refuses
        to show the rating of users who haven't opted in via ``;mg akari register``.
        The ``rating debug`` subcommand passes False so admins can inspect any
        shadow-rated player.

        ``include_decay=True`` (the ``+decay`` arg) threads decay days into the
        plotted history so absent-day slopes are visible; played days remain
        the marker anchors so they still stand out.

        ``excluded_ids`` (the ``+exclude=...`` arg) recomputes both the embed
        figures and the graph as if those users never played; the persisted
        snapshot stays untouched.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')

        filtered = bool(excluded_ids or included_ids)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    include_decay=include_decay,
                    excluded_ids=excluded_ids, included_ids=included_ids)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(
                    ctx.guild.id, member.id, include_decay=include_decay)
            if row is None:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} rating for '
                    f'`{_safe_member_name(member)}` yet.')
            if not history:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` has no rated '
                    f'{AKARI_GAME.display_name} days to plot yet.')
            per_member.append((member, row, history))

        series = [(history, _legend_name_for(ctx.guild, member))
                  for member, _row, history in per_member]
        discord_file = plot_akari_rating(series)

        if len(per_member) == 1:
            member, row, history = per_member[0]
            rating = round(row.rating)
            rank = rank_for_rating(rating)
            peak_rank = rank_for_rating(round(row.peak))
            # Last contest day's delta and performance (skip solo-day Nones).
            # row.last_delta on the snapshot is overwritten by daily decay steps
            # and rounds to +0 for most users — use the history to find their
            # last actual contest instead, matching how Performance is shown.
            last_contest = next((h for h in reversed(history)
                                 if h.performance is not None), None)
            last_change_str = (f'{last_contest.delta:+.0f}'
                               if last_contest is not None else '—')
            last_perf_str = (
                f'{round(last_contest.performance)} '
                f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                if last_contest is not None else '—')
            embed = discord.Embed(
                title=f'{AKARI_GAME.display_name} rating — {_safe_member_name(member)}',
                color=rank.color_embed,
            )
            embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
            embed.add_field(name='Peak', value=f'{round(row.peak)} ({peak_rank.title_abbr})')
            embed.add_field(name='Games', value=str(row.games))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            top_member, top_row, _ = max(per_member, key=lambda t: t[1].rating)
            top_rank = rank_for_rating(round(top_row.rating))
            del top_member  # only its row drives the embed colour
            lines = [
                f'**{_safe_member_name(member)}**: '
                f'{round(row.rating)} '
                f'({rank_for_rating(round(row.rating)).title_abbr})'
                for member, row, _ in per_member
            ]
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} ratings — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_performance(self, ctx, members, *, require_registered=True,
                                     excluded_ids=None, included_ids=None):
        """Per-user performance graph.

        Performance is the rating that, given the day's field, would seed the
        player at exactly their actual rank — i.e. their "rating-equivalent
        finish" for that contest, independent of their incoming rating.  Solo
        days have no field and are dropped from the plot.

        ``members`` is a list of one-or-more members; single-member uses the
        rich embed (Last / Best / Contests), multi-member uses a compact one
        with each player's latest performance.

        ``require_registered=True`` (the default, public-facing path) refuses
        to show performance for users who haven't opted in via ``;mg akari register``.
        The ``performance debug`` subcommand passes False so admins can inspect
        any shadow-rated player.  ``excluded_ids`` runs a fresh replay without
        those users so their presence doesn't shape this player's performance.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')

        filtered = bool(excluded_ids or included_ids)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(ctx.guild.id, member.id)
            if row is None:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} rating for '
                    f'`{_safe_member_name(member)}` yet.')
            contest_history = [h for h in history if h.performance is not None]
            if not contest_history:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` has no contested '
                    f'{AKARI_GAME.display_name} days to plot performance for yet.')
            per_member.append((member, row, history, contest_history))

        series = [(history, _legend_name_for(ctx.guild, member), round(row.rating))
                  for member, row, history, _ in per_member]
        discord_file = plot_akari_performance(series)

        if len(per_member) == 1:
            member, row, _history, contest_history = per_member[0]
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
        else:
            # Pick the embed colour from the strongest *recent* performance.
            best_per_member = [
                (member, contest_history[-1].performance)
                for member, _row, _history, contest_history in per_member
            ]
            top_rank = rank_for_rating(round(
                max(perf for _m, perf in best_per_member)))
            lines = [
                f'**{_safe_member_name(member)}**: '
                f'last {round(contest_history[-1].performance)} '
                f'({rank_for_rating(round(contest_history[-1].performance)).title_abbr})'
                for member, _row, _history, contest_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} performance — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_ratings_debug(self, ctx, *, excluded_ids=None,
                                        included_ids=None,
                                        include_inactive=False):
        """Admin view: leaderboard image including shadow-rated (unopted-in) users.

        Same image as ``;mg akari ratings`` but without the registration filter —
        so admins can see everyone's rating, with a ``✓`` marking opted-in users.
        Honours ``+exclude=...`` / ``+include=...`` / ``+inactive`` the same
        way as the public command.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if excluded_ids or included_ids:
            rows = self._akari_filtered_rating_rows(
                ctx.guild.id, excluded_ids=excluded_ids,
                included_ids=included_ids)
        else:
            rows = cf_common.user_db.get_akari_ratings(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} ratings yet. They appear once '
                f'players post results.')
        shown = self._active_ranking_rows(
            rows, include_inactive=include_inactive)
        if not shown:
            if include_inactive:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} players yet.')
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} players active in the last '
                f'{constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days. '
                f'Use `+inactive` to include dormant players.')
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        title = ('Daily Akari Ratings (all, incl. inactive)'
                 if include_inactive else 'Daily Akari Ratings (all)')
        discord_file = _get_akari_rating_table_image_file(
            ctx.guild, shown, registrants,
            title=title, mark_registered=True)
        await ctx.send(file=discord_file)

    async def _cmd_akari_history(self, ctx, member, *, require_registered=True,
                                 excluded_ids=None, included_ids=None):
        """Per-user paginated rating delta history (``;handles updates`` style).

        One line per contest the user played, newest first.  Solo days (single
        player) are skipped — they have no field, no contest delta, and don't
        appear on the rating graph either.  Decay days never had their own
        history points to begin with; their net effect surfaces in the next
        played day's rating.  ``excluded_ids`` recomputes the history without
        those users so each delta reflects the contest minus them.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered and not cf_common.user_db.is_akari_registered(
                ctx.guild.id, member.id):
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has not opted in to '
                f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')

        history = self._akari_user_history(
            ctx.guild.id, member.id,
            excluded_ids=excluded_ids, included_ids=included_ids)
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

    async def _cmd_akari_stats_puzzle(self, ctx, selector_arg, *,
                                       show_all=False, excluded_ids=None,
                                       included_ids=None):
        """Render a per-puzzle results image annotated with pre-puzzle ratings.

        ``show_all=False`` (public path): only opted-in users get the rating
        + tier colour; everyone else stays plain.  ``show_all=True`` (the
        ``stats debug`` subcommand, mod-only) annotates every player including
        shadow-rated ones, mirroring how ``ratings debug`` reveals opt-outs.
        ``excluded_ids`` hides those users from the displayed table *and*
        runs the rating annotation without them, so deltas reflect the
        smaller field.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        selector = _maybe_parse_puzzle_selector(selector_arg)
        if selector is None:
            raise MinigameCogError(
                f'Expected a puzzle number or date, got `{selector_arg}`.')
        selector_type, selector_value = selector
        if selector_type == 'puzzle':
            rows = cf_common.user_db.get_minigame_results_for_guild(
                ctx.guild.id, AKARI_GAME.name,
                plo=selector_value, phi=selector_value + 1)
            title = f'{AKARI_GAME.display_name} #{selector_value} Results'
        else:
            day_start = dt.datetime.combine(selector_value, dt.time.min).timestamp()
            day_end = day_start + 24 * 60 * 60
            rows = cf_common.user_db.get_minigame_results_for_guild(
                ctx.guild.id, AKARI_GAME.name, dlo=day_start, dhi=day_end)
            title = f'{AKARI_GAME.display_name} {selector_value.isoformat()} Results'

        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)

        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} results found for `{selector_arg}`.')

        # Annotation requires a single puzzle worth of rows (1 puzzle/day).
        # For a multi-puzzle slice (theoretical), fall back to plain rendering.
        puzzle_numbers = {int(row.puzzle_number) for row in rows}
        puzzle_info = None
        registrants = None
        if len(puzzle_numbers) == 1:
            puzzle_info = self._akari_puzzle_change_info(
                ctx.guild.id, next(iter(puzzle_numbers)),
                excluded_ids=excluded_ids, included_ids=included_ids)
            if show_all:
                # Debug: pretend every rated player is registered for display.
                registrants = set(puzzle_info.keys())
            else:
                registrants = cf_common.user_db.get_akari_registrants(
                    ctx.guild.id)

        discord_file = _get_akari_puzzle_table_image_file(
            ctx.guild, rows, title,
            puzzle_info=puzzle_info, registrants=registrants)
        await ctx.send(file=discord_file)

    async def _cmd_stats(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        # Akari accepts +decay / +exclude=... / +include=... anywhere in the
        # arg list — strip those before falling into the per-user /
        # per-puzzle dispatch so the remaining tokens are just the selector
        # (or a member name).
        excluded_ids = set()
        included_ids = set()
        if game.name == AKARI_GAME.name:
            (remaining, _include_decay, excluded_ids, included_ids,
             _include_inactive) = await self._extract_akari_filters(ctx, args)
            args = tuple(remaining)
        if game.name == 'akari' and len(args) == 1:
            if _maybe_parse_puzzle_selector(args[0]) is not None:
                await self._cmd_akari_stats_puzzle(
                    ctx, args[0],
                    excluded_ids=excluded_ids, included_ids=included_ids)
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

    # ── Akari commands: ;akari … (also mirrored onto ;mg for backcompat) ──

    @commands.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
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

    @akari.command(name='register', brief='Restore Daily Akari rating visibility',
                   usage='[@user (mods only)]')
    async def akari_register(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        changed = cf_common.user_db.register_akari_user(
            ctx.guild.id, target.id)
        who = ('You are' if target.id == ctx.author.id
               else f'`{_safe_member_name(target)}` is')
        if changed:
            msg = (f'{who} opted back in to {AKARI_GAME.display_name} ratings.')
        else:
            msg = (f'{who} already visible in {AKARI_GAME.display_name} ratings '
                   f'(everyone is opted in by default).')
        await ctx.send(embed=discord_common.embed_success(msg))

    @akari.command(name='unregister', brief='Opt out of Daily Akari ratings',
                   usage='[@user (mods only)]')
    async def akari_unregister(self, ctx, member: CaseInsensitiveMember = None):
        target = self._resolve_registrar_target(ctx, member)
        changed = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, target.id, time.time())
        who = ('You are' if target.id == ctx.author.id
               else f'`{_safe_member_name(target)}` is')
        if changed:
            msg = (f'{who} opted out of {AKARI_GAME.display_name} ratings. '
                   f'Results are still recorded; run `;mg akari register` to opt back in.')
        else:
            msg = f'{who} already opted out.'
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
        # Auto opt them out so the rating display state stays consistent and
        # the opt-out sticks past any later unban.
        opted_out = cf_common.user_db.unregister_akari_user(
            ctx.guild.id, member.id, time.time())
        lines = [f'`{_safe_member_name(member)}` is now banned from '
                 f'{AKARI_GAME.display_name} ingestion. New results from '
                 f'them will be dropped silently.']
        if opted_out:
            lines.append('Also opted out of ratings.')
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

    @akari.group(name='stats', brief='Show personal stats with graphs',
                 usage='[@user] [filters...] | [day | puzzle_id | #puzzle_id]',
                 invoke_without_command=True)
    async def akari_stats(self, ctx, *args):
        await self._cmd_stats(ctx, AKARI_GAME, *args)

    @akari_stats.command(name='debug',
                         brief='(Mod) Puzzle results with ratings for ALL players',
                         usage='<puzzle_id|date> [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_stats_debug(self, ctx, *args):
        (remaining, _include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._extract_akari_filters(ctx, args)
        if len(remaining) != 1:
            raise MinigameCogError(
                'Usage: `;mg akari stats debug <puzzle_id|date> '
                '[+exclude=…] [+include=…]`.')
        await self._cmd_akari_stats_puzzle(
            ctx, remaining[0], show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari.command(name='remove', brief='Remove a user result for a puzzle',
                   usage='@user puzzle_id')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, AKARI_GAME, member, puzzle_id)

    @akari.command(name='add', brief='Manually add a result for a user/puzzle',
                   usage='@user puzzle_id <perfect|N%> <time>')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_add(self, ctx, member: CaseInsensitiveMember,
                        puzzle_id: int, result: str, time: str):
        await self._cmd_akari_add(ctx, member, puzzle_id, result, time)

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

    @akari.group(name='ratings', brief='Show Akari rating leaderboard',
                 usage='[+inactive] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_ratings(self, ctx, *args):
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive) = await self._extract_akari_filters(ctx, args)
        await self._cmd_akari_ratings(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive)

    @akari.group(name='rating',
                 brief='Show registered users\' Akari rating graph',
                 usage='[@user1 @user2 ...] [+decay] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_rating(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(ctx, args)
        await self._cmd_akari_rating(
            ctx, members, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari_rating.command(name='debug',
                          brief='(Mod) Rating graph for any user (incl. shadow-rated)',
                          usage='@user1 [@user2 ...] [+decay] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_rating_debug(self, ctx, *args):
        (members, include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        await self._cmd_akari_rating(
            ctx, members, require_registered=False,
            include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari.group(name='performance', aliases=['perf'],
                 brief='Show registered users\' Akari performance graph',
                 usage='[@user1 @user2 ...] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_performance(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(ctx, args)
        await self._cmd_akari_performance(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari_performance.command(name='debug',
                               brief='(Mod) Performance graph for any user (incl. shadow-rated)',
                               usage='@user1 [@user2 ...] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_performance_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        await self._cmd_akari_performance(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari.group(name='history',
                 brief='Paginated rating delta log for a registered user',
                 usage='[@user] [+exclude=…] [+include=…]',
                 invoke_without_command=True)
    async def akari_history(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(ctx, args)
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari_history.command(name='debug',
                           brief='(Mod) Rating delta log for any user (incl. shadow-rated)',
                           usage='@user [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_history_debug(self, ctx, *args):
        (members, _include_decay, excluded_ids, included_ids,
         _include_inactive) = await self._parse_akari_rating_args(
            ctx, args, member_required=True)
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_akari_history(
            ctx, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @akari_ratings.command(name='recompute', brief='(Mod) Rebuild the rating snapshot')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_recompute(self, ctx):
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{AKARI_GAME.display_name} ratings recomputed.'))

    @akari_ratings.command(name='debug', aliases=['all'],
                           brief='(Mod) Leaderboard incl. shadow-rated (unopted-in) users',
                           usage='[+inactive] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def akari_ratings_debug(self, ctx, *args):
        (_remaining, _include_decay, excluded_ids, included_ids,
         include_inactive) = await self._extract_akari_filters(ctx, args)
        await self._cmd_akari_ratings_debug(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids,
            include_inactive=include_inactive)

    # ── Queens commands ─────────────────────────────────────────────────

    @commands.group(name='queens', aliases=['queen', 'linkedinqueens'],
                    brief='LinkedIn Queens commands',
                    invoke_without_command=True)
    async def queens(self, ctx):
        await ctx.send_help(ctx.command)

    @queens.command(name='show', brief='Show LinkedIn Queens settings')
    async def queens_show(self, ctx):
        await self._cmd_queens_show(ctx)

    @queens.command(name='register',
                    brief='Link a Discord user to a LinkedIn Queens name',
                    usage='LinkedIn Name [+anon]')
    async def queens_register(self, ctx, first: str = None, *,
                              linkedin: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if _is_queens_anonymous_modal_request(first, linkedin):
            await ctx.send(
                embed=discord_common.embed_neutral(
                    'Click the button below to enter your LinkedIn name '
                    'privately. Only you can use this prompt, and your '
                    'LinkedIn name will not be posted in the channel.'),
                view=_QueensAnonymousRegisterView(self, ctx.author.id))
            return
        member, linkedin_text, anonymous = await self._resolve_queens_registration_args(
            ctx, first, linkedin)
        claimed = self._cmd_queens_register_link(
            ctx, member, linkedin_text, anonymous=anonymous)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        who = (
            'You are'
            if member.id == ctx.author.id
            else f'`{display_name}` is'
        )
        await ctx.send(embed=discord_common.embed_success('\n'.join([
            f'{who} registered for {QUEENS_GAME.display_name} as '
            f'`{_queens_public_link_name(link)}`.',
            *(
                [f'Claimed {claimed} stored Queens result(s) and recomputed ratings.']
                if claimed else []
            ),
            self._queens_connection_instruction(ctx.guild.id),
        ])))

    @queens.command(name='unregister',
                    brief='Remove a user LinkedIn Queens link',
                    usage='[@user]')
    async def queens_unregister(self, ctx, member: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if member is None:
            target = ctx.author
        else:
            target = await self._resolve_member(ctx, member)
            target = self._resolve_registrar_target(ctx, target)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, target.id)
        removed = cf_common.user_db.delete_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, target.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(target)}` is not registered for '
                f'{QUEENS_GAME.display_name}.')
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {QUEENS_GAME.display_name} link for '
            f'`{self._queens_public_user_name(ctx.guild, target.id, {str(target.id): link})}`.'))

    @queens.command(name='links', brief='List registered LinkedIn Queens names')
    async def queens_links(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        rows = cf_common.user_db.get_minigame_player_links(
            ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} links registered.')
        lines = []
        for row in rows:
            display_name = self._queens_public_user_name(
                ctx.guild, row.user_id, {str(row.user_id): row})
            lines.append(
                f'- {display_name}: `{_queens_public_link_name(row)}`')
        pages = []
        for chunk in paginator.chunkify(lines, _QUEENS_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=f'{QUEENS_GAME.display_name} links',
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    @queens.group(name='connection', aliases=['account'],
                  brief='Show or set the LinkedIn account players connect to',
                  invoke_without_command=True)
    async def queens_connection(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        account = self._get_queens_connection_account(ctx.guild.id)
        if account is None:
            raise MinigameCogError(
                'No LinkedIn connection account configured yet.')
        await ctx.send(embed=discord_common.embed_neutral(
            self._queens_connection_instruction(ctx.guild.id)))

    @queens_connection.command(name='set',
                               brief='(Mod) Set the LinkedIn connection account',
                               usage='LinkedIn Name profile_url')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_connection_set(self, ctx, *, linkedin: str):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        name, external_url = _split_queens_connection_account_text(linkedin)
        self._set_queens_connection_account(ctx.guild.id, name, external_url)
        await ctx.send(embed=discord_common.embed_success(
            self._queens_connection_instruction(ctx.guild.id)))

    @queens_connection.command(name='clear',
                               brief='(Mod) Clear the LinkedIn connection account')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_connection_clear(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._clear_queens_connection_account(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            'Cleared the LinkedIn Queens connection account.'))

    # ── Scraper-driven commands (login / play / update / settings) ─────

    @queens.command(
        name='install',
        brief='(Mod) Install Playwright + Chromium for the scraper')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_install(self, ctx):
        """Install Playwright and the Chromium browser into the bot's Python
        environment, without needing shell access to the host.

        Requires:
          - Internet egress (to download from PyPI and Playwright's CDN)
          - ~200 MB free disk
          - Write access to ``sys.executable``'s site-packages and to the
            bot user's cache directory (default: ``~/.cache/ms-playwright``)

        Idempotent: re-running is a no-op when both are already installed.
        Runs as the bot user with whatever permissions it already has — no
        sudo, no system packages.  If Chromium fails to launch later due to
        missing system libraries (``libnss3``, ``libxkbcommon0``, etc.),
        that's a host-level fix only you can do via SSH.
        """
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        msg = await ctx.send(embed=discord_common.embed_neutral(
            'Installing scraper dependencies. This downloads ~170 MB and '
            'takes 1–3 minutes.\n\n'
            'Step 1/2: `pip install playwright` …'))

        rc, out = await self._run_install_step(
            [sys.executable, '-m', 'pip', 'install', '--upgrade', 'playwright'],
            timeout=300)
        if rc != 0:
            raise MinigameCogError(
                f'`pip install playwright` failed (rc={rc}). Tail:\n'
                f'```{(out or "(no output)")[-1500:]}```')

        await msg.edit(embed=discord_common.embed_neutral(
            '✓ Step 1/2: `pip install playwright` complete.\n\n'
            'Step 2/2: `playwright install chromium` (~170 MB) …'))

        rc, out = await self._run_install_step(
            [sys.executable, '-m', 'playwright', 'install', 'chromium'],
            timeout=900)
        if rc != 0 and 'does not support' in (out or ''):
            # Host OS isn't in Playwright's hard-coded platform matrix
            # (e.g. Ubuntu 26.04).  Retry forcing the LTS binary.
            await msg.edit(embed=discord_common.embed_neutral(
                f'✓ Step 1/2 complete.\n\n'
                f'Step 2/2: host OS not in Playwright\'s matrix — '
                f'retrying with `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE='
                f'{_QUEENS_PLAYWRIGHT_PLATFORM}` …'))
            rc, out = await self._run_install_step(
                [sys.executable, '-m', 'playwright', 'install', 'chromium'],
                timeout=900,
                extra_env={'PLAYWRIGHT_HOST_PLATFORM_OVERRIDE':
                           _QUEENS_PLAYWRIGHT_PLATFORM})
        if rc != 0:
            raise MinigameCogError(
                f'`playwright install chromium` failed (rc={rc}). Tail:\n'
                f'```{(out or "(no output)")[-1500:]}```')

        await msg.edit(embed=discord_common.embed_success(
            '✓ Playwright + Chromium installed for this bot.\n'
            'Next: upload your LinkedIn session with `;queens login` '
            '(attach `extra/.queens_state.json` generated on your laptop).'))

    @staticmethod
    async def _run_install_step(cmd, *, timeout, extra_env=None):
        """Spawn an install subprocess and capture combined stdout+stderr.

        Returns ``(returncode, captured_text)``.  Never raises; timeouts come
        back as ``returncode == -1``.  ``extra_env`` is merged on top of
        ``os.environ`` for the subprocess — used to inject
        ``PLAYWRIGHT_HOST_PLATFORM_OVERRIDE`` on bleeding-edge Ubuntu.
        """
        env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        if extra_env:
            env.update(extra_env)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
        except (FileNotFoundError, PermissionError) as exc:
            return -2, f'Could not launch `{cmd[0]}`: {exc}'
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return -1, '(timed out — try running it manually on the host)'
        return proc.returncode, stdout.decode('utf-8', errors='replace')

    @queens.command(
        name='login',
        brief='(Mod) Upload a fresh LinkedIn session file',
        usage='[LinkedIn Name] (attach extra/.queens_state.json to the message)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_login(self, ctx, *, linkedin_name: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        attachments = list(getattr(ctx.message, 'attachments', None) or [])
        json_atts = [a for a in attachments
                     if getattr(a, 'filename', '').lower().endswith('.json')]
        if not json_atts:
            raise MinigameCogError(
                'Attach a `.queens_state.json` file (produced by running '
                '`python extra/queens_scrape.py login` on any machine with '
                'a browser) to this message.')
        attachment = json_atts[0]
        size = int(getattr(attachment, 'size', 0) or 0)
        if size and size > _QUEENS_STATE_MAX_BYTES:
            raise MinigameCogError(
                f'Attachment is {size} bytes — refusing anything over '
                f'{_QUEENS_STATE_MAX_BYTES}.')
        raw = await attachment.read()
        try:
            data = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MinigameCogError(
                f'Attachment is not valid JSON: {exc}.')
        cookies = data.get('cookies') if isinstance(data, dict) else None
        if not isinstance(cookies, list):
            raise MinigameCogError(
                'JSON does not look like a Playwright storage_state '
                '(no `cookies` array).')
        has_li_at = any(
            isinstance(c, dict) and c.get('name') == 'li_at'
            for c in cookies)
        if not has_li_at:
            raise MinigameCogError(
                'No `li_at` cookie found — this does not look like a '
                'LinkedIn session.')

        state_path = self._queens_state_path(ctx.guild.id)
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_bytes(raw)
        except OSError as exc:
            raise MinigameCogError(
                f'Could not write session file to `{state_path}`: {exc}.')

        # Clear any stale state from the old design where the uploading
        # mod was registered as the bot's Discord-side avatar.  Going
        # forward, the bot account has no Discord-user mapping; "You"
        # rows in scraped leaderboards are dropped categorically.
        cf_common.user_db.delete_guild_config(
            ctx.guild.id, _QUEENS_IMPORTER_KEY)

        lines = [f'Session saved to `{state_path}`.']

        # Optionally detect + display the LinkedIn account name for
        # transparency.  It's purely informational — no Discord user
        # gets linked to it, no rating consequences.
        if linkedin_name and linkedin_name.strip():
            detected = linkedin_name.strip()
        else:
            detected, err = await self._run_queens_whoami(ctx.guild.id)
            if detected is None:
                lines.append(
                    f'(Could not detect LinkedIn name: {err})')
                detected = None
        if detected:
            cf_common.user_db.set_guild_config(
                ctx.guild.id, _QUEENS_LINKEDIN_NAME_KEY, detected)
            lines.append(f'LinkedIn account: `{detected}`')
        lines.append('Ready — try `;queens play` to verify.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @queens.command(
        name='play',
        brief='(Mod) Solve today\'s puzzle + refresh the leaderboard')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_play(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        state_path = self._queens_state_path(ctx.guild.id)
        if not state_path.exists():
            raise MinigameCogError(
                f'No LinkedIn session at `{state_path}`. A mod needs to '
                'run `;queens login` (with the state file attached) first.')

        await ctx.send(embed=discord_common.embed_neutral(
            'Running the scraper now — this can take up to '
            f'{_QUEENS_SCRAPER_TIMEOUT}s while the puzzle solves.'))
        payload, error = await self._run_queens_scraper(
            ctx.guild.id, auto_play=True)
        if error is not None:
            raise MinigameCogError(error)
        status = payload.get('status')
        if status != 'ok':
            raise MinigameCogError(self._queens_status_message(status))
        await self._do_queens_import(ctx, payload, source_label='Play')

    @queens.command(
        name='update',
        brief='Refresh the LinkedIn Queens leaderboard '
              f'(rate-limited to once per {_QUEENS_UPDATE_THROTTLE_SECONDS}s)')
    async def queens_update(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        kvs_key = f'{_QUEENS_UPDATE_THROTTLE_PREFIX}{ctx.guild.id}'
        last = cf_common.user_db.kvs_get(kvs_key)
        if last:
            try:
                elapsed = time.time() - float(last)
            except (TypeError, ValueError):
                elapsed = _QUEENS_UPDATE_THROTTLE_SECONDS
            if elapsed < _QUEENS_UPDATE_THROTTLE_SECONDS:
                wait = int(_QUEENS_UPDATE_THROTTLE_SECONDS - elapsed) + 1
                raise MinigameCogError(
                    f'`;queens update` is rate-limited. Try again in {wait}s.')

        state_path = self._queens_state_path(ctx.guild.id)
        if not state_path.exists():
            raise MinigameCogError(
                f'No LinkedIn session at `{state_path}`. A mod needs to '
                'run `;queens login` first.')
        # Set the throttle BEFORE the slow subprocess so concurrent users
        # don't both pass the gate.
        cf_common.user_db.kvs_set(kvs_key, str(time.time()))

        payload, error = await self._run_queens_scraper(
            ctx.guild.id, auto_play=False)
        if error is not None:
            raise MinigameCogError(error)
        status = payload.get('status')
        if status == 'not_played':
            raise MinigameCogError(self._queens_status_message(status))
        if status != 'ok':
            raise MinigameCogError(self._queens_status_message(status))
        await self._do_queens_import(
            ctx, payload, source_label='Update')

    @queens.command(
        name='settings',
        brief='Show the LinkedIn Queens scraper config for this guild')
    async def queens_settings(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        state_path = self._queens_state_path(ctx.guild.id)
        path_default = state_path == _QUEENS_DEFAULT_STATE_PATH
        state_exists = state_path.exists()
        li_name = cf_common.user_db.get_guild_config(
            ctx.guild.id, _QUEENS_LINKEDIN_NAME_KEY)
        last_update = cf_common.user_db.kvs_get(
            f'{_QUEENS_UPDATE_THROTTLE_PREFIX}{ctx.guild.id}')
        last_text = 'never'
        if last_update:
            try:
                last_text = dt.datetime.fromtimestamp(
                    float(last_update), tz=dt.timezone.utc
                ).strftime('%Y-%m-%d %H:%M:%S UTC')
            except (TypeError, ValueError):
                pass
        lines = [
            (f'LinkedIn account: `{li_name}`' if li_name
             else 'LinkedIn account: `unknown` (run `;queens login`)'),
            f'state file: `{state_path}`'
            + ('' if not path_default else ' (default)')
            + ('' if state_exists else ' — **missing!**'),
            f'last update: `{last_text}`',
            f'rate limit: `;queens update` once per '
            f'`{_QUEENS_UPDATE_THROTTLE_SECONDS}s`',
        ]
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @queens.command(
        name='backfill',
        brief='(Mod) Backfill a user\'s historical Queens results',
        usage='@user (attach queens_history.json)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_backfill(self, ctx, member: CaseInsensitiveMember):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)

        # User must already be registered so we know their LinkedIn name
        # for the match.
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if link is None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not registered for '
                f'{QUEENS_GAME.display_name}. They need to '
                '`;queens register Their LinkedIn Name` first.')

        attachments = list(getattr(ctx.message, 'attachments', None) or [])
        json_atts = [
            a for a in attachments
            if getattr(a, 'filename', '').lower().endswith('.json')]
        if not json_atts:
            raise MinigameCogError(
                'Attach a `queens_history.json` file (generated by '
                '`extra/queens_parse_messages.py`).')
        attachment = json_atts[0]
        size = int(getattr(attachment, 'size', 0) or 0)
        if size and size > _QUEENS_BACKFILL_MAX_BYTES:
            raise MinigameCogError(
                f'Attachment is {size} bytes — refusing anything over '
                f'{_QUEENS_BACKFILL_MAX_BYTES} bytes.')
        raw = await attachment.read()
        try:
            data = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MinigameCogError(f'Attachment is not valid JSON: {exc}.')
        if not isinstance(data, list):
            raise MinigameCogError(
                'JSON must be a list of result entries.')

        target_normalized = link.normalized_name
        matching = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            name = entry.get('linkedin_name', '')
            if not isinstance(name, str):
                continue
            if normalize_queens_name(name) == target_normalized:
                matching.append(entry)
        if not matching:
            raise MinigameCogError(
                f'No entries in the JSON match '
                f'`{_safe_member_name(member)}`\'s registered LinkedIn '
                'account.')

        saved = 0
        skipped = 0
        malformed = 0
        for entry in matching:
            try:
                puzzle_number = int(entry['puzzle_number'])
                time_seconds = int(entry['time_seconds'])
                no_hints = bool(entry.get('no_hints', False))
                no_mistakes = bool(entry.get('no_mistakes', False))
                puzzle_date_iso = entry.get('puzzle_date')
                if puzzle_date_iso:
                    puzzle_date = dt.date.fromisoformat(puzzle_date_iso)
                else:
                    puzzle_date = _queens_date_for_puzzle_number(puzzle_number)
            except (KeyError, TypeError, ValueError):
                malformed += 1
                continue

            # Additive: skip if any matching row (anchor or legacy
            # ordinal number) already exists for this user/puzzle.
            existing = None
            for pn in _queens_puzzle_numbers_for_date(puzzle_date):
                existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
                    ctx.guild.id, QUEENS_GAME.name, member.id, pn)
                if existing is not None:
                    break
            if existing is not None:
                skipped += 1
                continue

            cf_common.user_db.save_minigame_result(
                _queens_result_message_id(
                    ctx.guild.id, puzzle_date, member.id),
                ctx.guild.id, QUEENS_GAME.name, ctx.channel.id, member.id,
                puzzle_number, _queens_puzzle_date_text(puzzle_date),
                100 if no_mistakes else 0, time_seconds,
                no_hints and no_mistakes,
                f'backfill from LinkedIn export '
                f'({entry.get("sent_at_utc", "")})'
            )
            saved += 1

        if saved:
            self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)

        lines = [
            f'Backfilled **{saved}** result(s) for '
            f'`{_safe_member_name(member)}` '
            f'(LinkedIn: `{_queens_public_link_name(link)}`).',
        ]
        if skipped:
            lines.append(
                f'- Skipped **{skipped}** already-saved result(s).')
        if malformed:
            lines.append(
                f'- Ignored **{malformed}** malformed entry/entries.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @queens.command(
        name='state-path', aliases=['statepath'],
        brief='(Mod) Override where the scraper looks for state.json',
        usage='/abs/path/to/state.json | clear')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_state_path(self, ctx, *, path: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if path is None:
            raise MinigameCogError(
                'Usage: `;queens state-path /abs/path/to/state.json` '
                'or `;queens state-path clear`.')
        if path.strip().lower() == 'clear':
            cf_common.user_db.delete_guild_config(
                ctx.guild.id, _QUEENS_STATE_PATH_KEY)
            await ctx.send(embed=discord_common.embed_success(
                f'Cleared the override. Default is `{_QUEENS_DEFAULT_STATE_PATH}`.'))
            return
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _QUEENS_STATE_PATH_KEY, path.strip())
        await ctx.send(embed=discord_common.embed_success(
            f'Scraper will use `{path.strip()}` for the session file.'))

    @queens.command(name='ban',
                    brief='(Mod) Block a user from Queens imports/ratings',
                    usage='@user [reason...]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_ban(self, ctx, member: CaseInsensitiveMember, *,
                         reason: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        added = cf_common.user_db.ban_minigame_user(
            ctx.guild.id, QUEENS_GAME.name, member.id, time.time(),
            ctx.author.id, reason)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        if not added:
            raise MinigameCogError(
                f'`{display_name}` is already banned from '
                f'{QUEENS_GAME.display_name}.')
        cf_common.user_db.delete_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        lines = [
            f'`{display_name}` is now banned from '
            f'{QUEENS_GAME.display_name}. They will be skipped by imports, '
            'manual adds, and rating recomputes.',
            'Their LinkedIn Queens registration was removed.',
        ]
        if reason:
            lines.append(f'Reason: {reason}')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    @queens.command(name='unban',
                    brief='(Mod) Lift a Queens ban',
                    usage='@user')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_unban(self, ctx, member: CaseInsensitiveMember):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        removed = cf_common.user_db.unban_minigame_user(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if not removed:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not banned.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'`{_safe_member_name(member)}` is no longer banned from '
            f'{QUEENS_GAME.display_name}. They need to run '
            '`;queens register LinkedIn Name` again.'))

    @queens.command(name='bans',
                    brief='(Mod) List Queens bans')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_bans(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        rows = cf_common.user_db.get_minigame_bans(
            ctx.guild.id, QUEENS_GAME.name)
        if not rows:
            raise MinigameCogError(
                f'No active {QUEENS_GAME.display_name} bans.')
        lines = [_format_akari_ban_line(ctx.guild, row) for row in rows]
        title = f'{QUEENS_GAME.display_name} bans ({len(rows)})'
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            pages.append((None, discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    @queens.command(name='vs', brief='Head-to-head comparison',
                    usage='@user1 @user2 [filters...]')
    async def queens_vs(self, ctx, member1: CaseInsensitiveMember,
                        member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, QUEENS_GAME, member1, member2, *args)

    @queens.command(name='top', brief='Show fastest-result winners',
                    usage='[filters...]')
    async def queens_top(self, ctx, *args):
        await self._cmd_top(ctx, QUEENS_GAME, *args)

    @queens.command(name='streak', brief='Show current clean streak',
                    usage='[@user] [filters...]')
    async def queens_streak(self, ctx, *args):
        await self._cmd_queens_streak(ctx, *args)

    @queens.group(name='stats', brief='Show personal Queens stats',
                  usage='[@user] [filters...]',
                  invoke_without_command=True)
    async def queens_stats(self, ctx, *args):
        await self._cmd_queens_stats(ctx, *args)

    @queens.group(name='results', brief='Show Queens date leaderboard',
                  usage='[date|number] [+exclude=…] [+include=…]',
                  invoke_without_command=True)
    async def queens_results(self, ctx, *args):
        remaining, excluded_ids, included_ids = (
            await self._extract_queens_rating_filters(ctx, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;queens results [date|number] '
                '[+exclude=…] [+include=…]`.')
        date_arg = remaining[0] if remaining else dt.date.today().isoformat()
        await self._cmd_queens_stats_date(
            ctx, date_arg,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens_results.command(name='debug',
                            brief='(Mod) Date results with ratings for ALL players',
                            usage='[date|number] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_results_debug(self, ctx, *args):
        remaining, excluded_ids, included_ids = (
            await self._extract_queens_rating_filters(ctx, args))
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;queens results debug [date|number] '
                '[+exclude=…] [+include=…]`.')
        date_arg = remaining[0] if remaining else dt.date.today().isoformat()
        await self._cmd_queens_stats_date(
            ctx, date_arg, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens.group(name='import', brief='Preview a pasted Queens leaderboard',
                  usage='date <pasted leaderboard>',
                  invoke_without_command=True)
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_import(self, ctx, puzzle_date: str = None, *,
                            leaderboard: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if puzzle_date is None or leaderboard is None:
            raise MinigameCogError(
                'Usage: `;queens import DATE <pasted leaderboard>`.')
        preview = self._make_queens_import_preview(ctx, puzzle_date, leaderboard)
        self._queens_pending_imports[(ctx.guild.id, ctx.author.id)] = preview
        await ctx.send(embed=discord_common.embed_neutral(
            self._format_queens_import_preview(ctx, preview)))

    @queens_import.command(name='confirm',
                           brief='Save the latest Queens import preview')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_import_confirm(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        key = (ctx.guild.id, ctx.author.id)
        preview = self._queens_pending_imports.pop(key, None)
        if preview is None:
            raise MinigameCogError(
                'No pending Queens import preview. Run `;queens import` first.')
        saved = self._save_queens_import(ctx, preview)
        unresolved = (
            f' Stored {saved.unresolved} unresolved result(s) for later registration.'
            if saved.unresolved else ''
        )
        await ctx.send(embed=discord_common.embed_success(
            f'Saved {saved.resolved} registered {QUEENS_GAME.display_name} '
            f'result(s) for #{preview.puzzle_number} '
            f'{preview.puzzle_date.isoformat()}.{unresolved}'))

    @queens.command(name='add',
                    brief='Manually add a Queens result',
                    usage='<@user|LinkedIn Name> date|number time [status...]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_add(self, ctx, *, args: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        player_text, parsed_date, time_text, status = (
            self._parse_queens_add_args(args))
        user_id, label, linked = await self._resolve_queens_linked_player(
            ctx, player_text)
        self._ensure_not_minigame_banned(
            ctx.guild.id, QUEENS_GAME, user_id, label)
        parsed_number = _queens_puzzle_number_for_date(parsed_date)
        no_hints, no_mistakes, _status_text = queens_status_flags(status)
        time_seconds = parse_queens_time(time_text)
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            cf_common.user_db.delete_minigame_result_for_user_puzzle(
                ctx.guild.id, QUEENS_GAME.name, user_id, puzzle_number)
        cf_common.user_db.save_minigame_result(
            _queens_result_message_id(ctx.guild.id, parsed_date, user_id),
            ctx.guild.id, QUEENS_GAME.name, ctx.channel.id, user_id,
            parsed_number, _queens_puzzle_date_text(parsed_date),
            100 if no_mistakes else 0, time_seconds, no_hints and no_mistakes,
            f'{linked.external_name}\n{status}\n{time_text}',
        )
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Added {QUEENS_GAME.display_name} result for '
            f'`{label}` on #{parsed_number} {parsed_date.isoformat()}: '
            f'**{format_duration(time_seconds)}**.'))

    @queens.command(name='remove', brief='Remove a Queens result',
                    usage='<@user|LinkedIn Name> date|number')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_remove(self, ctx, *, args: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        player_text, parsed_date = self._parse_queens_remove_args(args)
        user_id, label, _linked = await self._resolve_queens_linked_player(
            ctx, player_text)
        rc = 0
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            rc += cf_common.user_db.delete_minigame_result_for_user_puzzle(
                ctx.guild.id, QUEENS_GAME.name, user_id, puzzle_number)
        if not rc:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} result found for '
                f'`{label}` on {parsed_date.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {QUEENS_GAME.display_name} result for '
            f'`{label}` on #{_queens_puzzle_number_for_date(parsed_date)} '
            f'{parsed_date.isoformat()}.'))

    @queens.command(name='clear', aliases=['delete'],
                    brief='(Mod) Remove all Queens results for a date',
                    usage='date|number')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_clear(self, ctx, puzzle_date: str = None):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if puzzle_date is None:
            raise MinigameCogError('Usage: `;queens clear DATE/#`.')
        parsed_date = _parse_queens_date_or_number(puzzle_date)
        parsed_number = _queens_puzzle_number_for_date(parsed_date)
        deleted = 0
        unresolved_deleted = 0
        for puzzle_number in _queens_puzzle_numbers_for_date(parsed_date):
            deleted += cf_common.user_db.delete_minigame_results_for_puzzle(
                ctx.guild.id, QUEENS_GAME.name, puzzle_number)
            unresolved_deleted += (
                cf_common.user_db.delete_minigame_unresolved_results_for_puzzle(
                    ctx.guild.id, QUEENS_GAME.name, puzzle_number))
        if not deleted and not unresolved_deleted:
            raise MinigameCogError(
                f'No {QUEENS_GAME.display_name} results found for '
                f'{parsed_date.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} registered and {unresolved_deleted} unresolved '
            f'{QUEENS_GAME.display_name} result(s) for '
            f'#{parsed_number} {parsed_date.isoformat()}.'))

    @queens.group(name='ratings', brief='Show Queens rating leaderboard',
                  usage='[+exclude=…] [+include=…]',
                  invoke_without_command=True)
    async def queens_ratings(self, ctx, *args):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        remaining, excluded_ids, included_ids = (
            await self._extract_queens_rating_filters(ctx, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;queens ratings [+exclude=…] [+include=…]`.')
        await self._cmd_queens_ratings(
            ctx, excluded_ids=excluded_ids, included_ids=included_ids)

    @queens.group(name='rating',
                  brief='Show Queens rating graph',
                  usage='[@user1 @user2 ...] [+exclude=…] [+include=…]',
                  invoke_without_command=True)
    async def queens_rating(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(ctx, args))
        await self._cmd_queens_rating(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens_rating.command(name='debug',
                           brief='(Mod) Rating graph for any rated user',
                           usage='@user1 [@user2 ...] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_rating_debug(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True))
        await self._cmd_queens_rating(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens.group(name='performance', aliases=['perf'],
                  brief='Show Queens performance graph',
                  usage='[@user1 @user2 ...] [+exclude=…] [+include=…]',
                  invoke_without_command=True)
    async def queens_performance(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(ctx, args))
        await self._cmd_queens_performance(
            ctx, members,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens_performance.command(name='debug',
                                brief='(Mod) Performance graph for any rated user',
                                usage='@user1 [@user2 ...] [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_performance_debug(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True))
        await self._cmd_queens_performance(
            ctx, members, require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens.group(name='history',
                  brief='Paginated Queens rating delta log',
                  usage='[@user] [+exclude=…] [+include=…]',
                  invoke_without_command=True)
    async def queens_history(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(ctx, args))
        if len(members) != 1:
            raise MinigameCogError(
                '`history` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, members[0],
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens_history.command(name='debug',
                            brief='(Mod) Rating delta log for any rated user',
                            usage='@user [+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_history_debug(self, ctx, *args):
        members, excluded_ids, included_ids = (
            await self._parse_queens_rating_args(
                ctx, args, member_required=True))
        if len(members) != 1:
            raise MinigameCogError(
                '`history debug` shows one user at a time — pick one.')
        await self._cmd_queens_history(
            ctx, members[0], require_registered=False,
            excluded_ids=excluded_ids, included_ids=included_ids)

    @queens_ratings.command(name='recompute',
                            brief='(Mod) Rebuild the Queens rating snapshot')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_ratings_recompute(self, ctx):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        self._recompute_minigame_ratings(ctx.guild.id, QUEENS_GAME)
        await ctx.send(embed=discord_common.embed_success(
            f'{QUEENS_GAME.display_name} ratings recomputed.'))

    @queens_ratings.command(name='debug', aliases=['all'],
                            brief='(Mod) Leaderboard including unregistered rated users',
                            usage='[+exclude=…] [+include=…]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def queens_ratings_debug(self, ctx, *args):
        remaining, excluded_ids, included_ids = (
            await self._extract_queens_rating_filters(ctx, args))
        if remaining:
            raise MinigameCogError(
                'Usage: `;queens ratings debug [+exclude=…] [+include=…]`.')
        await self._cmd_queens_ratings(
            ctx, show_all=True,
            excluded_ids=excluded_ids, included_ids=included_ids)

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

    @akari_slash.command(name='ratings', description='Show Akari rating leaderboard')
    async def slash_akari_ratings(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_akari_ratings(_SlashCtx(interaction))
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='rating', description="Show a user's Akari rating graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        decay='Include every day (with decay slopes), not only days played')
    async def slash_akari_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        decay: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_rating(
                _SlashCtx(interaction), [target], include_decay=decay)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='performance', description="Show a user's Akari performance graph")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_performance(_SlashCtx(interaction), [target])
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)
        except Exception:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(interaction, 'An unexpected error occurred.')

    @akari_slash.command(name='history', description="Show a user's Akari rating delta log")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
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

    @akari_slash.command(name='add', description='Manually add a result for a user/puzzle')
    @app_commands.describe(
        member='Player', puzzle_id='Puzzle number',
        result='`perfect` or `N%` (e.g. 92%)',
        time='Time as M:SS or H:MM:SS (e.g. 1:34)')
    async def slash_akari_add(
        self, interaction: discord.Interaction,
        member: discord.Member, puzzle_id: int, result: str, time: str,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_akari_add(
                _SlashCtx(interaction), member, puzzle_id, result, time)
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

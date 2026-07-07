"""Cog-side LinkedIn Queens helpers: constants, namedtuples, arg parsing,
formatting, and the anonymous-registration modal/view.

These are the module-level pieces ``minigames.py`` used to carry for Queens.
They live here to keep the cog module small; ``minigames.py`` re-exports the
names the test suite imports.
"""

import datetime as dt
import hashlib
import pathlib
import re
from collections import namedtuple
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import discord

from tle.util import discord_common
from tle.cogs._minigame_common import (
    format_duration, normalize_puzzle_date, pick_best_results,
    previous_streak_day,
)
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import MinigameCogError


_QUEENS_RESOLVED_ENTRY_FIELDS = (
    'user_id linkedin_name time_seconds no_hints no_mistakes')
_QueensResolvedEntry = namedtuple('_QueensResolvedEntry',
                                  _QUEENS_RESOLVED_ENTRY_FIELDS)
_QueensImportPreview = namedtuple(
    '_QueensImportPreview',
    'puzzle_date puzzle_number resolved unresolved raw_content',
)
_QueensImportSaveResult = namedtuple(
    '_QueensImportSaveResult',
    'resolved unresolved',
)
_QueensBackfillResult = namedtuple(
    '_QueensBackfillResult',
    'link matched saved skipped malformed',
)
_QueensPendingRegistration = namedtuple(
    '_QueensPendingRegistration',
    (
        'guild member channel_id linked_by name normalized_name '
        'anonymous created_at'
    ),
)

_URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)
_QUEENS_HISTORY_PER_PAGE = 15
_QUEENS_CONNECTION_ACCOUNT_KEY = 'queens_connection_account'
_QUEENS_DEFAULT_CONNECTION_ACCOUNT = {
    'name': 'TLE Queens',
    'url': 'https://www.linkedin.com/in/tle-queens-33a339415/',
}
_QUEENS_ANONYMOUS_LINK_MARKER = 'tle:queens:anonymous'
_QUEENS_ANONYMOUS_LABEL = 'Anonymous'
_QUEENS_ANONYMOUS_FLAGS = {'+anon', '+anonymous'}
_QUEENS_PENDING_REGISTRATION_DELAY = 60
_QUEENS_CONNECT_TIMEOUT = 90
_QUEENS_ANCHOR_DATE = dt.date(2026, 6, 8)
_QUEENS_ANCHOR_NUMBER = 769

# Scraper config — stored per-guild in guild_config.
#  - Discord user id of the importer (resolved from `;queens login` whoami)
#  - Optional override for the storage_state.json path
# Rate-limit bookkeeping for `;queens update` lives in kvs under
# `queens_update_throttle:{guild_id}`.
_QUEENS_IMPORTER_KEY = 'queens_importer_user'  # legacy — cleared on login
_QUEENS_LINKEDIN_NAME_KEY = 'queens_linkedin_name'  # display only
_QUEENS_ADMINS_KEY = 'queens_admin_user_ids'
_QUEENS_STATE_PATH_KEY = 'queens_state_path'
_QUEENS_UPDATE_THROTTLE_PREFIX = 'queens_update_throttle:'
_QUEENS_UPDATE_THROTTLE_SECONDS = 60
_QUEENS_DAILY_UPDATE_LAST_PREFIX = 'queens_daily_update_last:'
_QUEENS_DAILY_UPDATE_CHECK_INTERVAL = 60
_QUEENS_DAILY_UPDATE_PRECISE_WINDOW = 300
_QUEENS_DAILY_UPDATE_TIME = '00:00:10'
_QUEENS_DAILY_UPDATE_TZ = 'US/Pacific'
_QUEENS_AUTO_PLAY_MIN_SECONDS = 180
_QUEENS_SCRAPER_TIMEOUT = 480  # seconds — playwright start + delayed auto-play
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
# Uploaded snapshot for ``;mg akari diff``.  A full backup DB zips to a few MiB;
# an akari-only export is tiny.  25 MiB covers a zipped full backup with room to
# spare while still rejecting anything absurd.
_AKARI_DIFF_MAX_BYTES = 25 * 1024 * 1024
_IMPORT_BATCH_SIZE = 500
_IMPORT_RATE_DELAY = 0.5
# tle/cogs/_minigame_queens_cog.py → repo root → extra/queens_scrape.py
_QUEENS_SCRAPER_SCRIPT = (
    pathlib.Path(__file__).resolve().parent.parent.parent
    / 'extra' / 'queens_scrape.py'
)
_QUEENS_DEFAULT_STATE_PATH = (
    _QUEENS_SCRAPER_SCRIPT.parent / '.queens_state.json'
)


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


def _queens_update_target_date(results_day):
    today = dt.datetime.now(ZoneInfo(_QUEENS_DAILY_UPDATE_TZ)).date()
    if results_day == 'yesterday':
        return today - dt.timedelta(days=1)
    return dt.datetime.now(dt.timezone.utc).date()


def _queens_daily_update_target_datetime(now):
    parts = [int(part) for part in _QUEENS_DAILY_UPDATE_TIME.split(':')]
    hour, minute = parts[:2]
    second = parts[2] if len(parts) > 2 else 0
    return now.replace(hour=hour, minute=minute, second=second, microsecond=0)


def _parse_queens_update_args(args):
    results_day = 'today'
    for arg in args:
        text = str(arg).strip().casefold()
        if text in ('+today', 'today'):
            results_day = 'today'
        elif text in ('+yesterday', '+yday', '+yestrday', 'yesterday'):
            results_day = 'yesterday'
        else:
            raise MinigameCogError(
                'Usage: `;queens update [+yesterday]`.')
    return results_day


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


def _queens_streak_info(rows, weekdays=None):
    best = _queens_best_results_by_date(rows)
    if not best:
        return 0, 0, None

    latest_day = max(best)
    current = 0
    day = latest_day
    while day in best and best[day].is_perfect:
        current += 1
        day = previous_streak_day(day, weekdays)

    longest = 0
    run = 0
    previous_day = None
    for day in sorted(best):
        if best[day].is_perfect:
            is_consecutive = (
                previous_day is not None
                and previous_streak_day(day, weekdays) == previous_day
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
        async def send(content=None, *, embed=None, **kwargs):
            await interaction.response.send_message(
                content=content, embed=embed, ephemeral=True, **kwargs)

        ctx = SimpleNamespace(
            guild=interaction.guild,
            author=interaction.user,
            channel=SimpleNamespace(id=getattr(interaction, 'channel_id', None)),
            send=send,
            reveal_queens_anonymous_name=True,
        )
        try:
            await self.cog._cmd_queens_register(
                ctx, interaction.user, self.linkedin_name.value,
                anonymous=True)
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

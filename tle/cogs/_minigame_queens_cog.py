"""Cog-side LinkedIn-game helpers: constants, namedtuples, arg parsing,
formatting, and the anonymous-registration modal/view.

Written for Queens and now shared by every LinkedIn game (Queens, Tango);
the ``_queens_*`` names are kept because the cog modules and the test suite
import them.  Helpers that depend on the game's calendar take ``game`` and
read ``game.linkedin``; the Queens-bound wrappers at the bottom preserve the
historical zero-``game`` signatures.
"""

import datetime as dt
import hashlib
import re
from collections import namedtuple
from types import SimpleNamespace

import discord

from tle.util import discord_common
from tle.cogs._minigame_common import (
    format_duration, normalize_puzzle_date, pick_best_results,
    previous_streak_day,
)
# The Queens calendar (anchor, both date/number directions, and the Pacific
# "today") lives in ``_minigame_queens``.  Re-exported here so every existing
# ``from _minigame_queens_cog import ...`` keeps working.
from tle.cogs._minigame_queens import (  # noqa: F401  (re-exports)
    QUEENS_GAME, _QUEENS_ANCHOR_DATE, _QUEENS_ANCHOR_NUMBER,
    _QUEENS_TIME_ZONE, _queens_current_puzzle_date,
    _queens_date_for_puzzle_number, _queens_puzzle_number_for_date,
    queens_best_result_sort_key, queens_result_group_key,
)
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

_URL_RE = re.compile(r'https?://\S+', re.IGNORECASE)
_QUEENS_HISTORY_PER_PAGE = 15
# Stored in ``external_url`` on the (shared) LinkedIn link row.  The
# ``queens`` in the sentinel is historical; it marks the link anonymous for
# every LinkedIn game and must not change without a data migration.
_QUEENS_ANONYMOUS_LINK_MARKER = 'tle:queens:anonymous'
_QUEENS_ANONYMOUS_LABEL = 'Anonymous'
_QUEENS_ANONYMOUS_FLAGS = {'+anon', '+anonymous'}

_QUEENS_ADMINS_KEY = QUEENS_GAME.linkedin.admins_key
# Backfill JSON files can be much larger (years of history × many
# players).  10 MiB covers any realistic LinkedIn export.
_QUEENS_BACKFILL_MAX_BYTES = 10 * 1024 * 1024
# Uploaded snapshot for ``;mg akari diff``.  A full backup DB zips to a few MiB;
# an akari-only export is tiny.  25 MiB covers a zipped full backup with room to
# spare while still rejecting anything absurd.
_AKARI_DIFF_MAX_BYTES = 25 * 1024 * 1024
_IMPORT_BATCH_SIZE = 500
_IMPORT_RATE_DELAY = 0.5


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
        f'Could not parse date `{date_text}`. Use `YYYY-MM-DD`.')


def _parse_linkedin_date_or_number(game, value):
    """A date in any accepted format, or a ``#123`` / ``123`` puzzle number."""
    try:
        return _parse_queens_date(value)
    except MinigameCogError:
        text = str(value).strip()
        if text.startswith('#'):
            text = text[1:]
        if text.isdigit():
            return game.linkedin.date_for_number(int(text))
        raise


def _queens_puzzle_date_text(puzzle_date):
    return normalize_puzzle_date(puzzle_date).isoformat()


def _linkedin_result_message_id(game, guild_id, puzzle_date, user_id):
    """Deterministic synthetic message id for a projected LinkedIn result.

    Keyed by game so the same person's Queens and Tango rows for one date
    never collide in ``minigame_result``.  Queens' key is unchanged from
    before Tango existed, so existing rows re-sync without churn.
    """
    date_text = _queens_puzzle_date_text(puzzle_date)
    raw = f'{guild_id}:{game.name}:{date_text}:{user_id}'.encode('utf-8')
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


def _queens_public_link_sort_key(link):
    """Sort links using only values that are safe to expose publicly."""
    return (
        _queens_public_link_name(link).casefold(),
        str(getattr(link, 'user_id', '')),
    )


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
    # Time-only sort keys shared by every LinkedIn game.
    return pick_best_results(
        rows,
        sort_key_fn=queens_best_result_sort_key,
        group_key_fn=queens_result_group_key,
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
    def __init__(self, cog, game):
        super().__init__(title=f'Register for {game.display_name}')
        self.cog = cog
        self.game = game
        self.linkedin_name = discord.ui.TextInput(
            label='LinkedIn display name',
            placeholder='Name as it appears on the LinkedIn leaderboard',
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
                ctx, self.game, interaction.user, self.linkedin_name.value,
                anonymous=True)
        except MinigameCogError as exc:
            await interaction.response.send_message(
                embed=discord_common.embed_alert(str(exc)),
                ephemeral=True)


class _QueensAnonymousRegisterView(discord.ui.View):
    def __init__(self, cog, game, requester_id):
        super().__init__(timeout=300)
        self.cog = cog
        self.game = game
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
            _QueensAnonymousRegisterModal(self.cog, self.game))


# ── Queens-bound wrappers (historical signatures) ───────────────────────

def _parse_queens_date_or_number(value):
    return _parse_linkedin_date_or_number(QUEENS_GAME, value)


def _queens_puzzle_numbers_for_date(puzzle_date):
    return QUEENS_GAME.linkedin.puzzle_numbers_for_date(puzzle_date)


def _queens_result_message_id(guild_id, puzzle_date, user_id):
    return _linkedin_result_message_id(
        QUEENS_GAME, guild_id, puzzle_date, user_id)

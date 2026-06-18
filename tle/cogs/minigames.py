"""Minigames cog: Daily Akari, LinkedIn Queens, and GuessThe.Game.

The cog is large, so its implementation is split across mixin modules in this
package (``_mgimpl_*`` for logic, ``_mgcmds_*`` for command/slash groups) plus
shared helper modules (``_minigame_*``).  ``Minigames`` below stitches the
mixins together; this module also re-exports every symbol the test suite
imports by name or monkeypatches via ``tle.cogs.minigames``.
"""

import logging

# Module objects / names the test suite patches as attributes of this module
# (``minigames_module.cairo``/``Pango``/``PangoCairo``/``discord``/``ZoneInfo``);
# importing them here makes those names resolve to the shared module objects, so
# the patches propagate to the table renderers in ``_minigame_tables``.  They are
# unused in this module's own body, hence the per-line F401 suppressions — keep
# them; an auto-import-pruner (ruff --fix / autoflake) would otherwise silently
# remove these patch points and break the tests and table rendering.
import cairo  # noqa: F401
import discord  # noqa: F401
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo  # noqa: F401
from zoneinfo import ZoneInfo  # noqa: F401

from discord.ext import commands

from tle.util import discord_common
# Kept so tests can patch ``minigames_module.paginator.paginate``.
from tle.util import paginator  # noqa: F401

# ``expected_puzzle_number`` is reached via the ``_mg()`` indirection and
# ``normalize_puzzle_date`` via ``minigames_module.normalize_puzzle_date`` in
# tests, so both must stay module attributes here even though unused in-body.
from tle.cogs._minigame_akari import AKARI_GAME, expected_puzzle_number  # noqa: F401
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_common import normalize_puzzle_date  # noqa: F401

# ── Re-exports for the test suite and downstream importers ──────────────
from tle.cogs._minigame_helpers import (  # noqa: F401
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, queens_mod_only,
    _FollowupChannel, _SlashCtx, _ScheduledCtx,
    _safe_member_name, _safe_user_name, _safe_cf_handle, _legend_name_for,
    _format_score, _format_akari_history_line, _format_minigame_history_line,
    _format_akari_ban_line,
)
from tle.cogs._minigame_tables import (  # noqa: F401
    _PuzzlePlayerInfo, _maybe_parse_puzzle_selector, _format_akari_result_status,
    _sort_akari_puzzle_results, _akari_puzzle_table_rows,
    _format_akari_puzzle_table, _get_akari_puzzle_table_image,
    _get_akari_puzzle_table_image_file, _queens_results_table_rows,
    _get_queens_results_table_image_file, _akari_rating_table_rows,
    _akari_row_text_color, _get_akari_rating_table_image_file,
    _akari_weekly_table_rows, _get_akari_weekly_table_image_file,
    _AKARI_HISTORY_PER_PAGE,
)
from tle.cogs._minigame_stats import (  # noqa: F401
    plot_akari_performance, plot_akari_rating,
    plot_akari_stats, plot_guessgame_stats, plot_queens_stats,
)
from tle.cogs._minigame_slash_consts import (  # noqa: F401
    _TIMEFRAME_CHOICES, _MODE_CHOICES,
)
from tle.cogs._minigame_queens_filters import (  # noqa: F401
    _parse_queens_weekday_filter_arg, _split_queens_weekday_filter,
    _filter_queens_weekday_rows, _split_queens_rating_date_filter,
    _split_queens_recalculate_filter, _filter_queens_rating_date_rows,
    _filter_queens_rating_date_history, _format_queens_weekday_filter,
    _queens_weekday_filter_suffix, _format_queens_date_filter,
    _queens_filter_suffix, _filter_queens_contested_rating_history,
)
from tle.cogs._minigame_queens_cog import (  # noqa: F401
    _QueensResolvedEntry, _QueensImportPreview, _QueensImportSaveResult,
    _QueensBackfillResult, _QueensPendingRegistration,
    _QueensAnonymousRegisterModal, _QueensAnonymousRegisterView,
    _QUEENS_CONNECTION_ACCOUNT_KEY, _QUEENS_DEFAULT_CONNECTION_ACCOUNT,
    _QUEENS_ANONYMOUS_LINK_MARKER, _QUEENS_ANONYMOUS_LABEL,
    _QUEENS_ANONYMOUS_FLAGS, _QUEENS_PENDING_REGISTRATION_DELAY,
    _QUEENS_CONNECT_TIMEOUT, _QUEENS_IMPORTER_KEY, _QUEENS_LINKEDIN_NAME_KEY,
    _QUEENS_ADMINS_KEY, _QUEENS_STATE_PATH_KEY, _QUEENS_UPDATE_THROTTLE_PREFIX,
    _QUEENS_UPDATE_THROTTLE_SECONDS, _QUEENS_DAILY_UPDATE_LAST_PREFIX,
    _QUEENS_DAILY_UPDATE_CHECK_INTERVAL, _QUEENS_DAILY_UPDATE_PRECISE_WINDOW,
    _QUEENS_DAILY_UPDATE_TIME, _QUEENS_DAILY_UPDATE_TZ,
    _QUEENS_AUTO_PLAY_MIN_SECONDS, _QUEENS_SCRAPER_TIMEOUT,
    _QUEENS_WHOAMI_TIMEOUT, _QUEENS_PLAYWRIGHT_PLATFORM,
    _QUEENS_STATE_MAX_BYTES, _QUEENS_BACKFILL_MAX_BYTES,
    _QUEENS_HISTORY_PER_PAGE, _QUEENS_SCRAPER_SCRIPT, _QUEENS_DEFAULT_STATE_PATH,
    _AKARI_DIFF_MAX_BYTES, _IMPORT_BATCH_SIZE, _IMPORT_RATE_DELAY,
    _parse_queens_date, _queens_puzzle_number_for_date,
    _queens_date_for_puzzle_number, _parse_queens_date_or_number,
    _queens_update_target_date, _queens_daily_update_target_datetime,
    _parse_queens_update_args, _queens_puzzle_numbers_for_date,
    _queens_puzzle_date_text, _queens_result_message_id, _format_queens_date,
    _is_queens_link_anonymous, _queens_public_link_name,
    _split_queens_anonymous_flag, _is_queens_anonymous_modal_request,
    _clean_queens_linkedin_name, _split_queens_connection_account_text,
    _format_queens_result, _queens_best_results_by_date, _queens_streak_info,
)

# Implementation mixins (plain classes — logic only)
from tle.cogs._mgimpl_core import ImplCoreMixin
from tle.cogs._mgimpl_rating import ImplRatingMixin
from tle.cogs._mgimpl_queensreg import ImplQueensRegMixin
from tle.cogs._mgimpl_queensregb import ImplQueensRegBMixin
from tle.cogs._mgimpl_queensimport import ImplQueensImportMixin
from tle.cogs._mgimpl_queenscmd import ImplQueensCmdMixin
from tle.cogs._mgimpl_queenscmdb import ImplQueensCmdBMixin
from tle.cogs._mgimpl_queensscraper import ImplQueensScraperMixin
from tle.cogs._mgimpl_queensbackfill import ImplQueensBackfillMixin
from tle.cogs._mgimpl_queenstext import ImplQueensTextMixin
from tle.cogs._mgimpl_queenstextb import ImplQueensTextBMixin
from tle.cogs._mgimpl_ingest import ImplIngestMixin
from tle.cogs._mgimpl_import import ImplImportMixin
from tle.cogs._mgimpl_sharedcmd import ImplSharedCmdMixin
from tle.cogs._mgimpl_akaria import ImplAkariAMixin
from tle.cogs._mgimpl_akarib import ImplAkariBMixin
from tle.cogs._mgimpl_stats import ImplStatsMixin
from tle.cogs._mgimpl_export import ImplExportMixin

# Command / slash mixins (carry the discord.py command groups)
from tle.cogs._mgcmds_akari import AkariCmdsMixin
from tle.cogs._mgcmds_queens import QueensCmdsMixin
from tle.cogs._mgcmds_guessgame import GuessGameCmdsMixin
from tle.cogs._mgcmds_akarislash import AkariSlashMixin
from tle.cogs._mgcmds_queensslash import QueensSlashMixin

logger = logging.getLogger(__name__)


class Minigames(
    # Command/slash groups first so their callbacks win name lookups where it
    # matters; impl mixins supply the ``_cmd_*`` / helper methods they call.
    AkariCmdsMixin,
    QueensCmdsMixin,
    GuessGameCmdsMixin,
    AkariSlashMixin,
    QueensSlashMixin,
    ImplCoreMixin,
    ImplRatingMixin,
    ImplQueensRegMixin,
    ImplQueensRegBMixin,
    ImplQueensImportMixin,
    ImplQueensCmdMixin,
    ImplQueensCmdBMixin,
    ImplQueensScraperMixin,
    ImplQueensBackfillMixin,
    ImplQueensTextMixin,
    ImplQueensTextBMixin,
    ImplIngestMixin,
    ImplImportMixin,
    ImplSharedCmdMixin,
    ImplAkariAMixin,
    ImplAkariBMixin,
    ImplStatsMixin,
    ImplExportMixin,
    commands.Cog,
):
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
        self._queens_pending_registrations = {}
        self._queens_connect_tasks = {}
        self._queens_update_timers = {}

    @discord_common.send_error_if(MinigameCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Minigames(bot))

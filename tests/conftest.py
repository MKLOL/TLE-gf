"""Test configuration — bypasses heavy imports so DB-layer tests can run
without the full bot environment (aiohttp, discord.py, etc.).

Strategy: Pre-register stubs for all heavy modules and tle subpackages,
then manually load only the specific files we need for testing.

The external-dependency stubs (Step 1) live in ``_conftest_external_stubs``
(imported first below) to keep this file under the 500-line limit.
"""
import importlib
import sys
import types
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Step 1: Stub external dependencies (discord.py, aiohttp, matplotlib, ...) ──
# Importing this module performs the sys.modules stubbing as a side effect; it
# MUST run before any tle.* module is loaded below.
from tests import _conftest_external_stubs  # noqa: F401,E402  (side-effecting import)

_commands_mod = sys.modules['discord.ext.commands']
_discord_mod = sys.modules['discord']

# ── Step 2: Stub tle internal packages ──────────────────────────────────
# We need stubs for every tle.* module that user_db_conn.py imports
# (codeforces_api, codeforces_common) so they don't trigger real loading.

_tle_stubs = [
    'tle',
    'tle.util',
    'tle.util.db',
    'tle.util.codeforces_api',
    'tle.util.codeforces_common',
    'tle.util.cache_system2',
    'tle.util.events',
    'tle.util.tasks',
    'tle.util.handledict',
    'tle.util.paginator',
    'tle.util.discord_common',
    'tle.util.graph_common',
    'tle.constants',
]

for pkg_name in _tle_stubs:
    if pkg_name not in sys.modules:
        mod = types.ModuleType(pkg_name)
        # Determine the filesystem path for packages
        parts = pkg_name.split('.')
        pkg_dir = os.path.join(_root, *parts)
        if os.path.isdir(pkg_dir):
            mod.__path__ = [pkg_dir]
        mod.__package__ = pkg_name
        mod.__all__ = []
        sys.modules[pkg_name] = mod

# tle.constants needs actual values that user_db_conn.py and starboard.py use
constants_mod = sys.modules['tle.constants']
constants_mod._DEFAULT_STAR_COLOR = 0xffaa10
constants_mod._DEFAULT_STAR = '\N{WHITE MEDIUM STAR}'
constants_mod.TLE_ADMIN = 'Admin'
constants_mod.TLE_MODERATOR = 'Moderator'
constants_mod.NOTO_SANS_CJK_BOLD_FONT_PATH = '/tmp/fake-font.ttf'
constants_mod.NOTO_COLOR_EMOJI_FONT_PATH = '/tmp/fake-color-emoji.ttf'
constants_mod.AKARI_START_RATING = 1200
constants_mod.AKARI_RATING_DAMPING = 0.25
constants_mod.AKARI_DECAY_BASE = 0.04
constants_mod.AKARI_DECAY_MAX = 0.08
constants_mod.AKARI_DECAY_GRACE = 0
constants_mod.AKARI_MAX_PUZZLE_LOOKAHEAD = 2
constants_mod.AKARI_RANKING_MAX_INACTIVE_DAYS = 30
constants_mod.QUEENS_DECAY_BASE = 0.04
constants_mod.QUEENS_DECAY_MAX = 0.08
constants_mod.QUEENS_DECAY_GRACE = 0
constants_mod.TANGO_DECAY_BASE = 0.04
constants_mod.TANGO_DECAY_MAX = 0.08
constants_mod.TANGO_DECAY_GRACE = 0
constants_mod.LLM_MODELS = ('model-a', 'model-b')
constants_mod.GEMINI_API_KEYS = ''
constants_mod.XAI_API_KEYS = ''
constants_mod.XAI_MODEL = 'grok-test'
constants_mod.XAI_MODELS = ('grok-test',)
constants_mod.XAI_MAX_OUTPUT_TOKENS = 1536
constants_mod.XAI_ROUTER_MAX_OUTPUT_TOKENS = 256
constants_mod.XAI_USER_RATE_LIMIT = 15
constants_mod.XAI_USER_RATE_WINDOW_SECONDS = 60 * 60
constants_mod.XAI_DAILY_REQUEST_LIMIT = 200
constants_mod.XAI_INPUT_USD_PER_MILLION = 2.00
constants_mod.XAI_OUTPUT_USD_PER_MILLION = 6.00
constants_mod.XAI_DAILY_BUDGET_USD = 0.50
constants_mod.XAI_REQUEST_RESERVE_INPUT_TOKENS = 6000
constants_mod.LLM_REQUEST_TIMEOUT_SECONDS = 90
constants_mod.LLM_ROUTER_TIMEOUT_SECONDS = 15
constants_mod.LLM_QUEUE_TIMEOUT_SECONDS = 10
constants_mod.LLM_GEMINI_CONCURRENCY = 3
constants_mod.LLM_XAI_CONCURRENCY = 2
constants_mod.LLM_TELEMETRY_RETENTION_DAYS = 30
constants_mod.LLM_MAX_PROMPT_CHARS = 4000
constants_mod.LLM_CONTEXT_ENABLED = True
constants_mod.LLM_CONTEXT_MESSAGES = 50
constants_mod.LLM_CONTEXT_WINDOW_SECONDS = 600
constants_mod.LLM_CONTEXT_GAP_SECONDS = 600
constants_mod.LLM_CONTEXT_RECENT_MAX_AGE_SECONDS = 21600
constants_mod.LLM_REPLY_BEFORE = 25
constants_mod.LLM_REPLY_AFTER = 24
constants_mod.LLM_MAX_OUTPUT_TOKENS = 900
constants_mod.LLM_MAX_IMAGES = 4
constants_mod.LLM_MAX_IMAGE_BYTES = 4 * 1024 * 1024
constants_mod.LLM_MAX_TOTAL_IMAGE_BYTES = 12 * 1024 * 1024

# tle.util.codeforces_common needs a user_db attribute and parse_date for starboard cog
import time as _time
import datetime as _datetime
cf_common = sys.modules['tle.util.codeforces_common']
cf_common.user_db = None
cf_common.fix_urls = lambda user: user  # no-op in tests
cf_common.ResolveHandleError = type('ResolveHandleError', (_commands_mod.CommandError,), {})

# Stub cf.User (codeforces_api) as a namedtuple so _make() works
from collections import namedtuple as _nt
from typing import NamedTuple as _NamedTuple, Optional as _Optional, List as _List, Iterable as _Iterable
_cf_api = sys.modules['tle.util.codeforces_api']
_cf_api.User = _nt('User', 'handle firstName lastName country city organization '
                    'contribution rating maxRating lastOnlineTimeSeconds '
                    'registrationTimeSeconds friendOfCount titlePhoto')
_cf_api.RatingChange = _nt('RatingChange',
                            'contestId contestName handle rank '
                            'ratingUpdateTimeSeconds oldRating newRating')
_cf_api.GYM_ID_THRESHOLD = 100000

class _Contest(_NamedTuple):
    id: int
    name: str
    startTimeSeconds: _Optional[int] = None
    durationSeconds: _Optional[int] = None
    type: str = 'CF'
    phase: str = 'FINISHED'
    preparedBy: _Optional[str] = None
    def matches(self, markers):
        def f(s): return ''.join(x for x in s.lower() if x.isalnum())
        return any(f(m) in f(self.name) for m in markers)

class _Member(_NamedTuple):
    handle: str

class _Party(_NamedTuple):
    contestId: _Optional[int] = None
    members: _List[_Member] = []
    participantType: str = 'CONTESTANT'
    teamId: _Optional[int] = None
    teamName: _Optional[str] = None
    ghost: bool = False
    room: _Optional[int] = None
    startTimeSeconds: _Optional[int] = None

class _Problem(_NamedTuple):
    contestId: _Optional[int] = None
    problemsetName: _Optional[str] = None
    index: str = 'A'
    name: str = 'Test'
    type: str = 'PROGRAMMING'
    points: _Optional[float] = None
    rating: _Optional[int] = None
    tags: _List[str] = []
    def matches_all_tags(self, match_tags):
        match_tags = set(match_tags)
        return all(any(mt in t for t in self.tags) for mt in match_tags) if match_tags else True
    def matches_any_tag(self, match_tags):
        match_tags = set(match_tags)
        return any(any(mt in t for t in self.tags) for mt in match_tags) if match_tags else False

class _Submission(_NamedTuple):
    id: int = 0
    contestId: _Optional[int] = None
    problem: _Problem = _Problem()
    author: _Party = _Party()
    programmingLanguage: str = 'C++'
    verdict: _Optional[str] = 'OK'
    creationTimeSeconds: int = 0
    relativeTimeSeconds: int = 0

_cf_api.Contest = _Contest
_cf_api.Member = _Member
_cf_api.Party = _Party
_cf_api.Problem = _Problem
_cf_api.Submission = _Submission

# Stub tle.util.db so codeforces_common can import it
if 'tle.util.db' not in sys.modules:
    _db_stub = types.ModuleType('tle.util.db')
    _db_stub.__path__ = []
    sys.modules['tle.util.db'] = _db_stub

# tle.util.discord_common needs stubs for starboard.py imports
_dc = sys.modules['tle.util.discord_common']
_dc.once = lambda f: f
_dc.send_error_if = lambda *errs: (lambda f: f)
_dc.embed_success = lambda desc: None
_dc.embed_neutral = lambda desc, **kw: None
_dc.embed_alert = lambda desc: None
_dc.random_cf_color = lambda: 0
_dc.cf_color_embed = lambda **kw: None
_dc.set_author_footer = lambda embed, user: None
_dc.attach_image = lambda embed, img_file: None
_dc._ALERT_AMBER = 0xFFBF00
_dc.FeatureDisabledSilent = type('FeatureDisabledSilent', (Exception,), {})
_dc.requires_guild_feature = lambda feature: (lambda f: f)

# tle.util.tasks needs stubs for task_spec decorator and Waiter
_tasks = sys.modules['tle.util.tasks']


class _FakeWaiter:
    @staticmethod
    def fixed_delay(delay, run_first=False):
        return _FakeWaiter()


class _FakeTaskSpec:
    """Stub for tasks.TaskSpec descriptor — makes @tasks.task_spec a no-op decorator."""
    def __init__(self, func):
        self._func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self

    def start(self):
        pass

    def stop(self):
        pass


def _task_spec_decorator(*, name, waiter=None, exception_handler=None):
    def decorator(func):
        return _FakeTaskSpec(func)
    return decorator


_tasks.task_spec = _task_spec_decorator
_tasks.Waiter = _FakeWaiter

# tle.util.paginator needs stubs
_pg = sys.modules['tle.util.paginator']
_pg.chunkify = lambda seq, n: [seq[i:i+n] for i in range(0, len(seq), n)]
_pg.paginate = lambda *a, **kw: None
_pg.NoPagesError = type('NoPagesError', (Exception,), {})

# ── Step 3: Load the actual modules we want to test ─────────────────────

def _load_module(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_util_path = os.path.join(_root, 'tle', 'util')

# Extra discord stubs needed by codeforces_common.py
_discord_mod.Member = type('Member', (), {})
_discord_mod.Role = type('Role', (), {})
_discord_mod.MessageReference = type('MessageReference', (), {
    '__init__': lambda self, **kw: None,
})

# Stub EventSystem so codeforces_common can instantiate it at module level
_events = sys.modules['tle.util.events']
_events.EventSystem = type('EventSystem', (), {'__init__': lambda self: None})
_events.listener_spec = lambda **kw: (lambda f: f)
_events.RatingChangesUpdate = type('RatingChangesUpdate', (), {})

# codeforces_common.py — load for real so SubFilter, parse helpers, etc. are testable.
# Its imports (codeforces_api, cache_system2, db, events) are all stubbed above.
_load_module('tle.util.codeforces_common', os.path.join(_util_path, 'codeforces_common.py'))

_db_path = os.path.join(_root, 'tle', 'util', 'db')


# upgrades.py has no heavy deps — just logging
_load_module('tle.util.db.upgrades', os.path.join(_db_path, 'upgrades.py'))

# cache_db_upgrades.py — depends on upgrades.py (loaded above)
_load_module('tle.util.db.cache_db_upgrades', os.path.join(_db_path, 'cache_db_upgrades.py'))

# starboard_db.py — standalone mixin, no heavy deps
_load_module('tle.util.db.starboard_db', os.path.join(_db_path, 'starboard_db.py'))

# migration_db.py — standalone mixin, no heavy deps
_load_module('tle.util.db.migration_db', os.path.join(_db_path, 'migration_db.py'))

# minigame_db.py — standalone mixin, no heavy deps
_load_module('tle.util.db.minigame_db', os.path.join(_db_path, 'minigame_db.py'))

# user_db_conn.py imports discord.ext.commands, tle.util.codeforces_*, starboard_db, migration_db
# All are stubbed/loaded above, so this should work now
_load_module('tle.util.db.user_db_conn', os.path.join(_db_path, 'user_db_conn.py'))

# user_db_upgrades.py imports from tle.util.db.upgrades (already loaded)
_load_module('tle.util.db.user_db_upgrades', os.path.join(_db_path, 'user_db_upgrades.py'))

# starboard cog — load helpers, backfill mixin, then main cog for pure-function tests
# Needs tle.cogs package stub
if 'tle.cogs' not in sys.modules:
    _cogs_mod = types.ModuleType('tle.cogs')
    _cogs_mod.__path__ = [os.path.join(_root, 'tle', 'cogs')]
    _cogs_mod.__package__ = 'tle.cogs'
    sys.modules['tle.cogs'] = _cogs_mod
_cogs_path = os.path.join(_root, 'tle', 'cogs')
_load_module('tle.cogs._starboard_helpers', os.path.join(_cogs_path, '_starboard_helpers.py'))
_load_module('tle.cogs._starboard_backfill', os.path.join(_cogs_path, '_starboard_backfill.py'))
_load_module('tle.cogs._starboard_render', os.path.join(_cogs_path, '_starboard_render.py'))
_load_module('tle.cogs.starboard', os.path.join(_cogs_path, 'starboard.py'))
_load_module('tle.cogs._migrate_helpers', os.path.join(_cogs_path, '_migrate_helpers.py'))
_load_module('tle.cogs._migrate_retry', os.path.join(_cogs_path, '_migrate_retry.py'))
_load_module('tle.cogs.migrate', os.path.join(_cogs_path, 'migrate.py'))
_load_module('tle.cogs.rpoll', os.path.join(_cogs_path, 'rpoll.py'))
_load_module('tle.cogs.codeforces', os.path.join(_cogs_path, 'codeforces.py'))
_load_module('tle.cogs.handles', os.path.join(_cogs_path, 'handles.py'))

# graph_common stubs for versus.py
_gc = sys.modules['tle.util.graph_common']
_gc.rating_color_cycler = None  # stub — not used in pure-function tests
_gc.get_current_figure_as_file = lambda: None

_load_module('tle.cogs.versus', os.path.join(_cogs_path, 'versus.py'))

# table.py — pure module, no heavy deps
_load_module('tle.util.table', os.path.join(_util_path, 'table.py'))

# graphs.py — load for perftable pure-function tests
_load_module('tle.cogs.graphs', os.path.join(_cogs_path, 'graphs.py'))

# minigame cog modules
_load_module('tle.cogs._minigame_common', os.path.join(_cogs_path, '_minigame_common.py'))
_load_module('tle.cogs._minigame_akari', os.path.join(_cogs_path, '_minigame_akari.py'))
_load_module('tle.cogs._minigame_guessgame', os.path.join(_cogs_path, '_minigame_guessgame.py'))
_load_module('tle.cogs._minigame_queens', os.path.join(_cogs_path, '_minigame_queens.py'))
_load_module('tle.cogs.minigames', os.path.join(_cogs_path, 'minigames.py'))
_load_module('tle.cogs.greatday', os.path.join(_cogs_path, 'greatday.py'))

# LLM stack — pure helpers plus the cog (its deps are all stubbed above)
_load_module('tle.util.db.llm_db', os.path.join(_db_path, 'llm_db.py'))
_load_module('tle.util.llm_keypool', os.path.join(_util_path, 'llm_keypool.py'))
_load_module('tle.util.llm_models', os.path.join(_util_path, 'llm_models.py'))
_load_module('tle.util.gemini_api', os.path.join(_util_path, 'gemini_api.py'))
_load_module('tle.util.xai_api', os.path.join(_util_path, 'xai_api.py'))
_load_module('tle.cogs._llm_context', os.path.join(_cogs_path, '_llm_context.py'))
_load_module('tle.cogs._llm_transcript', os.path.join(_cogs_path, '_llm_transcript.py'))
_load_module('tle.cogs._llm_history', os.path.join(_cogs_path, '_llm_history.py'))
_load_module('tle.cogs._llm_pipeline', os.path.join(_cogs_path, '_llm_pipeline.py'))
_load_module('tle.cogs._llm_format', os.path.join(_cogs_path, '_llm_format.py'))
_load_module('tle.cogs._llm_ask', os.path.join(_cogs_path, '_llm_ask.py'))
_load_module('tle.cogs.llm', os.path.join(_cogs_path, 'llm.py'))

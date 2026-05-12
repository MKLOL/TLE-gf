"""Test configuration — bypasses heavy imports so DB-layer tests can run
without the full bot environment (aiohttp, discord.py, etc.).

Strategy: Pre-register stubs for all heavy modules and tle subpackages,
then manually load only the specific files we need for testing.
"""
import importlib
import sys
import types
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Step 1: Stub ALL external dependencies ──────────────────────────────
_STUB_MODULES = [
    'aiohttp', 'aiohttp.web',
    'discord', 'discord.ext', 'discord.ext.commands',
    'seaborn', 'matplotlib', 'matplotlib.pyplot', 'matplotlib.ticker',
    'matplotlib.dates', 'matplotlib.font_manager',
    'matplotlib.patches', 'matplotlib.lines',
    'numpy', 'pandas', 'pandas.plotting',
    'lxml', 'lxml.html',
    'PIL', 'PIL.Image', 'PIL.ImageFont', 'PIL.ImageDraw',
    'cairo', 'gi', 'gi.repository',
    'aiocache',
]

for mod_name in _STUB_MODULES:
    if mod_name not in sys.modules:
        stub = types.ModuleType(mod_name)
        stub.__path__ = []
        stub.__all__ = []
        sys.modules[mod_name] = stub

# numpy stubs for versus.py
_np = sys.modules['numpy']
_np.arange = lambda *a, **kw: []
_np.linspace = lambda *a, **kw: []
_np.convolve = lambda *a, **kw: []
_np.median = lambda *a, **kw: 0
_np.ones = lambda *a, **kw: []

# pandas stubs
_pd = sys.modules['pandas']
_pd_plotting = sys.modules['pandas.plotting']
_pd_plotting.register_matplotlib_converters = lambda: None
_pd.plotting = _pd_plotting

# matplotlib stubs
_mpl_ticker = sys.modules['matplotlib.ticker']
_mpl_ticker.MultipleLocator = type('MultipleLocator', (), {'__init__': lambda self, *a: None})

# Add specific attributes that get imported at module level
_commands_mod = sys.modules['discord.ext.commands']
_commands_mod.CommandError = type('CommandError', (Exception,), {})

# Stub commands.Cog so starboard.py can be imported for pure-function tests
class _StubCog:
    @staticmethod
    def listener():
        return lambda f: f
_commands_mod.Cog = _StubCog
_commands_mod.has_role = lambda role: (lambda f: f)
_commands_mod.has_any_role = lambda *roles: (lambda f: f)
_commands_mod.command = lambda **kw: (lambda f: f)
_commands_mod.Converter = type('Converter', (), {})
def _member_converter_convert(self, ctx, argument):
    """Stub convert — tries guild.get_member_named (case-sensitive like real discord.py)."""
    raise _commands_mod.BadArgument(f'Member "{argument}" not found.')

_commands_mod.MemberConverter = type('MemberConverter', (), {
    '__call__': lambda self, *a, **kw: None,
    'convert': _member_converter_convert,
})
_commands_mod.TextChannelConverter = type('TextChannelConverter', (), {'convert': lambda self, *a, **kw: None})
_commands_mod.ThreadConverter = type('ThreadConverter', (), {'convert': lambda self, *a, **kw: None})
_commands_mod.BadArgument = type('BadArgument', (_commands_mod.CommandError,), {})
# commands.errors.CommandError is used by resolve_handles
_commands_errors = types.ModuleType('discord.ext.commands.errors')
_commands_errors.CommandError = _commands_mod.CommandError
_commands_mod.errors = _commands_errors
sys.modules['discord.ext.commands.errors'] = _commands_errors

class _StubGroupResult:
    """Fake return value of @commands.group() — supports chained .command() and .group()."""
    def __init__(self, func):
        self.__name__ = getattr(func, '__name__', 'stub')
        self.__doc__ = getattr(func, '__doc__', None)
        self.__wrapped__ = func
    def command(self, **kw):
        return lambda f: _StubGroupResult(f)
    def group(self, **kw):
        return lambda f: _StubGroupResult(f)
    def __call__(self, *a, **kw):
        pass

_commands_mod.group = lambda **kw: (lambda f: _StubGroupResult(f))

# Stub discord.app_commands for slash command support
_app_commands = types.ModuleType('discord.app_commands')
_app_commands.__path__ = []

class _StubAppGroup:
    """Fake app_commands.Group — supports .command() decorator and class-var use."""
    def __init__(self, *, name='', description='', parent=None, guild_only=False):
        self.name = name
        self.description = description
    def command(self, **kw):
        return lambda f: f

class _StubChoice:
    def __init__(self, *, name, value):
        self.name = name
        self.value = value
    def __class_getitem__(cls, item):
        return cls

_app_commands.Group = _StubAppGroup
_app_commands.Choice = _StubChoice
_app_commands.describe = lambda **kw: (lambda f: f)
_app_commands.choices = lambda **kw: (lambda f: f)

sys.modules['discord.app_commands'] = _app_commands
sys.modules['discord'].app_commands = _app_commands

# Stub discord module attributes used by starboard.py
_discord_mod = sys.modules['discord']
class _StubEmbed:
    """Minimal Embed stub that tracks fields, title, footer, image, and author."""
    def __init__(self, **kw):
        self.color = kw.get('color')
        self.timestamp = kw.get('timestamp')
        self.title = kw.get('title')
        self.description = kw.get('description')
        self.fields = []
        self.footer = None
        self.image_url = None
        self.author_data = None

    def add_field(self, *, name=None, value=None, inline=True):
        self.fields.append({'name': name, 'value': value, 'inline': inline})

    def set_image(self, *, url=None):
        self.image_url = url

    def set_footer(self, *, text=None, icon_url=None):
        self.footer = {'text': text, 'icon_url': icon_url}

    def set_author(self, *, name=None, icon_url=None, url=None):
        self.author_data = {'name': name, 'icon_url': icon_url, 'url': url}

    def to_dict(self):
        d = {}
        if self.title: d['title'] = self.title
        if self.description: d['description'] = self.description
        if self.color is not None: d['color'] = int(self.color) if not isinstance(self.color, int) else self.color
        if self.fields: d['fields'] = list(self.fields)
        if self.footer: d['footer'] = self.footer
        if self.image_url: d['image'] = {'url': self.image_url}
        if self.author_data: d['author'] = self.author_data
        if self.timestamp: d['timestamp'] = str(self.timestamp)
        return d

    @classmethod
    def from_dict(cls, data):
        e = cls(
            title=data.get('title'),
            description=data.get('description'),
            color=data.get('color'),
            timestamp=data.get('timestamp'),
        )
        for f in data.get('fields', []):
            e.add_field(name=f.get('name'), value=f.get('value'), inline=f.get('inline', True))
        if data.get('footer'):
            e.set_footer(text=data['footer'].get('text'), icon_url=data['footer'].get('icon_url'))
        if data.get('image'):
            e.set_image(url=data['image'].get('url'))
        if data.get('author'):
            a = data['author']
            e.set_author(name=a.get('name'), icon_url=a.get('icon_url'), url=a.get('url'))
        return e

_discord_mod.Embed = _StubEmbed
_discord_mod.MessageType = type('MessageType', (), {'default': 0, 'reply': 1})

class _StubColor:
    @staticmethod
    def from_rgb(r, g, b):
        return (r << 16) | (g << 8) | b
_discord_mod.Color = _StubColor
_discord_mod.Colour = _StubColor
_discord_mod.DeletedReferencedMessage = type('DeletedReferencedMessage', (), {})
_discord_mod.Object = type('Object', (), {'__init__': lambda self, *, id=None: setattr(self, 'id', id)})
_discord_mod.NotFound = type('NotFound', (Exception,), {})
_discord_mod.Forbidden = type('Forbidden', (Exception,), {})
_discord_mod.HTTPException = type('HTTPException', (Exception,), {})
_discord_mod.ButtonStyle = type('ButtonStyle', (), {'secondary': 2, 'primary': 1})
_discord_mod.Interaction = type('Interaction', (), {})
_discord_mod.TextChannel = type('TextChannel', (), {})

_gi = sys.modules['gi']
_gi.require_version = lambda *a, **kw: None
_gi_repo = sys.modules['gi.repository']
_gi_repo.Pango = types.SimpleNamespace(
    font_description_from_string=lambda s: s,
    EllipsizeMode=types.SimpleNamespace(END=0),
)
_gi_repo.PangoCairo = types.SimpleNamespace(
    create_layout=lambda ctx: types.SimpleNamespace(
        set_font_description=lambda *a, **kw: None,
        set_ellipsize=lambda *a, **kw: None,
        set_width=lambda *a, **kw: None,
        set_markup=lambda *a, **kw: None,
    ),
    show_layout=lambda *a, **kw: None,
)

_pil_image = sys.modules['PIL.Image']
_pil_image.new = lambda *a, **kw: object()
_pil_imagefont = sys.modules['PIL.ImageFont']
_pil_imagefont.truetype = lambda *a, **kw: object()
_pil_imagedraw = sys.modules['PIL.ImageDraw']
_pil_imagedraw.Draw = lambda *a, **kw: object()

# discord.utils stubs
_discord_utils = types.ModuleType('discord.utils')
def _escape_mentions(text):
    return text.replace('@everyone', '@\u200beveryone').replace('@here', '@\u200bhere')
_discord_utils.escape_mentions = _escape_mentions
sys.modules['discord.utils'] = _discord_utils
_discord_mod.utils = _discord_utils

# discord.ui stubs for rpoll
_discord_ui = types.ModuleType('discord.ui')
_discord_ui.__path__ = []
class _StubView:
    def __init__(self, *, timeout=None):
        self.timeout = timeout
        self.children = []
    def add_item(self, item):
        self.children.append(item)
class _StubButton:
    def __init__(self, *, style=None, emoji=None, custom_id=None, label=None):
        self.style = style
        self.emoji = emoji
        self.custom_id = custom_id
        self.label = label
_discord_ui.View = _StubView
_discord_ui.Button = _StubButton
sys.modules['discord.ui'] = _discord_ui
_discord_mod.ui = _discord_ui

sys.modules['aiocache'].cached = lambda *a, **kw: (lambda f: f)  # no-op decorator

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
_load_module('tle.cogs.minigames', os.path.join(_cogs_path, 'minigames.py'))
_load_module('tle.cogs.greatday', os.path.join(_cogs_path, 'greatday.py'))

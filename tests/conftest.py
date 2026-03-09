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
    'numpy', 'pandas', 'pandas.plotting',
    'lxml', 'lxml.html',
    'PIL', 'PIL.Image',
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
_commands_mod.command = lambda **kw: (lambda f: f)

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

_discord_mod.Embed = _StubEmbed
_discord_mod.MessageType = type('MessageType', (), {'default': 0, 'reply': 1})

class _StubColor:
    @staticmethod
    def from_rgb(r, g, b):
        return (r << 16) | (g << 8) | b
_discord_mod.Color = _StubColor
_discord_mod.Colour = _StubColor
_discord_mod.DeletedReferencedMessage = type('DeletedReferencedMessage', (), {})
_discord_mod.NotFound = type('NotFound', (Exception,), {})
_discord_mod.Forbidden = type('Forbidden', (Exception,), {})
_discord_mod.HTTPException = type('HTTPException', (Exception,), {})
_discord_mod.ButtonStyle = type('ButtonStyle', (), {'secondary': 2, 'primary': 1})
_discord_mod.Interaction = type('Interaction', (), {})

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

# tle.util.codeforces_common needs a user_db attribute and parse_date for starboard cog
import time as _time
import datetime as _datetime
cf_common = sys.modules['tle.util.codeforces_common']
cf_common.user_db = None
cf_common.fix_urls = lambda user: user  # no-op in tests
cf_common.ResolveHandleError = type('ResolveHandleError', (_commands_mod.CommandError,), {})

# Stub cf.User (codeforces_api) as a namedtuple so _make() works
from collections import namedtuple as _nt
_cf_api = sys.modules['tle.util.codeforces_api']
_cf_api.User = _nt('User', 'handle first_name last_name country city organization '
                    'contribution rating maxRating last_online_time '
                    'registration_time friend_of_count title_photo')

class _ParamParseError(_commands_mod.CommandError):
    pass

def _parse_date(arg):
    try:
        if len(arg) == 8:
            fmt = '%d%m%Y'
        elif len(arg) == 6:
            fmt = '%m%Y'
        elif len(arg) == 4:
            fmt = '%Y'
        else:
            raise ValueError
        return _time.mktime(_datetime.datetime.strptime(arg, fmt).timetuple())
    except ValueError:
        raise _ParamParseError(f'{arg} is an invalid date argument')

cf_common.parse_date = _parse_date
cf_common.ParamParseError = _ParamParseError

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

# tle.util.paginator needs stubs
_pg = sys.modules['tle.util.paginator']
_pg.chunkify = lambda seq, n: [seq[i:i+n] for i in range(0, len(seq), n)]
_pg.paginate = lambda *a, **kw: None
_pg.NoPagesError = type('NoPagesError', (Exception,), {})

# ── Step 3: Load the actual modules we want to test ─────────────────────
_db_path = os.path.join(_root, 'tle', 'util', 'db')


def _load_module(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# upgrades.py has no heavy deps — just logging
_load_module('tle.util.db.upgrades', os.path.join(_db_path, 'upgrades.py'))

# starboard_db.py — standalone mixin, no heavy deps
_load_module('tle.util.db.starboard_db', os.path.join(_db_path, 'starboard_db.py'))

# user_db_conn.py imports discord.ext.commands, tle.util.codeforces_*, and starboard_db
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
_load_module('tle.cogs.starboard', os.path.join(_cogs_path, 'starboard.py'))
_load_module('tle.cogs.rpoll', os.path.join(_cogs_path, 'rpoll.py'))

# graph_common stubs for versus.py
_gc = sys.modules['tle.util.graph_common']
_gc.rating_color_cycler = None  # stub — not used in pure-function tests
_gc.get_current_figure_as_file = lambda: None

_load_module('tle.cogs.versus', os.path.join(_cogs_path, 'versus.py'))

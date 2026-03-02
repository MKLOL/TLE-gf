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
    'seaborn', 'matplotlib', 'matplotlib.pyplot',
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

# Add specific attributes that get imported at module level
sys.modules['discord.ext.commands'].CommandError = type('CommandError', (Exception,), {})
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

# tle.util.codeforces_common needs a user_db attribute for starboard cog
cf_common = sys.modules['tle.util.codeforces_common']
cf_common.user_db = None

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

# user_db_conn.py imports discord.ext.commands and tle.util.codeforces_*
# Both are stubbed above, so this should work now
_load_module('tle.util.db.user_db_conn', os.path.join(_db_path, 'user_db_conn.py'))

# user_db_upgrades.py imports from tle.util.db.upgrades (already loaded)
_load_module('tle.util.db.user_db_upgrades', os.path.join(_db_path, 'user_db_upgrades.py'))

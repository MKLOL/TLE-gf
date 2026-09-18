"""External-dependency stubs for the test environment.

Extracted from ``conftest.py`` to keep it under the 500-line limit. Importing
this module registers lightweight stubs for all heavy third-party packages
(discord.py, aiohttp, matplotlib, PIL, gi/cairo, aiocache, ...) in
``sys.modules`` as an import-time side effect. conftest imports this FIRST,
before any ``tle`` module is loaded.
"""
import sys
import types

# ── Stub ALL external dependencies ──────────────────────────────────────
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

# aiohttp stubs for gemini_api (which binds ClientError at import time)
_aiohttp = sys.modules['aiohttp']
_aiohttp.ClientError = type('ClientError', (Exception,), {})
_aiohttp.ClientTimeout = type('ClientTimeout', (), {
    '__init__': lambda self, **kw: None})


class _StubClientSession:
    """Never performs I/O — tests inject responses at a higher layer."""

    def __init__(self, *args, **kwargs):
        self.closed = False

    async def close(self):
        self.closed = True


_aiohttp.ClientSession = _StubClientSession

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
    def listener(*args, **kwargs):
        return lambda f: f
_commands_mod.Cog = _StubCog
_commands_mod.has_role = lambda role: (lambda f: f)
_commands_mod.has_any_role = lambda *roles: (lambda f: f)
def _stub_command(**kw):
    def decorator(f):
        f.__wrapped__ = f
        return f
    return decorator
_commands_mod.command = _stub_command
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
_commands_mod.CheckFailure = type('CheckFailure', (_commands_mod.CommandError,), {})
_commands_mod.MissingAnyRole = type('MissingAnyRole', (_commands_mod.CheckFailure,), {})
# commands.errors.CommandError is used by resolve_handles
_commands_errors = types.ModuleType('discord.ext.commands.errors')
_commands_errors.CommandError = _commands_mod.CommandError
_commands_mod.errors = _commands_errors
sys.modules['discord.ext.commands.errors'] = _commands_errors

class _StubGroupResult:
    """Fake return value of @commands.group() — supports chained .command() and .group()."""
    def __init__(self, func, attrs=None):
        attrs = attrs or {}
        self.__name__ = getattr(func, '__name__', 'stub')
        self.__doc__ = getattr(func, '__doc__', None)
        self.__wrapped__ = func
        # discord.py exposes the undecorated coroutine here, which lets tests
        # invoke a command without a Bot.
        self.callback = func
        self.name = attrs.get('name', self.__name__)
        self.aliases = list(attrs.get('aliases', ()))
        self.brief = attrs.get('brief')
        self.help = attrs.get('help', self.__doc__)
        self.usage = attrs.get('usage')
        self.extras = attrs.get('extras', {})
        self.parent = None
        self.all_commands = {}
    def command(self, **kw):
        return self._register(kw)
    def group(self, **kw):
        return self._register(kw)
    def _register(self, attrs):
        def decorator(func):
            child = _StubGroupResult(func, attrs)
            child.parent = self
            self.all_commands[child.name] = child
            for alias in child.aliases:
                self.all_commands[alias] = child
            return child
        return decorator
    def __call__(self, *a, **kw):
        pass

_commands_mod.group = lambda **kw: (lambda f: _StubGroupResult(f, kw))

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
_discord_mod.File = type('File', (), {
    '__init__': lambda self, fp=None, filename=None, **kw: (
        setattr(self, 'fp', fp),
        setattr(self, 'filename', filename),
    ) and None,
})
_discord_mod.Message = type('Message', (), {})
_discord_mod.MessageType = type('MessageType', (), {'default': 0, 'reply': 1})
_discord_mod.MessageReferenceType = type(
    'MessageReferenceType', (), {'default': 0, 'reply': 0, 'forward': 1})
_discord_mod.MessageSnapshot = type('MessageSnapshot', (), {})

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
    return text.replace('@everyone', '@​everyone').replace('@here', '@​here')
def _escape_markdown(text):
    return str(text).replace('\\', '\\\\').replace('*', '\\*').replace('_', '\\_')
_discord_utils.escape_mentions = _escape_mentions
_discord_utils.escape_markdown = _escape_markdown
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
        self.callback = None
class _StubModal:
    def __init__(self, *, title=None):
        self.title = title
        self.children = []
    def add_item(self, item):
        self.children.append(item)
class _StubTextInput:
    def __init__(self, *, label=None, placeholder=None, required=True,
                 max_length=None):
        self.label = label
        self.placeholder = placeholder
        self.required = required
        self.max_length = max_length
        self.value = ''
_discord_ui.View = _StubView
_discord_ui.Button = _StubButton
_discord_ui.Modal = _StubModal
_discord_ui.TextInput = _StubTextInput
sys.modules['discord.ui'] = _discord_ui
_discord_mod.ui = _discord_ui

sys.modules['aiocache'].cached = lambda *a, **kw: (lambda f: f)  # no-op decorator

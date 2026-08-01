import asyncio
import importlib.util
from pathlib import Path
import sys
import types

from discord.ext import commands
import pytest
import tle.util
from tle.cogs import llm as llm_cog

tle_util = sys.modules['tle.util']


class _DefaultHelpCommand:
    """Small base class that exercises TleHelp's routing overrides."""

    def __init__(self):
        self.context = None
        self.paginator = types.SimpleNamespace(pages=[])

    async def command_callback(self, ctx, /, *, command=None):
        self.context = ctx
        if command == 'missing':
            await self.send_error_message('No command called "missing" found.')
        else:
            await self.send_pages()

    async def filter_commands(self, cmds, *, sort=False, key=None):
        return list(cmds)


@pytest.fixture
def help_module(monkeypatch):
    monkeypatch.setattr(
        commands, 'DefaultHelpCommand', _DefaultHelpCommand, raising=False,
    )
    monkeypatch.setattr(
        commands,
        'CheckFailure',
        type('CheckFailure', (Exception,), {}),
        raising=False,
    )
    for name in ('codeforces_api', 'db', 'paginator', 'tasks'):
        module = types.ModuleType(f'tle.util.{name}')
        monkeypatch.setitem(sys.modules, module.__name__, module)
        monkeypatch.setattr(tle_util, name, module, raising=False)

    path = Path(__file__).parents[1] / 'tle' / 'util' / 'discord_common.py'
    spec = importlib.util.spec_from_file_location(
        '_test_help_visibility_discord_common', path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Destination:
    def __init__(self):
        self.calls = []

    async def send(self, content=None, **kwargs):
        self.calls.append((content, kwargs))


def _context(*, user_id=123, interaction=None):
    ctx = types.SimpleNamespace(
        author=types.SimpleNamespace(id=user_id, calls=[]),
        channel=_Destination(),
        interaction=interaction,
        calls=[],
    )

    async def send(content=None, **kwargs):
        ctx.calls.append((content, kwargs))

    ctx.send = send
    return ctx


def test_explicit_help_stays_in_the_channel(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.paginator.pages = ['gitgud help']

    asyncio.run(help_command.command_callback(ctx, command='gitgud'))

    assert ctx.channel.calls == [('gitgud help', {})]
    assert ctx.author.calls == []
    assert help_command.get_destination() is ctx.channel


def test_explicit_invalid_help_stays_in_the_channel(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()

    asyncio.run(help_command.command_callback(ctx, command='missing'))

    assert ctx.channel.calls == [('No command called "missing" found.', {})]
    assert ctx.author.calls == []


def test_automatic_prefix_help_is_public(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['group help', 'second page']

    asyncio.run(help_command.send_pages())

    assert ctx.author.calls == []
    assert ctx.channel.calls == [
        ('group help', {}),
        ('second page', {}),
    ]


def test_automatic_interaction_help_is_also_public(help_module):
    ctx = _context(interaction=object())
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['first page', 'second page']

    asyncio.run(help_command.send_pages())

    assert ctx.channel.calls == [
        ('first page', {}),
        ('second page', {}),
    ]
    assert ctx.calls == []
    assert ctx.author.calls == []


def test_automatic_error_is_public(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx

    asyncio.run(help_command.send_error_message('Try another command.'))

    assert ctx.channel.calls == [('Try another command.', {})]
    assert ctx.author.calls == []


def test_ai_group_help_is_one_compact_public_message(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx

    asyncio.run(help_command.send_group_help(llm_cog.Llm.llm))

    assert len(ctx.channel.calls) == 1
    text, kwargs = ctx.channel.calls[0]
    assert kwargs == {}
    assert 'AI — Gemini & Grok' in text
    assert ';help ai <command>' in text
    assert len(text) < 2000


def test_ai_focused_help_has_usage_without_sibling_dump(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx
    cooldown = llm_cog.Llm.llm.all_commands['cooldown']

    asyncio.run(help_command.send_command_help(cooldown))

    assert len(ctx.channel.calls) == 1
    text, kwargs = ctx.channel.calls[0]
    assert kwargs == {}
    assert ';ai cooldown [seconds] [+threads|+global]' in text
    assert 'shared prompt cooldowns' in text
    assert 'grokkeys' not in text

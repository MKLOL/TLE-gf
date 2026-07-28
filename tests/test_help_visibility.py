import asyncio
import importlib.util
from pathlib import Path
import sys
import types

from discord.ext import commands
import pytest
import tle.util

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

    async def send_message(self, content=None, **kwargs):
        self.calls.append((content, kwargs))


class _Interaction:
    def __init__(self, user_id):
        self.user = types.SimpleNamespace(id=user_id)
        self.response = _Destination()
        self.followup = _Destination()


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


def test_automatic_prefix_help_uses_requester_bound_private_button(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['group help', 'second page']

    asyncio.run(help_command.send_pages())

    assert ctx.author.calls == []
    assert len(ctx.channel.calls) == 1
    prompt, kwargs = ctx.channel.calls[0]
    assert prompt == 'Help is available privately.'
    assert kwargs['delete_after'] == 300
    view = kwargs['view']
    assert view.requester_id == 123

    interaction = _Interaction(123)
    asyncio.run(view.children[0].callback(interaction))

    assert interaction.response.calls == [
        ('group help', {'ephemeral': True}),
    ]
    assert interaction.followup.calls == [
        ('second page', {'ephemeral': True}),
    ]


def test_automatic_help_button_rejects_other_users(help_module):
    ctx = _context(user_id=123)
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['private help']

    asyncio.run(help_command.send_pages())
    view = ctx.channel.calls[0][1]['view']
    interaction = _Interaction(999)
    asyncio.run(view.children[0].callback(interaction))

    assert interaction.response.calls == [
        ('Only the requester can view this help.', {'ephemeral': True}),
    ]
    assert interaction.followup.calls == []


def test_automatic_interaction_help_is_sent_directly_ephemeral(help_module):
    ctx = _context(interaction=object())
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['first page', 'second page']

    asyncio.run(help_command.send_pages())

    assert ctx.calls == [
        ('first page', {'ephemeral': True}),
        ('second page', {'ephemeral': True}),
    ]
    assert ctx.channel.calls == []
    assert ctx.author.calls == []


def test_automatic_error_uses_private_help_button(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx

    asyncio.run(help_command.send_error_message('Try another command.'))

    view = ctx.channel.calls[0][1]['view']
    interaction = _Interaction(123)
    asyncio.run(view.children[0].callback(interaction))
    assert interaction.response.calls == [
        ('Try another command.', {'ephemeral': True}),
    ]

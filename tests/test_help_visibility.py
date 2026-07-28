import asyncio
import importlib.util
import logging
from pathlib import Path
import sys
import types

import discord
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
        self.messages = []

    async def send(self, content):
        self.messages.append(content)


class _ClosedDm:
    def __init__(self, user_id):
        self.id = user_id
        self.calls = 0

    async def send(self, content):
        self.calls += 1
        response = types.SimpleNamespace(
            status=403, reason='Forbidden', text='Forbidden',
        )
        raise discord.Forbidden(response, 'Forbidden')


def _context(*, author=None):
    return types.SimpleNamespace(
        author=author or _Destination(),
        channel=_Destination(),
    )


def test_explicit_help_stays_in_the_channel(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.paginator.pages = ['gitgud help']

    asyncio.run(help_command.command_callback(ctx, command='gitgud'))

    assert ctx.channel.messages == ['gitgud help']
    assert ctx.author.messages == []
    assert help_command.get_destination() is ctx.author


def test_explicit_invalid_help_stays_in_the_channel(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()

    asyncio.run(help_command.command_callback(ctx, command='missing'))

    assert ctx.channel.messages == ['No command called "missing" found.']
    assert ctx.author.messages == []


def test_automatic_help_is_sent_by_dm(help_module):
    ctx = _context()
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['group help']

    asyncio.run(help_command.send_pages())

    assert ctx.author.messages == ['group help']
    assert ctx.channel.messages == []


def test_closed_dms_do_not_leak_help_to_channel(help_module, caplog):
    author = _ClosedDm(123)
    ctx = _context(author=author)
    help_command = help_module.TleHelp()
    help_command.context = ctx
    help_command.paginator.pages = ['private help', 'another page']
    caplog.set_level(logging.INFO, logger=help_module.__name__)

    asyncio.run(help_command.send_pages())

    assert author.calls == 1
    assert ctx.channel.messages == []
    assert 'Could not DM automatic help to user 123' in caplog.text

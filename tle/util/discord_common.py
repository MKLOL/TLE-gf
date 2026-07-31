import asyncio
import logging
import functools
import random
import re

import discord
from discord.ext import commands

from tle.util import codeforces_api as cf
from tle.util import db
from tle.util import paginator
from tle.util import tasks

logger = logging.getLogger(__name__)

_CF_COLORS = (0xFFCA1F, 0x198BCC, 0xFF2020)
_SUCCESS_GREEN = 0x28A745
_ALERT_AMBER = 0xFFBF00
_BOT_PREFIX = ';'
_REDACTED_CREDENTIAL = '[credentials redacted]'

# Provider keys have stable prefixes, but allow only credential-like lengths so
# ordinary prose such as "xai-api" or "an AIza prefix" is left untouched.
_PROVIDER_CREDENTIAL_RE = re.compile(
    r'(?<![A-Za-z0-9_-])(?:'
    r'(?:xai-|AIza)[A-Za-z0-9_-]{20,}|'
    r'(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{20,}|'
    r'AKIA[A-Z0-9]{16}|'
    r'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})'
    r'(?![A-Za-z0-9_-])',
)
_LLM_KEY_COMMAND_RE = re.compile(
    r'\A(?P<command>\s*(?:;|<@!?\d+>\s*)(?:llm|ai)\s+'
    r'(?:keys|grokkeys|xkeys|xaikeys))(?=\s|\Z)[\s\S]*\Z',
    re.IGNORECASE,
)


def redact_credentials(value):
    """Return log-safe text with provider credentials removed.

    A key-management command's entire argument tail is sensitive even when a
    pasted value has an unfamiliar format. Provider-shaped tokens are also
    redacted wherever they occur, including exception messages.
    """
    text = str(value)
    command = _LLM_KEY_COMMAND_RE.match(text)
    if command is not None:
        text = f'{command.group("command")} {_REDACTED_CREDENTIAL}'
    return _PROVIDER_CREDENTIAL_RE.sub(_REDACTED_CREDENTIAL, text)


class RedactingFormatter(logging.Formatter):
    """Sanitize the fully rendered record, including exception traceback."""

    def format(self, record):
        return redact_credentials(super().format(record))


def embed_neutral(desc, color=None):
    return discord.Embed(description=str(desc), color=color)


def embed_success(desc):
    return discord.Embed(description=str(desc), color=_SUCCESS_GREEN)


def embed_alert(desc):
    return discord.Embed(description=str(desc), color=_ALERT_AMBER)


def random_cf_color():
    return random.choice(_CF_COLORS)


def cf_color_embed(**kwargs):
    return discord.Embed(**kwargs, color=random_cf_color())


async def send_paginated_embeds(ctx, description_pages, *, title=None, url=None,
                                wait_time=300):
    embeds = [
        cf_color_embed(title=title, url=url, description=description)
        for description in description_pages
    ]
    if len(embeds) == 1:
        await ctx.send(embed=embeds[0])
        return

    pages = [(None, embed) for embed in embeds]
    paginator.paginate(
        ctx.bot, ctx.channel, pages, wait_time=wait_time,
        set_pagenum_footers=True, author_id=ctx.author.id,
    )


def set_same_cf_color(embeds):
    color = random_cf_color()
    for embed in embeds:
        embed.color=color


def attach_image(embed, img_file):
    embed.set_image(url=f'attachment://{img_file.filename}')


def set_author_footer(embed, user):
    embed.set_footer(text=f'Requested by {user}', icon_url=user.avatar)


class FeatureDisabledSilent(commands.CheckFailure):
    """Raised when a guild feature flag is off. Silently swallowed by the error handler."""
    pass


def requires_guild_feature(feature):
    """Check decorator that silently blocks a command when *feature* is not enabled."""
    async def predicate(ctx):
        from tle.util import codeforces_common as cf_common
        if cf_common.user_db is None:
            raise FeatureDisabledSilent()
        val = cf_common.user_db.get_guild_config(ctx.guild.id, feature)
        if val != '1':
            raise FeatureDisabledSilent()
        return True
    return commands.check(predicate)


def send_error_if(*error_cls):
    """Decorator for `cog_command_error` methods. Decorated methods send the error in an alert embed
    when the error is an instance of one of the specified errors, otherwise the wrapped function is
    invoked.
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(cog, ctx, error):
            if isinstance(error, error_cls):
                await ctx.send(embed=embed_alert(error))
                error.handled = True
            else:
                await func(cog, ctx, error)
        return wrapper
    return decorator


async def bot_error_handler(ctx, exception):
    if getattr(exception, 'handled', False):
        # Errors already handled in cogs should have .handled = True
        return

    if isinstance(exception, FeatureDisabledSilent):
        return

    if isinstance(exception, db.DatabaseDisabledError):
        await ctx.send(embed=embed_alert('Sorry, the database is not available. Some features are disabled.'))
    elif isinstance(exception, commands.NoPrivateMessage):
        await ctx.send(embed=embed_alert('Commands are disabled in private channels'))
    elif isinstance(exception, commands.DisabledCommand):
        await ctx.send(embed=embed_alert('Sorry, this command is temporarily disabled'))
    elif isinstance(exception, (cf.CodeforcesApiError, commands.UserInputError)):
        await ctx.send(embed=embed_alert(exception))
    else:
        msg = 'Ignoring exception in command {}:'.format(ctx.command)
        exc_info = type(exception), exception, exception.__traceback__
        extra = {
            # Never attach a raw credential-bearing command to a LogRecord:
            # file handlers and third-party handlers receive the same record.
            "message_content": redact_credentials(ctx.message.content),
            "jump_url": ctx.message.jump_url
        }
        logger.exception(msg, exc_info=exc_info, extra=extra)


def once(func):
    """Decorator that wraps the given async function such that it is executed only once."""
    first = True

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal first
        if first:
            first = False
            await func(*args, **kwargs)

    return wrapper


def on_ready_event_once(bot):
    """Decorator that uses bot.event to set the given function as the bot's on_ready event handler,
    but does not execute it more than once.
    """
    def register_on_ready(func):
        @bot.event
        @once
        async def on_ready():
            await func()

    return register_on_ready


async def presence(bot):
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.listening,
        name='your commands'))
    await asyncio.sleep(60)

    @tasks.task(name='OrzUpdate',
               waiter=tasks.Waiter.fixed_delay(5*60))
    async def presence_task(_):
        while True:
            target = random.choice([
                member for member in bot.get_all_members()
                if 'Purgatory' not in {role.name for role in member.roles}
            ])
            await bot.change_presence(activity=discord.Game(
                name=f'{target.display_name} orz'))
            await asyncio.sleep(10 * 60)

    presence_task.start()


class TleHelp(commands.DefaultHelpCommand):
    """Send both requested and automatically-triggered help publicly."""

    def get_destination(self):
        return self.context.channel

    async def _send_help_pages(self, pages):
        for page in pages:
            await self.get_destination().send(page)

    async def send_error_message(self, error, /):
        await self._send_help_pages((error,))

    async def send_pages(self):
        await self._send_help_pages(tuple(self.paginator.pages))

    async def filter_commands(self, cmds, *, sort=False, key=None):
        """Like the default, but also drops commands whose ``extras`` declares a
        ``help_hidden_when`` predicate that returns truthy for this invocation.

        This lets a command hide itself from the listing based on runtime state
        (e.g. ``;bet here`` once a channel is configured) without making it
        permanently ``hidden`` — it still works and ``;help <group> <name>``
        still shows it.
        """
        filtered = await super().filter_commands(cmds, sort=sort, key=key)
        ctx = self.context
        result = []
        for command in filtered:
            predicate = (command.extras or {}).get('help_hidden_when')
            if predicate is not None:
                try:
                    if predicate(ctx):
                        continue
                except Exception:
                    pass
            result.append(command)
        return result

    def add_command_formatting(self, command):
        """A utility function to format the non-indented block of commands and groups.

        Parameters
        ------------
        command: :class:`Command`
            The command to format.
        """

        if command.description:
            self.paginator.add_line(command.description, empty=True)

        signature = _BOT_PREFIX + command.qualified_name
        if len(command.aliases) > 0:
            aliases = '|'.join(command.aliases)
            signature += '|'+aliases
        if command.usage:
            signature += " "+command.usage
        self.paginator.add_line(signature, empty=True)

        if command.help:
            try:
                self.paginator.add_line(command.help, empty=True)
            except RuntimeError:
                for line in command.help.splitlines():
                    self.paginator.add_line(line)
                self.paginator.add_line()

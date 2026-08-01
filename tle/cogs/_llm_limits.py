"""Configurable regular-user admission limits for Grok."""
from dataclasses import dataclass
import logging
import math
import re

from tle import constants
from tle.util import discord_common


logger = logging.getLogger(__name__)

CONFIG_KEY = 'llm_grok_user_rate'
MAX_REQUESTS = 1000
MIN_WINDOW_SECONDS = 60
MAX_WINDOW_SECONDS = 24 * 60 * 60
_DEFAULT_REQUESTS = 15
_DEFAULT_WINDOW_SECONDS = 60 * 60
_DURATION_RE = re.compile(r'\A(?P<number>\d+)(?P<unit>[smhd]?)\Z', re.I)
_UNIT_SECONDS = {'': 1, 's': 1, 'm': 60, 'h': 3600, 'd': 86400}


@dataclass(frozen=True)
class UserRateLimit:
    requests: int
    window_seconds: int
    source: str

    @property
    def enabled(self):
        return self.requests > 0


class GrokGuardError(Exception):
    def __init__(self, reason, retry_at=None, setting=None):
        super().__init__(reason)
        self.reason = reason
        self.retry_at = retry_at
        self.setting = setting or default_setting()


def default_setting():
    """Return the validated process default, never an accidental unlimited."""
    requests = _bounded_int(
        getattr(constants, 'XAI_USER_RATE_LIMIT', None), _DEFAULT_REQUESTS,
        minimum=1, maximum=MAX_REQUESTS)
    window = _bounded_int(
        getattr(constants, 'XAI_USER_RATE_WINDOW_SECONDS', None),
        _DEFAULT_WINDOW_SECONDS, minimum=MIN_WINDOW_SECONDS,
        maximum=MAX_WINDOW_SECONDS)
    return UserRateLimit(requests, window, 'default')


def resolve(database, guild_id):
    """Resolve a guild override, falling back safely on missing/corrupt data."""
    fallback = default_setting()
    getter = getattr(database, 'get_guild_config', None)
    if getter is None:
        return fallback
    try:
        raw = getter(guild_id, CONFIG_KEY)
    except Exception:  # noqa: BLE001 - a config read must fail conservatively
        logger.exception('Could not read Grok user rate for guild=%s', guild_id)
        return fallback
    if raw is None:
        return fallback
    if raw == 'off':
        return UserRateLimit(0, fallback.window_seconds, 'server override')
    try:
        requests_text, window_text = str(raw).split(':', 1)
        requests, window = int(requests_text), int(window_text)
    except (TypeError, ValueError):
        logger.warning('Ignoring invalid Grok user rate for guild=%s', guild_id)
        return fallback
    if (not 1 <= requests <= MAX_REQUESTS
            or not MIN_WINDOW_SECONDS <= window <= MAX_WINDOW_SECONDS):
        logger.warning('Ignoring out-of-range Grok user rate for guild=%s',
                       guild_id)
        return fallback
    return UserRateLimit(requests, window, 'server override')


async def configure(cog, ctx, arguments):
    """Show or change this server's regular-user Grok allowance."""
    if not await cog._require_guild_moderator(ctx):
        return
    database = cog._llm_db()
    current = resolve(database, ctx.guild.id)
    action, setting, error = _parse(arguments, current)
    if error is not None:
        await ctx.send(embed=discord_common.embed_alert(error))
        return
    if action == 'show':
        await ctx.send(embed=discord_common.embed_neutral(
            _status(current) + '\nAdmin and Moderator roles bypass this '
            'personal limit; shared safeguards still apply.'))
        return
    if action == 'default':
        database.delete_guild_config(ctx.guild.id, CONFIG_KEY)
        setting = default_setting()
    elif action == 'off':
        database.set_guild_config(ctx.guild.id, CONFIG_KEY, 'off')
    else:
        database.set_guild_config(
            ctx.guild.id, CONFIG_KEY,
            f'{setting.requests}:{setting.window_seconds}')
    logger.warning(
        'Grok user rate changed by user=%s guild=%s action=%s value=%s',
        ctx.author.id, ctx.guild.id, action,
        'off' if not setting.enabled else
        f'{setting.requests}/{setting.window_seconds}s')
    await ctx.send(embed=discord_common.embed_success(
        _status(setting) + '\nShared safeguards still apply.'))


def guard_message(error):
    when = 'later'
    if error.retry_at is not None:
        stamp = max(0, int(math.ceil(error.retry_at)))
        when = f'<t:{stamp}:R> (<t:{stamp}:F>)'
    if error.reason == 'user':
        setting = error.setting
        return (
            f'You have used all {setting.requests} Grok requests available '
            f'in this server over the last {_duration(setting.window_seconds)}. '
            f'Try again {when}.')
    return f'Grok\'s shared daily allowance is used up. Try again {when}.'


def _parse(arguments, current):
    values = tuple(str(value).strip().casefold() for value in arguments)
    if not values:
        return 'show', current, None
    if len(values) == 1 and values[0] in ('default', 'reset', 'inherit'):
        return 'default', default_setting(), None
    if len(values) == 1 and values[0] in ('off', '0'):
        return 'off', UserRateLimit(
            0, current.window_seconds, 'server override'), None
    if len(values) > 2:
        return None, None, _usage()
    try:
        requests = int(values[0])
    except ValueError:
        return None, None, _usage()
    if not 1 <= requests <= MAX_REQUESTS:
        return None, None, (
            f'Requests must be from 1 to {MAX_REQUESTS}; use `off` to '
            'remove the personal limit.')
    window = current.window_seconds
    if len(values) == 2:
        window = _parse_duration(values[1])
        if window is None:
            return None, None, (
                'Window must be from 1 minute to 24 hours, such as `30m` '
                'or `1h`.')
    setting = UserRateLimit(requests, window, 'server override')
    return 'set', setting, None


def _parse_duration(value):
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        return None
    seconds = int(match.group('number')) * _UNIT_SECONDS[match.group('unit')]
    if not MIN_WINDOW_SECONDS <= seconds <= MAX_WINDOW_SECONDS:
        return None
    return seconds


def _status(setting):
    if not setting.enabled:
        return 'The regular-user Grok limit is **off** in this server.'
    source = ' (default)' if setting.source == 'default' else ''
    request_word = 'request' if setting.requests == 1 else 'requests'
    return (
        f'Regular users get **{setting.requests} {request_word} per '
        f'{_duration(setting.window_seconds)}** in this server{source}.')


def _duration(seconds):
    for size, singular in ((86400, 'day'), (3600, 'hour'), (60, 'minute')):
        if seconds % size == 0:
            value = seconds // size
            if value == 1:
                return singular
            return f'{value} {singular}s'
    return f'{seconds} seconds'


def _bounded_int(value, fallback, *, minimum, maximum=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return fallback
    if value < minimum or (maximum is not None and value > maximum):
        return fallback
    return value


def _usage():
    return ('Usage: `;ai ratelimit [requests] [window]`, `;ai ratelimit '
            'off`, or `;ai ratelimit default`. Example: `15 1h`.')

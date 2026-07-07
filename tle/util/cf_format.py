"""Pure formatting and argument-parsing helpers for Codeforces commands.

These were split out of ``codeforces_common`` to keep that module under the
project's 500-line limit. They have no dependency on the module-level globals
(``user_db``, ``cache2``, ...) so they live cleanly on their own. They are
re-exported from ``codeforces_common`` for backwards compatibility, so existing
``cf_common.<name>`` references keep working.
"""

import datetime
import math
import time

from discord.ext import commands


class FilterError(commands.CommandError):
    pass


class ParamParseError(FilterError):
    pass


def time_format(seconds):
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return days, hours, minutes, seconds


def pretty_time_format(seconds, *, shorten=False, only_most_significant=False, always_seconds=False):
    days, hours, minutes, seconds = time_format(seconds)
    timespec = [
        (days, 'day', 'days'),
        (hours, 'hour', 'hours'),
        (minutes, 'minute', 'minutes'),
    ]
    timeprint = [(cnt, singular, plural) for cnt, singular, plural in timespec if cnt]
    if not timeprint or always_seconds:
        timeprint.append((seconds, 'second', 'seconds'))
    if only_most_significant:
        timeprint = [timeprint[0]]

    def format_(triple):
        cnt, singular, plural = triple
        return f'{cnt}{singular[0]}' if shorten else f'{cnt} {singular if cnt == 1 else plural}'

    return ' '.join(map(format_, timeprint))


def get_start_and_end_of_month(time):
    time = time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_time = int(time.timestamp())
    if time.month == 12:
        time = time.replace(month=1, year=time.year + 1)
    else:
        time = time.replace(month=time.month + 1)
    end_time = int(time.timestamp())
    return start_time, end_time


def days_ago(t):
    days = (time.time() - t) / (60 * 60 * 24)
    if days < 1:
        return 'today'
    if days < 2:
        return 'yesterday'
    return f'{math.floor(days)} days ago'


def filter_flags(args, params):
    args = list(args)
    flags = [False] * len(params)
    rest = []
    for arg in args:
        try:
            flags[params.index(arg)] = True
        except ValueError:
            rest.append(arg)
    return flags, rest


def negate_flags(*args):
    return [not x for x in args]


def parse_date(arg):
    try:
        if '-' in arg or '/' in arg:
            # Separator forms accepted by both Akari and Queens commands:
            # ISO (2026-06-01) and day-first (01-06-2026), with / or -.
            cleaned = arg.replace('/', '-')
            for fmt in ('%Y-%m-%d', '%d-%m-%Y'):
                try:
                    return time.mktime(
                        datetime.datetime.strptime(cleaned, fmt).timetuple())
                except ValueError:
                    continue
            raise ValueError
        if len(arg) == 8:
            fmt = '%d%m%Y'
        elif len(arg) == 6:
            fmt = '%m%Y'
        elif len(arg) == 4:
            fmt = '%Y'
        else:
            raise ValueError
        return time.mktime(datetime.datetime.strptime(arg, fmt).timetuple())
    except ValueError:
        raise ParamParseError(f'{arg} is an invalid date argument')


def parse_tags(args, *, prefix):
    tags = [x[1:] for x in args if x[0] == prefix]
    return tags


def parse_rating(args, default_value=None):
    for arg in args:
        if arg.isdigit():
            return int(arg)
    return default_value


def parse_daterange(args):
    dlo = 0
    dhi = 10**10
    for arg in args:
        if arg[0:2] == 'd<':
            dhi = min(dhi, parse_date(arg[2:]))
        elif arg[0:3] == 'd>=':
            dlo = max(dlo, parse_date(arg[3:]))
    return (dlo, dhi)

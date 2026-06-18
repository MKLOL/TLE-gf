"""Fetch Daily Akari puzzle difficulty metadata from its public JSON API."""

import datetime as dt
import logging

import aiohttp


logger = logging.getLogger(__name__)

BASE_URL = 'https://dailyakari.com'
_AIOHTTP_CLIENT_ERROR = getattr(aiohttp, 'ClientError', OSError)


def _timezone_offset_minutes():
    offset = dt.datetime.now().astimezone().utcoffset() or dt.timedelta()
    # JavaScript Date.getTimezoneOffset(), expected by Daily Akari, is UTC-local.
    return round(-offset.total_seconds() / 60)


async def _get_json(session, path, params):
    async with session.get(f'{BASE_URL}/{path}', params=params) as response:
        if response.status != 200:
            body = await response.text()
            raise RuntimeError(
                f'Daily Akari {path} returned HTTP {response.status}: {body[:160]}')
        # Daily Akari serves its JSON as ``text/plain``; without
        # ``content_type=None`` aiohttp raises ContentTypeError and refuses to
        # decode it.  A genuinely non-JSON body still raises ValueError, which
        # callers already treat as a soft failure.
        return await response.json(content_type=None)


async def fetch_akari_difficulties(puzzle_numbers, *, session=None):
    """Return available ``{puzzle_number: difficulty}`` metadata.

    The archive is fetched page-wise (rather than one request per puzzle), then
    today's separate endpoint fills the current puzzle.  Failures return the
    partial data collected so callers can fall back to neutral difficulty 3.
    """
    wanted = {int(number) for number in puzzle_numbers if int(number) > 0}
    if not wanted:
        return {}

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    found = {}
    params = {'tz_offset': _timezone_offset_minutes()}
    try:
        try:
            current = await _get_json(session, 'dailypuzzle', params)
            number = int(current.get('dailyNumber', 0))
            difficulty = int(current.get('difficulty', 0))
            if number in wanted and 1 <= difficulty <= 5:
                found[number] = difficulty
        except (_AIOHTTP_CLIENT_ERROR, RuntimeError, TypeError, ValueError):
            logger.warning('Could not fetch current Daily Akari difficulty',
                           exc_info=True)

        remaining = wanted - set(found)
        page = 1
        # At ~50 entries/page this covers years of results while retaining a
        # hard stop if the upstream pagination contract ever changes.
        while remaining and page <= 30:
            query = {
                **params,
                'playable': '0',
                'difficulty': '0',
                'sort': '0',
                'page': str(page),
            }
            try:
                payload = await _get_json(session, 'archivelist', query)
            except (_AIOHTTP_CLIENT_ERROR, RuntimeError, TypeError, ValueError):
                logger.warning('Could not fetch Daily Akari archive page %s',
                               page, exc_info=True)
                break
            entries = payload.get('entries') or []
            if not entries:
                break
            page_numbers = []
            for entry in entries:
                try:
                    number = int(entry['dailyNumber'])
                    difficulty = int(entry['difficulty'])
                except (KeyError, TypeError, ValueError):
                    continue
                page_numbers.append(number)
                if number in remaining and 1 <= difficulty <= 5:
                    found[number] = difficulty
            remaining = wanted - set(found)
            if not payload.get('areMore', payload.get('are_more', True)):
                break
            # The archive is sorted newest-first; once a page's oldest puzzle
            # has dropped below every number we still want, nothing deeper can
            # match.  Derive ``oldest`` from the numbers we actually parsed so a
            # single malformed entry can't abort the whole fetch.
            if page_numbers:
                oldest = min(page_numbers)
                if oldest <= min(remaining, default=oldest):
                    break
            page += 1
    finally:
        if own_session:
            await session.close()
    return found

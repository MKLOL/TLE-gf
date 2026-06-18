"""Daily Akari public difficulty API client tests."""

import asyncio

from tle.util.akari_difficulty import fetch_akari_difficulties


class _Response:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self):
        return self.payload

    async def text(self):
        return ''


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        if url.endswith('/dailypuzzle'):
            return _Response({'dailyNumber': 529, 'difficulty': 4})
        page = int(params['page'])
        if page == 1:
            return _Response({
                'entries': [
                    {'dailyNumber': 528, 'difficulty': 3},
                    {'dailyNumber': 527, 'difficulty': 2},
                ],
                'areMore': True,
            })
        return _Response({
            'entries': [{'dailyNumber': 526, 'difficulty': 5}],
            'areMore': False,
        })


def test_fetches_current_and_paginates_archive():
    session = _Session()
    found = asyncio.run(fetch_akari_difficulties(
        {526, 527, 529}, session=session))
    assert found == {529: 4, 527: 2, 526: 5}
    assert [url.rsplit('/', 1)[-1] for url, _params in session.calls] == [
        'dailypuzzle', 'archivelist', 'archivelist']


class _MalformedSession:
    """An archive page with a null / non-numeric ``dailyNumber`` entry."""

    def get(self, url, params=None):
        if url.endswith('/dailypuzzle'):
            return _Response({'dailyNumber': 0, 'difficulty': 0})
        page = int(params['page'])
        if page == 1:
            return _Response({
                'entries': [
                    {'dailyNumber': 600, 'difficulty': 3},
                    {'dailyNumber': None, 'difficulty': 4},
                    {'dailyNumber': 'oops', 'difficulty': 2},
                ],
                'areMore': True,
            })
        return _Response({
            'entries': [{'dailyNumber': 598, 'difficulty': 5}],
            'areMore': False,
        })


def test_malformed_archive_entry_does_not_abort_fetch():
    # A bad entry must be skipped, not crash the whole paginated fetch, so the
    # good puzzles on the same and later pages are still returned.
    found = asyncio.run(fetch_akari_difficulties(
        {600, 598}, session=_MalformedSession()))
    assert found == {600: 3, 598: 5}

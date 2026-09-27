"""Actual MV3 review handoff, native window reuse and passive-source regression.

Only a disposable extension profile, synthetic LinkedIn DOM and loopback API
are used. This suite never confirms an import or posts a personal result.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading
import time

from playwright.sync_api import expect, sync_playwright

from tests.games_extension_browser import EXTENSION, TOKEN
from tests.games_linkedin_tabbed_browser import FIXTURE, URL


def preview(number):
    return {
        'preview_id': f'synthetic-{number}', 'expires_at': time.time() + 600,
        'game': 'queens', 'game_name': 'LinkedIn Queens', 'puzzle_number': 879,
        'puzzle_date': '2026-09-26', 'registered': 1, 'unresolved': 3, 'skipped': 0,
        'rows': [{
            'name': f'Example LinkedIn Player {number}-{i} ' + (
                'Long synthetic name ' * 3 if i == 3 else ''),
            'discord_name': 'Example Discord account' if i == 0 else None,
            'registered': i == 0, 'is_own': i == 1, 'rated': i != 2,
            'time_seconds': 10 + i, 'no_hints': True, 'no_mistakes': i % 2 == 0,
        } for i in range(4)],
    }


def ready(page, number):
    page.wait_for_url(f'**/review.html#synthetic-{number}')
    expect(page.locator('#review-content')).to_be_visible()
    expect(page.locator('#confirm')).to_be_enabled()
    expect(page.locator('#players')).to_contain_text(f'Example LinkedIn Player {number}-0')


def run():
    calls, previews = [], []
    catalog = {
        'guild_id': '1', 'guild_name': 'Example server',
        'user_id': '10', 'user_name': 'Example moderator',
        'games': [{'id': 'queens', 'path': 'queens', 'name': 'LinkedIn Queens',
            'enabled': True, 'can_import': True, 'today': '2026-09-26',
            'anchor_date': '2026-06-08', 'anchor_number': 769}],
    }

    class API(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == '/v1/games'
            self.respond(catalog)

        def do_POST(self):
            calls.append(self.path)
            assert self.path == '/v1/games/imports/preview', 'Unexpected result submission'
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            assert body['game'] == 'queens' and body['puzzle_number'] == 879
            value = preview(len(previews) + 1)
            previews.append(value)
            self.respond(value)

        def respond(self, body):
            assert self.headers['Authorization'] == 'Bearer ' + TOKEN
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    api = ThreadingHTTPServer(('127.0.0.1', 0), API)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix='tle-review-runtime-') as temp, sync_playwright() as p:
            extension = Path(temp) / 'extension'
            shutil.copytree(EXTENSION, extension)
            manifest = json.loads((extension / 'manifest.json').read_text())
            assert 'tabs' not in manifest['permissions']
            # Grant fixture/loopback access only in this disposable copy.
            manifest['host_permissions'] = ['https://www.linkedin.com/*', 'http://127.0.0.1/*']
            (extension / 'manifest.json').write_text(json.dumps(manifest))
            context = p.chromium.launch_persistent_context(str(Path(temp) / 'profile'),
                headless=True, channel='chromium', no_viewport=True,
                args=[f'--disable-extensions-except={extension}', f'--load-extension={extension}'])
            try:
                check(context, api.server_port, calls)
            finally:
                context.close()
    finally:
        api.shutdown()
        api.server_close()


def check(context, port, calls):
    worker = context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker')
    root = 'chrome-extension://' + worker.url.split('/')[2]
    options = context.new_page()
    options.goto(root + '/options.html')
    options.evaluate('(config) => chrome.storage.local.set(config)', {
        'server': f'http://127.0.0.1:{port}', 'token': TOKEN,
        'autoLinkedIn': False, 'autoAkari': False,
    })
    linkedin_requests = []
    context.on('request', lambda request: linkedin_requests.append(request.url)
        if request.url.startswith('https://www.linkedin.com/') else None)
    context.route('https://www.linkedin.com/**',
        lambda route: route.fulfill(body=FIXTURE, content_type='text/html'))
    source = context.new_page()
    source.goto(URL)
    original = source.content()
    popup = context.new_page()
    popup.goto(root + '/popup.html')
    expect(popup.locator('#status')).to_contain_text('Ready.')
    source.bring_to_front()
    with context.expect_event('page') as opened:
        popup.evaluate("document.querySelector('#read').click()")
    review = opened.value
    ready(review, 1)
    expect(popup.locator('#read')).to_be_enabled()
    assert review.locator('#players tr').count() == 4
    assert calls == ['/v1/games/imports/preview']

    # Real tabs.get hides URLs without tabs permission, even for our own page.
    tab = options.evaluate('''async () => {
        const {reviewWindow} = await chrome.storage.session.get('reviewWindow');
        return chrome.tabs.get(reviewWindow.tabId);
    }''')
    assert 'url' not in tab
    page_count = len(context.pages)
    source.bring_to_front()
    popup.evaluate("document.querySelector('#read').click()")
    ready(review, 2)
    expect(popup.locator('#read')).to_be_enabled()
    assert len(context.pages) == page_count

    # Popup destruction may lose window metadata; trusted contexts still find it.
    options.evaluate("chrome.storage.session.remove('reviewWindow')")
    popup.locator('#open-review').click()
    expect(popup.locator('#read')).to_be_enabled()
    assert len(context.pages) == page_count
    assert options.evaluate("chrome.storage.session.get('reviewWindow').then(data => data.reviewWindow.previewId)") == 'synthetic-2'

    # Wide review dedicates useful space to rows; smaller windows keep actions reachable.
    review.set_viewport_size({'width': 1080, 'height': 800})
    assert review.locator('.table-scroll').bounding_box()['height'] >= 300
    review.set_viewport_size({'width': 640, 'height': 668})
    size = review.evaluate('[innerWidth, innerHeight]')
    button = review.locator('#confirm').bounding_box()
    assert button['x'] >= 0 and button['y'] >= 0
    assert button['x'] + button['width'] <= size[0] + 1
    assert button['y'] + button['height'] <= size[1] + 1
    assert review.evaluate('document.documentElement.scrollWidth <= innerWidth')
    review.locator('[data-filter="matched"]').click()
    assert review.locator('#players tr').count() == 1
    assert 'all 4' in review.locator('#confirm').inner_text()

    # A review tab navigated elsewhere must never be overwritten by saved metadata.
    review.goto('about:blank')
    with context.expect_event('page') as recreated:
        popup.locator('#open-review').click()
    replacement = recreated.value
    ready(replacement, 2)
    assert review.url == 'about:blank'
    replacement.locator('#cancel').click()
    expect(replacement.locator('#empty-state')).to_be_visible()
    assert not options.evaluate("chrome.storage.session.get('pendingImport').then(data => data.pendingImport)")
    assert calls == ['/v1/games/imports/preview'] * 2
    assert source.content() == original
    assert linkedin_requests == [URL]


if __name__ == '__main__':
    run()
    print('Native MV3 review handoff, window reuse, metadata recovery, resizing and cancellation passed; no result submissions or LinkedIn changes.')

"""Real MV3 automatic self posting; synthetic game pages and loopback API only."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION
from tests.games_linkedin_auto_browser import FIXTURE


def run():
    posts, gets = [], []
    catalog = {'guild_name': 'Test', 'user_name': 'Member', 'games': [
        {'id': 'tango', 'path': 'tango', 'name': 'LinkedIn Tango', 'today': '2026-09-26',
         'anchor_date': '2026-09-04', 'anchor_number': 697, 'enabled': True, 'can_import': False},
        {'id': 'queens', 'path': 'queens', 'name': 'LinkedIn Queens', 'today': '2026-09-26',
         'anchor_date': '2026-06-08', 'anchor_number': 769, 'enabled': True, 'can_import': False},
    ]}

    class API(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == '/v1/games'
            gets.append(self.path)
            self.respond(catalog)

        def do_POST(self):
            assert self.path == '/v1/games/results'  # Never a bulk import endpoint.
            assert self.headers['Authorization'] == 'Bearer tlegames_' + 'a' * 43
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            posts.append(body)
            self.respond({'puzzle_number': body['puzzle_number'], 'duplicate': False,
                          'message_url': 'https://discord.test/message'})

        def respond(self, body):
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    api = ThreadingHTTPServer(('127.0.0.1', 0), API)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix='tle-linkedin-auto-') as temp, sync_playwright() as p:
            extension = Path(temp) / 'extension'
            shutil.copytree(EXTENSION, extension)
            manifest = json.loads((extension / 'manifest.json').read_text())
            manifest['host_permissions'] = ['https://www.linkedin.com/*', 'https://dailyakari.com/*', 'http://127.0.0.1/*']
            (extension / 'manifest.json').write_text(json.dumps(manifest))
            context = p.chromium.launch_persistent_context(str(Path(temp) / 'profile'), headless=True,
                channel='chromium', args=[f'--disable-extensions-except={extension}', f'--load-extension={extension}'])
            try:
                worker = context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker')
                options = context.new_page()
                options.goto('chrome-extension://' + worker.url.split('/')[2] + '/options.html')
                assert options.locator('#auto-akari').is_checked()
                assert options.locator('#auto-linkedin').is_checked()
                # Neither switch is stored. Automatic behavior must really default on.
                options.evaluate('(config) => chrome.storage.local.set(config)', {
                    'server': f'http://127.0.0.1:{api.server_port}', 'token': 'tlegames_' + 'a' * 43})
                assert options.evaluate('chrome.runtime.sendMessage({type:"configure-auto"})')['ok']
                assert set(options.evaluate('chrome.scripting.getRegisteredContentScripts().then(rows=>rows.map(row=>row.id))')) == {'tle-akari', 'tle-linkedin'}
                context.route('https://www.linkedin.com/**', lambda route: route.fulfill(
                    body=FIXTURE if '/preload/' in route.request.url else '<p>Feed or playing screen</p>', content_type='text/html'))
                page = context.new_page()
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto('https://www.linkedin.com/feed/')
                page.set_content('<iframe src="/preload/"></iframe>')
                page.frame_locator('iframe').locator('.pr-top__header').wait_for()
                page.wait_for_timeout(2200)
                assert not posts and not gets  # No feed scraping or server requests.
                # LinkedIn enters games without reloading the outer feed document.
                page.evaluate("history.pushState({},'', '/games/tango/results/')")
                wait_status(options, 719)
                assert posts == [{'game': 'tango', 'puzzle_date': '2026-09-26', 'puzzle_number': 719,
                                  'time_seconds': 39, 'accuracy': 100, 'is_perfect': True}]
                assert page.frames[1].evaluate('typeof chrome?.storage') == 'undefined'
                # Arbitrary rendering mutations must not repost a completion.
                page.frames[1].evaluate("document.querySelector('.pr-golden-chiclet__text').textContent='0:39'")
                page.wait_for_timeout(2300)
                assert len(posts) == 1
                options.evaluate("chrome.storage.local.set({autoLinkedIn:false,autoAkari:false})")
                assert options.evaluate('chrome.runtime.sendMessage({type:"configure-auto"})')['ok']
                assert options.evaluate('chrome.scripting.getRegisteredContentScripts()') == []
                # Already-injected readers stay alive; the worker must enforce the opt-out.
                page.evaluate("history.pushState({},'', '/games/queens/results/')")
                page.frames[1].evaluate('''() => {
                  document.querySelector('.pr-game-results__toolbar-title').textContent='Queens';
                  document.querySelector('.pr-top__subtext').textContent='Queens #879';
                }''')
                page.wait_for_timeout(2300)
                assert len(posts) == 1
                options.reload()
                assert not options.locator('#auto-linkedin').is_checked()
                assert not options.locator('#auto-akari').is_checked()
                # Re-enable then load a fresh game document to verify Queens too.
                options.evaluate("chrome.storage.local.set({autoLinkedIn:true})")
                assert options.evaluate('chrome.runtime.sendMessage({type:"configure-auto"})')['ok']
                page.goto('https://www.linkedin.com/games/queens/results/')
                page.set_content(FIXTURE.replace('Tango', 'Queens').replace('#719', '#879'))
                wait_status(options, 879)
                assert posts[-1]['game'] == 'queens' and len(posts) == 2
                # An older displayed result is not automatically backfilled.
                page.goto('https://www.linkedin.com/games/tango/results/')
                page.set_content(FIXTURE.replace('#719', '#718'))
                page.wait_for_timeout(2300)
                assert len(posts) == 2
                assert not errors, errors
                print('Real default-on LinkedIn automatic posting, SPA/preload, own-only payload, deduplication and opt-out passed.')
            finally:
                context.close()
    finally:
        api.shutdown()
        api.server_close()


def wait_status(options, number):
    options.evaluate('''async n => {
        const deadline=Date.now()+8000;
        while(Date.now()<deadline) {
            const {linkedinStatus}=await chrome.storage.local.get('linkedinStatus');
            if(linkedinStatus?.ok && linkedinStatus.text.includes('#'+n)) return;
            await new Promise(resolve=>setTimeout(resolve,25));
        }
        throw new Error('Automatic posting did not finish');
    }''', number)


if __name__ == '__main__':
    run()

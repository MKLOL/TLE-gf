"""Actual MV3 readers remain passive on synthetic/anonymized LinkedIn pages."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION, TOKEN
from tests.games_linkedin_auto_browser import FIXTURE
from tests.games_linkedin_runtime_browser import wait_status
from tests.games_linkedin_tabbed_browser import FIXTURE as BOARD, URL as BOARD_URL


def snapshot(page):
    return page.evaluate('''() => ({
      url: location.href,
      html: document.documentElement.outerHTML,
      frame: document.querySelector('iframe').contentDocument.documentElement.outerHTML,
      local: JSON.stringify(Object.entries(localStorage).sort()),
      session: JSON.stringify(Object.entries(sessionStorage).sort()),
      cookie: document.cookie,
    })''')


def check_main_world(page):
    for frame in page.frames:
        assert frame.evaluate('''() => [typeof TleLeaderboard,
          typeof TleOwnLinkedInResult, typeof TleReadLeaderboard,
          typeof tleLinkedInAutomatic].every(value => value === 'undefined')''')
        assert frame.evaluate('passiveEvents') == []


def run():
    posts, requests = [], []
    catalog = {'games': [{'id': 'tango', 'path': 'tango', 'name': 'Tango',
        'today': '2026-09-26', 'anchor_date': '2026-09-04', 'anchor_number': 697,
        'enabled': True}]}

    class API(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == '/v1/games'
            self.respond(catalog)

        def do_POST(self):
            assert self.path == '/v1/games/results'
            posts.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            self.respond({'puzzle_number': 719, 'duplicate': False, 'message_url': 'https://discord.test/result'})

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
        with tempfile.TemporaryDirectory(prefix='tle-readonly-') as temp, sync_playwright() as p:
            extension = Path(temp) / 'extension'
            shutil.copytree(EXTENSION, extension)
            manifest = json.loads((extension / 'manifest.json').read_text())
            manifest['host_permissions'] = ['https://www.linkedin.com/*', 'http://127.0.0.1/*']
            (extension / 'manifest.json').write_text(json.dumps(manifest))
            context = p.chromium.launch_persistent_context(str(Path(temp) / 'profile'), headless=True,
                channel='chromium', args=[f'--disable-extensions-except={extension}', f'--load-extension={extension}'])
            try:
                context.on('request', lambda request: requests.append(request.url))
                context.add_init_script('''window.passiveEvents = [];
                  for (const event of ['click', 'dblclick', 'pointerdown', 'pointerup',
                    'mousedown', 'mouseup', 'keydown', 'keyup', 'input', 'change', 'submit']) {
                    document.addEventListener(event, value => passiveEvents.push(value.type), true);
                  }''')
                worker = context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker')
                options = context.new_page()
                options.goto('chrome-extension://' + worker.url.split('/')[2] + '/options.html')

                def route_page(route):
                    url = route.request.url
                    body = (BOARD if 'board' in url else FIXTURE) if '/preload/' in url else (
                        '<iframe src="/preload/board"></iframe>' if '/leaderboard/' in url
                        else '<iframe src="/preload/result"></iframe>')
                    route.fulfill(body=body, content_type='text/html')

                context.route('https://www.linkedin.com/**', route_page)
                page = context.new_page()
                page.goto('https://www.linkedin.com/games/tango/results/')
                page.frame_locator('iframe').locator('.pr-top__header').wait_for()
                page.evaluate('''() => {
                  localStorage.setItem('passive-seed', 'unchanged');
                  sessionStorage.setItem('passive-seed', 'unchanged');
                }''')
                before = snapshot(page)
                options.evaluate('(config) => chrome.storage.local.set(config)', {
                    'server': f'http://127.0.0.1:{api.server_port}', 'token': TOKEN, 'autoAkari': False,
                    'autoLinkedIn': True, 'autoLinkedInConsentVersion': 1})
                assert options.evaluate('chrome.runtime.sendMessage({type:"configure-auto"})')['ok']
                scripts = options.evaluate('chrome.scripting.getRegisteredContentScripts()')
                assert len(scripts) == 1 and scripts[0]['world'] == 'ISOLATED'
                assert not scripts[0]['allFrames']
                page.reload()
                page.frame_locator('iframe').locator('.pr-top__header').wait_for()
                wait_status(options, 719)
                page.wait_for_timeout(2300)  # Includes a second automatic scan.
                assert snapshot(page) == before
                check_main_world(page)
                assert len(posts) == 1 and posts[0]['time_seconds'] == 39
                linkedin = [url for url in requests if url.startswith('https://www.linkedin.com/')]
                assert linkedin == [before['url'], 'https://www.linkedin.com/preload/result'] * 2, linkedin

                # Manual reads also execute in the isolated world and change no
                # LinkedIn DOM, storage, URL, or interaction events.
                page.goto(BOARD_URL)
                page.frame_locator('iframe').locator('.pr-connections-leaderboard__section').wait_for()
                before = snapshot(page)
                count = len(requests)
                result = options.evaluate('''async () => {
                  const [tab] = await chrome.tabs.query({url:'https://www.linkedin.com/*'});
                  await chrome.scripting.executeScript({target:{tabId:tab.id},
                    files:['leaderboard.js','linkedin-result.js','extract.js']});
                  return (await chrome.scripting.executeScript({target:{tabId:tab.id},
                    func:()=>globalThis.TleReadLeaderboard(true)}))[0].result;
                }''')
                assert result['count'] == 1 and result['puzzleNumber'] == 879
                page.wait_for_timeout(2300)
                assert snapshot(page) == before
                check_main_world(page)
                assert len(requests) == count and len(posts) == 1
                print('MV3 LinkedIn readers: isolated world, unchanged DOM/storage, no interactions or extra LinkedIn requests; own result only to loopback API.')
            finally:
                context.close()
    finally:
        api.shutdown()
        api.server_close()


if __name__ == '__main__':
    run()

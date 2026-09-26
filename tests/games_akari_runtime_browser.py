"""Actual isolated-world Akari -> extension worker -> loopback HTTP check.

Run from the repository root with Playwright + Chromium installed.
Only a temporary extension copy gains required host permissions; no permission
prompt is exercised. All game requests are intercepted; no personal profile or
real API/token is used.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import tempfile
import threading

from playwright.sync_api import sync_playwright


def run():
    seen = []

    class API(BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.path == '/v1/games/results'
            assert self.headers['Authorization'] == 'Bearer tlegames_' + 'a' * 43
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            seen.append(body)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'puzzle_number': body['puzzle_number'], 'duplicate': False,
                'message_url': 'https://discord.test/message',
            }).encode())

        def log_message(self, *args):
            pass

    api = ThreadingHTTPServer(('127.0.0.1', 0), API)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix='tle-review-akari-') as temp, sync_playwright() as p:
            extension = Path(temp) / 'extension'
            shutil.copytree(Path(__file__).resolve().parents[1] / 'extensions/linkedin-games', extension)
            manifest = json.loads((extension / 'manifest.json').read_text())
            manifest['host_permissions'] = ['https://dailyakari.com/*', 'http://127.0.0.1/*']
            (extension / 'manifest.json').write_text(json.dumps(manifest))
            context = p.chromium.launch_persistent_context(
                str(Path(temp) / 'profile'), headless=True, channel='chromium', args=[
                    f'--disable-extensions-except={extension}', f'--load-extension={extension}'])
            try:
                worker = context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker')
                options = context.new_page()
                options.goto('chrome-extension://' + worker.url.split('/')[2] + '/options.html')
                options.evaluate('(cfg) => chrome.storage.local.set(cfg)', {
                    'server': f'http://127.0.0.1:{api.server_port}',
                    'token': 'tlegames_' + 'a' * 43, 'autoAkari': True,
                })
                assert options.evaluate('chrome.runtime.sendMessage({type:"configure-akari"})')['ok']
                registered = options.evaluate('chrome.scripting.getRegisteredContentScripts()')
                assert registered[0]['world'] == 'ISOLATED'
                assert registered[0]['runAt'] == 'document_start'
                context.route('https://dailyakari.com/**', lambda route: route.fulfill(
                    content_type='text/html', body='<iframe src="/akari"></iframe>'
                    if route.request.url.endswith('.com/') else '<p>Game frame</p>'))
                page = context.new_page()
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto('https://dailyakari.com/')
                for index in range(3):
                    if index:
                        page.frames[1].goto('https://dailyakari.com/akari')
                    frame = page.frames[1]
                    frame.evaluate("localStorage.setItem('settings',JSON.stringify({showAccuracy:true}))")
                    page.evaluate('''i => document.querySelector('iframe').contentWindow.postMessage({
                        puzzlink:'https://puzz.link/p?akari/example', isArchive:false, puzzleId:'1786',
                        dailyDateKey:'2026-09-'+(26-i), noRot:false}, location.origin)''', index)
                    page.wait_for_timeout(50)
                    frame.evaluate('''i => parent.postMessage({animTimeBtn:true,
                        timeResult:'0:'+(39+i),accuracy:i===0?0.996:1},location.origin)''', index)
                    options.evaluate('''async n => {
                        const deadline = Date.now() + 5000;
                        while (Date.now() < deadline) {
                            const {akariStatus} = await chrome.storage.local.get('akariStatus');
                            if (akariStatus?.ok && akariStatus.text.includes('#'+n)) return;
                            await new Promise(resolve => setTimeout(resolve, 25));
                        }
                        throw new Error('Timed out waiting for the background post');
                    }''', 629-index)
                assert [(value['puzzle_number'], value['time_seconds']) for value in seen] == [
                    (629, 39), (628, 40), (627, 41)]
                assert seen[0]['accuracy'] == 100 and seen[0]['is_perfect'] is False
                assert all(value['is_perfect'] is True for value in seen[1:])
                assert not errors, errors
                print('Real MV3 isolated content capture, worker/API posting, accuracy, and frame reloads passed.')
            finally:
                context.close()
    finally:
        api.shutdown()
        api.server_close()


if __name__ == '__main__':
    run()

"""Shared synthetic extension/storage/API harness for review-window checks."""
import copy
import json
from pathlib import Path
import time
from urllib.parse import urlparse


EXTENSION = Path(__file__).resolve().parents[1] / 'extensions/linkedin-games'
TOKEN = 'tlegames_' + 'a' * 43


class ReviewBrowser:
    def __init__(self, browser, *, pending=True, window_mode='success', extracted=None):
        self.context = browser.new_context(viewport={'width': 1080, 'height': 800})
        self.context.set_default_timeout(5000)
        self.errors, self.calls = [], []
        self.delays, self.waiting, self.failures = set(), {}, {}
        self.bad_json = set()
        self.catalog = {
            'guild_id': '1', 'guild_name': 'Example server',
            'user_id': '10', 'user_name': 'Example moderator',
            'games': [{'id': 'queens', 'path': 'queens', 'name': 'LinkedIn Queens',
                       'enabled': True, 'can_import': True, 'today': '2026-09-26',
                       'anchor_date': '2026-06-08', 'anchor_number': 769}],
        }
        self.preview = {
            'preview_id': 'synthetic-preview', 'expires_at': time.time() + 600,
            'game': 'queens', 'game_name': 'LinkedIn Queens', 'puzzle_number': 879,
            'puzzle_date': '2026-09-26', 'registered': 2, 'unresolved': 18,
            'skipped': 1, 'unplayed': 3,
            'rows': [
                self.row('<img src=x onerror="alert(1)">', 'Example Discord Member', 10),
                self.row('Anonymous', 'Anonymous Discord Member', 11),
                self.row('You', None, 12, own=True),
                self.row('Unassigned Player', None, 13, rated=False),
            ] + [self.row(f'Example Player {index} ' + 'Long synthetic name ' * 4, None, 20 + index)
                 for index in range(16)],
        }
        self.extracted = extracted or {
            'gamePath': 'queens', 'puzzleNumber': 879, 'unplayed': 3,
            'leaderboard': 'Private synthetic source name\n0:11\nYou\n0:12',
        }
        if self.extracted['gamePath'] == 'tango':
            self.catalog['games'][0].update(id='tango', path='tango', name='LinkedIn Tango',
                                            anchor_date='2026-09-04', anchor_number=697)
            self.preview.update(game='tango', game_name='LinkedIn Tango', puzzle_number=719)
        initial = {
            'local': {'server': 'http://api.test', 'token': TOKEN,
                      'autoLinkedIn': False, 'autoAkari': False},
            'session': {'pendingImport': self.snapshot()} if pending else {},
        }
        self.context.route('**/*', self.route)
        self.context.add_init_script(self.chrome_mock(initial, window_mode))

    @staticmethod
    def row(name, discord, seconds, *, own=False, rated=True):
        return {'name': name, 'discord_name': discord, 'registered': discord is not None,
                'is_own': own, 'rated': rated, 'time_seconds': seconds,
                'no_hints': True, 'no_mistakes': True}

    def snapshot(self, preview_id=None):
        data = copy.deepcopy(self.preview)
        if preview_id:
            data['preview_id'] = preview_id
        return {'server': 'http://api.test', 'guild': '1', 'user': '10',
                'guildName': 'Example server', 'userName': 'Example moderator', 'data': data}

    def route(self, route):
        parsed = urlparse(route.request.url)
        if parsed.hostname in ('api.test', 'other-api.test'):
            kind = 'catalog' if parsed.path == '/v1/games' else (
                'preview' if parsed.path.endswith('/preview') else 'confirm')
            assert parsed.path == '/v1/games' or parsed.path.startswith('/v1/games/imports/')
            self.calls.append({'kind': kind, 'url': route.request.url,
                               'method': route.request.method,
                               'body': route.request.post_data_json if route.request.method == 'POST' else None})
            if kind in self.delays:
                self.waiting.setdefault(kind, []).append(route)
            else:
                self.reply(route, kind)
            return
        assert parsed.hostname == 'extension.test', f'Unexpected external request: {parsed.hostname}'
        filename = parsed.path.rsplit('/', 1)[-1]
        source = EXTENSION / filename
        if not source.is_file():
            route.fulfill(status=404, body='Missing synthetic route')
            return
        mime = ('text/javascript' if filename.endswith('.js') else
                'text/css' if filename.endswith('.css') else 'text/html')
        route.fulfill(body=source.read_text(), content_type=mime)

    def reply(self, route, kind):
        if kind in self.bad_json:
            route.fulfill(status=502, body='<html>Temporary proxy error</html>', content_type='text/html')
            return
        status = self.failures.get(kind)
        data = ({'error': 'Review unavailable. Read again.'} if status else
                self.catalog if kind == 'catalog' else self.preview if kind == 'preview' else
                {'registered': 2, 'unresolved': 18, 'unchanged': 0})
        route.fulfill(status=status or 200, json=data)

    def release(self, kind):
        self.reply(self.waiting[kind].pop(0), kind)

    def wait_request(self, page, kind):
        for _ in range(100):
            if self.waiting.get(kind):
                return
            page.wait_for_timeout(20)
        raise AssertionError(f'No delayed {kind} request arrived')

    def page(self, path='review.html#synthetic-preview'):
        page = self.context.new_page()
        page.on('pageerror', lambda error: self.errors.append(str(error)))
        page.goto('http://extension.test/' + path, wait_until='domcontentloaded')
        return page

    def confirms(self):
        return [call for call in self.calls if call['kind'] == 'confirm']

    def close(self):
        assert not self.errors, self.errors
        self.context.close()

    def chrome_mock(self, initial, window_mode):
        return r'''(() => {
          const initial = INITIAL, listeners = [];
          for (const key of ['local', 'session']) {
            if (!localStorage.getItem(key)) localStorage.setItem(key, JSON.stringify(initial[key]));
          }
          const diff = (old, current) => Object.fromEntries([...new Set([
            ...Object.keys(old), ...Object.keys(current)])].filter(key =>
              JSON.stringify(old[key]) !== JSON.stringify(current[key])).map(key =>
                [key, {oldValue: old[key], newValue: current[key]}]));
          addEventListener('storage', event => {
            if (['local', 'session'].includes(event.key)) {
              const changes = diff(JSON.parse(event.oldValue || '{}'), JSON.parse(event.newValue || '{}'));
              listeners.forEach(fn => fn(changes, event.key));
            }
          });
          const area = name => {
            const get = () => JSON.parse(localStorage.getItem(name));
            const write = obj => {
              const changes = diff(get(), obj);
              localStorage.setItem(name, JSON.stringify(obj));
              listeners.forEach(fn => fn(changes, name));
            };
            return {get: async () => structuredClone(get()),
              set: async data => write({...get(), ...data}),
              remove: async keys => {
                const data = get();
                for (const key of Array.isArray(keys) ? keys : [keys]) delete data[key];
                write(data);
              }, setAccessLevel: async () => {}};
          };
          const log = (kind, data) => {
            if (data.url) localStorage.setItem('reviewUrl', data.url);
            const events = JSON.parse(localStorage.getItem('uiCalls') || '[]');
            events.push({kind, data, pending: JSON.parse(localStorage.getItem('session')).pendingImport});
            localStorage.setItem('uiCalls', JSON.stringify(events));
          };
          const mode = WINDOW_MODE;
          window.chrome = {
            storage: {local: area('local'), session: area('session'), onChanged: {
              addListener: fn => listeners.push(fn),
              removeListener: fn => { const i = listeners.indexOf(fn); if (i >= 0) listeners.splice(i, 1); }}},
            permissions: {contains: async () => true},
            runtime: {getURL: path => 'http://extension.test/' + path,
              getContexts: async () => {
                const documentUrl = localStorage.getItem('reviewUrl');
                return documentUrl && !localStorage.getItem('windowClosed') ?
                  [{contextType: 'TAB', tabId: 92, windowId: 91, documentUrl}] : [];
              },
              openOptionsPage: () => log('settings', {})},
            tabs: {query: async () => [{id: 1}],
              getCurrent: async () => {
                localStorage.setItem('reviewUrl', location.href);
                return {id: 92, windowId: 91};
              },
              create: async data => { log('tab.create', data);
                if (mode === 'both-failure') throw Error('Tabs API unavailable');
                return {id: 92, windowId: 91, url: data.url}; },
              get: async id => { if (localStorage.getItem('windowClosed')) throw Error('closed');
                return {id, windowId: 91}; },
              update: async (id, data) => { log('tab.update', {id, ...data}); return {id, windowId: 91}; }},
            scripting: {executeScript: async () => [{result: EXTRACTED}]},
          };
          if (mode !== 'unavailable') chrome.windows = {
            create: async data => { log('window.create', data);
              if (['failure', 'both-failure'].includes(mode)) throw Error('Window API unavailable');
              return {id: 91, tabs: [{id: 92}]}; },
            get: async id => { if (localStorage.getItem('windowClosed')) throw Error('closed');
              return {id, tabs: [{id: 92}]}; },
            update: async (id, data) => { log('window.update', {id, ...data}); return {id}; },
          };
        })();'''.replace('INITIAL', json.dumps(initial)).replace('WINDOW_MODE', json.dumps(window_mode)).replace(
            'EXTRACTED', json.dumps(self.extracted))

"""Synthetic import review UX, confirmation, and connection-lifecycle checks."""
import json
import time

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION, TOKEN


def check(browser):
    page = browser.new_page(viewport={'width': 480, 'height': 600})
    errors, calls = [], []
    failure_status = None
    page.on('pageerror', lambda error: errors.append(str(error)))
    catalog = {
        'guild_id': '1', 'guild_name': 'Example server',
        'user_name': 'Example moderator', 'user_id': '10',
        'games': [{'id': 'queens', 'path': 'queens', 'name': 'LinkedIn Queens',
                   'enabled': True, 'can_import': True, 'today': '2026-09-26',
                   'anchor_date': '2026-06-08', 'anchor_number': 769}],
    }
    preview = {
        'preview_id': 'synthetic-preview', 'expires_at': time.time() + 600,
        'game_name': 'LinkedIn Queens', 'game': 'queens', 'puzzle_number': 879,
        'puzzle_date': '2026-09-26', 'registered': 2, 'unresolved': 14, 'skipped': 1,
        'rows': [
            {'name': '<img src=x onerror="alert(1)">', 'discord_name': 'Example Discord Member',
             'registered': True, 'is_own': False, 'rated': True, 'time_seconds': 10,
             'no_hints': True, 'no_mistakes': True},
            {'name': 'Anonymous #7', 'discord_name': 'Anonymous Discord Member',
             'registered': True, 'is_own': False, 'rated': True, 'time_seconds': 11,
             'no_hints': True, 'no_mistakes': False},
            {'name': 'You', 'discord_name': None,
             'registered': False, 'is_own': True, 'rated': True, 'time_seconds': 12,
             'no_hints': True, 'no_mistakes': True},
            {'name': 'Unassigned Player', 'discord_name': None,
             'registered': False, 'is_own': False, 'rated': False, 'time_seconds': 13,
             'no_hints': False, 'no_mistakes': True},
        ] + [
            {'name': f'Example Player {index}', 'discord_name': None,
             'registered': False, 'is_own': False, 'rated': True, 'time_seconds': 20 + index,
             'no_hints': False, 'no_mistakes': False}
            for index in range(12)
        ],
    }
    extracted = {'gamePath': 'queens', 'puzzleNumber': 879,
                 'leaderboard': 'Private Example LinkedIn Name\n0:11\nYou\n0:12', 'unplayed': 3}

    def route_request(route):
        url = route.request.url
        if url.startswith(('http://api.test/', 'http://other-api.test/')):
            calls.append(url)
            if failure_status and url.endswith('/confirm'):
                route.fulfill(status=failure_status, json={'error': 'Review unavailable. Read again.'})
                return
            response = catalog if url.endswith('/v1/games') else (
                preview if url.endswith('/preview') else
                {'registered': 2, 'unresolved': 14, 'unchanged': 0})
            route.fulfill(json=response)
        else:
            filename = url.split('/')[-1] or 'popup.html'
            mime = ('text/javascript' if filename.endswith('.js') else
                    'text/css' if filename.endswith('.css') else 'text/html')
            route.fulfill(body=(EXTENSION / filename).read_text(), content_type=mime)

    page.route('**/*', route_request)
    page.add_init_script('''(() => {
      const defaults = {local: CONFIG, session: {}};
      const state = window.name ? JSON.parse(window.name) : defaults;
      const changed = [];
      const persist = () => { window.name = JSON.stringify(state); };
      const area = (obj, name) => ({
        get: async () => structuredClone(obj),
        set: async data => {
          const changes = Object.fromEntries(Object.entries(data).map(([key, value]) =>
            [key, {oldValue: obj[key], newValue: value}]));
          Object.assign(obj, data); persist();
          changed.forEach(listener => listener(changes, name));
        },
        remove: async keys => {
          for (const key of Array.isArray(keys) ? keys : [keys]) delete obj[key];
          persist();
        },
        setAccessLevel: async () => {},
      });
      window.chrome = {
        storage: {local: area(state.local, 'local'), session: area(state.session, 'session'),
          onChanged: {addListener: listener => changed.push(listener)}},
        permissions: {contains: async () => true},
        runtime: {openOptionsPage: () => {}},
        tabs: {query: async () => [{id: 1}]},
        scripting: {executeScript: async () => [{result: EXTRACTED}]},
      };
    })();'''.replace('CONFIG', json.dumps({'server': 'http://api.test', 'token': TOKEN,
                                        'autoLinkedIn': False, 'autoAkari': False}))
       .replace('EXTRACTED', json.dumps(extracted)))

    def load():
        page.goto('http://extension.test/popup.html')
        page.wait_for_function("document.querySelector('#connection').textContent.includes('Example server')")

    def read():
        page.locator('#read').click()
        page.locator('#preview').wait_for(state='visible')

    def confirms():
        return [url for url in calls if url.endswith('/confirm')]

    load()
    read()
    assert not confirms()
    assert page.locator('#read-tools').is_hidden()
    assert page.locator('#preview-question').inner_text() == 'Would you like to import these results?'
    assert page.evaluate('document.activeElement.id') == 'preview-question'
    assert 'LinkedIn' in page.locator('thead').inner_text()
    assert 'Discord' in page.locator('thead').inner_text()
    rows = page.locator('#players tr')
    assert rows.count() == 16
    assert 'Matches Discord: Example Discord Member' in rows.nth(0).inner_text()
    assert 'Anonymous #7' in rows.nth(1).inner_text()
    assert 'Private Example LinkedIn Name' not in page.locator('body').inner_text()
    assert 'Your result · will be saved until you register' in rows.nth(2).inner_text()
    assert 'Unassigned · will be saved for later' in rows.nth(3).inner_text()
    assert 'Unrated' in rows.nth(3).inner_text()
    assert page.locator('#players img, #players script').count() == 0
    assert '2026-09-26' in page.locator('#summary').inner_text()
    assert 'skipped' in page.locator('#summary').inner_text()
    assert '3 without a completed score' in page.locator('#summary').inner_text()
    assert page.locator('.table-scroll').evaluate('node => node.scrollHeight > node.clientHeight')
    assert page.locator('#confirm').inner_text() == 'Yes, import results'
    assert page.locator('#cancel').inner_text() == 'Not now'
    page.screenshot(path='/tmp/tle-games-import-review.png', full_page=True)
    confirm_box = page.locator('#confirm').bounding_box()
    page.locator('.table-scroll').evaluate('node => { node.scrollTop = node.scrollHeight; }')
    last_box, table_box = rows.last.bounding_box(), page.locator('.table-scroll').bounding_box()
    assert last_box['y'] + last_box['height'] <= table_box['y'] + table_box['height'] + 1
    assert page.locator('#confirm').bounding_box() == confirm_box

    # Cancel never calls confirm. A later double click confirms only once.
    page.locator('#cancel').click()
    assert page.locator('#preview').is_hidden() and not confirms()
    assert page.locator('#read-tools').is_visible()
    read()
    page.evaluate("document.querySelector('#confirm').click(); document.querySelector('#confirm').click()")
    page.wait_for_function("document.querySelector('#status').textContent.startsWith('Imported')")
    assert len(confirms()) == 1

    # Closing/reopening the popup restores only a live review for its connection.
    read()
    load()
    page.locator('#preview').wait_for(state='visible')
    assert page.locator('#read-tools').is_hidden() and len(confirms()) == 1
    page.evaluate('''async () => {
      const data = await chrome.storage.session.get('pendingImport');
      data.pendingImport.data.expires_at = 0;
      await chrome.storage.session.set(data);
    }''')
    load()
    assert page.locator('#preview').is_hidden()
    assert page.locator('#read-tools').is_visible()

    # A changed server or authenticated account cannot receive an old confirmation.
    read()
    page.evaluate("chrome.storage.local.set({server:'http://other-api.test'})")
    page.locator('#confirm').click()
    page.wait_for_function("document.querySelector('#preview').hidden")
    assert len(confirms()) == 1
    assert 'changed' in page.locator('#status').inner_text().lower()
    page.evaluate("chrome.storage.local.set({server:'http://api.test'})")
    read()
    catalog['user_id'] = '11'
    page.locator('#confirm').click()
    page.wait_for_function("document.querySelector('#preview').hidden")
    assert len(confirms()) == 1
    assert 'changed' in page.locator('#status').inner_text().lower()

    # A different account must not restore a preview bound to the former one.
    read()
    catalog['user_id'] = '12'
    load()
    assert page.locator('#preview').is_hidden() and len(confirms()) == 1
    assert page.locator('#read-tools').is_visible()

    # Expiry and stale/unauthorized server replies expose the fresh-read controls.
    preview['expires_at'] = time.time() - 1
    read()
    page.locator('#confirm').click()
    page.wait_for_function("document.querySelector('#preview').hidden")
    assert 'expired' in page.locator('#status').inner_text().lower()
    assert len(confirms()) == 1
    preview['expires_at'] = time.time() + 600
    for failure_status in (401, 403, 404, 409):
        read()
        before = len(confirms())
        page.locator('#confirm').click()
        page.wait_for_function("document.querySelector('#preview').hidden")
        assert len(confirms()) == before + 1
        assert page.locator('#read-tools').is_visible()
        assert not page.evaluate("chrome.storage.session.get('pendingImport').then(data => data.pendingImport)")
    assert not errors, errors
    assert confirm_box['y'] + confirm_box['height'] <= 600, confirm_box
    page.close()


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        browser.close()
    print('Import review mappings, privacy, compact layout, confirmation, restoration and connection guards passed.')

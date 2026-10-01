"""Automatic settings remain controllable during API failure and pending saves."""
from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION, TOKEN


def check(browser, legacy=False):
    page = browser.new_page()
    state = {'mode': 'success', 'pending': []}
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    catalog = {'guild_name': 'Test', 'user_name': 'Member', 'games': []}

    def route_request(route):
        if route.request.url.startswith('http://api.test/'):
            if state['mode'] == 'delay':
                state['pending'].append(route)
            elif state['mode'] == 'failure':
                route.fulfill(status=401, json={'error': 'Expired token'})
            else:
                route.fulfill(json=catalog)
            return
        filename = route.request.url.split('/')[-1]
        route.fulfill(body=(EXTENSION / filename).read_text(), content_type=(
            'text/javascript' if filename.endswith('.js') else 'text/css' if filename.endswith('.css') else 'text/html'))

    page.route('**/*', route_request)
    page.add_init_script('''(() => {
      window.saved = {server:'http://api.test',token:TOKEN,...LEGACY};
      window.activations=[]; window.grants=[];
      window.chrome={
        storage:{local:{get:async()=>({...saved}),set:async value=>Object.assign(saved,value),
          remove:async key=>{delete saved[key]},setAccessLevel:async()=>{}},session:{remove:async()=>{}}},
        permissions:{request:async request=>{grants.push(request);return true},contains:async()=>true},
        runtime:{sendMessage:async value=>{activations.push(value);return {ok:true}}},
      };
    })()'''.replace('TOKEN', repr(TOKEN)).replace('LEGACY', '{autoLinkedIn:true}' if legacy else '{}'))
    page.goto('http://extension.test/options.html')
    assert not page.locator('#auto-linkedin').is_checked()
    assert page.locator('#auto-akari').is_checked()
    page.click('button[type=submit]')
    page.wait_for_function("document.querySelector('#status').textContent.startsWith('Connected')")
    assert page.evaluate('saved.autoLinkedIn===false && saved.autoAkari')
    assert set(page.evaluate('grants[0].origins')) == {
        'http://api.test/*', 'https://dailyakari.com/*'}
    # Only actively checking and saving LinkedIn opts into background posting.
    page.check('#auto-linkedin')
    page.click('button[type=submit]')
    page.wait_for_function('saved.autoLinkedIn===true && saved.autoLinkedInConsentVersion===1')
    assert 'https://www.linkedin.com/*' in page.evaluate('grants.at(-1).origins')

    # Opt-out is local: no Save, no token validity, no API availability needed.
    state['mode'] = 'failure'
    page.uncheck('#auto-linkedin')
    page.uncheck('#auto-akari')
    page.wait_for_function('saved.autoLinkedIn===false && saved.autoAkari===false')
    assert page.evaluate('activations.length') >= 3
    page.click('button[type=submit]')
    page.wait_for_function("document.querySelector('#status').textContent==='Expired token'")
    assert page.evaluate('saved.autoLinkedIn===false && saved.autoAkari===false')

    # An old Save cannot undo an opt-out made while its API call was pending.
    state['mode'] = 'delay'
    page.check('#auto-linkedin')
    page.fill('#token', 'tlegames_' + 'b' * 43)
    page.click('button[type=submit]')
    page.wait_for_function("document.querySelector('#status').textContent==='Checking connection…'")
    wait_pending(page, state)
    page.uncheck('#auto-linkedin')
    state['pending'].pop().fulfill(json=catalog)
    page.wait_for_timeout(100)
    assert page.evaluate('saved.autoLinkedIn') is False and page.evaluate('saved.token') == TOKEN

    # Forget is also final if a delayed Save was about to replace the token.
    page.click('button[type=submit]')
    page.wait_for_function("document.querySelector('#status').textContent==='Checking connection…'")
    wait_pending(page, state)
    page.click('#forget')
    page.wait_for_function("!('token' in saved)")
    state['pending'].pop().fulfill(json=catalog)
    page.wait_for_timeout(100)
    assert not page.evaluate("'token' in saved")
    # A newer Save supersedes an older one even when responses arrive backwards.
    page.fill('#token', 'tlegames_' + 'b' * 43)
    page.click('button[type=submit]')
    wait_pending(page, state)
    old_request = state['pending'].pop()
    page.fill('#token', 'tlegames_' + 'c' * 43)
    page.click('button[type=submit]')
    wait_pending(page, state)
    state['pending'].pop().fulfill(json=catalog)
    page.wait_for_function("saved.token==='tlegames_'+ 'c'.repeat(43)")
    old_request.fulfill(json=catalog)
    page.wait_for_timeout(100)
    assert page.evaluate('saved.token') == 'tlegames_' + 'c' * 43
    assert page.locator('#status').inner_text().startswith('Connected')
    assert not errors, errors
    page.close()


def wait_pending(page, state):
    for _ in range(80):
        if len(state['pending']) == 1:
            return
        page.wait_for_timeout(25)
    raise AssertionError('Expected one pending catalog request')


if __name__ == '__main__':
    with sync_playwright() as p:
        browser = p.chromium.launch()
        check(browser)
        check(browser, legacy=True)
        browser.close()
    print('LinkedIn opt-in for new/legacy settings, site grants, offline opt-out, pending Save, and Forget races passed.')

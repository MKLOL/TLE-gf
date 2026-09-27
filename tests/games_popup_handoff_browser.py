"""Popup hands persisted previews to a reusable window, never to confirm POST."""
from playwright.sync_api import sync_playwright

from tests.games_review_browser_support import ReviewBrowser


def idle(page):
    page.wait_for_function("!document.querySelector('#read').disabled")


def ui_calls(page):
    return page.evaluate("JSON.parse(localStorage.getItem('uiCalls') || '[]')")


def wait_handoff(page):
    page.wait_for_function("JSON.parse(localStorage.getItem('uiCalls') || '[]').some(row => ['window.create','tab.create'].includes(row.kind))")
    idle(page)


def check_basic(browser, extracted=None):
    env = ReviewBrowser(browser, pending=False, extracted=extracted)
    page = env.page('popup.html')
    idle(page)
    page.locator('#read').click()
    wait_handoff(page)
    creates = [call for call in ui_calls(page) if call['kind'] == 'window.create']
    assert len(creates) == 1
    opened = creates[0]
    assert opened['data']['type'] == 'popup'
    assert opened['data']['width'] >= 1000 and opened['data']['height'] >= 750
    assert opened['data']['url'].endswith('review.html#synthetic-preview')
    assert opened['pending']['data']['preview_id'] == 'synthetic-preview'
    assert opened['pending']['guild'] == '1' and opened['pending']['user'] == '10'
    assert not env.confirms() and page.locator('#confirm').count() == 0
    preview_calls = [call for call in env.calls if call['kind'] == 'preview']
    assert len(preview_calls) == 1
    assert preview_calls[0]['body']['game'] == env.extracted['gamePath']
    assert preview_calls[0]['body']['puzzle_number'] == env.extracted['puzzleNumber']
    assert page.locator('#open-review').is_visible()

    # Reopening keeps the same pending snapshot and focuses an existing review.
    page.reload()
    idle(page)
    assert page.locator('#open-review').is_visible()
    # The toolbar can close before windows.create stores its result. Context
    # discovery must find the durable page even with no saved window metadata.
    page.evaluate("chrome.storage.session.remove('reviewWindow')")
    assert 'url' not in page.evaluate('chrome.tabs.get(92)')
    page.locator('#open-review').click()
    idle(page)
    assert len([call for call in ui_calls(page) if call['kind'] == 'window.create']) == 1
    assert any(call['kind'] == 'window.update' for call in ui_calls(page))
    assert not env.confirms()

    # A user-closed review is recreated without rereading or submitting results.
    page.evaluate("localStorage.setItem('windowClosed', '1')")
    page.locator('#open-review').click()
    idle(page)
    assert len([call for call in ui_calls(page) if call['kind'] == 'window.create']) == 2
    assert len([call for call in env.calls if call['kind'] == 'preview']) == 1
    # A recycled tab ID must not navigate an unrelated page to the review.
    page.evaluate("localStorage.removeItem('windowClosed'); localStorage.setItem('reviewUrl', 'http://unrelated.test/')")
    before_updates = len([call for call in ui_calls(page) if call['kind'] == 'tab.update'])
    page.locator('#open-review').click()
    idle(page)
    assert len([call for call in ui_calls(page) if call['kind'] == 'window.create']) == 3
    assert len([call for call in ui_calls(page) if call['kind'] == 'tab.update']) == before_updates
    # On older Chromium without getContexts, an unverifiable tab is not reused.
    page.evaluate('delete chrome.runtime.getContexts')
    page.locator('#open-review').click()
    idle(page)
    assert len([call for call in ui_calls(page) if call['kind'] == 'window.create']) == 4
    page.locator('#cancel').click()
    page.wait_for_function("chrome.storage.session.get().then(data => !data.pendingImport)")
    assert not env.confirms()
    assert page.locator('#open-review').is_hidden()
    env.close()


def check_fallback(browser):
    for mode in ('unavailable', 'failure'):
        env = ReviewBrowser(browser, pending=False, window_mode=mode)
        page = env.page('popup.html')
        idle(page)
        page.locator('#read').click()
        wait_handoff(page)
        tabs = [call for call in ui_calls(page) if call['kind'] == 'tab.create']
        assert len(tabs) == 1 and tabs[0]['data']['url'].endswith('review.html#synthetic-preview')
        assert tabs[0]['pending']['data']['preview_id'] == 'synthetic-preview'
        assert not env.confirms()
        page.locator('#cancel').click()
        assert not env.confirms()
        env.close()

    # Even if both opening APIs fail, the user can retry the saved preview.
    env = ReviewBrowser(browser, pending=False, window_mode='both-failure')
    page = env.page('popup.html')
    idle(page)
    page.locator('#read').click()
    page.wait_for_function("document.querySelector('#status').textContent.includes('Could not open')")
    idle(page)
    assert page.locator('#open-review').is_visible()
    assert page.evaluate("chrome.storage.session.get().then(data => data.pendingImport.data.preview_id)") == 'synthetic-preview'
    assert not env.confirms()
    env.close()


def check_progress_and_restore_guards(browser):
    env = ReviewBrowser(browser, pending=False)
    env.delays.add('catalog')
    page = env.page('popup.html')
    env.wait_request(page, 'catalog')
    assert page.locator('#status').is_visible()
    assert page.locator('#status').inner_text().strip()
    assert page.locator('#read').is_disabled()
    page.evaluate('window.reviewTestNow = Date.now; Date.now = () => reviewTestNow() + 6000')
    page.locator('#wait-note').wait_for(state='visible')
    assert 'elapsed' in page.locator('#wait-note').inner_text()
    page.evaluate('Date.now = reviewTestNow')
    env.delays.clear()
    env.release('catalog')
    idle(page)
    env.delays.add('preview')
    page.locator('#read').click()
    env.wait_request(page, 'preview')
    assert 'match' in page.locator('#status').inner_text().lower()
    assert page.locator('#read').is_disabled() and not ui_calls(page)
    assert not env.confirms()
    env.delays.clear()
    env.release('preview')
    wait_handoff(page)
    assert not env.confirms()

    # Account changes and expired snapshots cannot reopen an old review.
    env.catalog['user_id'] = '11'
    page.reload()
    idle(page)
    assert page.locator('#open-review').is_hidden()
    env.catalog['user_id'] = '10'
    expired = env.snapshot()
    expired['data']['expires_at'] = 0
    page.evaluate('value => chrome.storage.session.set({pendingImport:value})', expired)
    page.reload()
    idle(page)
    assert page.locator('#open-review').is_hidden() and not env.confirms()
    env.close()


def check_proxy_failure_and_retry(browser):
    env = ReviewBrowser(browser, pending=False)
    env.bad_json.add('catalog')
    page = env.page('popup.html')
    idle(page)
    message = page.locator('#status').inner_text()
    assert '502' in message and 'try again' in message.lower()
    assert 'unexpected token' not in message.lower() and '<html>' not in message.lower()
    assert 'loading' not in (page.locator('#status').get_attribute('class') or '')
    assert page.locator('#read').is_enabled() and not env.confirms()
    env.bad_json.clear()
    page.locator('#read').click()
    wait_handoff(page)
    assert not env.confirms()
    env.close()


def check(browser):
    check_basic(browser)
    check_fallback(browser)
    check_progress_and_restore_guards(browser)
    check_proxy_failure_and_retry(browser)


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        browser.close()
    print('Popup persistence, reusable review window, tab fallback, progress and restore guards passed.')

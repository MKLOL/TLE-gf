"""Synthetic large-window review and snapshot/confirmation lifecycle checks."""
from playwright.sync_api import sync_playwright

from tests.games_review_browser_support import ReviewBrowser


def ready(page):
    page.locator('#review-content').wait_for(state='visible')
    page.wait_for_function("!document.querySelector('#confirm').disabled")


def blocked(page):
    page.locator('#empty-state').wait_for(state='visible')
    assert page.locator('#confirm').is_hidden() or page.locator('#confirm').is_disabled()


def check_layout_and_filters(browser):
    env = ReviewBrowser(browser)
    page = env.page()
    ready(page)
    assert not env.confirms()
    assert 'Would you like' in page.locator('#preview-question').inner_text()
    headers = page.locator('thead th').all_text_contents()
    assert any('LinkedIn' in header for header in headers)
    assert any('Discord' in header for header in headers)
    rows = page.locator('#players tr')
    assert rows.count() == 20
    assert rows.nth(0).locator('td').count() >= 4
    assert 'Example Discord Member' in rows.nth(0).inner_text()
    assert 'Anonymous Discord Member' in rows.nth(1).inner_text()
    assert 'until you register' in rows.nth(2).inner_text()
    assert 'Unrated' in rows.nth(3).inner_text()
    assert 'Private synthetic source name' not in page.locator('body').inner_text()
    assert page.locator('#players img, #players script').count() == 0
    assert env.preview['rows'][-1]['name'].strip() in rows.last.inner_text()
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    footer = page.locator('#review-footer').bounding_box()
    assert footer['y'] + footer['height'] <= 801
    rows.last.scroll_into_view_if_needed()
    assert page.locator('#confirm').is_visible()
    page.locator('[data-filter="matched"]').click()
    assert page.locator('#players tr:visible').count() == 2
    page.locator('[data-filter="unassigned"]').click()
    assert page.locator('#players tr:visible').count() == 18
    page.locator('[data-filter="all"]').click()
    page.locator('#search').fill('Example Discord Member')
    assert page.locator('#players tr:visible').count() == 1
    assert '20' in page.locator('#review-footer').inner_text()
    assert 'all' in page.locator('#review-footer').inner_text().lower()
    page.locator('#search').fill('no matching synthetic name')
    assert page.locator('#players tr:visible').count() == 0
    assert not env.confirms()
    page.locator('#search').fill('')
    assert page.locator('#players tr:visible').count() == 20
    env.close()


def check_cancel_reopen_and_snapshot_binding(browser):
    env = ReviewBrowser(browser)
    page = env.page()
    ready(page)
    page.close()
    page = env.page()
    ready(page)
    assert len(page.locator('#players tr').all()) == 20 and not env.confirms()
    page.locator('#cancel').click()
    blocked(page)
    assert not page.evaluate("chrome.storage.session.get().then(data => data.pendingImport)")
    assert not env.confirms()
    page.reload()
    blocked(page)
    env.close()

    # Old windows must not replace their table or remove a newer pending preview.
    env = ReviewBrowser(browser)
    old = env.page()
    ready(old)
    newer = env.snapshot('newer-preview')
    newer['data']['rows'][0]['name'] = 'Newest synthetic player'
    control = env.page('popup.html')
    control.evaluate('value => chrome.storage.session.set({pendingImport:value})', newer)
    blocked(old)
    assert control.evaluate("chrome.storage.session.get().then(data => data.pendingImport.data.preview_id)") == 'newer-preview'
    old.evaluate("document.querySelector('#cancel').click(); document.querySelector('#confirm').click()")
    old.wait_for_timeout(50)
    assert not env.confirms()
    assert control.evaluate("chrome.storage.session.get().then(data => data.pendingImport.data.preview_id)") == 'newer-preview'
    old.reload()
    blocked(old)
    assert not env.confirms()
    # Reusing the window changes only the URL hash; it must reinitialize the page.
    old.evaluate("location.hash = 'newer-preview'")
    ready(old)
    assert 'Newest synthetic player' in old.locator('#players').inner_text()
    assert not env.confirms()
    env.close()


def check_confirmation_guards(browser):
    for change in ('server', 'token', 'user', 'guild', 'expired'):
        env = ReviewBrowser(browser)
        page = env.page()
        ready(page)
        if change == 'server':
            page.evaluate("chrome.storage.local.set({server:'http://other-api.test'})")
        elif change == 'token':
            page.evaluate("chrome.storage.local.set({token:'tlegames_' + 'b'.repeat(43)})")
        elif change in ('user', 'guild'):
            env.catalog[f'{change}_id'] = '999'
        else:
            page.evaluate('Date.now = () => 9000000000000')
        page.evaluate("document.querySelector('#confirm').click()")
        blocked(page)
        assert not env.confirms(), change
        env.close()

    for failure in (401, 403, 404, 409):
        env = ReviewBrowser(browser)
        env.failures['confirm'] = failure
        page = env.page()
        ready(page)
        page.locator('#confirm').click()
        blocked(page)
        assert len(env.confirms()) == 1
        assert not page.evaluate("chrome.storage.session.get().then(data => data.pendingImport)")
        env.close()


def check_slow_requests_and_races(browser):
    env = ReviewBrowser(browser)
    env.delays.add('catalog')
    page = env.page()
    env.wait_request(page, 'catalog')
    assert page.locator('#status').is_visible()
    assert page.locator('#status').inner_text().strip()
    assert page.locator('#activity').is_visible()
    page.evaluate('window.reviewTestNow = Date.now; Date.now = () => reviewTestNow() + 6000')
    page.locator('#elapsed').wait_for(state='visible')
    page.wait_for_function("parseInt(document.querySelector('#elapsed').textContent, 10) >= 6")
    page.evaluate('Date.now = reviewTestNow')
    assert page.locator('#confirm').is_hidden() or page.locator('#confirm').is_disabled()
    assert not env.confirms()
    env.delays.clear()
    env.release('catalog')
    ready(page)

    # A new snapshot arriving while the confirmation check awaits cannot be submitted.
    env.delays.add('catalog')
    page.locator('#confirm').click()
    env.wait_request(page, 'catalog')
    page.evaluate('value => chrome.storage.session.set({pendingImport:value})', env.snapshot('newer-preview'))
    env.delays.clear()
    env.release('catalog')
    blocked(page)
    assert not env.confirms()
    assert page.evaluate("chrome.storage.session.get().then(data => data.pendingImport.data.preview_id)") == 'newer-preview'
    env.close()

    # Initial transient failures offer a reload retry without losing the snapshot.
    env = ReviewBrowser(browser)
    env.failures['catalog'] = 503
    page = env.page()
    blocked(page)
    assert page.locator('#retry-load').is_visible()
    assert not env.confirms()
    env.failures.clear()
    page.locator('#retry-load').click()
    ready(page)
    assert page.locator('#expiry').inner_text().strip()
    assert not env.confirms()
    env.close()

    # A temporary server failure preserves exactly this snapshot for an explicit retry.
    env = ReviewBrowser(browser)
    page = env.page()
    ready(page)
    env.failures['confirm'] = 503
    page.locator('#confirm').click()
    page.wait_for_function("document.querySelector('#status').textContent.includes('Review unavailable')")
    ready(page)
    assert len(env.confirms()) == 1
    assert page.evaluate("chrome.storage.session.get().then(data => data.pendingImport.data.preview_id)") == 'synthetic-preview'
    env.failures.clear()
    page.locator('#confirm').click()
    blocked(page)
    assert len(env.confirms()) == 2
    assert len({call['url'] for call in env.confirms()}) == 1
    env.close()

    # Explicit filtered confirmation submits the bound full snapshot only once.
    env = ReviewBrowser(browser)
    page = env.page()
    ready(page)
    page.locator('#search').fill('Example Discord Member')
    env.delays.add('confirm')
    page.evaluate("document.querySelector('#confirm').click(); document.querySelector('#confirm').click()")
    env.wait_request(page, 'confirm')
    assert page.locator('#status').is_visible() and page.locator('#status').inner_text().strip()
    assert page.locator('#confirm').is_disabled()
    assert page.locator('#cancel').is_disabled()
    assert env.confirms() == [{'kind': 'confirm',
        'url': 'http://api.test/v1/games/imports/synthetic-preview/confirm', 'method': 'POST', 'body': {}}]
    env.delays.clear()
    env.release('confirm')
    blocked(page)
    assert 'import' in page.locator('#empty-state').inner_text().lower()
    assert not page.evaluate("chrome.storage.session.get().then(data => data.pendingImport)")
    env.close()


def check(browser):
    check_layout_and_filters(browser)
    check_cancel_reopen_and_snapshot_binding(browser)
    check_confirmation_guards(browser)
    check_slow_requests_and_races(browser)


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        browser.close()
    print('Large review layout, filtering, privacy, explicit confirmation, progress and snapshot guards passed.')

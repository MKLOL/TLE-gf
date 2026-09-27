"""Captured tabbed leaderboard regressions; synthetic identities and no network."""
from pathlib import Path

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION

FIXTURE = (Path(__file__).parent / 'fixtures/linkedin/queens-tabbed-leaderboard.html').read_text()
BASE = 'https://www.linkedin.com/games/queens/results/leaderboard/connections/'
URL = BASE + '?gameUrn=urn%3Ali%3Afsd_game%3A(synthetic-member%2C3%2C879)'


def check(browser):
    page = browser.new_page()
    page.route('**/*', lambda route: route.fulfill(body=FIXTURE, content_type='text/html'))
    page.goto(URL)
    for filename in ('leaderboard.js', 'extract.js'):
        page.add_script_tag(path=str(EXTENSION / filename))

    def read():
        return page.evaluate('TleReadLeaderboard()')

    def reset(url=URL):
        page.evaluate('(url) => history.replaceState({}, "", url)', url)
        page.set_content(FIXTURE)

    def select_yesterday():
        page.evaluate('''() => {
            const tabs = document.querySelectorAll('[role="tab"]');
            tabs[0].setAttribute('aria-selected', 'false');
            tabs[1].setAttribute('aria-selected', 'true');
        }''')

    today = read()
    assert today['puzzleNumber'] == 879 and today['count'] == 17 and today['unplayed'] == 3, today
    assert today['rows'][15]['name'] == 'You'
    assert 'No hints!' in today['rows'][0]['status']
    assert 'No mistakes!' in today['rows'][1]['status']
    assert 'No hints & no mistakes!' in today['rows'][3]['status']
    own = page.evaluate('TleReadLeaderboard(true)')
    assert own['count'] == 1 and own['rows'][0]['time'] == '0:25', own
    assert own['leaderboard'].startswith('You\n')
    # Keep the displayed LinkedIn name when the own row also has a You marker.
    # The API needs both to preserve an unregistered importer's named result.
    page.evaluate('''() => {
        const row = document.querySelectorAll('.pr-connections-leaderboard-player__container')[15];
        row.querySelector('.pr-connections-leaderboard-player__name-text').textContent = 'Example Player';
        const marker = document.createElement('span'); marker.textContent = 'You';
        row.querySelector('.pr-connections-leaderboard-player__content-column').append(marker);
    }''')
    named = read()
    assert named['count'] == 17 and named['rows'][15]['name'] == 'Example Player'
    assert named['rows'][15]['isYou']
    assert 'Example Player\nYou\n' in named['leaderboard']
    own = page.evaluate('TleReadLeaderboard(true)')
    assert own['count'] == 1 and own['leaderboard'].startswith('Example Player\nYou\n')
    reset()

    # Renderer: handleTabClick never changes the base URL, and Yesterday fetches
    # gameUrn with delta:1. This must not import Yesterday under Today's edition.
    select_yesterday()
    assert read()['puzzleNumber'] == 878
    assert page.url == URL
    # An old open board remains attached to its own edition across midnight.
    reset(URL.replace('879)', '870)'))
    assert read()['puzzleNumber'] == 870
    select_yesterday()
    assert read()['puzzleNumber'] == 869

    for url in (BASE, BASE + '?gameUrn=garbage', URL + '&gameUrn=duplicate',
                URL.replace('879)', '9007199254740993)'), URL.replace('879)', '0)')):
        reset(url)
        assert 'puzzle number' in read()['error']
    reset()
    page.evaluate("document.querySelector('[aria-selected=true]').setAttribute('aria-selected','false')")
    assert 'selected leaderboard day' in read()['error']
    reset()
    page.evaluate("document.querySelector('[aria-selected=false]').setAttribute('aria-selected','true')")
    assert 'selected leaderboard day' in read()['error']
    reset()
    page.evaluate("document.querySelector('[aria-selected=true]').textContent='Unknown day'")
    assert 'selected leaderboard day' in read()['error']
    reset()
    page.evaluate("document.querySelector('.pr-connections-leaderboard__tabs-container').remove()")
    assert 'puzzle number' in read()['error']  # URL by itself is insufficient.
    reset()
    page.evaluate('''() => {
        const loader = document.createElement('div');
        loader.className = 'artdeco-loader'; loader.textContent = 'Loading';
        document.querySelector('section').append(loader);
    }''')
    assert 'still loading' in read()['error']
    page.evaluate("document.querySelector('.artdeco-loader').hidden=true")
    assert read()['puzzleNumber'] == 879
    reset(URL.replace('/queens/', '/tango/'))
    page.evaluate("document.querySelector('.pr-connections-leaderboard__toolbar-title').textContent='Tango Leaderboard'")
    assert read()['gamePath'] == 'tango' and read()['puzzleNumber'] == 879
    page.close()


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        browser.close()
    print('Captured tabbed leaderboard, Yesterday edition, placeholders, badges, and ambiguity guards passed.')

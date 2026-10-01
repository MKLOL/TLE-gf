"""Captured tabbed leaderboard regressions; synthetic identities and no network."""
from pathlib import Path

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION
from tests.games_popup_handoff_browser import idle, wait_handoff
from tests.games_review_browser_support import ReviewBrowser

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
    # Yesterday repeats the owner's result in a pinned footer (live 2026-10-01).
    # That footer has an insight tag instead of the original clean-solve badges.
    page.evaluate('''() => {
        const owner = [...document.querySelectorAll('.pr-connections-leaderboard-player__container')]
            .find(row => row.querySelector('.pr-connections-leaderboard-player__name-text').textContent === 'You');
        const footer = document.createElement('div');
        footer.className = 'pr-connections-leaderboard__sticky-section pr-connections-leaderboard__sticky-section--yesterday';
        const copy = owner.cloneNode(true);
        copy.querySelector('.pr-connections-leaderboard-player__subtitle').innerHTML =
            '<span class="pr-connections-leaderboard-player__subtitle-insight-tag">Top 1% today!</span>';
        footer.append(copy); document.querySelector('section').append(footer);
    }''')
    yesterday = read()
    assert yesterday['count'] == 17, yesterday
    assert sum(row['isYou'] for row in yesterday['rows']) == 1
    assert yesterday['leaderboardDay'] == 'Yesterday' and yesterday['puzzleNumber'] == 878
    assert page.evaluate('TleReadLeaderboard(true)')['count'] == 1
    # Conflicting sticky data signals an unfinished tab change; never guess.
    page.evaluate("document.querySelector('.pr-connections-leaderboard__sticky-section .pr-connections-leaderboard-player__score').textContent='0:26'")
    assert 'pinned score' in read()['error']
    page.evaluate("document.querySelector('.pr-connections-leaderboard__sticky-section .pr-connections-leaderboard-player__score').textContent='0:25'")
    # When only the footer contains the owner, retain that result once.
    page.evaluate('''() => {
        const owner = [...document.querySelectorAll('.pr-connections-leaderboard-player__container')]
            .find(row => !row.closest('.pr-connections-leaderboard__sticky-section') &&
                row.querySelector('.pr-connections-leaderboard-player__name-text').textContent === 'You');
        owner.remove();
    }''')
    assert read()['count'] == 17
    assert page.evaluate('TleReadLeaderboard(true)')['count'] == 1
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


def check_import_dates(browser):
    """Actual extraction -> popup request -> review, on both supported games."""
    source = browser.new_page()
    source.route('**/*', lambda route: route.fulfill(body=FIXTURE, content_type='text/html'))
    for game, edition in [('queens', 883), ('tango', 723)]:
        source.goto(URL.replace('/queens/', f'/{game}/').replace('879)', f'{edition})'))
        source.evaluate('(title) => document.querySelector(".pr-connections-leaderboard__toolbar-title").textContent=title',
                        f'{game.title()} Leaderboard')
        for filename in ('leaderboard.js', 'extract.js'):
            source.add_script_tag(path=str(EXTENSION / filename))
        # The browser's wall clock is deliberately unrelated to this old board.
        source.evaluate('Date.now = () => Date.UTC(2030, 0, 1)')
        for day, number, date in [('Today', edition, '2026-09-30'),
                                  ('Yesterday', edition - 1, '2026-09-29')]:
            source.evaluate('''day => {
                for (const tab of document.querySelectorAll('[role="tab"]'))
                    tab.setAttribute('aria-selected', String(tab.textContent.trim() === day));
            }''', day)
            extracted = source.evaluate('TleReadLeaderboard()')
            assert extracted['puzzleNumber'] == number and extracted['leaderboardDay'] == day
            env = ReviewBrowser(browser, pending=False, extracted=extracted)
            env.preview.update(puzzle_number=number, puzzle_date=date)
            popup = env.page('popup.html')
            idle(popup)
            popup.click('#read')
            wait_handoff(popup)
            requests = [call['body'] for call in env.calls if call['kind'] == 'preview']
            assert len(requests) == 1
            assert requests[0]['game'] == game
            assert requests[0]['puzzle_number'] == number
            assert requests[0]['puzzle_date'] == date
            assert day in popup.locator('#summary').inner_text()
            review = env.page()
            review.locator('#review-content').wait_for(state='visible')
            assert day in review.locator('#puzzle-meta').inner_text()
            assert date in review.locator('#puzzle-meta').inner_text()
            assert not env.confirms()
            env.close()
    source.close()


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        check_import_dates(browser)
        browser.close()
    print('Captured tabbed leaderboard, Yesterday edition, placeholders, badges, and ambiguity guards passed.')

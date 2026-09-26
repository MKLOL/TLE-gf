"""Read-only LinkedIn completion extraction on renderer-derived fixture markup."""
from pathlib import Path

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION

FIXTURE = (Path(__file__).parent / 'fixtures/linkedin/tango-result.html').read_text()


def check(browser):
    page = browser.new_page()
    page.route('**/*', lambda route: route.fulfill(body=FIXTURE, content_type='text/html'))
    page.goto('https://www.linkedin.com/games/tango/results/')
    for script in ('leaderboard.js', 'linkedin-result.js'):
        page.add_script_tag(path=str(EXTENSION / script))
    read = lambda: page.evaluate('TleOwnLinkedInResult.read()')
    expected = {'gamePath': 'tango', 'puzzleNumber': 719, 'timeSeconds': 39}
    assert read() == expected  # Ignores averages, rankings and other players.
    saved = page.content()
    page.evaluate("document.querySelector('.pr-top__headline').textContent='Practice makes perfect!'")
    assert read() is None
    page.set_content(saved)
    page.evaluate("document.querySelector('.pr-golden-chiclet__text').textContent='Solved in 3 guesses'")
    assert read() is None
    page.set_content(saved)
    page.evaluate("document.querySelector('.pr-golden-chiclet__text').style.visibility='hidden'")
    assert read() is None  # An average is not used as a fallback score.
    page.set_content(saved)
    page.evaluate("document.querySelector('.pr-top__subtext').textContent='Queens #879'")
    assert read() is None
    page.set_content(saved)
    page.evaluate("document.querySelector('.pr-game-results__toolbar-title').textContent='Queens'")
    assert read() is None
    page.set_content(saved + saved)
    assert read() is None
    page.set_content(saved)
    page.evaluate('''() => {
      const other = document.querySelector('.pr-golden-chiclet__text').cloneNode(true);
      other.textContent='0:40'; document.querySelector('.pr-top__header').append(other);
    }''')
    assert read() is None  # Ambiguous result never posts.
    page.set_content(saved)
    page.evaluate("document.querySelector('.pr-golden-chiclet__text').textContent='0:99'")
    assert read() is None
    # Ordinary and beginner render branches use their actual 'Solved in' copy.
    for beginner in (False, True):
        page.set_content(saved)
        page.evaluate('''beginner => {
          document.querySelector('.pr-golden-chiclet__carousel').remove();
          const score=document.createElement('div');
          score.className=beginner ? 'pr-beginner-player__better-than-chiclet-game-score' : 'pr-top__subtext';
          score.innerHTML='solved in <strong>1:02:03</strong>';
          document.querySelector('.pr-top__header').append(score);
          document.querySelector('.pr-top__headline').textContent='See you tomorrow!';
        }''', beginner)
        assert read() == {**expected, 'timeSeconds': 3723}
    # The playing page is never a completion, even if stale results remain in DOM.
    page.goto('https://www.linkedin.com/games/tango/')
    for script in ('leaderboard.js', 'linkedin-result.js'):
        page.add_script_tag(path=str(EXTENSION / script))
    assert read() is None
    # Actual LinkedIn architecture: outer document is a feed shell; results in preload.
    page.goto('https://www.linkedin.com/games/tango/results/')
    for script in ('leaderboard.js', 'linkedin-result.js'):
        page.add_script_tag(path=str(EXTENSION / script))
    page.set_content('<p>Feed</p><iframe src="/preload/"></iframe>')
    page.frame_locator('iframe').locator('.pr-top__header').wait_for()
    assert read() == expected
    page.evaluate("document.querySelector('iframe').style.display='none'")
    assert read() is None
    page.close()


if __name__ == '__main__':
    with sync_playwright() as p:
        browser = p.chromium.launch()
        check(browser)
        browser.close()
    print('LinkedIn completed-result extraction, renderer variants, frame handling, and rejection checks passed.')

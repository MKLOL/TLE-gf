"""Isolated Chromium checks: no personal profile, LinkedIn access, or live imports.

Run: python -m tests.games_extension_browser (requires playwright + Chromium).
"""
from pathlib import Path
import tempfile

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / 'extensions' / 'linkedin-games'
TOKEN = 'tlegames_' + 'a' * 43
CAPTURE = (ROOT / 'tests/fixtures/linkedin/pinpoint-leaderboard.html').read_text()


def timed_fixture(page):
    """Captured rows + renderer-confirmed badge markup; timed values are synthetic."""
    page.evaluate('''() => {
      document.querySelector('.pr-connections-leaderboard__toolbar-title').textContent = 'Tango Leaderboard';
      document.querySelector('.pr-connections-leaderboard__header-puzzle-id').textContent = 'Puzzle No. 719';
      const rows = [...document.querySelectorAll('.pr-connections-leaderboard-player__container')];
      const scores = ['0:29', '0:10', '0:27', '1:57', '0:38', '0:38', '–'];
      rows.forEach((row, index) => {
        row.querySelector('.pr-connections-leaderboard-player__score').textContent = scores[index];
        const badge = document.createElement('div');
        badge.className = 'pr-connections-leaderboard-player__subtitle';
        const copy = document.createElement('span');
        copy.className = 'pr-connections-leaderboard-player__subtitle-copy';
        const emoji = document.createElement('span');
        emoji.textContent = index === 1 ? '🤓💎' : index === 2 ? '🤓' : index === 5 ? '💎' : '';
        copy.textContent = index === 1 ? 'No hints & no mistakes!' : index === 2 ? 'No hints!' : index === 5 ? 'No mistakes!' : '';
        const paragraph = document.createElement('p');
        paragraph.append(emoji, copy);
        badge.append(paragraph);
        if (index === 3) { badge.hidden = true; badge.setAttribute('aria-label', 'No hints & no mistakes'); }
        row.querySelector('.pr-connections-leaderboard-player__content-column').append(badge);
        if (index === 4) row.querySelector('.pr-connections-leaderboard-player__reaction-area').append('🤓💎');
      });
    }''')


def extraction_checks(browser):
    page = browser.new_page()
    page.route('**/*', lambda route: route.fulfill(body=CAPTURE, content_type='text/html'))
    page.goto('https://www.linkedin.com/games/tango/results/leaderboard/connections/?gameUrn=ignored')
    timed_fixture(page)
    page.add_script_tag(path=str(EXTENSION / 'leaderboard.js'))
    result = page.evaluate((EXTENSION / 'extract.js').read_text())
    assert not result.get('error'), result
    assert result['gamePath'] == 'tango' and result['puzzleNumber'] == 719
    assert result['count'] == 6 and result['unplayed'] == 1
    assert result['leaderboard'] == (
        'You\n0:29\n\nPlayer 1\n🤓💎 No hints & no mistakes!\n0:10\n\n'
        'Player 2\n🤓 No hints!\n0:27\n\nPlayer 3\n1:57\n\n'
        'Player 4\n0:38\n\nPlayer 5\n💎 No mistakes!\n0:38')
    captured_timed = page.content()
    page.evaluate('''() => {
      const nudge = document.querySelector('.pr-connections-leaderboard-player__container').cloneNode(true);
      nudge.querySelector('.pr-connections-leaderboard-player__score').remove();
      const button = document.createElement('button');
      button.className = 'pr-connections-leaderboard-player__nudge-button';
      button.textContent = 'Nudge';
      nudge.append(button);
      document.querySelector('section').append(nudge);
    }''')
    nudged = page.evaluate((EXTENSION / 'extract.js').read_text())
    assert nudged['leaderboard'] == result['leaderboard'] and nudged['unplayed'] == 2
    page.evaluate('document.querySelector(".pr-connections-leaderboard-player__container").classList.add("pr-connections-leaderboard-player__container-blur")')
    assert 'hidden some results' in page.evaluate((EXTENSION / 'extract.js').read_text())['error']
    page.evaluate('document.querySelector("section").style.display = "none"')
    assert 'No connections leaderboard' in page.evaluate((EXTENSION / 'extract.js').read_text())['error']
    page.set_content(captured_timed + captured_timed)
    assert 'More than one' in page.evaluate((EXTENSION / 'extract.js').read_text())['error']
    # A non-LinkedIn page cannot masquerade as a game through its path.
    assert page.evaluate('TleLeaderboard.pageGame("https://evil.test/games/tango/")') is None
    # LinkedIn's actual frame architecture: the top document is the feed,
    # while the displayed board is in the same-origin preload document.
    page.route('**/preload/**', lambda route: route.fulfill(body=captured_timed, content_type='text/html'))
    page.set_content('<p>Feed text must never be parsed: Wrong Person 0:01</p><iframe src="/preload/?_bprMode=vanilla"></iframe>')
    page.frame_locator('iframe').locator('.pr-connections-leaderboard__section').wait_for()
    assert page.evaluate((EXTENSION / 'extract.js').read_text()) == result
    page.evaluate('document.querySelector("iframe").style.visibility = "hidden"')
    assert 'No connections leaderboard' in page.evaluate((EXTENSION / 'extract.js').read_text())['error']
    # The captured Pinpoint game has guesses, not seconds; reject it accurately.
    page.goto('https://www.linkedin.com/games/pinpoint/results/leaderboard/connections/')
    page.add_script_tag(path=str(EXTENSION / 'leaderboard.js'))
    assert 'no readable solve time' in page.evaluate((EXTENSION / 'extract.js').read_text())['error']
    page.close()
    return result


def popup_checks(browser, extracted):
    from tests.games_popup_handoff_browser import check_basic
    check_basic(browser, extracted)


def extension_load_check(playwright):
    # Real Manifest V3 loading and real chrome.storage, using a disposable profile.
    with tempfile.TemporaryDirectory(prefix='tle-games-browser-') as profile:
        context = playwright.chromium.launch_persistent_context(
            profile, headless=True, channel='chromium', args=[
                f'--disable-extensions-except={EXTENSION}', f'--load-extension={EXTENSION}'])
        page = context.new_page()
        page.goto('chrome://extensions')
        page.wait_for_function('document.querySelector("extensions-manager")?.shadowRoot')
        extension_id = page.evaluate('''() => {
            const list = document.querySelector('extensions-manager').shadowRoot
                .querySelector('extensions-item-list').shadowRoot;
            return [...list.querySelectorAll('extensions-item')]
                .find(item => item.data.name === 'TLE Games').data.id;
        }''')
        page.goto(f'chrome-extension://{extension_id}/popup.html')
        page.wait_for_function('document.querySelector("#status").textContent.includes("Settings")')
        assert page.locator('#read').is_enabled()
        page.goto(f'chrome-extension://{extension_id}/options.html')
        assert page.locator('#token').get_attribute('type') == 'password'
        context.close()


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        popup_checks(browser, extraction_checks(browser))
        browser.close()
        extension_load_check(playwright)
    print('Browser extraction, badge handling, popup review handoff/cancel, and MV3 loading passed.')

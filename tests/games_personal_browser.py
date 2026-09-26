"""Personal capture checks in disposable Chromium; never uses a personal profile."""
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

from tests.games_extension_browser import EXTENSION, CAPTURE, TOKEN, timed_fixture


def akari_capture(browser):
    page = browser.new_page()
    page.route('**/*', lambda route: route.fulfill(content_type='text/html', body=(
        '<iframe src="/akari"></iframe><iframe id="other" src="/other"></iframe>'
        if route.request.url.endswith('.com/') else '<p>Game frame</p>')))
    page.goto('https://dailyakari.com/')
    frame = page.frames[1]
    frame.evaluate('''() => {
      window.received = [];
      window.chrome = {runtime: {sendMessage: async message => { received.push(message); return {ok:true}; }}};
      localStorage.setItem('settings', JSON.stringify({showAccuracy:true}));
    }''')
    frame.add_script_tag(path=str(EXTENSION / 'akari.js'))

    def level(archive=False):
        page.evaluate('''archive => document.querySelector('iframe').contentWindow.postMessage({
          puzzlink:'https://puzz.link/p?akari/example', isArchive:archive,
          puzzleId:'1786', dailyDateKey:'2026-09-26', noRot:false}, location.origin)''', archive)

    def finish(accuracy=1, time='0:39'):
        frame.evaluate('''value => parent.postMessage({animTimeBtn:true,timeResult:value.time,
          accuracy:value.accuracy}, location.origin)''', {'time': time, 'accuracy': accuracy})
        page.wait_for_timeout(50)

    level()
    finish()
    got = frame.evaluate('received')
    assert got == [{'type': 'akari-result', 'body': {
        'game': 'akari', 'puzzle_date': '2026-09-26', 'puzzle_number': 629,
        'time_seconds': 39, 'accuracy': 100, 'is_perfect': True}}], got
    finish()
    assert len(frame.evaluate('received')) == 1
    finish(0.996)
    assert frame.evaluate('received.at(-1).body') == {**got[0]['body'], 'is_perfect': False}
    finish(0.87, '1:02:03')
    assert frame.evaluate('received.at(-1).body.time_seconds') == 3723
    assert frame.evaluate('received.at(-1).body.accuracy') == 87
    before = len(frame.evaluate('received'))
    # Same-origin sibling frames and top-page spoofed completions are ignored.
    page.evaluate("postMessage({animTimeBtn:true,timeResult:'0:01',accuracy:1},location.origin)")
    page.frames[2].evaluate("parent.postMessage({animTimeBtn:true,timeResult:'0:01',accuracy:1},location.origin)")
    finish(-1)
    finish(1, '0:99')
    assert len(frame.evaluate('received')) == before
    level(True)
    finish()
    assert len(frame.evaluate('received')) == before
    level()
    frame.evaluate("localStorage.setItem('settings', '{}')")
    finish()
    assert frame.evaluate('received.at(-1).type') == 'akari-status'
    page.close()


def personal_popup(browser):
    page = browser.new_page()
    errors, calls = [], []
    page.on('pageerror', lambda error: errors.append(str(error)))
    catalog = {'user_id': '10', 'user_name': 'Member', 'guild_name': 'Test server',
               'games': [{'id': 'tango', 'path': 'tango', 'name': 'LinkedIn Tango',
                          'anchor_date': '2026-09-04', 'anchor_number': 697,
                          'enabled': True, 'can_import': False, 'today': '2026-09-26'}]}
    extraction = {'gamePath': 'tango', 'puzzleNumber': 719,
                  'rows': [{'name': 'You', 'isYou': True, 'time': '0:29', 'status': ''}]}

    def route_request(route):
        url = route.request.url
        if url.startswith('http://api.test/'):
            calls.append((url, route.request.post_data_json if route.request.method == 'POST' else None))
            route.fulfill(json=catalog if url.endswith('/v1/games') else {
                'game': 'tango', 'puzzle_number': 719, 'posted': True, 'registered': True, 'duplicate': False})
        else:
            filename = url.split('/')[-1]
            route.fulfill(body=(EXTENSION / filename).read_text(), content_type=(
                'text/javascript' if filename.endswith('.js') else 'text/css' if filename.endswith('.css') else 'text/html'))
    page.route('**/*', route_request)
    page.add_init_script('''(() => {
      const local = {server:'http://api.test',token:TOKEN}, session={};
      const area = obj => ({get:async()=>obj,set:async data=>Object.assign(obj,data),
        remove:async key=>{delete obj[key]},setAccessLevel:async()=>{}});
      window.chrome={storage:{local:area(local),session:area(session)},
        permissions:{contains:async()=>true},runtime:{openOptionsPage:()=>{}},
        tabs:{query:async()=>[{id:1}]},scripting:{executeScript:async()=>[{result:DATA}]}};
    })()'''.replace('TOKEN', json.dumps(TOKEN)).replace('DATA', json.dumps(extraction)))
    page.goto('http://extension.test/popup.html')
    page.wait_for_function("document.querySelector('#connection').textContent.includes('Member')")
    assert not page.locator('#read').is_visible()
    page.click('#own')
    page.wait_for_selector('#own-preview', state='visible')
    assert '0:29' in page.locator('#own-summary').inner_text()
    assert not any(url.endswith('/results') for url, body in calls)
    page.click('#cancel-own')
    page.click('#own')
    page.wait_for_selector('#own-preview', state='visible')
    page.click('#post-own')
    page.wait_for_function("document.querySelector('#status').textContent.includes('registered and posted')")
    assert [body for url, body in calls if url.endswith('/results')] == [{
        'game': 'tango', 'puzzle_date': '2026-09-26', 'puzzle_number': 719,
        'time_seconds': 29, 'accuracy': 100, 'is_perfect': True}]
    assert not errors, errors
    page.close()


def own_only_extraction(browser):
    page = browser.new_page()
    page.route('**/*', lambda route: route.fulfill(body=CAPTURE, content_type='text/html'))
    page.goto('https://www.linkedin.com/games/tango/results/leaderboard/connections/')
    timed_fixture(page)
    page.add_script_tag(path=str(EXTENSION / 'leaderboard.js'))
    page.evaluate((EXTENSION / 'extract.js').read_text())
    # Unreadable/hidden OTHER scores must not block self-only posting.
    page.evaluate('''() => {
      const rows=document.querySelectorAll('.pr-connections-leaderboard-player__container');
      rows[1].classList.add('pr-connections-leaderboard-player__container-blur');
      rows[2].querySelector('.pr-connections-leaderboard-player__score').textContent='unsupported';
    }''')
    result = page.evaluate('TleReadLeaderboard(true)')
    assert result['count'] == 1 and result['rows'][0]['isYou'] and result['rows'][0]['time'] == '0:29', result
    assert result['leaderboard'] == 'You\n0:29'
    page.close()


if __name__ == '__main__':
    with sync_playwright() as p:
        browser = p.chromium.launch()
        akari_capture(browser)
        personal_popup(browser)
        own_only_extraction(browser)
        browser.close()
    print('Akari event capture, ranking fields, source/archive guards, and member LinkedIn posting passed.')

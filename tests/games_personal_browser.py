"""Personal capture checks in disposable Chromium; never uses a personal profile."""
import asyncio
import json
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright

from tests.games_extension_browser import EXTENSION, CAPTURE, TOKEN, timed_fixture

RESULT = (Path(__file__).parent / 'fixtures/linkedin/tango-result.html').read_text()


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


async def personal_popup(browser, game='tango', leaderboard=False):
    number = 719 if game == 'tango' else 879
    source = await browser.new_page()
    fixture = CAPTURE if leaderboard else RESULT.replace('Tango', game.title()).replace('#719', f'#{number}')
    await source.route('**/*', lambda route: route.fulfill(body=fixture, content_type='text/html'))
    await source.goto(f'https://www.linkedin.com/games/{game}/results/' + ('leaderboard/connections/' if leaderboard else ''))
    if leaderboard:
        await source.evaluate('''() => {
          document.querySelector('.pr-connections-leaderboard__toolbar-title').textContent='Tango Leaderboard';
          document.querySelector('.pr-connections-leaderboard__header-puzzle-id').textContent='Puzzle No. 719';
          document.querySelector('.pr-connections-leaderboard-player__score').textContent='0:29';
        }''')
        # The manual fallback must ignore unreadable scores belonging to others.
        await source.evaluate("document.querySelectorAll('.pr-connections-leaderboard-player__score')[1].textContent='unsupported'")
    page = await browser.new_page()
    errors, calls, injected = [], [], []
    page.on('pageerror', lambda error: errors.append(str(error)))
    source.on('pageerror', lambda error: errors.append(str(error)))
    catalog = {'user_id': '10', 'user_name': 'Member', 'guild_name': 'Test server',
               'games': [{'id': game, 'path': game, 'name': f'LinkedIn {game.title()}',
                          'anchor_date': '2026-09-26', 'anchor_number': number,
                          'enabled': True, 'can_import': False, 'today': '2026-09-26'}]}

    async def execute_in_game(request):
        # Execute the real injected files and serialized functions on a game page,
        # rather than returning a canned extraction that can hide missing readers.
        result = None
        for filename in request.get('files') or []:
            injected.append(filename)
            result = await source.evaluate((EXTENSION / filename).read_text())
        if request.get('func'):
            result = await source.evaluate('(' + request['func'] + ')()')
        return [{'result': result}]

    await page.expose_function('executeInGame', execute_in_game)

    async def route_request(route):
        url = route.request.url
        if url.startswith('http://api.test/'):
            calls.append((url, route.request.post_data_json if route.request.method == 'POST' else None))
            await route.fulfill(json=catalog if url.endswith('/v1/games') else {
                'game': game, 'puzzle_number': number, 'posted': True, 'registered': True, 'duplicate': False})
        else:
            filename = url.split('/')[-1]
            await route.fulfill(body=(EXTENSION / filename).read_text(), content_type=(
                'text/javascript' if filename.endswith('.js') else 'text/css' if filename.endswith('.css') else 'text/html'))
    await page.route('**/*', route_request)
    await page.add_init_script('''(() => {
      const local = {server:'http://api.test',token:TOKEN}, session={};
      const area = obj => ({get:async()=>obj,set:async data=>Object.assign(obj,data),
        remove:async key=>{delete obj[key]},setAccessLevel:async()=>{}});
      window.chrome={storage:{local:area(local),session:area(session)},
        permissions:{contains:async()=>true},runtime:{openOptionsPage:()=>{}},
        tabs:{query:async()=>[{id:1}]},scripting:{executeScript:async request=>executeInGame({
          files:request.files,func:request.func?.toString()})}};
    })()'''.replace('TOKEN', json.dumps(TOKEN)))
    await page.goto('http://extension.test/popup.html')
    await page.wait_for_function("document.querySelector('#connection').textContent.includes('Member')")
    assert not await page.locator('#read').is_visible()
    await page.click('#own')
    await page.wait_for_function("!document.querySelector('#own').disabled")
    assert await page.locator('#own-preview').is_visible(), await page.locator('#status').inner_text()
    assert ('0:29' if leaderboard else '0:39') in await page.locator('#own-summary').inner_text()
    assert ('extract.js' in injected) == leaderboard
    assert not any(url.endswith('/results') for url, body in calls)
    await page.click('#cancel-own')
    assert not any(url.endswith('/results') for url, body in calls)
    await page.click('#own')
    await page.wait_for_function("!document.querySelector('#own').disabled")
    assert await page.locator('#own-preview').is_visible(), await page.locator('#status').inner_text()
    await page.click('#post-own')
    await page.wait_for_function("document.querySelector('#status').textContent.includes('registered and posted')")
    assert [body for url, body in calls if url.endswith('/results')] == [{
        'game': game, 'puzzle_date': '2026-09-26', 'puzzle_number': number,
        'time_seconds': 29 if leaderboard else 39, 'accuracy': 100, 'is_perfect': True}]
    assert not any('/imports/' in url for url, body in calls)
    assert not errors, errors
    await page.close()
    await source.close()


async def personal_popups():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        await personal_popup(browser, 'tango')
        await personal_popup(browser, 'queens')
        await personal_popup(browser, leaderboard=True)
        await browser.close()


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
        own_only_extraction(browser)
        browser.close()
    asyncio.run(personal_popups())
    print('Akari capture, actual Queens/Tango own-result readers, leaderboard fallback, and confirmed personal posting passed.')

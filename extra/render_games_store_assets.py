"""Render vector icons and the real settings screen in isolated Chromium.

Requires Playwright and its Chromium browser. No personal browser profile,
credentials, game website requests, or score submissions are used.
"""
from pathlib import Path
import tempfile

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / 'extensions' / 'linkedin-games'
OUTPUT = ROOT / 'docs' / 'chrome-web-store'


def render_icons(browser):
    svg = (EXTENSION / 'icons' / 'icon.svg').read_text()
    for size in (16, 32, 48, 128):
        page = browser.new_page(viewport={'width': size, 'height': size})
        page.set_content('<style>body{margin:0}svg{display:block;width:100%;height:100%}</style>' + svg)
        page.screenshot(path=str(EXTENSION / 'icons' / f'icon-{size}.png'), omit_background=True)
        page.close()
    page = browser.new_page(viewport={'width': 440, 'height': 280})
    page.set_content('''<style>
      * {box-sizing:border-box} body {margin:0;width:440px;height:280px;background:#235d4e;
        color:#f3f7e9;display:flex;align-items:center;justify-content:center;gap:18px;
        font-family:system-ui,sans-serif}
      svg {width:112px;height:112px} h1 {font-size:34px;letter-spacing:-1.5px;margin:0}
      p {font-size:13px;color:#d7e5cb;margin:6px 0 0}
      </style>''' + svg + '<div><h1>TLE Games</h1><p>Your daily scores. Shared.</p></div>')
    page.screenshot(path=str(OUTPUT / 'promo-440x280.png'))
    page.close()


def settings_screenshots(playwright):
    with tempfile.TemporaryDirectory(prefix='tle-settings-preview-') as profile:
        context = playwright.chromium.launch_persistent_context(
            profile, headless=True, channel='chromium',
            viewport={'width': 1280, 'height': 800}, args=[
                f'--disable-extensions-except={EXTENSION}', f'--load-extension={EXTENSION}'])
        context.route('http://**/*', lambda route: route.abort())
        context.route('https://**/*', lambda route: route.abort())
        workers = context.service_workers
        worker = workers[0] if workers else context.wait_for_event('serviceworker')
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('chrome-extension://' + worker.url.split('/')[2] + '/options.html')
        page.wait_for_function('() => document.querySelector("#server").value.length > 0')
        assert page.locator('#token').input_value() == ''
        assert page.locator('#auto-linkedin').is_checked()
        assert page.locator('#auto-akari').is_checked()
        page.screenshot(path=str(OUTPUT / 'screenshot-settings-1280x800.png'))
        previews = ROOT / 'dist'
        previews.mkdir(exist_ok=True)
        for width in (360, 760, 900):
            page.set_viewport_size({'width': width, 'height': 900})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            assert page.locator('button[type=submit]').is_visible()
            page.screenshot(path=str(previews / f'settings-{width}.png'), full_page=True)
        page.locator('#auto-linkedin').focus()
        page.keyboard.press('Space')
        assert not page.locator('#auto-linkedin').is_checked()
        page.wait_for_function('() => document.querySelector("#status").textContent.includes("off")')
        assert not errors, errors
        context.close()


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        render_icons(browser)
        browser.close()
        settings_screenshots(playwright)
    print('Rendered icons, promo, and real settings page; responsive and keyboard checks passed.')


if __name__ == '__main__':
    main()

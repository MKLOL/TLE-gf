"""Compatibility entry point for full review-window and popup lifecycle checks."""
from playwright.sync_api import sync_playwright

from tests.games_popup_handoff_browser import check as check_popup
from tests.games_review_window_browser import check as check_review


def check(browser):
    check_popup(browser)
    check_review(browser)


if __name__ == '__main__':
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        check(browser)
        browser.close()
    print('Popup handoff and full review mappings, privacy, filters and confirmation lifecycle passed.')

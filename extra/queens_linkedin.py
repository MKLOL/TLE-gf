"""LinkedIn navigation, consent handling, leaderboard scraping, and invitation
helpers for the standalone Queens scraper.

All Playwright-touching helpers that are *not* puzzle-board manipulation live
here.  The board-play helpers (which import ``_dismiss_consent_banner`` from
this module) live in ``queens_board``.
"""
import asyncio
import re

from queens_config import (
    _INVITATION_CANDIDATES_JS,
    _LEADERBOARD_END_PATTERNS,
    _LEADERBOARD_SELECTORS,
    _LEADERBOARD_START_RE,
    _MAX_SEE_MORE_CLICKS,
    _SEE_FULL_RE,
    _SEE_MORE_RE,
    _SESSION_EXPIRED_URL_FRAGMENTS,
    _YESTERDAY_RE,
    normalize_queens_name,
)


async def _extract_linkedin_self_name(page):
    """Return the logged-in user's display name, or ``None`` if we can't.

    Navigates to ``/in/me/`` — LinkedIn redirects that to the user's own
    profile URL — and parses ``document.title``, which is reliably formatted
    as ``"<Name> | LinkedIn"`` across UI redesigns.  Best-effort: never
    raises.

    Disturbs the current page (navigates away), so callers that need to
    stay on the games page should run this on a separate ``page`` /
    context, or skip it.
    """
    try:
        await page.goto('https://www.linkedin.com/in/me/',
                        wait_until='domcontentloaded', timeout=30000)
    except Exception:
        return None
    if any(f in page.url for f in _SESSION_EXPIRED_URL_FRAGMENTS):
        return None
    # Let the title settle — LinkedIn often loads with a placeholder title
    # ("LinkedIn") and updates to "<Name> | LinkedIn" once the profile data
    # arrives.  Poll up to ~5s.
    name = None
    for _ in range(10):
        try:
            title = await page.title()
        except Exception:
            title = ''
        if title and '|' in title:
            head = title.split('|', 1)[0].strip()
            if head and head.lower() != 'linkedin':
                name = head
                break
        await asyncio.sleep(0.5)
    return name


async def _dismiss_consent_banner(page):
    """Click any visible cookie/consent banner so it doesn't intercept later clicks.

    LinkedIn shows a fixed-position "We respect your privacy" bar at the top
    of the page on first visit; until you accept or reject, every click into
    the body underneath either misses or hits the banner instead.
    """
    clicked = await page.evaluate(r"""
    () => {
      const actionRe = /^(accept(?: all)?(?: cookies)?|reject(?: all)?(?: cookies)?|got it|ok|agree)$/i;
      const consentRe = /cookie|privacy|consent|we respect your privacy|use cookies|personalized ads/i;
      const invitationRe = /invitation|mutual connection|followers?|ignore/i;
      const visible = (el) => {
        const rect = el.getBoundingClientRect();
        const cs = getComputedStyle(el);
        return rect.width > 0 && rect.height > 0
          && cs.visibility !== 'hidden'
          && cs.display !== 'none';
      };
      const buttons = Array.from(document.querySelectorAll('button,[role="button"]'));
      for (const button of buttons) {
        const label = (
          button.innerText
          || button.getAttribute('aria-label')
          || button.getAttribute('value')
          || ''
        ).replace(/\s+/g, ' ').trim();
        if (!visible(button) || !actionRe.test(label)) continue;
        let context = '';
        let node = button;
        for (let depth = 0; node && node !== document.body && depth < 5; depth += 1) {
          context += ' ' + (node.innerText || '');
          node = node.parentElement;
        }
        if (!consentRe.test(context) || invitationRe.test(context)) continue;
        button.click();
        return true;
      }
      return false;
    }
    """)
    if clicked:
        await page.wait_for_timeout(300)


async def _click_first(page, *candidates, timeout=3000):
    """Try each ``(role, name_regex)`` candidate until one click succeeds.

    Returns True iff a click landed.  We try locator roles in order, then a
    plain text-based fallback — LinkedIn's "See full leaderboard" is rendered
    as a styled button in some experiments and a plain ``<a>`` in others.
    """
    for role, name in candidates:
        try:
            loc = page.get_by_role(role, name=name)
            if not await loc.count():
                continue
            target = loc.first
            if not await target.is_visible():
                continue
            await target.scroll_into_view_if_needed(timeout=1000)
            await target.click(timeout=timeout)
            return True
        except Exception:
            continue
    # Text-fallback: click the first visible node whose text matches.
    for _role, name in candidates:
        try:
            if hasattr(name, 'pattern'):
                loc = page.get_by_text(name).first
            else:
                loc = page.locator(f'text={name}').first
            if not await loc.count():
                continue
            if not await loc.is_visible():
                continue
            await loc.scroll_into_view_if_needed(timeout=1000)
            await loc.click(timeout=timeout)
            return True
        except Exception:
            continue
    return False


async def _expand_to_full_leaderboard(page):
    """Click "See full leaderboard" if present, then wait for the new page.

    Best-effort — silently no-ops on any failure (the parser is forgiving and
    the /results/ top-3 is still useful output).
    """
    await _dismiss_consent_banner(page)
    clicked = await _click_first(
        page,
        ('link', _SEE_FULL_RE),
        ('button', _SEE_FULL_RE),
    )
    if not clicked:
        return
    # Either the URL changes (full page navigation) or the leaderboard
    # widget swaps content in place.  Wait for whichever happens first.
    try:
        await page.wait_for_url('**/leaderboard/**', timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state('domcontentloaded', timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(
            'text=/\\d{1,2}:\\d{2}/', timeout=10000)
    except Exception:
        pass


async def _switch_results_day(page, day):
    """Switch the results page to a non-default day tab if requested."""
    if day != 'yesterday':
        return False
    await _dismiss_consent_banner(page)
    clicked = await _click_first(
        page,
        ('tab', _YESTERDAY_RE),
        ('button', _YESTERDAY_RE),
        ('link', _YESTERDAY_RE),
        timeout=4000,
    )
    if not clicked:
        clicked = await page.evaluate(r"""
        () => {
          const re = /^yesterday$/i;
          const visible = (el) => {
            const rect = el.getBoundingClientRect();
            const cs = getComputedStyle(el);
            return rect.width > 0 && rect.height > 0
              && cs.visibility !== 'hidden'
              && cs.display !== 'none';
          };
          for (const el of document.querySelectorAll('button,a,[role="tab"],[role="button"]')) {
            const label = (
              el.innerText
              || el.getAttribute('aria-label')
              || ''
            ).replace(/\s+/g, ' ').trim();
            if (visible(el) && re.test(label)) {
              el.click();
              return true;
            }
          }
          return false;
        }
        """)
    if clicked:
        try:
            await page.wait_for_timeout(1200)
        except Exception:
            pass
    return bool(clicked)


async def _click_see_more_until_done(page):
    """Repeatedly click "See more" until it's gone, hidden, or the count stops growing.

    Caps at ``_MAX_SEE_MORE_CLICKS`` so a broken "is it gone yet?" check can't
    spin forever.  Returns the number of clicks actually performed.
    """
    clicks = 0
    last_count = -1
    for _ in range(_MAX_SEE_MORE_CLICKS):
        try:
            btn = page.get_by_role('button', name=_SEE_MORE_RE)
            count = await btn.count()
        except Exception:
            break
        if not count:
            break
        try:
            first = btn.first
            if not await first.is_visible():
                break
            if not await first.is_enabled():
                break
            await first.scroll_into_view_if_needed(timeout=2000)
            await first.click(timeout=3000)
        except Exception:
            break
        clicks += 1
        # Give the new rows a beat to render before checking again.
        try:
            await page.wait_for_timeout(800)
        except Exception:
            pass
        # Detect "the button is still there but nothing's loading anymore"
        # by counting time-shaped lines.  If two consecutive clicks don't
        # grow the count, we're done.
        try:
            now_count = await page.locator(
                'text=/^\\s*\\d{1,2}:\\d{2}\\s*$/').count()
        except Exception:
            now_count = -1
        if now_count == last_count and now_count != -1:
            break
        last_count = now_count
    return clicks


async def _load_received_invitations(page, *, click_more=True):
    """Best-effort scroll/load pass for the received invitations page."""
    last_height = -1
    for _ in range(6):
        if click_more:
            clicked = await _click_first(
                page,
                ('button', _SEE_MORE_RE),
                ('link', _SEE_MORE_RE),
                timeout=2000,
            )
            if clicked:
                await page.wait_for_timeout(800)
                continue
        try:
            height = await page.evaluate('document.body.scrollHeight')
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(700)
        except Exception:
            break
        if height == last_height:
            break
        last_height = height


async def _find_matching_invitation(page, remaining):
    """Return ``(candidate, normalized_name)`` for the first exact name match."""
    candidates = await page.evaluate(_INVITATION_CANDIDATES_JS)
    for candidate in candidates:
        candidate_names = candidate.get('names') or []
        normalized_candidates = {
            normalize_queens_name(name)
            for name in candidate_names
            if normalize_queens_name(name)
        }
        for normalized in remaining:
            if normalized in normalized_candidates:
                return candidate, normalized
    return None, None


async def _extract_leaderboard_text(page):
    for selector in _LEADERBOARD_SELECTORS:
        try:
            loc = page.locator(selector).first
            if not await loc.count():
                continue
            text = await loc.inner_text(timeout=2000)
        except Exception:
            continue
        if text and _looks_like_leaderboard(text):
            return selector, _crop_to_leaderboard(text)
    body_text = await page.locator('body').inner_text()
    return None, _crop_to_leaderboard(body_text)


def _crop_to_leaderboard(text):
    """Trim ``text`` down to just the lines between the friend leaderboard's
    start banner and its ``See full leaderboard`` footer.

    Falls back to the original ``text`` if either marker is absent — the
    parser is forgiving, and we'd rather feed it everything than feed it
    nothing.
    """
    if not text:
        return text
    lines = text.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if _LEADERBOARD_START_RE.search(line):
            start_idx = i + 1
            break
    if start_idx is None:
        return text
    end_idx = len(lines)
    for i in range(start_idx, len(lines)):
        if any(p in lines[i].lower() for p in _LEADERBOARD_END_PATTERNS):
            end_idx = i
            break
    return '\n'.join(lines[start_idx:end_idx])


def _looks_like_leaderboard(text):
    # Cheap heuristic: a leaderboard chunk has at least one MM:SS-looking
    # line.  Stops us from happily returning the nav bar.
    return bool(re.search(r'\b\d{1,2}:\d{2}\b', text))

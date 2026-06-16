"""Shared constants, regexes, page selectors, and the (optional) parser import
for the standalone LinkedIn Queens scraper.

This module is imported by the scraper script and its sibling helper modules.
It deliberately carries no Playwright dependency so the pure-solver path stays
importable without a browser stack.
"""
import pathlib
import re
import sys

# So we can import the existing parser without installing the bot as a package.
# The parser import is optional — pulling it in transitively imports the cog
# stack (discord.py, aiohttp, lxml, cairo, ...).  In standalone-scraper testing
# you don't need any of that: ``fetch`` will still show the raw extracted text,
# which is the actual signal we care about.  When the bot calls this script in
# JSON mode, it does its own in-process parse on ``raw_text``.
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
try:
    from tle.cogs._minigame_queens import (  # noqa: E402
        normalize_queens_name,
        parse_queens_leaderboard,
    )
except ImportError:
    parse_queens_leaderboard = None

    def normalize_queens_name(name):
        return ' '.join(str(name).strip().casefold().split())


_STATE_PATH = _REPO_ROOT / 'extra' / '.queens_state.json'
# The ``/results/`` page renders the friend leaderboard directly; ``/games/
# queens/`` shows the puzzle UI and only reveals the leaderboard after a
# completed play.
_QUEENS_URL = 'https://www.linkedin.com/games/queens/results/'
_QUEENS_PLAY_URL = 'https://www.linkedin.com/games/queens/'
_INVITATIONS_URL = 'https://www.linkedin.com/mynetwork/invitation-manager/received/'
_LOGIN_URL = 'https://www.linkedin.com/login'

# Tried in order.  The leaderboard sits inside the post-game / results modal
# on LinkedIn and the exact wrapper changes from time to time, so we cast a
# wide net and let the parser pull signal out of noise.  Each selector picks
# the first match.
_LEADERBOARD_SELECTORS = (
    '[data-test-id*="leaderboard" i]',
    '[aria-label*="leaderboard" i]',
    'section:has-text("Leaderboard")',
    'div:has-text("Leaderboard"):has-text(":")',  # has a time-like value
    'main',
)

# Safety cap on "See more" clicks.  Each click loads ~10 more entries (the
# exact page size is LinkedIn's choice) so 50 covers ~500 friends, which is
# more than any realistic GF community size.  Without a cap, a broken
# "is the button gone?" check would spin forever.
_MAX_SEE_MORE_CLICKS = 50

_SEE_FULL_RE = re.compile(r'see\s+full\s+leaderboard', re.IGNORECASE)
_SEE_MORE_RE = re.compile(r'^(see|show)\s+more', re.IGNORECASE)
_YESTERDAY_RE = re.compile(r'^yesterday$', re.IGNORECASE)

# LinkedIn's puzzle cells carry aria-labels of the form:
#   "<State> of color <ColorName>, row <N>, column <M>"
# where <State> is "Empty", "Cross", "Queen" (or similar) and <N>/<M> are
# 1-indexed.  This is by far the most stable handle we have — CSS class
# names are obfuscated hashes that change on every build, but the
# accessibility text is part of the product contract.
_READ_GRID_JS = r"""
() => {
  const labelRe = /^\s*(.+?)\s+of\s+color\s+(.+?),\s*row\s+(\d+),\s*column\s+(\d+)\s*$/i;
  const candidates = Array.from(document.querySelectorAll('[aria-label]'));
  const parsed = [];
  for (const el of candidates) {
    const label = el.getAttribute('aria-label') || '';
    const m = label.match(labelRe);
    if (!m) continue;
    // Prefer the outermost element with this label so clicks land on the
    // real interactive target rather than a nested decoration span.  We do
    // this by skipping if any ancestor up to <body> has the same label.
    let p = el.parentElement;
    let skip = false;
    while (p && p !== document.body) {
      if ((p.getAttribute('aria-label') || '') === label) { skip = true; break; }
      p = p.parentElement;
    }
    if (skip) continue;
    const cs = getComputedStyle(el);
    parsed.push({
      state: m[1].trim(),
      color: m[2].trim(),
      row: parseInt(m[3], 10) - 1,
      col: parseInt(m[4], 10) - 1,
      bg: cs.backgroundColor,
      label,
    });
  }
  if (!parsed.length) {
    return {error: 'no cells with row/column aria-labels found'};
  }
  const maxRow = Math.max(...parsed.map(c => c.row));
  const maxCol = Math.max(...parsed.map(c => c.col));
  const n = Math.max(maxRow, maxCol) + 1;
  if (parsed.length !== n * n) {
    return {error: `found ${parsed.length} labelled cells but expected ${n*n}`,
            n, sample: parsed.slice(0, 4)};
  }
  return {n, cells: parsed};
}
"""

# Some LinkedIn experiments label the X-marked state "Cross", "X", "Marked",
# etc.; we just look for anything that isn't "queen" or clearly-empty.
_QUEEN_RE = re.compile(r'queen|crown', re.IGNORECASE)
_EMPTY_RE = re.compile(r'empty|blank|none', re.IGNORECASE)

# Match "2 connections played today", "1 connection played yesterday",
# "10 connections played today", etc.  This line marks where the friend
# leaderboard begins; everything above it is the user's own scorecard +
# encouragement banners.
_LEADERBOARD_START_RE = re.compile(
    r'connection(?:s)?\s+played\s+(?:today|yesterday)', re.IGNORECASE)

# URL fragments that mean "LinkedIn bounced us to re-authenticate".  If the
# page lands on one of these after navigating to /games/queens/results/, the
# stored session is dead and the bot needs a fresh ``;queens login``.
_SESSION_EXPIRED_URL_FRAGMENTS = (
    '/login', '/uas/', '/checkpoint/', '/authwall', '/signup',
)
# "See full leaderboard" is the link below the friend list; everything after
# it is the player-stats panel (Plays / Win % / Best score / Max streak), the
# streak badges, and the "play another game" CTAs — none of which the parser
# should see.  Same idea for "See yesterday's results" if LinkedIn ever
# renames it.
_LEADERBOARD_END_PATTERNS = (
    'see full leaderboard',
    "see yesterday's results",
    "see yesterday’s results",
)

_INVITATION_CANDIDATES_JS = r"""
() => {
  function visible(el) {
    const rect = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    return rect.width > 0 && rect.height > 0
      && cs.visibility !== 'hidden'
      && cs.display !== 'none';
  }

  function cleanName(text) {
    let value = (text || '').replace(/\s+/g, ' ').trim();
    if (!value) return '';
    value = value.replace(/^accept\s+/i, '');
    value = value.replace(/^invitation\s+from\s+/i, '');
    value = value.replace(/[’']s\s+invitation.*$/i, '');
    value = value.replace(/\s+invitation.*$/i, '');
    value = value.replace(/[’']s$/i, '');
    value = value.replace(/^view\s+/i, '');
    value = value.replace(/\s+profile$/i, '');
    value = value.trim();
    if (!value) return '';
    const lower = value.toLowerCase();
    const rejects = [
      'accept', 'ignore', 'message', 'view profile', 'connect',
      'show more', 'see more'
    ];
    if (rejects.includes(lower)) return '';
    if (lower.includes('mutual connection')) return '';
    if (lower.includes('follower')) return '';
    if (lower.includes('invitation')) return '';
    if (value.length > 100) return '';
    return value;
  }

  const buttons = Array.from(document.querySelectorAll('button'));
  const out = [];
  buttons.forEach((button, buttonIndex) => {
    const label = (
      button.innerText || button.getAttribute('aria-label') || ''
    ).trim();
    const aria = (button.getAttribute('aria-label') || '').trim();
    if (!/^accept\b/i.test(label) && !/^accept\b/i.test(aria)) return;
    if (!visible(button)) return;

    let card = button;
    let node = button;
    for (let depth = 0; node && depth < 8; depth += 1, node = node.parentElement) {
      const text = (node.innerText || '').trim();
      if (!text) continue;
      const buttonCount = node.querySelectorAll('button').length;
      if (/accept/i.test(text) && (/ignore/i.test(text) || buttonCount <= 6)) {
        card = node;
        break;
      }
    }
    const cardText = (card.innerText || '').trim();
    const isInvitationAction = /ignore/i.test(cardText)
      || /invitation/i.test(cardText)
      || /invitation/i.test(aria);
    if (!isInvitationAction) return;

    const names = [];
    const push = (value) => {
      const cleaned = cleanName(value);
      if (cleaned && !names.includes(cleaned)) names.push(cleaned);
    };

    push(aria);
    for (const link of card.querySelectorAll('a[href*="/in/"]')) {
      push(link.innerText || link.getAttribute('aria-label') || '');
    }
    for (const el of card.querySelectorAll('[aria-label]')) {
      const value = el.getAttribute('aria-label') || '';
      if (/profile|invitation|accept/i.test(value)) push(value);
    }
    const lines = (card.innerText || '')
      .split(/\n+/)
      .map(line => line.trim())
      .filter(Boolean);
    for (const line of lines.slice(0, 8)) push(line);

    out.push({
      button_index: buttonIndex,
      names,
      text_preview: lines.slice(0, 8).join(' | '),
    });
  });
  return out;
}
"""

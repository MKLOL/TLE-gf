import os

DATA_DIR = 'data'
LOGS_DIR = 'logs'

ASSETS_DIR = os.path.join(DATA_DIR, 'assets')
DB_DIR = os.path.join(DATA_DIR, 'db')
MISC_DIR = os.path.join(DATA_DIR, 'misc')
TEMP_DIR = os.path.join(DATA_DIR, 'temp')

USER_DB_FILE_PATH = os.path.join(DB_DIR, 'user.db')
CACHE_DB_FILE_PATH = os.path.join(DB_DIR, 'cache.db')

FONTS_DIR = os.path.join(ASSETS_DIR, 'fonts')

NOTO_SANS_CJK_BOLD_FONT_PATH = os.path.join(FONTS_DIR, 'NotoSansCJK-Bold.ttc')
NOTO_SANS_CJK_REGULAR_FONT_PATH = os.path.join(FONTS_DIR, 'NotoSansCJK-Regular.ttc')
NOTO_COLOR_EMOJI_FONT_PATH = os.path.join(FONTS_DIR, 'NotoColorEmoji.ttf')
NOTO_EMOJI_FONT_PATH = os.path.join(FONTS_DIR, 'NotoEmoji.ttf')

CONTEST_WRITERS_JSON_FILE_PATH = os.path.join(MISC_DIR, 'contest_writers.json')

LOG_FILE_PATH = os.path.join(LOGS_DIR, 'tle.log')

ALL_DIRS = (attrib_value for attrib_name, attrib_value in list(globals().items())
            if attrib_name.endswith('DIR'))


def _int_env(name, default):
    """Read an integer setting, tolerating an unset-but-exported variable.

    ``environment.template`` teaches the ``export FOO=""`` idiom for settings
    you have not filled in yet, and a bare ``int('')`` raises at import time —
    before any logging or error handling exists, so the bot dies with a
    traceback and no explanation. Empty or unparseable values fall back to the
    default instead.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        # No logger yet at import time; the fallback keeps the bot bootable.
        print(f'WARNING: {name}={raw!r} is not an integer, using {default}')
        return default

TLE_ADMIN = os.environ.get('TLE_ADMIN', 'Admin')
TLE_MODERATOR = os.environ.get('TLE_MODERATOR', 'Moderator')

_DEFAULT_STAR_COLOR = 0xffaa10
_DEFAULT_STAR = '\N{WHITE MEDIUM STAR}'

# Daily Akari Codeforces-style rating (see tle/util/akari_rating.py).
# Everyone starts here; AKARI_RATING_DAMPING scales every CF per-contest change
# down (0.25 = a quarter of real CF) so daily play stays low-volatility.
AKARI_START_RATING = 1200
AKARI_RATING_DAMPING = 0.25
# Inactivity decay toward the default rating. Every consecutive skipped day
# pulls the rating toward AKARI_START_RATING by min(AKARI_DECAY_MAX,
# base*(streak-grace)) of the remaining gap, so absence bites harder the
# longer it lasts (ramping to a 5%/day cap). Grace defaults to zero — decay
# starts on the first absent day — but the knob is preserved so a server can
# reintroduce a free window without touching code.
AKARI_DECAY_BASE = 0.04
AKARI_DECAY_MAX = 0.08
AKARI_DECAY_GRACE = 0
# A puzzle number more than this many days beyond today's real puzzle is treated
# as bad data (e.g. a troll posting "Daily Akari 9999999999") and ignored for
# rating. The small margin tolerates timezones / posting just after midnight.
AKARI_MAX_PUZZLE_LOOKAHEAD = 2
# Players who haven't played within this many days are hidden from the ranking.
AKARI_RANKING_MAX_INACTIVE_DAYS = 30

# ── Soccer betting minigame (tle/cogs/betting.py) ──────────────────────────
# Live 1X2 odds and final scores come from The Odds API (the-odds-api.com).
# Set ODDS_API_KEY to a free-tier key to enable `;bet matches`/`;bet open` and
# auto-settlement. Without it the cog loads but those paths are disabled
# (mods can still settle markets manually).
ODDS_API_KEY = os.environ.get('ODDS_API_KEY')
# Results come from football-data.org when this is set (free, rate-limited, no
# per-call credit cost) so settlement can poll often and cheaply. Falls back to
# The Odds API scores endpoint when unset. Get a free token at
# football-data.org/client/register.
FOOTBALL_DATA_API_KEY = os.environ.get('FOOTBALL_DATA_API_KEY')
# Wallet economy. Everyone starts at BET_START_BALANCE; `;bet daily` grants a
# flat BET_DAILY_AMOUNT once per UTC day (unconditional). Stakes are uncapped —
# you can wager up to your whole balance.
BET_START_BALANCE = _int_env('BET_START_BALANCE', 1000)
BET_DAILY_AMOUNT = _int_env('BET_DAILY_AMOUNT', 100)
BET_MIN_STAKE = 1
# The bot auto-opens a betting market this long before kickoff, freezing the
# odds it reads at that moment for the life of the market. 6h, to give members
# more time to bet (odds barely move over the extra hours).
BET_OPEN_LEAD_SECONDS = 6 * 3600
# How long after kickoff before the auto-settle poller asks for a final score.
# A World Cup match runs ~2h (group) and up to ~2h45 with extra time +
# penalties (knockouts); 3h leaves margin for the final score to land.
BET_SETTLE_BUFFER_SECONDS = 3 * 3600

# ── ;llm (Google Gemini) ───────────────────────────────────────────────────
# Model ladder, tried in order, cheapest/highest-quota first. Free-tier quota
# is metered per project per model ("Rate limits are applied per project, not
# per API key" — ai.google.dev/gemini-api/docs/rate-limits), so each entry here
# is a genuinely separate allowance on the same key, not the same bucket
# renamed. Three models x N projects is the real size of the pool.
#
# Verified against ai.google.dev/gemini-api/docs/models (July 2026): all three
# are current and free-tier eligible. Note gemini-2.0-flash and
# gemini-2.0-flash-lite are SHUT DOWN — do not fall back to them.
LLM_MODELS = tuple(m.strip() for m in os.environ.get(
    'LLM_MODELS',
    'gemini-3.1-flash-lite,gemini-2.5-flash-lite,gemini-2.5-flash'
).split(',') if m.strip())
# Optional bootstrap keys, comma-separated, for provisioning without typing
# secrets into Discord. Keys added via `;llm keys` are stored in user.db and
# are merged with these on startup.
GEMINI_API_KEYS = os.environ.get('GEMINI_API_KEYS', '')
# xAI provisioning. The singular spelling matches xAI's examples; the plural
# accepts a comma-separated pool for operators who keep multiple teams/keys.
XAI_API_KEYS = ','.join(
    value for value in (
        os.environ.get('XAI_API_KEYS', '').strip(),
        os.environ.get('XAI_API_KEY', '').strip(),
    ) if value)
XAI_MODEL = os.environ.get('XAI_MODEL', 'grok-4.3').strip() or 'grok-4.3'
# Per-user call counts are still recorded (see `;llm keystatus`) so moderators
# can see who is consuming the shared allowance, but nothing is enforced —
# there is deliberately no per-user cap or cooldown.
LLM_MAX_PROMPT_CHARS = _int_env('LLM_MAX_PROMPT_CHARS', 4000)
# Reasoning tokens are drawn from this same budget on thinking models, so it
# has to cover the model's thinking as well as the reply — a value sized for
# the answer alone comes back empty on a hard question.
LLM_MAX_OUTPUT_TOKENS = _int_env('LLM_MAX_OUTPUT_TOKENS', 2048)
# Image attachments forwarded to the model (Gemini is multimodal, so a
# screenshot of a problem statement or a WA verdict just works). The total cap
# matters as much as the per-image one: inline data is base64-encoded, which
# inflates by 4/3, and Gemini rejects a request whose inline payload exceeds
# ~20 MB. 12 MB raw stays under that even at the limit.
LLM_MAX_IMAGES = _int_env('LLM_MAX_IMAGES', 4)
LLM_MAX_IMAGE_BYTES = _int_env('LLM_MAX_IMAGE_BYTES', 4 * 1024 * 1024)
LLM_MAX_TOTAL_IMAGE_BYTES = _int_env('LLM_MAX_TOTAL_IMAGE_BYTES', 12 * 1024 * 1024)
# Channel-history context. A cheap routing call first decides whether the
# question needs the conversation at all; when it does, these bound how much
# gets pulled in. Set LLM_CONTEXT_ENABLED=0 to skip routing entirely and halve
# the request count per question.
LLM_CONTEXT_ENABLED = os.environ.get('LLM_CONTEXT_ENABLED', '1').strip() != '0'
LLM_CONTEXT_MESSAGES = _int_env('LLM_CONTEXT_MESSAGES', 50)
LLM_CONTEXT_WINDOW_SECONDS = _int_env('LLM_CONTEXT_WINDOW_SECONDS', 600)
LLM_REPLY_BEFORE = _int_env('LLM_REPLY_BEFORE', 25)
LLM_REPLY_AFTER = _int_env('LLM_REPLY_AFTER', 24)

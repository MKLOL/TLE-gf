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

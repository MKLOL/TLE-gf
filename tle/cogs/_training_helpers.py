"""Pure helpers, enums, and the Game state machine for the training cog.

Split out of ``training.py`` to keep each module under the line limit. The
``Training`` cog imports everything it needs from here.
"""
import html
import io
import random
from enum import IntEnum

import discord
from discord.ext import commands

from tle.util.db.user_db_conn import TrainingProblemStatus
from tle.util import codeforces_common as cf_common

# stuff for drawing image
import cairo
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

FONTS = [
    'Noto Sans',
    'Noto Sans CJK JP',
    'Noto Sans CJK SC',
    'Noto Sans CJK TC',
    'Noto Sans CJK HK',
    'Noto Sans CJK KR',
    # extra/fonts.conf rejects Noto Color Emoji on old Cairo; fonts-color.conf
    # allows it only after startup verifies a compatible Cairo runtime.
    'Noto Color Emoji',
    'Noto Emoji',
]


_TRAINING_MIN_RATING_VALUE = 800
_TRAINING_MAX_RATING_VALUE = 3500


class TrainingMode(IntEnum):
    NORMAL = 0
    SURVIVAL = 1
    TIMED15 = 2
    TIMED30 = 3
    TIMED60 = 4
    TIMED1 = 5


class TrainingResult(IntEnum):
    SOLVED = 0
    TOOSLOW = 1
    SKIPPED = 2
    INVALIDATED = 3


class TrainingCogError(commands.CommandError):
    pass


def rating_to_color(rating):
    """returns (r, g, b) pixels values corresponding to rating"""
    # TODO: Integrate these colors with the ranks in codeforces_api.py
    BLACK = (10, 10, 10)
    RED = (255, 20, 20)
    BLUE = (0, 0, 200)
    GREEN = (0, 140, 0)
    ORANGE = (250, 140, 30)
    PURPLE = (160, 0, 120)
    CYAN = (0, 165, 170)
    GREY = (70, 70, 70)
    if rating is None or rating=='N/A':
        return BLACK
    if rating < 1200:
        return GREY
    if rating < 1400:
        return GREEN
    if rating < 1600:
        return CYAN
    if rating < 1900:
        return BLUE
    if rating < 2100:
        return PURPLE
    if rating < 2400:
        return ORANGE
    return RED



def get_fastest_solves_image(rankings):
    """return PIL image for rankings"""
    SMOKE_WHITE = (250, 250, 250)
    BLACK = (0, 0, 0)

    DISCORD_GRAY = (.212, .244, .247)

    ROW_COLORS = ((0.95, 0.95, 0.95), (0.9, 0.9, 0.9))

    WIDTH = 1000
    #HEIGHT = 900
    BORDER_MARGIN = 20
    COLUMN_MARGIN = 10
    HEADER_SPACING = 1.25
    WIDTH_RANK = 0.10*WIDTH
    WIDTH_NAME = 0.35*WIDTH
    LINE_HEIGHT = 40#(HEIGHT - 2*BORDER_MARGIN)/(20 + HEADER_SPACING)
    HEIGHT = int((len(rankings) + HEADER_SPACING) * LINE_HEIGHT + 2*BORDER_MARGIN)
    # Cairo+Pango setup
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, WIDTH, HEIGHT)
    context = cairo.Context(surface)
    context.set_line_width(1)
    context.set_source_rgb(*DISCORD_GRAY)
    context.rectangle(0, 0, WIDTH, HEIGHT)
    context.fill()
    layout = PangoCairo.create_layout(context)
    layout.set_font_description(Pango.font_description_from_string(','.join(FONTS) + ' 20'))
    layout.set_ellipsize(Pango.EllipsizeMode.END)

    def draw_bg(y, color_index):
        nxty = y + LINE_HEIGHT

        # Simple
        context.move_to(BORDER_MARGIN, y)
        context.line_to(WIDTH, y)
        context.line_to(WIDTH, nxty)
        context.line_to(0, nxty)
        context.set_source_rgb(*ROW_COLORS[color_index])
        context.fill()

    def draw_row(pos, username, handle, rating, color, y, bold=False):
        context.set_source_rgb(*[x/255.0 for x in color])

        context.move_to(BORDER_MARGIN, y)

        def draw(text, width=-1):
            text = html.escape(text)
            if bold:
                text = f'<b>{text}</b>'
            layout.set_width((width - COLUMN_MARGIN)*1000) # pixel = 1000 pango units
            layout.set_markup(text, -1)
            PangoCairo.show_layout(context, layout)
            context.rel_move_to(width, 0)

        draw(pos, WIDTH_RANK)
        draw(username, WIDTH_NAME)
        draw(handle, WIDTH_NAME)
        draw(rating)

    #

    y = BORDER_MARGIN

    # draw header
    draw_row('Rating', 'Name', 'Handle', 'Time', SMOKE_WHITE, y, bold=True)
    y += LINE_HEIGHT*HEADER_SPACING

    for i, (pos, name, handle, rating, time) in enumerate(rankings):
        color = rating_to_color(rating)
        draw_bg(y, i%2)
        timeFormatted = cf_common.pretty_time_format(time, shorten=True, always_seconds=True)
        draw_row(str(pos), f'{name}', f'{handle} ({rating if rating else "N/A"})' , timeFormatted, color, y)
        if rating and rating >= 3000:  # nutella
            draw_row('', name[0], handle[0], '', BLACK, y)
        y += LINE_HEIGHT

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    discord_file = discord.File(image_data, filename='fastesttraining.png')
    return discord_file




class Game:
    def __init__(self, mode, score=None, lives=None, timeleft=None):
        self.mode = int(mode)
        # existing game
        if score is not None:
            self.score = int(score)
            self.lives = int(lives) if lives is not None else lives
            self.timeleft = int(timeleft) if timeleft is not None else timeleft
            self.alive = True if self.lives is None or self.lives > 0 else False
            return
        # else we init a new game
        self.timeleft = self._getBaseTime()
        self.lives = self._getBaseLives()
        self.alive = True
        self.score = int(0)

    def _getModeStr(self):
        if self.mode == TrainingMode.NORMAL:
            return "Infinite"
        elif self.mode == TrainingMode.SURVIVAL:
            return "Survival"
        elif self.mode == TrainingMode.TIMED1:
            return "Timed 1 mins"
        elif self.mode == TrainingMode.TIMED15:
            return "Timed 15 mins"
        elif self.mode == TrainingMode.TIMED30:
            return "Timed 30 mins"
        elif self.mode == TrainingMode.TIMED60:
            return "Timed 60 mins"

    def _getBaseLives(self):
        if self.mode == TrainingMode.NORMAL:
            return None
        else:
            return 3

    def _getBaseTime(self):
        if self.mode == TrainingMode.NORMAL or self.mode == TrainingMode.SURVIVAL:
            return None
        if self.mode == TrainingMode.TIMED1:
            return int(1*60+1)
        if self.mode == TrainingMode.TIMED15:
            return int(15*60+1)
        if self.mode == TrainingMode.TIMED30:
            return int(30*60+1)
        if self.mode == TrainingMode.TIMED60:
            return int(60*60+1)

    def _newRating(self, success, rating):
        newRating = rating
        if success == TrainingResult.SOLVED:
            newRating += 100
        else:
            newRating -= 100
        newRating = min(newRating, 3500)
        newRating = max(newRating, 800)
        return newRating

    def doSolved(self, rating, duration):
        rating = int(rating)
        success = TrainingResult.SOLVED
        if self.mode != TrainingMode.NORMAL and self.mode != TrainingMode.SURVIVAL:
            if duration > self.timeleft:
                success = TrainingResult.TOOSLOW
                self.lives -= 1
                self.timeleft = self._getBaseTime()
                if self.lives is not None and self.lives == 0:
                    self.alive = False
            else:
                self.score += 1
                self.timeleft = int(
                    min(self.timeleft - duration + self._getBaseTime(), 2*self._getBaseTime()))
        else:
            self.score += 1
        newRating = self._newRating(success, rating)
        return success, newRating

    def doSkip(self, rating, duration):
        rating = int(rating)
        success = TrainingResult.SKIPPED
        if self.mode != TrainingMode.NORMAL:
            self.lives -= 1
            if self.lives is not None and self.lives == 0:
                self.alive = False

        self.timeleft = self._getBaseTime()
        newRating = self._newRating(success, rating)
        return success, newRating

    def doFinish(self, rating, duration):
        success = TrainingResult.INVALIDATED
        self.alive = False
        return success, rating

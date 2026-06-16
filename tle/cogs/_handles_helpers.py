import io
import html
import datetime
from collections import namedtuple

import cairo
import gi
gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')
from gi.repository import Pango, PangoCairo

import discord
from discord.ext import commands

from tle.util import codeforces_api as cf
from tle.util import discord_common
from tle.util import paginator
from tle.util import table

from PIL import Image, ImageDraw

_HANDLES_PER_PAGE = 15
_NAME_MAX_LEN = 20
_PAGINATE_WAIT_TIME = 5 * 60  # 5 minutes
_PRETTY_HANDLES_PER_PAGE = 10
_TOP_DELTAS_COUNT = 10
_MAX_RATING_CHANGES_PER_EMBED = 15
_UPDATE_HANDLE_STATUS_INTERVAL = 6 * 60 * 60  # 6 hours

_DIVISION_RATING_LOW = (2100, 1600, -1000)
_DIVISION_RATING_HIGH = (9999, 2099, 1599)
_LEADERBOARD_PER_PAGE = 10

_GudgitterRow = namedtuple('GudgitterRow', 'user_id handle rating score')


class HandleCogError(commands.CommandError):
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
    if rating is None or rating == 'N/A':
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


def get_gudgitters_image(rankings):
    """return PIL image for rankings"""
    SMOKE_WHITE = (250, 250, 250)
    BLACK = (0, 0, 0)

    DISCORD_GRAY = (.212, .244, .247)

    ROW_COLORS = ((0.95, 0.95, 0.95), (0.9, 0.9, 0.9))

    WIDTH = 900
    #HEIGHT = 900
    BORDER_MARGIN = 20
    COLUMN_MARGIN = 10
    HEADER_SPACING = 1.25
    WIDTH_RANK = 0.08*WIDTH
    WIDTH_NAME = 0.38*WIDTH
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
    draw_row('#', 'Name', 'Handle', 'Points', SMOKE_WHITE, y, bold=True)
    y += LINE_HEIGHT*HEADER_SPACING

    for i, (pos, name, handle, rating, score) in enumerate(rankings):
        color = rating_to_color(rating)
        draw_bg(y, i%2)
        draw_row(str(pos+1), f'{name}', f'{handle} ({rating if rating else "N/A"})' , str(score), color, y)
        if rating and rating >= 3000:  # nutella
            draw_row('', name[0], handle[0], '', BLACK, y)
        y += LINE_HEIGHT

    image_data = io.BytesIO()
    surface.write_to_png(image_data)
    image_data.seek(0)
    discord_file = discord.File(image_data, filename='gudgitters.png')
    return discord_file


def get_prettyhandles_image(rows, font):
    """return PIL image for rankings"""
    SMOKE_WHITE = (250, 250, 250)
    BLACK = (0, 0, 0)
    img = Image.new('RGB', (900, 450), color=SMOKE_WHITE)
    draw = ImageDraw.Draw(img)

    START_X, START_Y = 20, 20
    Y_INC = 32
    WIDTH_RANK = 64
    WIDTH_NAME = 340

    def draw_row(pos, username, handle, rating, color, y):
        x = START_X
        draw.text((x, y), pos, fill=color, font=font)
        x += WIDTH_RANK
        draw.text((x, y), username, fill=color, font=font)
        x += WIDTH_NAME
        draw.text((x, y), handle, fill=color, font=font)
        x += WIDTH_NAME
        draw.text((x, y), rating, fill=color, font=font)

    y = START_Y
    # draw header
    draw_row('#', 'Username', 'Handle', 'Rating', BLACK, y)
    y += int(Y_INC * 1.5)

    # trim name to fit in the column width
    def _trim(name):
        width = WIDTH_NAME - 10
        while font.getsize(name)[0] > width:
            name = name[:-4] + '...'  # "…" is printed as floating dots
        return name

    for pos, name, handle, rating in rows:
        name = _trim(name)
        handle = _trim(handle)
        color = rating_to_color(rating)
        draw_row(str(pos), name, handle, str(rating) if rating else 'N/A', color, y)
        if rating and rating >= 3000:  # nutella
            nutella_x = START_X + WIDTH_RANK
            draw.text((nutella_x, y), name[0], fill=BLACK, font=font)
            nutella_x += WIDTH_NAME
            draw.text((nutella_x, y), handle[0], fill=BLACK, font=font)
        y += Y_INC

    return img


def _make_profile_embed(member, user, *, mode):
    assert mode in ('set', 'get')
    if mode == 'set':
        desc = f'Handle for {member.mention} successfully set to **[{user.handle}]({user.url})**'
    else:
        desc = f'Handle for {member.mention} is currently set to **[{user.handle}]({user.url})**'
    if user.rating is None:
        embed = discord.Embed(description=desc)
        embed.add_field(name='Rating', value='Unrated', inline=True)
    else:
        embed = discord.Embed(description=desc, color=user.rank.color_embed)
        embed.add_field(name='Rating', value=user.rating, inline=True)
        embed.add_field(name='Rank', value=user.rank.title, inline=True)
    embed.set_thumbnail(url=f'{user.titlePhoto}')
    return embed


def _make_pages(users, title):
    chunks = paginator.chunkify(users, _HANDLES_PER_PAGE)
    pages = []
    done = 0

    style = table.Style('{:>}  {:<}  {:<}  {:<}')
    for chunk in chunks:
        t = table.Table(style)
        t += table.Header('#', 'Name', 'Handle', 'Rating')
        t += table.Line()
        for i, (member, handle, rating) in enumerate(chunk):
            name = member.display_name
            if len(name) > _NAME_MAX_LEN:
                name = name[:_NAME_MAX_LEN - 1] + '…'
            rank = cf.rating2rank(rating)
            rating_str = 'N/A' if rating is None else str(rating)
            t += table.Data(i + done, name, handle, f'{rating_str} ({rank.title_abbr})')
        table_str = '```\n'+str(t)+'\n```'
        embed = discord_common.cf_color_embed(description=table_str)
        pages.append((title, embed))
        done += len(chunk)
    return pages


def parse_date(arg):
    try:
        if len(arg) == 6:
            fmt = '%m%Y'
        # elif len(arg) == 4:
            # fmt = '%Y'
        else:
            raise ValueError
        return datetime.datetime.strptime(arg, fmt)
    except ValueError:
        raise HandleCogError(f'{arg} is an invalid date argument')


def _parse_gudgitter_args(args):
    division = None
    showall = False
    for arg in args:
        if arg[0:3] == 'div':
            try:
                division = int(arg[3])
                if division < 1 or division > 3:
                    raise HandleCogError('Division number must be within range [1-3]')
            except ValueError:
                raise HandleCogError(f'{arg} is an invalid div argument')
        if arg == '+all':
            showall = True
    return division, showall

import logging
from collections import namedtuple

from discord.ext import commands

from tle.util.db.user_db_conn import DuelType, Winner
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.elo import _ELO_CONSTANT

logger = logging.getLogger(__name__)

_DUEL_INVALIDATE_TIME = 5 * 60
_DUEL_EXPIRY_TIME = 5 * 60
_DUEL_RATING_DELTA = -400
_DUEL_OFFICIAL_CUTOFF = 3500
_DUEL_NO_DRAW_TIME = 5 * 60
_DUEL_MAX_RATIO = 3.0

_DUEL_STATUS_UNSOLVED = 0
_DUEL_STATUS_TESTING = -1
_DUEL_CHECK_ONGOING_INTERVAL = 60
_DUEL_MAX_DUEL_DURATION = 24 * 60 * 60

DuelRank = namedtuple(
    'Rank', 'low high title title_abbr color_graph color_embed')

DUEL_RANKS = (
    DuelRank(-10 ** 9, 1300, 'Newbie', 'N', '#CCCCCC', 0x808080),
    DuelRank(1300, 1400, 'Pupil', 'P', '#77FF77', 0x008000),
    DuelRank(1400, 1500, 'Specialist', 'S', '#77DDBB', 0x03a89e),
    DuelRank(1500, 1600, 'Expert', 'E', '#AAAAFF', 0x0000ff),
    DuelRank(1600, 1700, 'Candidate Master', 'CM', '#FF88FF', 0xaa00aa),
    DuelRank(1700, 1800, 'Master', 'M', '#FFCC88', 0xff8c00),
    DuelRank(1800, 1900, 'International Master', 'IM', '#FFBB55', 0xf57500),
    DuelRank(1900, 2000, 'Grandmaster', 'GM', '#FF7777', 0xff3030),
    DuelRank(2000, 2100, 'International Grandmaster',
             'IGM', '#FF3333', 0xff0000),
    DuelRank(2100, 10 ** 9, 'Legendary Grandmaster',
             'LGM', '#AA0000', 0xcc0000)
)


def rating2rank(rating):
    for rank in DUEL_RANKS:
        if rank.low <= rating < rank.high:
            return rank


def parse_nohandicap(args):
    for arg in args:
        if arg == "nohandicap":
            return True
    return False


class DuelCogError(commands.CommandError):
    pass


def elo_prob(player, opponent):
    return (1 + 10**((opponent - player) / 400))**-1


def elo_delta(player, opponent, win):
    return _ELO_CONSTANT * (win - elo_prob(player, opponent))


def get_cf_user(userid, guild_id):
    handle = cf_common.user_db.get_handle(userid, guild_id)
    return cf_common.user_db.fetch_cf_user(handle)


def complete_duel(duelid, guild_id, win_status, winner, loser, finish_time, score, dtype):
    winner_r = cf_common.user_db.get_duel_rating(winner.id, guild_id)
    loser_r = cf_common.user_db.get_duel_rating(loser.id, guild_id)
    delta = round(elo_delta(winner_r, loser_r, score))
    rc = cf_common.user_db.complete_duel(
        duelid, guild_id, win_status, finish_time, winner.id, loser.id, delta, dtype)
    if rc == 0:
        raise DuelCogError('Hey! No cheating!')

    if dtype == DuelType.UNOFFICIAL or dtype == DuelType.ADJUNOFFICIAL:
        return None

    winner_cf = get_cf_user(winner.id, guild_id)
    loser_cf = get_cf_user(loser.id, guild_id)
    desc = f'Rating change after **[{winner_cf.handle}]({winner_cf.url})** vs **[{loser_cf.handle}]({loser_cf.url})**:'
    embed = discord_common.cf_color_embed(description=desc)
    embed.add_field(name=f'{winner.display_name}',
                    value=f'{winner_r} -> {winner_r + delta}', inline=False)
    embed.add_field(name=f'{loser.display_name}',
                    value=f'{loser_r} -> {loser_r - delta}', inline=False)
    return embed


def _get_coefficient(problem_rating, lowerrated_rating, higherrated_rating):
    p_lowrated = 1 / (1 + 10**((problem_rating - lowerrated_rating) / 1000))
    p_highrated = 1 / (1 + 10**((problem_rating - higherrated_rating) / 1000))
    coeff = p_highrated / p_lowrated
    # cap values
    coeff = min(_DUEL_MAX_RATIO, max(1./_DUEL_MAX_RATIO, coeff))
    return coeff

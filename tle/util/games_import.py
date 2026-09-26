"""Short-lived, owner-bound import previews over the existing cog workflow.

Games are discovered from Minigames.GAMES / LinkedInDef; there is no second
catalog to maintain in the API or extension. Previewing never calls a DB writer.
"""
import datetime as dt
import logging
import secrets
import time
from types import SimpleNamespace

from tle.util.complaints import ComplaintError

logger = logging.getLogger(__name__)
PREVIEW_SECONDS = 600
MAX_PREVIEWS = 256


class GamesImportService:
    def __init__(self, bot, db_getter):
        self.bot = bot
        self._db_getter = db_getter
        self.pending = {}

    @property
    def db(self):
        return self._db_getter()

    @property
    def cog(self):
        cog = self.bot.get_cog('Minigames')
        if cog is None:
            raise ComplaintError(503, 'Games are unavailable.')
        return cog

    def catalog(self, guild, member):
        from tle.cogs._minigame_linkedin import linkedin_current_puzzle_date
        today = linkedin_current_puzzle_date()
        return {
            'guild_id': str(guild.id), 'guild_name': guild.name,
            'user_id': str(member.id), 'user_name': member.display_name,
            'games': [{
                'id': game.name, 'name': game.display_name,
                'path': game.name, 'today': today.isoformat(),
                'anchor_date': game.linkedin.anchor_date.isoformat(),
                'anchor_number': game.linkedin.anchor_number,
                'enabled': self.db.get_guild_config(guild.id, game.feature_flag) == '1',
                'can_import': self.cog._has_linkedin_mod_access(guild.id, game, member),
            } for game in self.cog._linkedin_games()],
        }

    def context(self, guild, member, game_name):
        if not isinstance(game_name, str):
            raise ComplaintError(400, 'game must be a game ID.')
        game = self.cog.GAMES.get(game_name)
        if game is None or not game.linkedin_identity:
            raise ComplaintError(400, 'Unsupported LinkedIn game.')
        if not self.cog._has_linkedin_mod_access(guild.id, game, member):
            raise ComplaintError(403, 'Moderator or delegated admin access required for this game.')
        if self.db.get_guild_config(guild.id, game.feature_flag) != '1':
            raise ComplaintError(403, f'{game.display_name} is not enabled in this server.')
        if self.db.is_minigame_banned(guild.id, game.name, member.id):
            raise ComplaintError(403, 'You are banned from this game.')
        channel_id = self.db.get_minigame_channel(guild.id, game.name)
        if channel_id is None:
            raise ComplaintError(409, f'Set the game channel with ;{game.name} here first.')
        return game, SimpleNamespace(
            guild=guild, author=member, channel=SimpleNamespace(id=int(channel_id)))

    def _make(self, ctx, game, date, content):
        from tle.cogs._minigame_helpers import MinigameCogError
        try:
            preview = self.cog._make_queens_import_preview(ctx, game, date, content)
        except MinigameCogError as exc:
            raise ComplaintError(400, str(exc)) from None
        # LinkedIn omits badges on the viewer's own result in some layouts.
        # Only the explicit You row receives the requested clean-result rule.
        from tle.cogs._minigame_linkedin import parse_linkedin_leaderboard
        if any(entry.is_you for entry in parse_linkedin_leaderboard(content)):
            preview = preview._replace(resolved=[
                entry._replace(no_hints=True, no_mistakes=True)
                if str(entry.user_id) == str(ctx.author.id) else entry
                for entry in preview.resolved])
        return preview

    def _rows(self, ctx, game, preview):
        from tle.cogs._minigame_queens_cog import _queens_public_link_name
        links = self.cog._queens_links_by_user(ctx.guild.id, game)
        optouts = self.db.get_minigame_optouts(ctx.guild.id, game.name)
        out_ids = {str(row.user_id) for row in optouts}
        out_names = {row.normalized_name for row in optouts}
        from tle.cogs._minigame_linkedin import normalize_linkedin_name
        sources = {}
        for number in game.linkedin.puzzle_numbers_for_date(preview.puzzle_date):
            for source in self.db.get_minigame_unresolved_results_for_puzzle(
                    ctx.guild.id, game.name, number):
                # Same canonical source precedence as materialization: an
                # explicit unrated result wins over an older duplicate.
                priority = (not bool(source.is_rated),
                            source.puzzle_number == preview.puzzle_number,
                            source.stored_at)
                old = sources.get(source.normalized_name)
                if old is None or priority > old[0]:
                    sources[source.normalized_name] = (priority, source)
        rows = []
        for entry in sorted((*preview.resolved, *preview.unresolved),
                            key=lambda row: row.time_seconds):
            link = links.get(str(entry.user_id))
            normalized = normalize_linkedin_name(entry.linkedin_name)
            source = sources.get(normalized)
            rated = (str(entry.user_id) not in out_ids if link else normalized not in out_names)
            if source is not None:
                rated = bool(source[1].is_rated)
            rows.append({
                'name': _queens_public_link_name(link) if link else entry.linkedin_name,
                'discord_name': self.cog._queens_public_user_name(
                    ctx.guild, entry.user_id, links) if link else None,
                'registered': entry.user_id is not None,
                'rated': rated,
                'rating_override': source[1].rating_override if source else None,
                'time_seconds': entry.time_seconds,
                'no_hints': entry.no_hints, 'no_mistakes': entry.no_mistakes,
            })
        return rows

    def preview(self, token, guild, member, body):
        from tle.cogs._minigame_linkedin import (
            linkedin_current_puzzle_date, normalize_linkedin_name,
            parse_linkedin_leaderboard,
        )
        game, ctx = self.context(guild, member, body['game'])
        content, date = body['leaderboard'], body['puzzle_date']
        if not isinstance(content, str) or not 1 <= len(content) <= 12000:
            raise ComplaintError(400, 'leaderboard must contain 1–12000 characters.')
        try:
            if not isinstance(date, str) or len(date) != 10:
                raise ValueError()
            parsed_date = dt.date.fromisoformat(date)
        except ValueError:
            raise ComplaintError(400, 'puzzle_date must be YYYY-MM-DD.') from None
        number = game.linkedin.number_for_date(parsed_date)
        if number < 1 or parsed_date > linkedin_current_puzzle_date():
            raise ComplaintError(400, 'Puzzle date is outside this game’s published history.')
        supplied_number = body['puzzle_number']
        if supplied_number is not None and (
                type(supplied_number) is not int or supplied_number != number):
            raise ComplaintError(400, 'Puzzle number and date do not match.')
        entries = parse_linkedin_leaderboard(content)
        if not 1 <= len(entries) <= 200 or sum(e.is_you for e in entries) > 1:
            raise ComplaintError(400, 'Expected 1–200 rows and at most one You row.')
        names = [normalize_linkedin_name(e.linkedin_name) for e in entries]
        if len(set(names)) != len(names):
            raise ComplaintError(400, 'Duplicate player rows; open one leaderboard and try again.')
        preview = self._make(ctx, game, date, content)
        rows = self._rows(ctx, game, preview)
        self._prune()
        # One current preview per credential; repeated clicks cannot exhaust memory.
        for key, value in list(self.pending.items()):
            if value['owner'] == token.id and value['result'] is None:
                del self.pending[key]
        if len(self.pending) >= MAX_PREVIEWS:
            raise ComplaintError(503, 'Too many import previews; try again later.')
        preview_id = secrets.token_urlsafe(24)
        expires_at = time.time() + PREVIEW_SECONDS
        self.pending[preview_id] = {
            'owner': token.id, 'guild_id': str(guild.id), 'user_id': str(member.id),
            'expires_at': expires_at, 'game': game.name, 'preview': preview,
            'rows': rows, 'channel_id': ctx.channel.id, 'result': None,
        }
        return {
            'preview_id': preview_id, 'expires_at': expires_at,
            'game': game.name, 'game_name': game.display_name,
            'puzzle_date': date, 'puzzle_number': number, 'rows': rows,
            'skipped': len(entries) - len(rows),
            'registered': len(preview.resolved), 'unresolved': len(preview.unresolved),
        }

    def _prune(self):
        now = time.time()
        self.pending = {key: value for key, value in self.pending.items()
                        if value['expires_at'] > now}

    def confirm(self, token, guild, member, preview_id):
        self._prune()
        saved = self.pending.get(preview_id)
        if saved is None or (saved['owner'], saved['guild_id'], saved['user_id']) != (
                token.id, str(guild.id), str(member.id)):
            raise ComplaintError(404, 'Preview expired or unavailable. Read the leaderboard again.')
        game, ctx = self.context(guild, member, saved['game'])
        if saved['result'] is not None:
            return saved['result']
        original = saved['preview']
        current = self._make(ctx, game, original.puzzle_date.isoformat(), original.raw_content)
        # Never apply changed identity/ban/privacy decisions behind a stale preview.
        if (current != original or self._rows(ctx, game, current) != saved['rows']
                or ctx.channel.id != saved['channel_id']):
            del self.pending[preview_id]
            raise ComplaintError(409, 'Player links or settings changed. Read the leaderboard again.')
        # There are no awaits between revalidation, saving, and receipt recording;
        # competing confirms on the shared bot event loop cannot interleave.
        result = self.cog._save_queens_import(ctx, game, current)
        saved['result'] = {
            'game': game.name, 'puzzle_date': original.puzzle_date.isoformat(),
            'puzzle_number': original.puzzle_number,
            'registered': result.resolved, 'unresolved': result.unresolved,
            'unchanged': len(current.resolved) + len(current.unresolved)
                         - result.resolved - result.unresolved,
        }
        saved['expires_at'] = time.time() + PREVIEW_SECONDS
        logger.info('Games import guild=%s user=%s token=%s game=%s date=%s rows=%s',
                    guild.id, member.id, token.id, game.name, original.puzzle_date,
                    result.resolved + result.unresolved)
        return saved['result']

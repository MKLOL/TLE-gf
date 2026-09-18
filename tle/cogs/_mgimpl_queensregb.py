"""LinkedIn-game unregister and legacy/materialized result migration. (Minigames cog impl mixin; see minigames.py)."""

import logging
import time
from types import SimpleNamespace


from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_common import (
    normalize_puzzle_date,
)
from tle.cogs._minigame_queens import (
    normalize_queens_name, parse_queens_leaderboard,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError,
)
from tle.cogs._minigame_queens_cog import (
    _queens_puzzle_date_text, _linkedin_result_message_id,
    _format_queens_date,
)

logger = logging.getLogger(__name__)


class ImplQueensRegBMixin:
    async def _cmd_queens_unregister(self, ctx, game, member):
        self._require_enabled(ctx.guild.id, game)
        target = self._resolve_queens_registrar_target(ctx, game, member)
        target_label = self._queens_public_user_name(ctx.guild, target.id)
        rating_opted_out = cf_common.user_db.is_minigame_opted_out(
            ctx.guild.id, game.name, target.id)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, game.link_key, target.id)
        if link is None:
            raise MinigameCogError(
                f'`{target_label}` is not registered for '
                f'{self._linkedin_games_label()}.')
        games = self._linkedin_games()
        for linked_game in games:
            self._migrate_legacy_queens_results_to_external(
                ctx.guild.id, linked_game)
            self._delete_queens_materialized_results_for_link(
                ctx.guild.id, linked_game, link)
        removed = cf_common.user_db.delete_minigame_player_link(
            ctx.guild.id, game.link_key, target.id)
        if not removed:
            raise MinigameCogError(
                f'Could not remove `{target_label}` from '
                f'{self._linkedin_games_label()}.')
        for linked_game in games:
            self._sync_queens_materialized_results(
                ctx.guild.id, linked_game, migrate_legacy=False)
            self._recompute_minigame_ratings(
                ctx.guild.id, linked_game, sync_results=False)
        optout_note = (
            f'Their existing {game.display_name} rating opt-out remains '
            'active.'
            if rating_opted_out
            else f'`;{game.name} unregister` does not create a rating '
                 'opt-out.'
        )
        await ctx.send(embed=discord_common.embed_success('\n'.join([
            f'Removed the {self._linkedin_games_label()} link for '
            f'`{target_label}`.',
            f'Stored results were kept under the LinkedIn name. {optout_note}',
        ])))

    def _queens_external_result_values(
            self, guild_id, game, channel_id, entry, puzzle_date, raw_content,
            *, is_rated=None, stored_at=None, source_message_id=None,
            rating_override=None):
        puzzle_date = normalize_puzzle_date(puzzle_date)
        normalized_name = normalize_queens_name(entry.linkedin_name)
        if is_rated is None:
            link = cf_common.user_db.get_minigame_player_link_by_name(
                guild_id, game.link_key, normalized_name)
            optout = (
                cf_common.user_db.get_minigame_optout(
                    guild_id, game.name, link.user_id)
                if link is not None
                else cf_common.user_db.get_minigame_optout_by_name(
                    guild_id, game.name, normalized_name)
            )
            is_rated = optout is None
        return (
            normalized_name,
            entry.linkedin_name,
            channel_id,
            game.linkedin.number_for_date(puzzle_date),
            _queens_puzzle_date_text(puzzle_date),
            100 if entry.no_mistakes else 0,
            entry.time_seconds,
            entry.no_hints and entry.no_mistakes,
            raw_content,
            is_rated,
            time.time() if stored_at is None else stored_at,
            source_message_id,
            rating_override,
        )

    def _save_queens_external_result(
            self, guild_id, game, channel_id, entry, puzzle_date, raw_content,
            *, is_rated=None, stored_at=None, source_message_id=None,
            rating_override=None):
        values = self._queens_external_result_values(
            guild_id, game, channel_id, entry, puzzle_date, raw_content,
            is_rated=is_rated, stored_at=stored_at,
            source_message_id=source_message_id,
            rating_override=rating_override)
        cf_common.user_db.apply_minigame_source_migration(
            guild_id, game.name, [values], [],
        )

    @staticmethod
    def _legacy_queens_entry_matches_row(entry, row):
        return (
            int(entry.time_seconds) == int(row.time_seconds)
            and (100 if entry.no_mistakes else 0) == int(row.accuracy)
            and int(entry.no_hints and entry.no_mistakes) == int(row.is_perfect)
        )

    def _legacy_queens_raw_source_identity(self, row):
        candidates = {}
        for entry in parse_queens_leaderboard(row.raw_content or ''):
            normalized = normalize_queens_name(entry.linkedin_name)
            if normalized == 'you':
                continue
            if self._legacy_queens_entry_matches_row(entry, row):
                candidates[normalized] = entry.linkedin_name
        if len(candidates) != 1:
            return None
        return next(iter(candidates.items()))

    def _legacy_queens_source_identity(self, row, link):
        raw_identity = self._legacy_queens_raw_source_identity(row)
        if raw_identity is not None:
            return raw_identity
        if link is not None:
            return link.normalized_name, link.external_name
        return None

    @staticmethod
    def _queens_source_row_key(game, normalized_name, row):
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        return (
            normalized_name,
            game.linkedin.number_for_date(puzzle_date),
            int(row.accuracy),
            int(row.time_seconds),
            int(row.is_perfect),
        )

    @staticmethod
    def _queens_source_identity_key(game, normalized_name, puzzle_date):
        puzzle_date = normalize_puzzle_date(puzzle_date)
        return (
            normalized_name,
            game.linkedin.number_for_date(puzzle_date),
        )

    def _queens_source_row_keys(self, guild_id, game, rows=None):
        if rows is None:
            rows = cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, game.name)
        return {
            self._queens_source_row_key(game, row.normalized_name, row)
            for row in rows
        }

    def _queens_source_identity_keys(self, guild_id, game, rows=None):
        if rows is None:
            rows = cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, game.name)
        return {
            self._queens_source_identity_key(
                game, row.normalized_name, row.puzzle_date)
            for row in rows
        }

    def _is_current_queens_projection_row(
            self, guild_id, game, row, link, sources_by_identity):
        if link is None:
            return False
        puzzle_date = normalize_puzzle_date(row.puzzle_date)
        identity_key = self._queens_source_identity_key(
            game, link.normalized_name, puzzle_date)
        source = sources_by_identity.get(identity_key)
        if source is None:
            return False
        expected_message_id = (
            source.source_message_id
            or _linkedin_result_message_id(
                game, guild_id, puzzle_date, link.user_id)
        )
        if str(row.message_id) != str(expected_message_id):
            return False
        return (
            self._queens_source_row_key(game, link.normalized_name, row)
            == self._queens_source_row_key(
                game, source.normalized_name, source)
        )

    def _delete_queens_materialized_results_for_link(
            self, guild_id, game, link):
        deleted = 0
        for row in cf_common.user_db.get_minigame_unresolved_results_for_name(
                guild_id, game.name, link.normalized_name):
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            for puzzle_number in game.linkedin.puzzle_numbers_for_date(
                    puzzle_date):
                deleted += cf_common.user_db.delete_minigame_result_for_user_puzzle(
                    guild_id, game.name, link.user_id, puzzle_number)
        return deleted

    @staticmethod
    def _same_queens_materialized_result(existing, source, link,
                                         puzzle_number, puzzle_date):
        if existing is None:
            return False
        return (
            str(existing.channel_id) == str(source.channel_id)
            and str(existing.user_id) == str(link.user_id)
            and int(existing.puzzle_number) == int(puzzle_number)
            and _format_queens_date(existing) == _queens_puzzle_date_text(puzzle_date)
            and int(existing.accuracy) == int(source.accuracy)
            and int(existing.time_seconds) == int(source.time_seconds)
            and int(existing.is_perfect) == int(source.is_perfect)
            and str(existing.raw_content) == str(source.raw_content)
        )

    def _sync_queens_materialized_results(self, guild_id, game, *,
                                          migrate_legacy=True):
        if migrate_legacy:
            self._migrate_legacy_queens_results_to_external(guild_id, game)
        links_by_name = {
            row.normalized_name: row
            for row in cf_common.user_db.get_minigame_player_links(
                guild_id, game.link_key)
        }
        if not links_by_name:
            return 0
        existing_rows = {
            (str(row.user_id), int(row.puzzle_number)): row
            for row in cf_common.user_db.get_live_minigame_results_for_guild(
                guild_id, game.name)
        }
        calendar = game.linkedin
        canonical_sources = {}
        for row in (
                cf_common.user_db
                .get_minigame_unresolved_results_for_guild(
                    guild_id, game.name)):
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            puzzle_number = calendar.number_for_date(puzzle_date)
            key = (row.normalized_name, puzzle_number)
            current = canonical_sources.get(key)
            priority = (
                int(not bool(row.is_rated)),
                int(int(row.puzzle_number) == puzzle_number),
                float(row.stored_at),
            )
            if current is None or priority > current[0]:
                canonical_sources[key] = (priority, row)

        pending = []
        for _priority, row in canonical_sources.values():
            link = links_by_name.get(row.normalized_name)
            if link is None:
                continue
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            message_id = (
                row.source_message_id
                or _linkedin_result_message_id(
                    game, guild_id, puzzle_date, link.user_id)
            )
            puzzle_number = calendar.number_for_date(puzzle_date)
            result_key = (str(link.user_id), int(puzzle_number))
            existing = existing_rows.get(result_key)
            if not bool(row.is_rated):
                if existing is not None:
                    cf_common.user_db.delete_minigame_result_for_user_puzzle(
                        guild_id, game.name, link.user_id,
                        puzzle_number)
                    existing_rows.pop(result_key, None)
                continue
            if (
                    existing is not None
                    and str(existing.message_id) == str(message_id)
                    and self._same_queens_materialized_result(
                        existing, row, link, puzzle_number, puzzle_date)):
                continue
            if existing is not None and str(existing.message_id) != str(message_id):
                cf_common.user_db.delete_minigame_result_for_user_puzzle(
                    guild_id, game.name, link.user_id,
                    puzzle_number)
            pending.append((
                message_id,
                guild_id,
                game.name,
                row.channel_id,
                link.user_id,
                puzzle_number,
                _queens_puzzle_date_text(puzzle_date),
                row.accuracy,
                row.time_seconds,
                row.is_perfect,
                row.raw_content,
            ))
            existing_rows[result_key] = SimpleNamespace(
                message_id=message_id,
                channel_id=row.channel_id,
                user_id=link.user_id,
                puzzle_number=puzzle_number,
                puzzle_date=_queens_puzzle_date_text(puzzle_date),
                accuracy=row.accuracy,
                time_seconds=row.time_seconds,
                is_perfect=row.is_perfect,
                raw_content=row.raw_content,
            )
        if not pending:
            return 0
        return cf_common.user_db.save_minigame_results(pending)

    def _claim_queens_unresolved_results(self, guild_id, game, user_id,
                                         normalized_name):
        name_optout = cf_common.user_db.get_minigame_optout_by_name(
            guild_id, game.name, normalized_name)
        if (
                name_optout is not None
                and str(name_optout.user_id) != str(user_id)
        ):
            # The LinkedIn identity has transferred to a different Discord
            # account. Keep already-stored per-result states, but stop the
            # former owner's opt-out from classifying future unlinked rows.
            cf_common.user_db.set_minigame_optout_identity(
                guild_id, game.name, name_optout.user_id, None)
        optout = cf_common.user_db.get_minigame_optout(
            guild_id, game.name, user_id)
        if optout is not None:
            identity_changed = (
                optout.normalized_name != normalized_name)
            cf_common.user_db.set_minigame_optout_identity(
                guild_id, game.name, user_id, normalized_name)
            if identity_changed:
                cf_common.user_db.mark_minigame_unresolved_results_unrated_since(
                    guild_id, game.name, normalized_name,
                    optout.opted_out_at)
        rows = cf_common.user_db.get_minigame_unresolved_results_for_name(
            guild_id, game.name, normalized_name)
        self._sync_queens_materialized_results(
            guild_id, game, migrate_legacy=False)
        return len(rows)

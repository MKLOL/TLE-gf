"""Canonicalize generic LinkedIn-game rows under their LinkedIn identities."""

import time
from types import SimpleNamespace

from tle.util import codeforces_common as cf_common

from tle.cogs._minigame_common import normalize_puzzle_date
from tle.cogs._minigame_queens_cog import (
    _queens_puzzle_date_text,
)


class ImplQueensSourcesMixin:
    def _migrate_legacy_queens_results_to_external(
            self, guild_id, game, *, delete_migrated=True):
        calendar = game.linkedin
        links_by_user = self._queens_links_by_user(guild_id, game)
        existing_source_rows = (
            cf_common.user_db.get_minigame_unresolved_results_for_guild(
                guild_id, game.name)
        )
        sources_by_identity = {
            self._queens_source_identity_key(
                game, row.normalized_name, row.puzzle_date): row
            for row in existing_source_rows
        }
        sources_by_message = {
            str(row.source_message_id): row
            for row in existing_source_rows
            if row.source_message_id is not None
        }
        opted_out_ids = self._minigame_opted_out_user_ids(guild_id, game)
        planned_sources = []
        planned_deletions = []
        source_message_ids_to_replace = set()
        created_sources = 0
        handled_identities = set()
        rows = cf_common.user_db.get_stored_minigame_results_for_guild(
            guild_id, game.name)

        def migration_order(row):
            try:
                message_id = int(row.message_id)
            except (TypeError, ValueError):
                message_id = 0
            storage_order = 0 if row.storage == 'imported' else 1
            return -message_id, storage_order

        for row in sorted(rows, key=migration_order):
            link = links_by_user.get(str(row.user_id))
            if (
                    row.storage == 'live'
                    and self._is_current_queens_projection_row(
                        guild_id, game, row, link, sources_by_identity)
            ):
                continue
            message_source = sources_by_message.get(str(row.message_id))
            identity = (
                (
                    message_source.normalized_name,
                    message_source.external_name,
                )
                if message_source is not None
                else self._legacy_queens_source_identity(row, link)
            )
            if identity is None:
                continue
            normalized_name, external_name = identity
            puzzle_date = normalize_puzzle_date(row.puzzle_date)
            identity_key = self._queens_source_identity_key(
                game, normalized_name, puzzle_date)
            existing_source = sources_by_identity.get(identity_key)
            adopts_missing_provenance = (
                existing_source is not None
                and existing_source.source_message_id is None
                and self._queens_source_row_key(
                    game, normalized_name, row)
                == self._queens_source_row_key(
                    game, existing_source.normalized_name, existing_source)
            )
            same_message_source = (
                existing_source is not None
                and (
                    (
                        existing_source.source_message_id is not None
                        and str(existing_source.source_message_id)
                        == str(row.message_id)
                    )
                    or adopts_missing_provenance
                )
            )
            message_source_moved = (
                message_source is not None
                and self._queens_source_identity_key(
                    game,
                    message_source.normalized_name,
                    message_source.puzzle_date,
                ) != identity_key
            )
            if message_source_moved:
                source_message_ids_to_replace.add(str(row.message_id))
            is_rated = (
                bool(message_source.is_rated)
                if message_source is not None
                else str(row.user_id) not in opted_out_ids
            )
            if (identity_key in handled_identities
                    or (existing_source is not None
                        and not same_message_source)):
                if delete_migrated:
                    planned_deletions.append((
                        row.storage, row.message_id, row.puzzle_number))
                continue
            source_message_id = row.message_id
            stored_at = (
                message_source.stored_at
                if message_source is not None
                else time.time()
            )
            rating_override = (
                message_source.rating_override
                if message_source is not None
                else None
            )
            planned_sources.append((
                normalized_name,
                external_name,
                row.channel_id,
                calendar.number_for_date(puzzle_date),
                _queens_puzzle_date_text(puzzle_date),
                row.accuracy,
                row.time_seconds,
                row.is_perfect,
                row.raw_content,
                is_rated,
                stored_at,
                source_message_id,
                rating_override,
            ))
            created_sources += int(existing_source is None)
            keep_live_projection = (
                row.storage == 'live'
                and link is not None
                and normalized_name == link.normalized_name
            )
            if delete_migrated and not keep_live_projection:
                planned_deletions.append((
                    row.storage, row.message_id, row.puzzle_number))
            handled_identities.add(identity_key)
            source = SimpleNamespace(
                external_name=external_name,
                normalized_name=normalized_name,
                puzzle_number=calendar.number_for_date(puzzle_date),
                puzzle_date=_queens_puzzle_date_text(puzzle_date),
                accuracy=row.accuracy,
                time_seconds=row.time_seconds,
                is_perfect=row.is_perfect,
                source_message_id=source_message_id,
                is_rated=is_rated,
                stored_at=stored_at,
                rating_override=rating_override,
            )
            sources_by_identity[identity_key] = source
            sources_by_message[str(source_message_id)] = source
        cf_common.user_db.apply_minigame_source_migration(
            guild_id, game.name, planned_sources, planned_deletions,
            source_message_ids_to_replace)
        return created_sources

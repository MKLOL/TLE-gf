"""Finalization (import into starboard tables) for the migration cog.

Mixin method used by ``tle.cogs.migrate.Migrate``. Splits the heavy
``;migrate complete`` import logic out of the cog body.
"""
import asyncio
import logging

from tle import constants

logger = logging.getLogger(__name__)


class MigrateCompleteMixin:
    """Import posted entries into the live starboard tables."""

    async def _complete_import(self, guild_id, db, posted_entries, emojis,
                               alias_map, new_channel_id):
        """Copy posted entries into starboard tables and create configs.

        Returns the number of imported entries.

        Uses raw SQL in a single transaction to avoid per-row commit() calls —
        3000+ individual commits block the event loop and kill the gateway.
        """
        main_emojis = set(emojis) - set(alias_map.keys()) if alias_map else set(emojis)

        conn = db.conn
        seen_msgs = set()
        imported = 0
        for i, entry in enumerate(posted_entries):
            resolved_emoji = alias_map.get(entry.emoji, entry.emoji) if alias_map else entry.emoji

            # Skip duplicate entries from merged aliases (same original_msg_id)
            dedup_key = (entry.original_msg_id, resolved_emoji)
            if dedup_key in seen_msgs:
                continue
            seen_msgs.add(dedup_key)

            # Compute merged star count if aliases exist
            star_count = entry.star_count or 0
            if alias_map:
                all_family = [resolved_emoji] + [k for k, v in alias_map.items()
                                                  if v == resolved_emoji]
                merged_count = db.get_merged_reactor_count(entry.original_msg_id, all_family)
                if merged_count > 0:
                    star_count = merged_count

            conn.execute(
                'INSERT OR IGNORE INTO starboard_message_v1 '
                '(original_msg_id, starboard_msg_id, guild_id, emoji, author_id, channel_id) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (str(entry.original_msg_id), str(entry.new_starboard_msg_id),
                 str(guild_id), resolved_emoji,
                 str(entry.author_id) if entry.author_id else None,
                 str(entry.source_channel_id) if entry.source_channel_id else None)
            )
            if star_count > 0:
                conn.execute(
                    'UPDATE starboard_message_v1 SET star_count = ? '
                    'WHERE original_msg_id = ? AND emoji = ?',
                    (star_count, str(entry.original_msg_id), resolved_emoji)
                )
            imported += 1

            # Yield to event loop periodically so Discord heartbeats aren't blocked
            if i % 500 == 499:
                conn.commit()
                logger.info(f'Migration complete: guild={guild_id} imported {imported} so far')
                await asyncio.sleep(0)

        # Create emoji configs for main emojis only
        for emoji in main_emojis:
            conn.execute(
                'INSERT INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) '
                'VALUES (?, ?, ?, ?) '
                'ON CONFLICT(guild_id, emoji) DO UPDATE SET threshold = excluded.threshold, '
                'color = excluded.color',
                (str(guild_id), emoji, 1, constants._DEFAULT_STAR_COLOR)
            )
            conn.execute(
                'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND emoji = ?',
                (str(new_channel_id), str(guild_id), emoji)
            )

        # Register aliases
        for alias_emoji, main_emoji in alias_map.items():
            conn.execute(
                'INSERT OR REPLACE INTO starboard_alias (guild_id, alias_emoji, main_emoji) '
                'VALUES (?, ?, ?)',
                (str(guild_id), alias_emoji, main_emoji)
            )

        # The raw-SQL import above bypasses the sticky-narcissus hook, so
        # seed marks for authors whose live self-react the crawl collected.
        # After alias registration so alias self-reacts resolve to mains.
        db.seed_narcissus_marks(guild_id)

        # Clean up migration data
        conn.execute(
            'DELETE FROM starboard_migration_entry WHERE guild_id = ?',
            (str(guild_id),)
        )
        conn.execute(
            'DELETE FROM starboard_migration WHERE guild_id = ?',
            (str(guild_id),)
        )

        conn.commit()
        return imported

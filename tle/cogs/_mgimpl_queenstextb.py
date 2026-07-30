"""Queens JSON backfill command body."""

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import (
    QUEENS_GAME,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _safe_member_name,
)
from tle.cogs._minigame_queens_cog import (
    _queens_public_link_name,
)


class ImplQueensTextBMixin:
    async def _cmd_queens_backfill(self, ctx, target):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        if target is None:
            raise MinigameCogError(
                'Usage: `;queens backfill @user|+all` '
                '(attach `queens_history.json`).')
        data = await self._read_queens_backfill_entries(ctx)
        self._migrate_legacy_queens_results_to_external(
            ctx.guild.id, delete_migrated=False)

        if target.strip().casefold() == '+all':
            result = self._save_queens_backfill_all(ctx, data)
            if not result['valid']:
                raise MinigameCogError(
                    'No valid LinkedIn Queens result entries found in the JSON.')
            saved = result['saved']
            skipped = result['skipped']
            malformed = result['malformed']
            if saved:
                self._sync_queens_materialized_results(
                    ctx.guild.id, migrate_legacy=False)
                self._recompute_minigame_ratings(
                    ctx.guild.id, QUEENS_GAME, sync_results=False)
            lines = [
                f'Backfilled **{saved}** LinkedIn-name result(s).',
                f'- Parsed **{result["valid"]}** valid JSON result(s).',
                f'- Saw **{len(result["registered_names"])}** registered '
                f'LinkedIn name(s) and **{len(result["unresolved_names"])}** '
                'unregistered LinkedIn name(s).',
            ]
            if skipped:
                lines.append(
                    f'- Skipped **{skipped}** already-saved result(s).')
            if malformed:
                lines.append(
                    f'- Ignored **{malformed}** malformed entry/entries.')
            await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))
            return

        member = await self._resolve_member(ctx, target)
        # User must already be registered so we know their LinkedIn name
        # for the match.
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        if link is None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` is not registered for '
                f'{QUEENS_GAME.display_name}. They need to '
                '`;queens register Their LinkedIn Name` first.')

        result = self._save_queens_backfill_for_link(ctx, link, data)
        if not result.matched:
            raise MinigameCogError(
                f'No entries in the JSON match '
                f'`{_safe_member_name(member)}`\'s registered LinkedIn '
                'account.')
        saved = result.saved
        skipped = result.skipped
        malformed = result.malformed
        if saved:
            self._sync_queens_materialized_results(
                ctx.guild.id, migrate_legacy=False)
            self._recompute_minigame_ratings(
                ctx.guild.id, QUEENS_GAME, sync_results=False)

        lines = [
            f'Backfilled **{saved}** result(s) for '
            f'`{_safe_member_name(member)}` '
            f'(LinkedIn: `{_queens_public_link_name(link)}`).',
        ]
        if skipped:
            lines.append(
                f'- Skipped **{skipped}** already-saved result(s).')
        if malformed:
            lines.append(
                f'- Ignored **{malformed}** malformed entry/entries.')
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

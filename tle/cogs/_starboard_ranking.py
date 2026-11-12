"""Leaderboard rendering shared by the starboard core."""
import discord

from tle.util import discord_common, paginator, ranking


class RankingMixin:
    # --- Leaderboard page builders ---

    @staticmethod
    def _get_user_id(row):
        """Extract user ID from a leaderboard row (author_id or user_id)."""
        return getattr(row, 'author_id', None) or row.user_id

    @staticmethod
    def _get_count(row):
        """Extract count from a leaderboard row."""
        for attr in ('message_count', 'total_stars', 'stars_given', 'self_stars'):
            val = getattr(row, attr, None)
            if val is not None:
                return val
        return 0

    @staticmethod
    def _row_suffix(row):
        """Suffix for rows carrying an `unreacted` column (narcissus): how
        many sticky self-stars had their reaction sneakily removed."""
        unreacted = getattr(row, 'unreacted', None)
        return f' ({unreacted} unreacted)' if unreacted else ''

    def _get_personal_rank_line(self, ctx, ranked, unit):
        """Get the invoking user's rank as a string for embedding. `ranked`
        is the output of `ranking.rank_items` — `(rank, row)` pairs in display
        order, so tied users share a rank."""
        user_id_str = str(ctx.author.id)
        for rank, row in ranked:
            if self._get_user_id(row) == user_id_str:
                count = self._get_count(row)
                return (f'\nYour rank: **#{rank}** with **{count}** {unit}'
                        f'{self._row_suffix(row)}')
        return '\nYou are not on this leaderboard yet.'

    def _make_leaderboard_pages(self, ctx, rows, emoji, title, unit):
        """Build paginated embed pages from leaderboard rows."""
        # Rank the whole list once with standard competition ranking so tied
        # counts share a rank rather than being split by the query's secondary
        # sort; ties also number correctly across page boundaries.
        ranked = ranking.rank_items(rows, self._get_count)
        personal = self._get_personal_rank_line(ctx, ranked, unit)
        per_page = 10
        chunks = paginator.chunkify(ranked, per_page)
        pages = []
        for chunk in chunks:
            lines = []
            for rank, row in chunk:
                user_id = self._get_user_id(row)
                count = self._get_count(row)
                member = ctx.guild.get_member(int(user_id))
                name = member.mention if member else f'<@{user_id}>'
                lines.append(f'**#{rank}** {name} — {count} {unit}{self._row_suffix(row)}')
            lines.append(personal)
            embed = discord.Embed(
                title=f'{emoji} {title}',
                description='\n'.join(lines),
                color=discord_common.random_cf_color()
            )
            pages.append((None, embed))
        return pages

    def _paginate_leaderboard(self, ctx, rows, emoji, title, unit):
        """Build leaderboard pages and start the paginator."""
        pages = self._make_leaderboard_pages(ctx, rows, emoji, title, unit)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300,
                           set_pagenum_footers=True, author_id=ctx.author.id)

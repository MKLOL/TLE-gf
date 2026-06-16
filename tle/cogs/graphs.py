import discord
import pandas as pd
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs import _graph_render as _render
from tle.cogs import _graph_render2 as _render2
from tle.cogs import _graph_render3 as _render3

# Re-export helpers so existing imports such as
# `from tle.cogs.graphs import _rating_at_percentile` keep working, and so that
# patch targets like `tle.cogs.graphs.cf_common` remain valid.
from tle.cogs._graph_helpers import (  # noqa: F401
    GraphCogError,
    _rating_at_percentile,
    nice_sub_type,
    _plot_rating,
    _plot_rating_by_date,
    _plot_rating_by_contest,
    _classify_submissions,
    _plot_scatter,
    _running_mean,
    _get_extremes,
    _plot_extreme,
    _plot_average,
)
from tle.cogs._graph_perftable import (  # noqa: F401
    _CONTEST_NAME_MAX,
    _truncate_name,
    _build_rated_rows,
    _build_vc_rows,
    _estimate_perf_from_cache,
    _build_cfvc_rows,
    _format_cfvc_table,
    _format_perftable,
)
from tle.cogs._graph_render2 import CONTEST_ACTIVE_TIME_CUTOFF  # noqa: F401

pd.plotting.register_matplotlib_converters()


class Graphs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()

    @commands.group(brief='Graphs for analyzing Codeforces activity',
                    invoke_without_command=True)
    async def plot(self, ctx):
        """Plot various graphs. Wherever Codeforces handles are accepted it is possible to
        use a server member's Discord name or @mention directly.
        Prefix -c to force a Codeforces handle (e.g. -ctourist)."""
        await ctx.send_help('plot')

    @plot.command(brief='Plot Codeforces rating graph', usage='[+zoom] [+number] [+peak] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def rating(self, ctx, *args: str):
        """Plots Codeforces rating graph for the handles provided."""
        await _render.rating(self, ctx, *args)

    @plot.command(brief='Plot Codeforces performance graph', aliases=['perf'], usage='[+zoom] [+peak] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def performance(self, ctx, *args: str):
        """Plots Codeforces performance graph for the handles provided."""
        await _render.performance(self, ctx, *args)

    @plot.command(brief='Plot Codeforces extremes graph',
                  usage='[handles] [+solved] [+unsolved] [+nolegend] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def extreme(self, ctx, *args: str):
        """Plots pairs of lowest rated unsolved problem and highest rated solved problem for every
        contest that was rated for the given user.
        """
        await _render.extreme(self, ctx, *args)

    @plot.command(brief="Show histogram of solved problems' rating on CF",
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [c+marker..] [i+index..]')
    async def solved(self, ctx, *args: str):
        """Shows a histogram of solved problems' rating on Codeforces for the handles provided.
        e.g. ;plot solved meooow +contest +virtual +outof +dp"""
        await _render.solved(self, ctx, *args)

    @plot.command(brief='Show histogram of solved problems on CF over time',
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [phase_days=] [c+marker..] [i+index..]')
    async def hist(self, ctx, *args: str):
        """Shows the histogram of problems solved on Codeforces over time for the handles provided"""
        await _render.hist(self, ctx, *args)

    @plot.command(brief='Plot count of solved CF problems over time',
                  usage='[handles] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [c+marker..] [i+index..]')
    async def curve(self, ctx, *args: str):
        """Plots the count of problems solved over time on Codeforces for the handles provided."""
        await _render.curve(self, ctx, *args)

    @plot.command(brief='Show history of problems solved by rating',
                  aliases=['chilli'], usage='[handle] [+practice] [+contest] [+virtual] [+outof] [+team] [+tag..] [~tag..] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [b=10] [s=3] [c+marker..] [i+index..] [+nolegend]')
    async def scatter(self, ctx, *args):
        """Plot Codeforces rating overlaid on a scatter plot of problems solved.
        Also plots a running average of ratings of problems solved in practice."""
        await _render.scatter(self, ctx, *args)

    @plot.command(brief='Show server rating distribution')
    async def distrib(self, ctx):
        """Plots rating distribution of users in this server"""
        await _render2.distrib(self, ctx)

    @plot.command(brief='Show Codeforces rating distribution', usage='[normal/log] [active/all] [contest_cutoff=5]')
    async def cfdistrib(self, ctx, mode: str = 'log', activity='active', contest_cutoff: int = 5):
        """Plots rating distribution of either active or all users on Codeforces, in either normal or log scale.
        Default mode is log, default activity is active (competed in last 90 days)
        Default contest cutoff is 5 (competed at least five times overall)
        """
        await _render2.cfdistrib(self, ctx, mode, activity, contest_cutoff)

    @plot.command(brief='Show percentile distribution on codeforces', usage='[+zoom] [+nomarker] [handles...] [+exact]')
    async def centile(self, ctx, *args: str):
        """Show percentile distribution of codeforces and mark given handles in the plot. If +zoom and handles are given, it zooms to the neighborhood of the handles."""
        await _render2.centile(self, ctx, *args)

    @plot.command(brief='Plot histogram of gudgiting')
    async def howgud(self, ctx, *members: discord.Member):
        await _render2.howgud(self, ctx, *members)

    @plot.command(brief='Plot distribution of server members by country')
    async def country(self, ctx, *countries):
        """Plots distribution of server members by countries. When no countries are specified, plots
         a bar graph of all members by country. When one or more countries are specified, plots a
         swarmplot of members by country and rating. Only members with registered handles and
         countries set on Codeforces are considered.
         """
        await _render3.country(self, ctx, *countries)

    @plot.command(brief='Show rating changes by rank', usage='contest_id [+server] [+zoom] [handles..]')
    async def visualrank(self, ctx, contest_id: int, *args: str):
        """Plot rating changes by rank. Add handles to specify a handle in the plot.
        if arguments contains `+server`, it will include just server members and not all codeforces users.
        Specify `+zoom` to zoom to the neighborhood of handles."""
        await _render3.visualrank(self, ctx, contest_id, *args)

    @plot.command(brief='Show speed of solving problems by rating',
                  usage='[handles...] [+contest] [+virtual] [+outof] [+scatter] [+median] [r>=rating] [r<=rating] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy] [s=3]')
    async def speed(self, ctx, *args):
        """Plot time spent on problems of particular rating during contest."""
        await _render3.speed(self, ctx, *args)

    @plot.command(brief='Show performance table for rated contests',
                  aliases=['ptable'],
                  usage='[+data] [+vc] [+cfvc] [handles...] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def perftable(self, ctx, *args: str):
        """Show a table of contest performances.

        By default shows rated contest performances from Codeforces.
        Use +vc to show TLE virtual contest performances (accepts @mentions).
        Use +cfvc to show Codeforces virtual participations (rank + estimated perf).
        Use +data for a plain-text dump (uploaded as file if too long).
        Prefix -c to force a Codeforces handle (e.g. -ctourist)."""
        await _render3.perftable(self, ctx, *args)

    @commands.command(brief='Show rating at a given percentile',
                      aliases=['ratingat', 'ratat'],
                      usage='<percentile>')
    async def ratingfor(self, ctx, percentile: float):
        """Look up the Codeforces rating at the given percentile (0–100).

        Example: ;ratingfor 99.5 — what rating puts you in the top 0.5%?
        """
        await _render2.ratingfor(self, ctx, percentile)

    @discord_common.send_error_if(GraphCogError, cf_common.ResolveHandleError,
                                  cf_common.FilterError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Graphs(bot))

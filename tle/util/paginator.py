import asyncio

import discord


def chunkify(sequence, chunk_size):
    """Utility method to split a sequence into fixed size chunks."""
    return [sequence[i: i + chunk_size] for i in range(0, len(sequence), chunk_size)]


class PaginatorError(Exception):
    pass


class NoPagesError(PaginatorError):
    pass


class PaginatorView(discord.ui.View):
    """Button-based paginator view."""

    def __init__(self, pages, author_id=None, wait_time=300):
        super().__init__(timeout=wait_time)
        self.pages = pages
        self.author_id = author_id
        self.cur_page = 0
        self.message = None
        self._update_buttons()

    def _update_buttons(self):
        first_page = self.cur_page == 0
        last_page = self.cur_page == len(self.pages) - 1
        self.first_button.disabled = first_page
        self.prev_button.disabled = first_page
        self.next_button.disabled = last_page
        self.last_button.disabled = last_page

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is not None and interaction.user.id != self.author_id:
            await interaction.response.send_message('Only the requester can navigate this.', ephemeral=True)
            return False
        return True

    async def _show_page(self, interaction: discord.Interaction):
        content, embed = self.pages[self.cur_page]
        self._update_buttons()
        await interaction.response.edit_message(content=content, embed=embed, view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

    @discord.ui.button(emoji='\N{BLACK LEFT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}',
                       style=discord.ButtonStyle.secondary)
    async def first_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cur_page = 0
        await self._show_page(interaction)

    @discord.ui.button(emoji='\N{BLACK LEFT-POINTING TRIANGLE}',
                       style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cur_page = max(0, self.cur_page - 1)
        await self._show_page(interaction)

    @discord.ui.button(emoji='\N{BLACK RIGHT-POINTING TRIANGLE}',
                       style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cur_page = min(len(self.pages) - 1, self.cur_page + 1)
        await self._show_page(interaction)

    @discord.ui.button(emoji='\N{BLACK RIGHT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}',
                       style=discord.ButtonStyle.secondary)
    async def last_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cur_page = len(self.pages) - 1
        await self._show_page(interaction)


def paginate(bot, channel, pages, *, wait_time, set_pagenum_footers=False,
             delete_after: float = None, author_id=None):
    if not pages:
        raise NoPagesError()
    if len(pages) > 1 and set_pagenum_footers:
        for i, (content, embed) in enumerate(pages):
            embed.set_footer(text=f'Page {i + 1} / {len(pages)}')

    async def send():
        content, embed = pages[0]
        if len(pages) == 1:
            await channel.send(content, embed=embed, delete_after=delete_after)
        else:
            view = PaginatorView(pages, author_id=author_id, wait_time=wait_time)
            msg = await channel.send(content, embed=embed, view=view,
                                     delete_after=delete_after)
            view.message = msg

    asyncio.create_task(send())

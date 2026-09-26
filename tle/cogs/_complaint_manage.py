"""Button-driven complaint removal for ``;complain manage``.

``;complain list`` stays read-only. This view shows a page of complaints with
one red button per complaint; pressing it removes that complaint and redraws
the page in place, so a moderator can clear a backlog without copying IDs into
``;complain remove``.

Pure helpers (``chunk_complaints``, ``manage_entry``, ``render_page``) hold the
paging and text rules so they can be tested without a Discord runtime.
"""
import datetime
import logging

import discord

from tle.cogs._complaint_tags import format_tags

logger = logging.getLogger(__name__)

PER_PAGE = 5
_BUTTONS_PER_ROW = 5
_MAX_PER_PAGE = 20  # 4 rows of buttons; row 0 holds navigation
_TEXT_PREVIEW = 300
_EMBED_DESCRIPTION_LIMIT = 3900
_TIMEOUT = 300


def chunk_complaints(complaints, per_page=PER_PAGE):
    """Split complaints into fixed-size pages, preserving order."""
    return [complaints[i:i + per_page]
            for i in range(0, len(complaints), per_page)] or [[]]


def manage_entry(complaint, tags=()):
    """One complaint as a compact line — enough to decide whether to remove it.

    Long reports are truncated: a page holds several complaints and each
    button press redraws the whole embed, so the description has to stay well
    inside the embed limit.
    """
    ts = datetime.datetime.fromtimestamp(
        complaint.created_at).strftime('%Y-%m-%d %H:%M')
    header = f'**#{complaint.id}** by <@{complaint.user_id}> ({ts})'
    if complaint.resolved_at is not None:
        header += ' — *resolved*'
    if tags:
        header += ' — ' + format_tags(tags)
    link = getattr(complaint, 'message_link', None)
    if link:
        header += f' — [context]({link})'
    text = complaint.text or ''
    if len(text) > _TEXT_PREVIEW:
        text = text[:_TEXT_PREVIEW - 1].rstrip() + '…'
    return f'{header}\n{text}'


def render_page(chunk, notice=None, tags_by_id=None):
    """Render one manage page's embed description."""
    tags_by_id = tags_by_id or {}
    if not chunk:
        body = '*No complaints left.*'
    else:
        body = '\n\n'.join(manage_entry(c, tags_by_id.get(c.id, ()))
                             for c in chunk)
    if notice:
        body = f'{notice}\n\n{body}'
    if len(body) > _EMBED_DESCRIPTION_LIMIT:
        body = body[:_EMBED_DESCRIPTION_LIMIT - 1] + '…'
    return body


class _RemoveButton(discord.ui.Button):
    def __init__(self, manager, complaint_id, row):
        super().__init__(style=discord.ButtonStyle.danger,
                         label=f'#{complaint_id}', row=row)
        self.manager = manager
        self.complaint_id = complaint_id

    async def callback(self, interaction):
        await self.manager.remove(interaction, self.complaint_id)


class _PageButton(discord.ui.Button):
    def __init__(self, manager, emoji, target, row, disabled):
        super().__init__(style=discord.ButtonStyle.secondary, emoji=emoji,
                         row=row, disabled=disabled)
        self.manager = manager
        self.target = target

    async def callback(self, interaction):
        await self.manager.show_page(interaction, self.target)


class ComplaintManageView(discord.ui.View):
    """Paginated complaint list whose entries can be removed in place."""

    def __init__(self, complaints, *, guild_id, author_id, delete,
                 can_manage=None, tags_by_id=None, per_page=PER_PAGE,
                 timeout=_TIMEOUT):
        super().__init__(timeout=timeout)
        self.complaints = list(complaints)
        self.tags_by_id = tags_by_id or {}
        self.guild_id = guild_id
        self.author_id = author_id
        self.delete = delete
        self.can_manage = can_manage
        if not 1 <= per_page <= _MAX_PER_PAGE:
            raise ValueError(
                f'per_page must be between 1 and {_MAX_PER_PAGE}; Discord '
                f'allows five rows of five components and row 0 is nav.')
        self.per_page = per_page
        self.page = 0
        self.notice = None
        self.message = None
        self.refresh()

    # -- state ---------------------------------------------------------
    @property
    def pages(self):
        return chunk_complaints(self.complaints, self.per_page)

    def current_chunk(self):
        pages = self.pages
        self.page = max(0, min(self.page, len(pages) - 1))
        return pages[self.page]

    def embed(self):
        pages = self.pages
        chunk = self.current_chunk()
        title = ('Manage complaints' if len(pages) == 1
                 else f'Manage complaints ({self.page + 1}/{len(pages)})')
        return discord.Embed(
            title=title,
            description=render_page(chunk, self.notice, self.tags_by_id),
            color=0xffaa10)

    def refresh(self):
        """Rebuild the buttons for the current page."""
        self.clear_items()
        pages = self.pages
        chunk = self.current_chunk()
        if len(pages) > 1:
            first, last = 0, len(pages) - 1
            at_start = self.page == first
            at_end = self.page == last
            for emoji, target, disabled in (
                    ('\N{BLACK LEFT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}',
                     first, at_start),
                    ('\N{BLACK LEFT-POINTING TRIANGLE}',
                     self.page - 1, at_start),
                    ('\N{BLACK RIGHT-POINTING TRIANGLE}',
                     self.page + 1, at_end),
                    ('\N{BLACK RIGHT-POINTING DOUBLE TRIANGLE WITH VERTICAL BAR}',
                     last, at_end)):
                self.add_item(_PageButton(self, emoji, target, 0, disabled))
        for index, complaint in enumerate(chunk):
            # Discord allows 5 buttons per row and 5 rows; row 0 is navigation.
            row = 1 + index // _BUTTONS_PER_ROW
            self.add_item(_RemoveButton(self, complaint.id, row))

    # -- interaction ---------------------------------------------------
    async def interaction_check(self, interaction):
        """Re-check the role on every press, not just at invoke time.

        The command's role gate runs once; the buttons then live for five
        minutes. A moderator stripped of the role mid-session — usually for
        misusing it — must not keep clearing the backlog. Checking the role
        rather than the invoker's identity also lets a second moderator work
        the same list, which matches ``;complain remove``.
        """
        if self.can_manage is not None:
            if not self.can_manage(interaction.user):
                await interaction.response.send_message(
                    'You need the Admin or Moderator role to manage '
                    'complaints.', ephemeral=True)
                return False
            return True
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                'Only the requester can manage this list.', ephemeral=True)
            return False
        return True

    async def _redraw(self, interaction):
        self.refresh()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def show_page(self, interaction, target):
        self.page = target
        self.notice = None
        await self._redraw(interaction)

    async def remove(self, interaction, complaint_id):
        try:
            removed = self.delete([complaint_id])
        except Exception:
            # A locked database must not leave the interaction unanswered;
            # the row is untouched, so say so and let them press again.
            logger.exception('Failed to remove complaint #%s in guild %s',
                             complaint_id, self.guild_id)
            self.notice = f'Could not remove **#{complaint_id}** — try again.'
            await self._redraw(interaction)
            return
        if removed:
            self.complaints = [c for c in self.complaints
                               if c.id != complaint_id]
            self.notice = f'Removed **#{complaint_id}**.'
            logger.info('Complaint #%s removed via manage by %s in guild %s',
                        complaint_id, self.author_id, self.guild_id)
        else:
            # Someone else removed it first; drop it from the view anyway so
            # the button cannot linger on a row that no longer exists.
            self.complaints = [c for c in self.complaints
                               if c.id != complaint_id]
            self.notice = f'**#{complaint_id}** was already gone.'
            logger.info('Complaint #%s already removed when %s pressed it '
                        'in guild %s', complaint_id, self.author_id,
                        self.guild_id)
        await self._redraw(interaction)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass

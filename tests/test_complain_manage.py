"""`;complain manage` — remove buttons next to each complaint."""
import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs._complaint_manage import (
    ComplaintManageView, chunk_complaints, manage_entry, render_page)


def _row(cid, text='something broke', *, link=None, resolved=False):
    return SimpleNamespace(
        id=cid, user_id='100', text=text, created_at=1_700_000_000.0,
        message_link=link,
        resolved_at=1_700_000_100.0 if resolved else None,
        resolution='done' if resolved else None,
        commit_url=None)


class _Response:
    def __init__(self):
        self.edits = []
        self.messages = []

    async def edit_message(self, **kwargs):
        self.edits.append(kwargs)

    async def send_message(self, content, **kwargs):
        self.messages.append((content, kwargs))


def _interaction(user_id=42):
    return SimpleNamespace(user=SimpleNamespace(id=user_id),
                           response=_Response())


class _Deleter:
    """Stands in for db.delete_complaints, scoped to one guild."""

    def __init__(self, known, returns=None):
        self.known = set(known)
        self.calls = []
        self.returns = returns

    def __call__(self, ids):
        self.calls.append(list(ids))
        if self.returns is not None:
            return self.returns
        hit = [i for i in ids if i in self.known]
        self.known -= set(hit)
        return len(hit)


def _view(rows, deleter=None, per_page=5, author_id=42):
    rows = list(rows)
    deleter = deleter or _Deleter([r.id for r in rows])
    return ComplaintManageView(rows, guild_id=1, author_id=author_id,
                               delete=deleter, per_page=per_page), deleter


def _labels(view):
    return [b.label for b in view.children if b.label]


class TestPaging:
    def test_chunks_preserve_order(self):
        rows = [_row(i) for i in range(7)]
        chunks = chunk_complaints(rows, 3)
        assert [len(c) for c in chunks] == [3, 3, 1]
        assert [c.id for c in chunks[0]] == [0, 1, 2]

    def test_empty_input_still_has_one_page(self):
        assert chunk_complaints([], 5) == [[]]

    def test_single_page_has_no_navigation(self):
        view, _ = _view([_row(1), _row(2)])
        assert _labels(view) == ['#1', '#2']

    def test_multiple_pages_add_navigation(self):
        view, _ = _view([_row(i) for i in range(12)])
        nav = [b for b in view.children if b.label is None]
        assert len(nav) == 4
        assert _labels(view) == ['#0', '#1', '#2', '#3', '#4']

    def test_navigation_moves_to_the_next_page(self):
        view, _ = _view([_row(i) for i in range(12)])
        asyncio.run(view.show_page(_interaction(), 1))
        assert _labels(view) == ['#5', '#6', '#7', '#8', '#9']


class TestEntryRendering:
    def test_long_report_is_truncated(self):
        entry = manage_entry(_row(1, 'x' * 900))
        assert len(entry) < 450
        assert entry.endswith('…')

    def test_resolved_is_marked(self):
        assert '*resolved*' in manage_entry(_row(1, resolved=True))

    def test_context_link_is_kept(self):
        link = 'https://discord.com/channels/1/2/3'
        assert f'[context]({link})' in manage_entry(_row(1, link=link))

    def test_empty_page_says_so(self):
        assert 'No complaints left' in render_page([])

    def test_notice_is_shown_above_the_entries(self):
        page = render_page([_row(1)], notice='Removed **#9**.')
        assert page.startswith('Removed **#9**.')


class TestRemoval:
    def test_button_removes_that_complaint(self):
        view, deleter = _view([_row(1), _row(2), _row(3)])
        interaction = _interaction()
        asyncio.run(view.remove(interaction, 2))

        assert deleter.calls == [[2]]
        assert [c.id for c in view.complaints] == [1, 3]
        assert _labels(view) == ['#1', '#3']
        assert 'Removed **#2**' in interaction.response.edits[0]['embed'].description

    def test_only_the_named_complaint_is_deleted(self):
        view, deleter = _view([_row(i) for i in range(1, 6)])
        asyncio.run(view.remove(_interaction(), 4))
        assert deleter.calls == [[4]]
        assert deleter.known == {1, 2, 3, 5}

    def test_already_deleted_row_is_dropped_not_stuck(self):
        """A stale button must not survive a redraw, or it can never be cleared."""
        view, _ = _view([_row(1), _row(2)], deleter=_Deleter([], returns=0))
        asyncio.run(view.remove(_interaction(), 1))
        assert [c.id for c in view.complaints] == [2]
        assert 'already gone' in view.notice

    def test_emptying_the_last_page_steps_back(self):
        view, _ = _view([_row(i) for i in range(6)], per_page=5)
        asyncio.run(view.show_page(_interaction(), 1))
        assert _labels(view) == ['#5']
        asyncio.run(view.remove(_interaction(), 5))
        assert view.page == 0
        assert _labels(view) == ['#0', '#1', '#2', '#3', '#4']

    def test_removing_everything_leaves_a_usable_embed(self):
        view, _ = _view([_row(1)])
        asyncio.run(view.remove(_interaction(), 1))
        assert view.complaints == []
        assert _labels(view) == []
        assert 'No complaints left' in view.embed().description


class TestAccess:
    def test_another_user_cannot_press_the_buttons(self):
        view, _ = _view([_row(1)], author_id=42)
        interaction = _interaction(user_id=99)
        assert asyncio.run(view.interaction_check(interaction)) is False
        assert interaction.response.messages[0][1]['ephemeral'] is True

    def test_the_requester_can(self):
        view, _ = _view([_row(1)], author_id=42)
        assert asyncio.run(view.interaction_check(_interaction(42))) is True

    def test_timeout_disables_every_button(self):
        view, _ = _view([_row(i) for i in range(12)])
        edited = []
        view.message = SimpleNamespace(
            edit=lambda **kw: asyncio.sleep(0, result=edited.append(kw)))
        asyncio.run(view.on_timeout())
        assert all(b.disabled for b in view.children)
        assert edited


class TestButtonWiring:
    """The stub used to shadow Button.callback, so nothing exercised these."""

    def _remove_buttons(self, view):
        return [b for b in view.children if b.label]

    def test_each_button_carries_its_own_complaint(self):
        view, deleter = _view([_row(7), _row(8), _row(9)])
        button = self._remove_buttons(view)[1]
        assert button.label == '#8'
        asyncio.run(button.callback(_interaction()))
        assert deleter.calls == [[8]]
        assert [c.id for c in view.complaints] == [7, 9]

    def test_navigation_buttons_target_the_right_pages(self):
        view, _ = _view([_row(i) for i in range(12)])
        nav = [b for b in view.children if b.label is None]
        assert [b.target for b in nav] == [0, -1, 1, 2]
        asyncio.run(nav[2].callback(_interaction()))
        assert view.page == 1
        nav = [b for b in view.children if b.label is None]
        assert [b.target for b in nav] == [0, 0, 2, 2]

    def test_navigation_is_disabled_at_the_ends(self):
        view, _ = _view([_row(i) for i in range(12)])
        nav = [b for b in view.children if b.label is None]
        assert [b.disabled for b in nav] == [True, True, False, False]
        asyncio.run(nav[3].callback(_interaction()))
        nav = [b for b in view.children if b.label is None]
        assert [b.disabled for b in nav] == [False, False, True, True]

    def test_buttons_spread_across_rows_within_discord_limits(self):
        view, _ = _view([_row(i) for i in range(20)], per_page=20)
        rows = [b.row for b in view.children if b.label]
        assert rows == [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5
        assert len(view.children) <= 25

    def test_a_page_too_large_for_discord_is_refused(self):
        with pytest.raises(ValueError, match='per_page'):
            _view([_row(i) for i in range(30)], per_page=21)


class TestAuthorizationAtPressTime:
    def _member(self, *roles):
        return SimpleNamespace(
            id=7, roles=[SimpleNamespace(name=r) for r in roles])

    def _view_with_role_check(self):
        from tle.cogs.complain import _has_manage_role
        rows = [_row(1)]
        return ComplaintManageView(
            rows, guild_id=1, author_id=42, delete=_Deleter([1]),
            can_manage=_has_manage_role)

    def test_a_moderator_who_lost_the_role_is_refused(self):
        """The command gate runs once; the buttons live for five minutes."""
        view = self._view_with_role_check()
        interaction = SimpleNamespace(user=self._member('Member'),
                                      response=_Response())
        assert asyncio.run(view.interaction_check(interaction)) is False
        assert 'Admin or Moderator' in interaction.response.messages[0][0]

    def test_a_second_moderator_may_work_the_same_list(self):
        view = self._view_with_role_check()
        interaction = SimpleNamespace(user=self._member('Moderator'),
                                      response=_Response())
        assert asyncio.run(view.interaction_check(interaction)) is True

    def test_admins_qualify_too(self):
        view = self._view_with_role_check()
        interaction = SimpleNamespace(user=self._member('Admin'),
                                      response=_Response())
        assert asyncio.run(view.interaction_check(interaction)) is True


class TestFailureHandling:
    def test_a_database_error_answers_the_interaction(self):
        """An unanswered interaction shows 'This interaction failed'."""
        def _boom(ids):
            raise RuntimeError('database is locked')

        view, _ = _view([_row(1), _row(2)], deleter=_boom)
        interaction = _interaction()
        asyncio.run(view.remove(interaction, 1))
        assert interaction.response.edits
        assert 'Could not remove' in view.notice
        assert [c.id for c in view.complaints] == [1, 2]

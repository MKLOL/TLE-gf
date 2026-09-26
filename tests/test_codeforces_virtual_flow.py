"""`;virtual` — offer, confirm, status and claim flows through the cog."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs import _codeforces_virtual as virtual
from tle.cogs._codeforces_helpers import CodeforcesCogError
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common

from tests.subfilter_common import _problem
from tests.virtual_common import (  # noqa: F401
    CONTESTS, DURATION, GUILD, HANDLE, NOW, USER, _ctx, _interaction, _offer, _sub, env)

class TestOffer:
    def test_the_offer_does_not_name_the_contest(self, env):
        ctx, embed, view = _offer(env)
        text = f'{embed.title} {embed.description}'
        assert 'Round 900' not in text
        assert 'Div. 2' in text
        assert view.contest.id == 1
        assert env.db.get_active_virtual_session(GUILD, USER) is None

    def test_confirming_reveals_and_opens_the_session(self, env):
        ctx, _, view = _offer(env)
        interaction = _interaction()
        asyncio.run(view.decide(interaction, True))
        session = env.db.get_active_virtual_session(GUILD, USER)
        assert session.contest_id == 1
        assert session.expires_at == virtual.expiry(NOW, DURATION)
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert 'Round 900' in embed.title
        assert '/contest/1/virtual' in embed.description

    def test_cancelling_reveals_nothing(self, env):
        ctx, _, view = _offer(env)
        interaction = _interaction()
        asyncio.run(view.decide(interaction, False))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert 'Round 900' not in f'{embed.title} {embed.description}'

    def test_someone_else_cannot_press(self, env):
        ctx, _, view = _offer(env)
        assert asyncio.run(view.interaction_check(_interaction(user_id=99))) is False

    def test_deciding_twice_does_nothing(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        asyncio.run(view.decide(_interaction(), True))
        assert len(env.db.virtual_session_contest_ids(GUILD, USER)) == 1

    def test_a_stale_offer_cannot_open_a_second_session(self, env):
        """An offer that lapsed, a fresh one confirmed, then the old button
        pressed in the race before Discord disables it: the DB rule holds."""
        ctx, _, stale = _offer(env)
        env.clock.now = NOW + virtual._OFFER_TIMEOUT + 1
        ctx, _, fresh = _offer(env)
        asyncio.run(fresh.decide(_interaction(), True))
        interaction = _interaction()
        asyncio.run(stale.decide(interaction, True))
        embed = interaction.response.edit_message.call_args.kwargs['embed']
        assert embed.title == 'Offer expired'
        assert len(env.db.virtual_session_contest_ids(GUILD, USER)) == 1

    def test_an_expired_offer_cannot_be_confirmed_without_a_replacement(self, env):
        _, _, view = _offer(env)
        env.clock.now = NOW + virtual._OFFER_TIMEOUT + 1
        interaction = _interaction()
        asyncio.run(view.decide(interaction, True))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert interaction.response.edit_message.call_args.kwargs['embed'].title == 'Offer expired'

    def test_a_replaced_offer_cannot_be_confirmed_early(self, env):
        _, _, view = _offer(env)
        env.cog._pending_offers()[USER] = object()
        asyncio.run(view.decide(_interaction(), True))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert USER in env.cog._pending_offers()

    def test_timeout_closes_the_offer_and_frees_its_slot(self, env):
        _, _, view = _offer(env)
        asyncio.run(view.on_timeout())
        assert view.decided
        assert USER not in env.cog._pending_offers()
        asyncio.run(view.decide(_interaction(), True))
        assert env.db.get_active_virtual_session(GUILD, USER) is None

    def test_the_offer_clock_starts_after_slow_api_work(self, env, monkeypatch):
        async def slow_visited(handles):
            env.clock.now += 15 * 60
            return set()

        monkeypatch.setattr(cf_common, 'get_visited_contests', slow_visited)
        _, _, view = _offer(env)
        assert view.offered_at == env.clock.now
        with pytest.raises(CodeforcesCogError, match='offer waiting'):
            asyncio.run(env.cog._virtual_impl(_ctx(env.db)))
        asyncio.run(view.decide(_interaction(), True))
        assert env.db.get_active_virtual_session(GUILD, USER) is not None

    def test_an_offered_contest_is_not_offered_again(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        env.clock.now = NOW + 10 * 3600  # first session long expired
        ctx, embed, view = _offer(env)
        assert view.contest.id == 3

    def test_one_offer_at_a_time(self, env):
        _offer(env)
        with pytest.raises(CodeforcesCogError, match='offer waiting'):
            asyncio.run(env.cog._virtual_impl(_ctx(env.db)))

    def test_a_failed_send_leaves_no_phantom_offer(self, env):
        ctx = _ctx(env.db)
        ctx.send = AsyncMock(side_effect=RuntimeError('no permission'))
        with pytest.raises(RuntimeError):
            asyncio.run(env.cog._virtual_impl(ctx))
        ctx, _, view = _offer(env)  # must not say "offer waiting"
        assert view.contest.id == 1

    def test_a_cancelled_offer_frees_the_slot(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), False))
        ctx, _, view = _offer(env)
        assert view.contest.id == 1

    def test_a_lapsed_offer_frees_the_slot(self, env):
        _offer(env)
        env.clock.now = NOW + virtual._OFFER_TIMEOUT + 1
        ctx, _, view = _offer(env)
        assert view.contest.id == 1

    def test_nothing_left_is_an_error(self, env, monkeypatch):
        monkeypatch.setattr(cf_common, 'is_contest_writer', lambda cid, h: True)
        with pytest.raises(CodeforcesCogError, match='No untouched'):
            asyncio.run(env.cog._virtual_impl(_ctx(env.db)))


class TestStatusAndClaim:
    def _start(self, env):
        ctx, _, view = _offer(env)
        asyncio.run(view.decide(_interaction(), True))
        return env.db.get_active_virtual_session(GUILD, USER)

    def test_virtual_while_active_shows_status_not_a_new_offer(self, env):
        self._start(env)
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_impl(ctx))
        kwargs = ctx.send.call_args.kwargs
        assert 'view' not in kwargs
        assert 'Round 900' in kwargs['embed'].title

    def test_claim_credits_solves_and_is_idempotent(self, env):
        session = self._start(env)
        env.subs.extend([_sub('A', rating=1700), _sub('C', rating=2200),
                         _sub('D', verdict='WRONG_ANSWER')])
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        # base rating 1900: A is -200 -> 3 points, C is +300 -> 23 points
        assert env.db.get_gudgitter_score(USER) == 26
        assert env.db.credited_virtual_problems(session.id) == {'A', 'C'}
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 26
        assert 'Nothing new' in ctx.send.call_args.kwargs['embed'].description

    def test_claim_after_expiry_finishes_the_session(self, env):
        self._start(env)
        env.subs.append(_sub('B'))
        env.clock.now = virtual.expiry(NOW, DURATION) + 61
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert 'is over' in ctx.send.call_args.args[0]
        assert env.db.get_gudgitter_score(USER) > 0

    def test_a_renamed_handle_cannot_lock_the_user_out(self, env, monkeypatch):
        """Codeforces stops resolving the session's handle after the reveal:
        the expired session must still close, or ;virtual is gone for good."""
        self._start(env)

        async def gone(*, handle):
            raise cf.HandleNotFoundError('not found')
        monkeypatch.setattr(cf, 'user', SimpleNamespace(status=gone), raising=False)
        env.clock.now = virtual.expiry(NOW, DURATION) + 61
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert 'closed without credit' in ctx.send.call_args.args[0]
        # and a fresh offer is possible again
        asyncio.run(env.cog._virtual_impl(_ctx(env.db)))

    def test_a_running_session_survives_a_transient_api_error(self, env, monkeypatch):
        self._start(env)

        async def flaky(*, handle):
            raise cf.CodeforcesApiError('busy')
        monkeypatch.setattr(cf, 'user', SimpleNamespace(status=flaky), raising=False)
        with pytest.raises(cf.CodeforcesApiError):
            asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_active_virtual_session(GUILD, USER) is not None

    @pytest.mark.parametrize('command', ['_virtual_impl', '_virtual_claim_impl'])
    def test_expired_solves_survive_a_transient_api_error(self, env, monkeypatch,
                                                        command):
        session = self._start(env)
        env.subs.append(_sub('B', rating=1900))
        env.clock.now = session.expires_at + 61
        healthy_status = cf.user.status

        async def flaky(*, handle):
            raise cf.CodeforcesApiError('busy')

        monkeypatch.setattr(cf.user, 'status', flaky)
        with pytest.raises(cf.CodeforcesApiError):
            asyncio.run(getattr(env.cog, command)(_ctx(env.db)))
        assert env.db.get_active_virtual_session(GUILD, USER).id == session.id

        monkeypatch.setattr(cf.user, 'status', healthy_status)
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_active_virtual_session(GUILD, USER) is None
        assert env.db.get_gudgitter_score(USER) == 8

    @pytest.mark.parametrize('verdict', [None, 'TESTING'])
    def test_expiry_waits_for_pending_judgements(self, env, verdict):
        session = self._start(env)
        env.subs.append(_sub('B', rating=1900, verdict=verdict))
        env.clock.now = session.expires_at + 61
        with pytest.raises(CodeforcesCogError, match='still judging'):
            asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_active_virtual_session(GUILD, USER).id == session.id
        env.subs[:] = [_sub('B', rating=1900)]
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_gudgitter_score(USER) == 8
        assert env.db.get_active_virtual_session(GUILD, USER) is None

    def test_unrelated_pending_submissions_do_not_hold_a_session_open(self, env):
        session = self._start(env)
        env.subs.extend([_sub('B', contest_id=3, verdict='TESTING'),
                         _sub('C', ptype='PRACTICE', verdict='TESTING')])
        env.clock.now = session.expires_at + 61
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_active_virtual_session(GUILD, USER) is None

    def test_a_forgotten_virtual_is_credited_before_the_next_offer(self, env):
        self._start(env)
        env.subs.append(_sub('B', rating=1900))
        env.clock.now = virtual.expiry(NOW, DURATION) + 61  # past the slack
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 8
        first, second = [c.kwargs for c in ctx.send.call_args_list]
        assert 'view' in second and second['view'].contest.id == 3

    def test_the_live_gitgud_problem_is_left_for_gotgud(self, env):
        session = self._start(env)
        env.db.new_challenge(str(USER), NOW, _problem(1, index='A', name='PA'), 0)
        env.subs.append(_sub('A', name='PA'))
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.credited_virtual_problems(session.id) == set()
        assert env.db.check_challenge(str(USER))[2] == 'PA'

    def test_completing_gitgud_does_not_make_its_solve_payable_again(self, env):
        session = self._start(env)
        env.db.new_challenge(str(USER), NOW, _problem(1, index='A', name='PA'), 0)
        env.subs.append(_sub('A', name='PA', rating=1900))
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        challenge = env.db.check_challenge(str(USER))
        assert env.db.complete_challenge(USER, challenge[0], NOW + 600, 8) == 1

        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_gudgitter_score(USER) == 8
        assert len(env.db.gitlog(str(USER))) == 1
        assert env.db.credited_virtual_problems(session.id) == set()

    @pytest.mark.parametrize('completed', [False, True])
    def test_mirrored_gitgud_problem_stays_reserved(self, env, completed):
        session = self._start(env)
        env.db.new_challenge(str(USER), NOW, _problem(2, index='C', name='PA'), 0)
        if completed:
            challenge = env.db.check_challenge(str(USER))
            env.db.complete_challenge(USER, challenge[0], NOW + 600, 8)
        env.subs.append(_sub('A', name='PA', rating=1900))
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_gudgitter_score(USER) == (8 if completed else 0)
        assert env.db.credited_virtual_problems(session.id) == set()

    def test_unrelated_rounds_can_have_the_same_problem_title(self, env, monkeypatch):
        session = self._start(env)
        other_round = CONTESTS[1]._replace(
            startTimeSeconds=CONTESTS[0].startTimeSeconds - 86400)
        monkeypatch.setattr(cf_common.cache2.contest_cache, 'get_contests_in_phase',
                            lambda phase: [CONTESTS[0], other_round])
        env.db.new_challenge(str(USER), NOW - 86400,
                            _problem(2, index='A', name='PA'), 0)
        challenge = env.db.check_challenge(str(USER))
        env.db.complete_challenge(USER, challenge[0], NOW - 86000, 8)
        env.subs.append(_sub('A', name='PA', rating=1900))
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.db.get_gudgitter_score(USER) == 16
        assert env.db.credited_virtual_problems(session.id) == {'A'}

    def test_unrated_problems_are_reported_not_paid(self, env):
        self._start(env)
        env.subs.append(_sub('F', rating=None))
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 0
        assert 'unrated' in ctx.send.call_args.kwargs['embed'].description

    def test_claims_use_the_confirmed_handle_and_rating(self, env, monkeypatch):
        """Re-identifying to a stronger account after the reveal, or a rating
        change mid-session, changes nothing: the session is the authority."""
        session = self._start(env)
        assert session.handle == HANDLE and session.base_rating == 1900

        async def resolve_other(ctx, converter, handles, **kw):
            return ['strongfriend']
        monkeypatch.setattr(cf_common, 'resolve_handles', resolve_other)
        env.db.rating = 3000
        env.subs.append(_sub('A', rating=1900))
        env.fetched.clear()
        asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))
        assert env.fetched == [HANDLE]
        assert env.db.get_gudgitter_score(USER) == 8  # delta 0 against 1900

    def test_special_problems_are_reported_not_paid(self, env):
        self._start(env)
        env.subs.append(_sub('G', name='special G', rating=1900))
        ctx = _ctx(env.db)
        asyncio.run(env.cog._virtual_claim_impl(ctx))
        assert env.db.get_gudgitter_score(USER) == 0
        assert 'G' in ctx.send.call_args.kwargs['embed'].description

    def test_claim_without_a_session_is_an_error(self, env):
        with pytest.raises(CodeforcesCogError, match='No virtual'):
            asyncio.run(env.cog._virtual_claim_impl(_ctx(env.db)))

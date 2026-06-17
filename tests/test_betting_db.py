"""Betting DB-layer tests: wallets, daily allowance, markets, placing/removing\nwagers, and pools."""
import sqlite3  # noqa: F401

import pytest  # noqa: F401

from tle.util.db.user_db_conn import (  # noqa: F401
    UserDbConn, namedtuple_factory, bet_fixture_key,
)
from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
)


class TestWallet:
    def test_ensure_creates_at_start(self, db):
        assert db.bet_get_balance(GUILD, USER_A) is None
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 1000
        assert db.bet_get_balance(GUILD, USER_A) == 1000
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [(r.action, r.amount, r.balance_after) for r in rows] == [
            ('init', 1000, 1000)]

    def test_ensure_idempotent(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        # second call must not reset a changed balance
        db.conn.execute('UPDATE bet_wallet SET balance = 50 WHERE user_id = ?',
                        (USER_A,))
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 50
        assert len(db.bet_wallet_history(GUILD, USER_A)) == 1

    def test_guild_isolation(self, db):
        db.bet_ensure_wallet('1', USER_A, 1000)
        assert db.bet_get_balance('2', USER_A) is None

    def test_transfer_moves_balance_and_logs_both_wallets(self, db):
        admin = '999'
        ok, reason, sender_bal, receiver_bal = db.bet_transfer(
            GUILD, USER_A, USER_B, 300, 1000, transferred_at=7.0,
            actor_id=admin)
        assert (ok, reason, sender_bal, receiver_bal) == (True, 'ok', 700, 1300)
        assert db.bet_get_balance(GUILD, USER_A) == 700
        assert db.bet_get_balance(GUILD, USER_B) == 1300

        sender_hist = db.bet_wallet_history(GUILD, USER_A)
        receiver_hist = db.bet_wallet_history(GUILD, USER_B)
        assert sender_hist[0].action == 'transfer_out'
        assert sender_hist[0].actor_id == admin
        assert sender_hist[0].amount == -300
        assert sender_hist[0].balance_after == 700
        assert sender_hist[0].note == USER_B
        assert receiver_hist[0].action == 'transfer_in'
        assert receiver_hist[0].actor_id == admin
        assert receiver_hist[0].amount == 300
        assert receiver_hist[0].balance_after == 1300
        assert receiver_hist[0].note == USER_A

    def test_transfer_rejects_insufficient_without_recipient_wallet(self, db):
        db.bet_set_balance(GUILD, USER_A, 50, 1000)
        ok, reason, sender_bal, receiver_bal = db.bet_transfer(
            GUILD, USER_A, USER_B, 60, 1000, transferred_at=7.0)
        assert (ok, reason, sender_bal, receiver_bal) == (
            False, 'insufficient', 50, None)
        assert db.bet_get_balance(GUILD, USER_A) == 50
        assert db.bet_get_balance(GUILD, USER_B) is None
        assert [r.action for r in db.bet_wallet_history(GUILD, USER_A)] == [
            'setbalance', 'init']


class TestDaily:
    def test_grants_once(self, db):
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is True and bal == 1100 and reason == 'ok'
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [r.action for r in rows[:2]] == ['daily', 'init']
        assert rows[0].amount == 100 and rows[0].actor_id == USER_A

    def test_second_claim_same_day_refused(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is False and bal == 1100 and reason == 'already'
        assert [r.action for r in db.bet_wallet_history(GUILD, USER_A)].count('daily') == 1

    def test_next_day_grants_again(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, _ = db.bet_claim_daily(GUILD, USER_A, '2026-06-16', 100, 1000)
        assert granted is True and bal == 1200


class TestMarket:
    def test_create_and_get(self, db):
        mid = _make_market(db)
        m = db.bet_market_get(mid)
        assert m.home_team == 'Spain'
        assert m.status == 'open'
        assert m.odds_home == 2.0

    def test_active_lookup(self, db):
        mid = _make_market(db)
        assert db.bet_market_get_active(GUILD, CH).market_id == mid
        assert db.bet_market_get_active(GUILD, 'other') is None

    def test_thread_lookup(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread(mid, THREAD)
        assert db.bet_market_get_active_by_thread(GUILD, THREAD).market_id == mid
        assert db.bet_market_get_active_by_thread(GUILD, 'nope') is None

    def test_thread_intro_tracking(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread_intro(mid, '444')
        assert db.bet_market_get(mid).thread_intro_id == '444'

    def test_exists_open_for_event(self, db):
        _make_market(db)
        assert db.bet_market_exists_open_for_event(GUILD, 'evt1') is True
        assert db.bet_market_exists_open_for_event(GUILD, 'evtX') is False

    def test_duplicate_open_event_returns_none(self, db):
        first = _make_market(db)
        second = db.bet_market_create(
            GUILD, '999', 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
            2.0, 3.0, 4.0, USER_A, 1.0)
        assert second is None
        assert len(db.bet_markets_open(GUILD)) == 1

        db.bet_settle(GUILD, first, 'home', 1, 0, 2.0)
        reopened = db.bet_market_create(
            GUILD, '999', 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', 20_000.0,
            2.0, 3.0, 4.0, USER_A, 3.0)
        assert reopened is not None

    def test_duplicate_open_fixture_with_new_event_id_returns_none(self, db):
        first = _make_market(db)
        second = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain', 10_900.0,
            2.0, 3.0, 4.0, USER_A, 1.0)
        assert first is not None
        assert second is None
        assert len(db.bet_markets_open(GUILD)) == 1

    def test_duplicate_fixture_can_reopen_after_terminal_status(self, db):
        first = _make_market(db)
        db.bet_void(GUILD, first, 2.0)
        reopened = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain', 20_000.0,
            2.0, 3.0, 4.0, USER_A, 3.0)
        assert reopened is not None

    def test_same_teams_different_utc_day_allowed(self, db):
        first = _make_market(db, commence=10_000.0)
        second = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain',
            10_000.0 + 86400, 2.0, 3.0, 4.0, USER_A, 1.0)
        assert first is not None
        assert second is not None

    def test_pending_settlement_by_cutoff(self, db):
        mid = _make_market(db, commence=1000.0)
        assert [m.market_id for m in db.bet_markets_pending_settlement(2000.0)] == [mid]
        assert db.bet_markets_pending_settlement(500.0) == []


class TestPlaceBet:
    def test_escrow_deducts(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        assert ok and reason == 'ok' and bal == 700
        assert db.bet_get_balance(GUILD, USER_A) == 700
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [(r.action, r.amount, r.balance_after, r.market_id)
                for r in rows[:2]] == [
                    ('wager_stake', -300, 700, mid),
                    ('init', 1000, 1000, None),
                ]

    def test_insufficient_balance(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 5000, 1.0, 1000)
        assert ok is False and reason == 'insufficient' and bal == 1000
        # no wager recorded, balance untouched
        assert db.bet_get_wager(mid, USER_A) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000

    def test_same_pick_rebet_refunds_previous_then_charges(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)  # bal 700
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'home', 200, 2.0, 1000)
        assert ok and bal == 800  # 700 + 300 refund - 200
        w = db.bet_get_wager(mid, USER_A, 'home')
        assert w.pick == 'home' and w.stake == 200  # odds derived from market
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [r.action for r in rows[:2]] == ['wager_stake', 'wager_refund']
        assert rows[0].amount == -200 and rows[0].balance_after == 800
        assert rows[1].amount == 300 and rows[1].balance_after == 1000

    def test_different_pick_adds_second_wager(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)  # bal 700
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'away', 200, 2.0, 1000)
        assert ok and bal == 500
        wagers = db.bet_get_wagers_for_user(mid, USER_A)
        assert [(w.pick, w.stake) for w in wagers] == [('home', 300), ('away', 200)]

    def test_same_pick_same_stake_is_noop(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 300, 2.0, 1000)
        assert ok and reason == 'unchanged' and bal == 700
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [r.action for r in rows] == ['wager_stake', 'init']

    def test_rebet_to_larger_stake_within_refunded_budget(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 1000, 1.0, 1000)  # all-in, bal 0
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'home', 1000, 2.0, 1000)
        assert ok and bal == 0  # refund 1000 then stake 1000 again

    def test_remove_one_pick_refunds_only_that_pick(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_A, 'away', 200, 2.0, 1000)
        ok, reason, bal, refunded = db.bet_remove_wager(
            GUILD, mid, USER_A, 'home', 3.0)
        assert ok and reason == 'removed'
        assert refunded == 300 and bal == 800
        assert [(w.pick, w.stake) for w in db.bet_get_wagers_for_user(mid, USER_A)] == [
            ('away', 200)]

    def test_remove_missing_pick_is_noop(self, db):
        mid = _make_market(db)
        ok, reason, bal, refunded = db.bet_remove_wager(
            GUILD, mid, USER_A, 'home', 3.0)
        assert ok is False and reason == 'missing'
        assert bal is None and refunded == 0

    def test_remove_all_user_wagers_only_one_market(self, db):
        mid = _make_market(db)
        other_mid = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Brazil', 'Japan', 20_000.0,
            2.0, 3.0, 4.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_A, 'away', 200, 2.0, 1000)
        db.bet_place(GUILD, other_mid, USER_A, 'home', 100, 3.0, 1000)

        ok, reason, bal, refunded, count = db.bet_remove_wagers_for_user(
            GUILD, mid, USER_A, 4.0)

        assert ok and reason == 'removed'
        assert refunded == 500 and count == 2 and bal == 900
        assert db.bet_get_wagers_for_user(mid, USER_A) == []
        assert [(w.pick, w.stake) for w in db.bet_get_wagers_for_user(
            other_mid, USER_A)] == [('home', 100)]

    def test_remove_all_user_wagers_missing_is_noop(self, db):
        mid = _make_market(db)
        ok, reason, bal, refunded, count = db.bet_remove_wagers_for_user(
            GUILD, mid, USER_A, 4.0)
        assert ok is False and reason == 'missing'
        assert bal is None and refunded == 0 and count == 0

    def test_place_rejects_settled_market(self, db):
        mid = _make_market(db)
        db.bet_settle(GUILD, mid, 'home', 1, 0, 5.0)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 100, 6.0, 1000)
        assert ok is False and reason == 'closed'
        assert bal is None
        assert db.bet_get_wager(mid, USER_A) is None

    def test_remove_rejects_settled_market(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 1, 0, 5.0)
        ok, reason, bal, refunded = db.bet_remove_wager(
            GUILD, mid, USER_A, 'home', 6.0)
        assert ok is False and reason == 'closed'
        assert bal == 1300 and refunded == 0
        assert db.bet_get_wager(mid, USER_A, 'home').stake == 300

    def test_remove_all_rejects_settled_market(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 1, 0, 5.0)
        ok, reason, bal, refunded, count = db.bet_remove_wagers_for_user(
            GUILD, mid, USER_A, 6.0)
        assert ok is False and reason == 'closed'
        assert bal == 1300 and refunded == 0 and count == 0
        assert db.bet_get_wager(mid, USER_A, 'home').stake == 300


class TestPool:
    def test_pool_groups_by_pick(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'home', 200, 1.0, 1000)
        pool = {p.pick: (p.cnt, p.total) for p in db.bet_pool(mid)}
        assert pool['home'] == (2, 300)

    def test_active_wagers_for_user_lists_open_markets(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread(mid, THREAD)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        rows = db.bet_active_wagers_for_user(GUILD, USER_A)
        assert len(rows) == 1
        assert rows[0].market_id == mid
        assert rows[0].thread_id == THREAD
        assert rows[0].pick == 'home'


USER_C = '300'


class TestGrantAll:
    def test_no_bonus_by_default(self, db):
        assert db.bet_get_start_bonus(GUILD) == 0

    def test_grants_all_existing_wallets_and_raises_bonus(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_ensure_wallet(GUILD, USER_B, 1000)
        changed, bonus = db.bet_grant_all(GUILD, 500, actor_id='999')
        assert (changed, bonus) == (2, 500)
        assert db.bet_get_balance(GUILD, USER_A) == 1500
        assert db.bet_get_balance(GUILD, USER_B) == 1500
        assert db.bet_get_start_bonus(GUILD) == 500

    def test_future_joiner_seeds_at_raised_balance(self, db):
        db.bet_grant_all(GUILD, 500)
        # The cog seeds new wallets at base + bonus; a member who never
        # participated keeps no row until then.
        assert db.bet_get_balance(GUILD, USER_C) is None
        seed = 1000 + db.bet_get_start_bonus(GUILD)
        assert db.bet_ensure_wallet(GUILD, USER_C, seed) == 1500

    def test_bonus_accumulates_across_grants(self, db):
        db.bet_grant_all(GUILD, 500)
        _, bonus = db.bet_grant_all(GUILD, 250)
        assert bonus == 750

    def test_only_touches_wallets_never_creates_them(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        changed, _ = db.bet_grant_all(GUILD, 500)
        assert changed == 1
        assert db.bet_get_balance(GUILD, USER_B) is None

    def test_revert_subtracts_and_lowers_bonus(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_grant_all(GUILD, 500)            # A -> 1500, bonus 500
        changed, bonus = db.bet_grant_all(GUILD, -500)
        assert (changed, bonus) == (1, 0)
        assert db.bet_get_balance(GUILD, USER_A) == 1000

    def test_revert_floors_wallets_and_bonus_at_zero(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_grant_all(GUILD, 500)
        db.bet_set_balance(GUILD, USER_A, 200, 1000)   # spent most of it
        _, bonus = db.bet_grant_all(GUILD, -800)       # over-claw
        assert db.bet_get_balance(GUILD, USER_A) == 0  # floored, not negative
        assert bonus == 0                              # floored, not -300

    def test_logs_ledger_entries(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_grant_all(GUILD, 500, actor_id='999')
        db.bet_grant_all(GUILD, -200, actor_id='999')
        hist = db.bet_wallet_history(GUILD, USER_A)
        actions = [(h.action, h.amount, h.balance_after) for h in hist]
        assert ('grantall', 500, 1500) in actions
        assert ('grantall_revert', -200, 1300) in actions

    def test_guild_isolation(self, db):
        db.bet_ensure_wallet('111', USER_A, 1000)
        db.bet_ensure_wallet('222', USER_A, 1000)
        db.bet_grant_all('111', 500)
        assert db.bet_get_balance('111', USER_A) == 1500
        assert db.bet_get_balance('222', USER_A) == 1000
        assert db.bet_get_start_bonus('222') == 0

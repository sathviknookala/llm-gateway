"""Hypothesis RuleBasedStateMachine over the ledger API.

A Python model tracks what every balance should be. Rules: fund, grant (some
over the pool), place_hold (some over the available balance, some already past
expiry), settle (actual from 0 to well over the hold), release, expire, replay
of an earlier request, and replay with altered inputs.

After every step: check_ledger() finds nothing, and every database balance
equals the model's.

Rules are sync, so the machine owns an event loop and one connection, and
runs each ledger call in its own transaction on that loop.
"""

import asyncio
import itertools
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)

from llmgw.ledger import accounts, holds, postings
from llmgw.ledger.checks import check_ledger
from llmgw.ledger.errors import IdempotencyConflict, InsufficientCredits, InvalidHoldState

N_TEAMS = 3
INITIAL_POOL = 20_000
INITIAL_GRANT = 1_000
TRUNCATE = "TRUNCATE api_keys, holds, entries, postings, balances, accounts, teams, orgs RESTART IDENTITY CASCADE"


@dataclass
class HoldModel:
    team: int
    amount: int
    past_due: bool
    state: str = "held"


class LedgerMachine(RuleBasedStateMachine):
    def __init__(self, database_url: str):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.conn = self.run(asyncpg.connect(database_url))
        self.run(self.conn.execute(TRUNCATE))
        self.org = self.call(accounts.create_org, name="org")
        self.teams = [
            self.call(accounts.create_team, org_id=self.org.org_id, name=f"t{i}")
            for i in range(N_TEAMS)
        ]
        self.ids = itertools.count()

        # the model
        self.funded = 0
        self.pool = 0
        self.spend = 0
        self.available = [0] * N_TEAMS
        self.held = [0] * N_TEAMS
        self.holds: dict[int, HoldModel] = {}
        self.closes: dict[str, dict] = {}  # close key -> request that closed it
        self.replayable: list[tuple] = []  # (fn, kwargs) of successful fund/grant/hold calls

        # Start funded, so most steps exercise holds rather than topping up.
        self.fund(INITIAL_POOL)
        for team in range(N_TEAMS):
            self.grant(team, INITIAL_GRANT)

    # --- bridge -------------------------------------------------------------

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    def call(self, fn, **kwargs):
        async def in_tx():
            async with self.conn.transaction():
                return await fn(self.conn, **kwargs)

        return self.run(in_tx())

    def expect(self, exc, fn, **kwargs):
        with pytest.raises(exc):
            self.call(fn, **kwargs)

    def teardown(self):
        self.run(self.conn.close())
        self.loop.close()

    # --- rules --------------------------------------------------------------

    @rule(amount=st.integers(1, 5_000))
    def fund(self, amount):
        kwargs = dict(org_id=self.org.org_id, amount=amount, idempotency_key=f"fund-{next(self.ids)}")
        self.call(postings.fund_org, **kwargs)
        self.funded += amount
        self.pool += amount
        self.replayable.append((postings.fund_org, kwargs))

    @rule(team=st.integers(0, N_TEAMS - 1), amount=st.integers(1, 3_000))
    def grant(self, team, amount):
        kwargs = dict(team_id=self.teams[team].team_id, amount=amount, idempotency_key=f"grant-{next(self.ids)}")
        if amount > self.pool:
            self.expect(InsufficientCredits, postings.grant, **kwargs)
            return
        self.call(postings.grant, **kwargs)
        self.pool -= amount
        self.available[team] += amount
        self.replayable.append((postings.grant, kwargs))

    @rule(team=st.integers(0, N_TEAMS - 1), amount=st.integers(1, 800), past_due=st.booleans())
    def place_hold(self, team, amount, past_due):
        offset = timedelta(minutes=-1) if past_due else timedelta(hours=1)
        kwargs = dict(
            team_id=self.teams[team].team_id,
            amount=amount,
            expires_at=datetime.now(UTC) + offset,
            request_id=f"req-{next(self.ids)}",
        )
        if amount > self.available[team]:
            self.expect(InsufficientCredits, holds.place_hold, **kwargs)
            return
        hold = self.call(holds.place_hold, **kwargs)
        assert hold.id not in self.holds
        self.holds[hold.id] = HoldModel(team=team, amount=amount, past_due=past_due)
        self.available[team] -= amount
        self.held[team] += amount
        self.replayable.append((holds.place_hold, kwargs))

    def _close_outcome(self, hold_id: int, key: str, request: dict):
        """What the ledger should do: 'apply', 'replay', or the exception it should raise."""
        prior = self.closes.get(key)
        if prior is not None:
            return "replay" if prior == request else IdempotencyConflict
        if self.holds[hold_id].state != "held":
            return InvalidHoldState
        return "apply"

    @precondition(lambda self: self.holds)
    @rule(data=st.data(), actual=st.integers(0, 1_200), flagged=st.booleans())
    def settle(self, data, actual, flagged):
        hold_id = data.draw(st.sampled_from(sorted(self.holds)), label="hold")
        h = self.holds[hold_id]
        key = f"settle:{hold_id}"
        request = {"actual": actual, "flagged": flagged}
        outcome = self._close_outcome(hold_id, key, request)
        if outcome not in ("apply", "replay"):
            self.expect(outcome, holds.settle, hold_id=hold_id, actual=actual, flagged=flagged)
            return

        result = self.call(holds.settle, hold_id=hold_id, actual=actual, flagged=flagged)
        charged = min(actual, h.amount)
        assert (result.charged, result.uncovered) == (charged, actual - charged)
        assert result.posting.flagged == (flagged or actual > h.amount)
        if outcome == "apply":
            h.state = "settled"
            self.closes[key] = request
            self.held[h.team] -= h.amount
            self.available[h.team] += h.amount - charged
            self.spend += charged

    def _refund(self, data, op: str):
        hold_id = data.draw(st.sampled_from(sorted(self.holds)), label="hold")
        h = self.holds[hold_id]
        key = f"{op}:{hold_id}"
        outcome = self._close_outcome(hold_id, key, {})
        if outcome == "apply" and op == "expire" and not h.past_due:
            outcome = InvalidHoldState
        fn = getattr(holds, op)
        if outcome not in ("apply", "replay"):
            self.expect(outcome, fn, hold_id=hold_id)
            return
        closed = self.call(fn, hold_id=hold_id)
        assert closed.state == ("released" if op == "release" else "expired")
        if outcome == "apply":
            h.state = closed.state
            self.closes[key] = {}
            self.held[h.team] -= h.amount
            self.available[h.team] += h.amount

    @precondition(lambda self: self.holds)
    @rule(data=st.data())
    def release(self, data):
        self._refund(data, "release")

    @precondition(lambda self: self.holds)
    @rule(data=st.data())
    def expire(self, data):
        self._refund(data, "expire")

    @precondition(lambda self: self.replayable)
    @rule(data=st.data())
    def replay(self, data):
        fn, kwargs = data.draw(st.sampled_from(self.replayable), label="replayed call")
        self.call(fn, **kwargs)  # the invariant checks that nothing moved

    @precondition(lambda self: self.replayable)
    @rule(data=st.data())
    def conflicting_replay(self, data):
        fn, kwargs = data.draw(st.sampled_from(self.replayable), label="altered call")
        self.expect(IdempotencyConflict, fn, **{**kwargs, "amount": kwargs["amount"] + 1})

    # --- invariant ----------------------------------------------------------

    @invariant()
    def books_match_model(self):
        assert self.run(check_ledger(self.conn)) == []
        rows = self.run(self.conn.fetch("SELECT account_id, amount FROM balances"))
        actual = {r["account_id"]: r["amount"] for r in rows}
        expected = {
            self.org.external: -self.funded,
            self.org.org_pool: self.pool,
            self.org.provider_spend: self.spend,
            self.org.retry_loss: 0,
        }
        for i, t in enumerate(self.teams):
            expected[t.available] = self.available[i]
            expected[t.held] = self.held[i]
        assert actual == expected


SETTINGS = settings(
    max_examples=100,
    stateful_step_count=50,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


def test_ledger_state_machine(database_url):
    run_state_machine_as_test(lambda: LedgerMachine(database_url), settings=SETTINGS)

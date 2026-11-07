"""Hypothesis RuleBasedStateMachine over the ledger API.

Rules: grant, place_hold, settle (0 <= actual <= held), release, expire, and
replay of any earlier idempotency key.

Invariants after every step:
- every posting's entries sum to zero
- no non_negative balance is below zero
- each balance row equals the sum of that account's entries
- a replay changes nothing
- per team: available + held + provider spend == total granted
"""

import pytest

pytest.skip("M1: ledger operations not implemented yet", allow_module_level=True)

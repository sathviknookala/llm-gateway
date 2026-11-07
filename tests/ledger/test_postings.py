"""llmgw.ledger.postings: fund, grant, idempotent replay.

To cover:
- fund_org and grant move the expected balances
- replaying a key with the same request returns the same posting and writes nothing
- replaying a key with a different request raises IdempotencyConflict
- an overdraft raises InsufficientCredits, not a raw asyncpg error
"""

import pytest

pytest.skip("M1: ledger.postings not implemented yet", allow_module_level=True)

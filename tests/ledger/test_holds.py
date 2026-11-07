"""llmgw.ledger.holds: place, settle, release, expire.

To cover:
- the worked example through the API: hold 120, settle 45 -> 955 available, 0 held, 45 spend
- settle with actual == 0 and actual == amount; actual > amount raises
- a closed hold can't be closed again by a different path (InvalidHoldState)
- replaying the same close is idempotent
- settle with flagged=True marks the posting
"""

import pytest

pytest.skip("M1: ledger.holds not implemented yet", allow_module_level=True)

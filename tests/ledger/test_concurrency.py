"""Concurrent ledger operations, run through the `pool` fixture with asyncio.gather.

To cover:
- team has 100 available; two holds of 80 race: exactly one succeeds, the other
  raises InsufficientCredits, held == 80
- settle and expire race on one hold: exactly one closes it, one posting written
- many concurrent grants and holds across teams finish without deadlocks
"""

import pytest

pytest.skip("M1: ledger operations not implemented yet", allow_module_level=True)

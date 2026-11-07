"""Ledger value types. Amounts are integer micro-credits."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class AccountKind(StrEnum):
    EXTERNAL = "external"
    ORG_POOL = "org_pool"
    PROVIDER_SPEND = "provider_spend"
    RETRY_LOSS = "retry_loss"
    TEAM_AVAILABLE = "team_available"
    TEAM_HELD = "team_held"


ORG_ACCOUNT_KINDS = (
    AccountKind.EXTERNAL,
    AccountKind.ORG_POOL,
    AccountKind.PROVIDER_SPEND,
    AccountKind.RETRY_LOSS,
)
TEAM_ACCOUNT_KINDS = (AccountKind.TEAM_AVAILABLE, AccountKind.TEAM_HELD)


class PostingKind(StrEnum):
    FUND = "fund"
    GRANT = "grant"
    HOLD = "hold"
    SETTLE = "settle"
    RELEASE = "release"
    EXPIRE = "expire"


class HoldState(StrEnum):
    HELD = "held"
    SETTLED = "settled"
    RELEASED = "released"
    EXPIRED = "expired"


@dataclass(frozen=True)
class EntrySpec:
    """One leg of a posting, before it is written."""

    account_id: int
    amount: int


@dataclass(frozen=True)
class Posting:
    id: int
    org_id: int
    kind: PostingKind
    idempotency_key: str
    flagged: bool
    created_at: datetime


@dataclass(frozen=True)
class Hold:
    id: int
    team_id: int
    amount: int
    state: HoldState
    hold_posting_id: int
    close_posting_id: int | None
    settled_amount: int | None
    expires_at: datetime

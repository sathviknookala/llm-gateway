-- Ledger tables. Amounts are integer micro-credits (bigint), never floats.
-- Invariant triggers live in V2.

CREATE TABLE orgs (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text        NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE teams (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      bigint      NOT NULL REFERENCES orgs (id),
    name        text        NOT NULL,
    weight      integer     NOT NULL DEFAULT 1 CHECK (weight > 0),
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

-- external:        counterparty for funding the org pool; the only account allowed to go negative
-- org_pool:        credits the org has funded but not yet granted to teams
-- provider_spend:  actual cost charged for provider calls (reconciled against the provider's bill)
-- retry_loss:      provider cost of failed attempts, absorbed by the org
-- team_available:  a team's spendable credits
-- team_held:       a team's credits reserved by open holds
CREATE TYPE account_kind AS ENUM (
    'external', 'org_pool', 'provider_spend', 'retry_loss',
    'team_available', 'team_held'
);

CREATE TABLE accounts (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      bigint       NOT NULL REFERENCES orgs (id),
    team_id     bigint       REFERENCES teams (id),
    kind        account_kind NOT NULL,
    created_at  timestamptz  NOT NULL DEFAULT now(),
    -- team accounts belong to a team; org accounts don't
    CHECK ((kind IN ('team_available', 'team_held')) = (team_id IS NOT NULL))
);

CREATE UNIQUE INDEX accounts_one_per_org_kind  ON accounts (org_id, kind)  WHERE team_id IS NULL;
CREATE UNIQUE INDEX accounts_one_per_team_kind ON accounts (team_id, kind) WHERE team_id IS NOT NULL;

-- One hot row per account, maintained by a trigger on entries (V2).
-- non_negative is copied from the account kind so the CHECK needs no lookup.
CREATE TABLE balances (
    account_id    bigint  PRIMARY KEY REFERENCES accounts (id),
    amount        bigint  NOT NULL DEFAULT 0,
    non_negative  boolean NOT NULL,
    CONSTRAINT balance_non_negative CHECK (amount >= 0 OR NOT non_negative)
);

CREATE TYPE posting_kind AS ENUM (
    'fund', 'grant', 'hold', 'settle', 'release', 'expire'
);

CREATE TABLE postings (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id           bigint       NOT NULL REFERENCES orgs (id),
    kind             posting_kind NOT NULL,
    idempotency_key  text         NOT NULL UNIQUE,
    -- hash of the request that created the posting: same key + different hash = conflict
    request_hash     bytea        NOT NULL,
    -- settled on a counted estimate because usage never arrived; reconciliation corrects it
    flagged          boolean      NOT NULL DEFAULT false,
    created_at       timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE entries (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    posting_id  bigint      NOT NULL REFERENCES postings (id),
    account_id  bigint      NOT NULL REFERENCES accounts (id),
    amount      bigint      NOT NULL CHECK (amount <> 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX entries_by_posting ON entries (posting_id);
CREATE INDEX entries_by_account ON entries (account_id);

CREATE TYPE hold_state AS ENUM ('held', 'settled', 'released', 'expired');

CREATE TABLE holds (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team_id           bigint      NOT NULL REFERENCES teams (id),
    amount            bigint      NOT NULL CHECK (amount > 0),
    state             hold_state  NOT NULL DEFAULT 'held',
    hold_posting_id   bigint      NOT NULL UNIQUE REFERENCES postings (id),
    close_posting_id  bigint      UNIQUE REFERENCES postings (id),
    settled_amount    bigint,
    expires_at        timestamptz NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    closed_at         timestamptz,
    CHECK ((state = 'held') = (close_posting_id IS NULL)),
    CHECK ((state = 'held') = (closed_at IS NULL)),
    CHECK ((state = 'settled') = (settled_amount IS NOT NULL)),
    CHECK (settled_amount IS NULL OR settled_amount BETWEEN 0 AND amount)
);

-- The sweeper scans open holds by expiry.
CREATE INDEX holds_open_by_expiry ON holds (expires_at) WHERE state = 'held';

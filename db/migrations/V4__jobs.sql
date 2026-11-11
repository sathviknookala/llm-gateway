-- Job queue (asynchronous path). A job owns exactly one hold, placed in the
-- same transaction as the job row. Attempts that fail after the provider may
-- have billed them post their cost to the org's retry_loss account.

-- Added here, first used at runtime: a value added by ALTER TYPE can't be used
-- in the transaction that adds it.
ALTER TYPE posting_kind ADD VALUE 'retry_loss';

CREATE TYPE job_state AS ENUM ('queued', 'running', 'succeeded', 'failed', 'dead');

-- succeeded:  the attempt's result was stored and the hold settled
-- retryable:  failed in a way worth retrying (provider 429/5xx, cut stream, timeout)
-- permanent:  refused for good (provider 4xx); the job failed
-- lost:       the lease expired and the sweeper reclaimed it; cost unknown
-- fenced:     a worker finished after losing its lease; its cost is booked, the job untouched
CREATE TYPE attempt_outcome AS ENUM ('succeeded', 'retryable', 'permanent', 'lost', 'fenced');

CREATE TABLE jobs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team_id          bigint      NOT NULL REFERENCES teams (id),
    hold_id          bigint      NOT NULL UNIQUE REFERENCES holds (id),
    -- the client's Idempotency-Key, if it sent one
    idempotency_key  text,
    -- sha256 of the stored request: same key + different hash = conflict
    request_hash     bytea       NOT NULL,
    model            text        NOT NULL,
    -- the client's body with the output cap applied and stream fields removed
    request          jsonb       NOT NULL,
    prompt_estimate  integer     NOT NULL CHECK (prompt_estimate > 0),
    max_tokens       integer     NOT NULL CHECK (max_tokens > 0),
    state            job_state   NOT NULL DEFAULT 'queued',
    -- attempts started: the retry budget. A rate-limit requeue gives one back.
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL CHECK (max_attempts > 0),
    -- +1 on every lease and never decremented: the fencing token
    lease_token      bigint      NOT NULL DEFAULT 0,
    run_after        timestamptz NOT NULL DEFAULT now(),
    locked_until     timestamptz,
    worker           text,
    deadline         timestamptz NOT NULL,
    last_error       text,
    result           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    UNIQUE (team_id, idempotency_key),
    CHECK (attempts BETWEEN 0 AND max_attempts),
    CHECK ((state = 'running') = (locked_until IS NOT NULL)),
    CHECK ((state = 'succeeded') = (result IS NOT NULL)),
    CHECK ((state IN ('succeeded', 'failed', 'dead')) = (finished_at IS NOT NULL))
);

-- Workers lease ready jobs in this order; the sweeper scans leases and deadlines.
CREATE INDEX jobs_ready ON jobs (run_after, id) WHERE state = 'queued';
CREATE INDEX jobs_leased ON jobs (locked_until) WHERE state = 'running';
CREATE INDEX jobs_queued_deadline ON jobs (deadline) WHERE state = 'queued';

-- One row per finished attempt. Written once; the only update is lost -> fenced,
-- when a worker whose lease was reclaimed reports its cost after all.
CREATE TABLE job_attempts (
    job_id                 bigint          NOT NULL REFERENCES jobs (id),
    attempt                integer         NOT NULL CHECK (attempt > 0),
    outcome                attempt_outcome NOT NULL,
    cost                   bigint          NOT NULL DEFAULT 0 CHECK (cost >= 0),
    -- cost is a counted estimate, not the provider's usage
    flagged                boolean         NOT NULL DEFAULT false,
    retry_loss_posting_id  bigint          UNIQUE REFERENCES postings (id),
    error                  text,
    finished_at            timestamptz     NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, attempt),
    CHECK (retry_loss_posting_id IS NULL OR outcome IN ('retryable', 'fenced'))
);

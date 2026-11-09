-- API keys for the gateway. Only a SHA-256 hash of each key is stored; the
-- plaintext is shown once, when the key is created.

CREATE TABLE api_keys (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    team_id     bigint      NOT NULL REFERENCES teams (id),
    key_hash    bytea       NOT NULL UNIQUE CHECK (octet_length(key_hash) = 32),
    -- first characters of the plaintext, so an operator can tell keys apart
    prefix      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE INDEX api_keys_by_team ON api_keys (team_id);

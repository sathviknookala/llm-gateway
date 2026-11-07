-- Ledger invariants enforced by the database.

-- 1. Every account gets a balance row. Only `external` may go negative.
CREATE FUNCTION ledger_create_balance() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO balances (account_id, amount, non_negative)
    VALUES (NEW.id, 0, NEW.kind <> 'external');
    RETURN NULL;
END
$$;

CREATE TRIGGER accounts_create_balance
    AFTER INSERT ON accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_create_balance();

-- 2. Balances move in the same transaction as their entries. The CHECK on
--    balances fires here, immediately, so an overdraft fails the INSERT.
--    Callers insert a posting's entries in account_id order to keep row-lock
--    order consistent and avoid deadlocks.
CREATE FUNCTION ledger_apply_entry() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE balances SET amount = amount + NEW.amount WHERE account_id = NEW.account_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER entries_apply_to_balance
    AFTER INSERT ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_entry();

-- 3. Every posting has at least two entries summing to zero, checked at COMMIT.
--    Fires from both tables so a posting with no entries is caught too.
CREATE FUNCTION ledger_check_posting_balanced() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pid    bigint;
    total  numeric;
    n      integer;
BEGIN
    IF TG_TABLE_NAME = 'entries' THEN
        pid := NEW.posting_id;
    ELSE
        pid := NEW.id;
    END IF;

    SELECT coalesce(sum(amount), 0), count(*) INTO total, n
    FROM entries WHERE posting_id = pid;

    IF n < 2 OR total <> 0 THEN
        RAISE EXCEPTION 'posting % is unbalanced: % entries summing to %', pid, n, total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER postings_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_posting_balanced();

CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_posting_balanced();

-- 4. The ledger is append-only. Corrections are new postings.
CREATE FUNCTION ledger_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is not allowed: the ledger is append-only', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END
$$;

CREATE TRIGGER entries_append_only
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();

CREATE TRIGGER postings_append_only
    BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();

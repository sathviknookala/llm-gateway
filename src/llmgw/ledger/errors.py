"""Ledger errors."""


class LedgerError(Exception):
    pass


class InsufficientCredits(LedgerError):
    """A posting would take a non-negative balance below zero."""


class IdempotencyConflict(LedgerError):
    """An idempotency key was reused with a different request."""


class InvalidHoldState(LedgerError):
    """The hold was already closed (settled, released or expired) by another path."""

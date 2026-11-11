"""Job queue errors."""


class JobError(Exception):
    pass


class JobNotFound(JobError):
    pass


class JobConflict(JobError):
    """An Idempotency-Key was reused with a different request."""


class LeaseLost(JobError):
    """The lease token no longer matches: the job was reclaimed or re-leased."""

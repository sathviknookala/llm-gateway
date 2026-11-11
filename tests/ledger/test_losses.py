"""llmgw.ledger.losses.post_retry_loss."""

import pytest

from llmgw.ledger import accounts, losses, postings
from llmgw.ledger.errors import IdempotencyConflict
from llmgw.ledger.models import PostingKind


async def test_posts_external_to_retry_loss_even_with_an_empty_pool(conn, tx, org):
    # Grant the whole pool away: the loss must still post.
    t = await tx(accounts.create_team, org_id=org.org_id, name="B")
    await tx(postings.grant, team_id=t.team_id, amount=10_000, idempotency_key="all")
    assert await accounts.balance(conn, org.org_pool) == 0
    external_before = await accounts.balance(conn, org.external)

    p = await tx(losses.post_retry_loss, team_id=t.team_id, job_id=1, attempt=1, amount=70, flagged=True)

    assert p.kind == PostingKind.RETRY_LOSS
    assert p.idempotency_key == "retry_loss:1:1"
    assert p.flagged
    assert await accounts.balance(conn, org.retry_loss) == 70
    assert await accounts.balance(conn, org.external) == external_before - 70
    assert await accounts.balance(conn, t.available) == 10_000


async def test_replay_returns_the_same_posting(conn, tx, team, org):
    kw = dict(team_id=team.team_id, job_id=4, attempt=2, amount=30, flagged=False)
    first = await tx(losses.post_retry_loss, **kw)
    second = await tx(losses.post_retry_loss, **kw)
    assert first.id == second.id
    assert await accounts.balance(conn, org.retry_loss) == 30


async def test_same_key_different_amount_conflicts(tx, team):
    await tx(losses.post_retry_loss, team_id=team.team_id, job_id=4, attempt=2, amount=30, flagged=False)
    with pytest.raises(IdempotencyConflict):
        await tx(losses.post_retry_loss, team_id=team.team_id, job_id=4, attempt=2, amount=31, flagged=False)


async def test_amount_must_be_positive(tx, team):
    with pytest.raises(ValueError):
        await tx(losses.post_retry_loss, team_id=team.team_id, job_id=1, attempt=1, amount=0, flagged=False)

"""Whole-ledger consistency check. Read-only.

Returns a list of human-readable violations; an empty list means the books
are consistent. Used by the property and concurrency tests after every step,
and later by fault tests and an ops `check-ledger` mode. Run it in a
REPEATABLE READ transaction for a consistent snapshot under live traffic.
"""

import asyncpg


async def check_ledger(conn: asyncpg.Connection) -> list[str]:
    problems: list[str] = []

    for r in await conn.fetch(
        """
        SELECT b.account_id, b.amount, coalesce(sum(e.amount), 0) AS from_entries
        FROM balances b LEFT JOIN entries e ON e.account_id = b.account_id
        GROUP BY b.account_id, b.amount
        HAVING b.amount <> coalesce(sum(e.amount), 0)
        """
    ):
        problems.append(
            f"account {r['account_id']}: balance {r['amount']} != sum of entries {r['from_entries']}"
        )

    for r in await conn.fetch(
        """
        SELECT p.id, count(e.id) AS n, coalesce(sum(e.amount), 0) AS total
        FROM postings p LEFT JOIN entries e ON e.posting_id = p.id
        GROUP BY p.id
        HAVING count(e.id) < 2 OR coalesce(sum(e.amount), 0) <> 0
        """
    ):
        problems.append(f"posting {r['id']}: {r['n']} entries summing to {r['total']}")

    total = await conn.fetchval("SELECT coalesce(sum(amount), 0) FROM balances")
    if total != 0:
        problems.append(f"balances sum to {total}, not 0")

    for r in await conn.fetch(
        """
        SELECT a.team_id, b.amount AS held,
               coalesce((SELECT sum(h.amount) FROM holds h
                          WHERE h.team_id = a.team_id AND h.state = 'held'), 0) AS open_holds
        FROM accounts a JOIN balances b ON b.account_id = a.id
        WHERE a.kind = 'team_held'
        """
    ):
        if r["held"] != r["open_holds"]:
            problems.append(
                f"team {r['team_id']}: held balance {r['held']} != open holds {r['open_holds']}"
            )

    for r in await conn.fetch(
        """
        SELECT p.id, p.kind FROM postings p
        WHERE (p.kind = 'hold' AND NOT EXISTS (SELECT 1 FROM holds h WHERE h.hold_posting_id = p.id))
           OR (p.kind IN ('settle', 'release', 'expire')
               AND NOT EXISTS (SELECT 1 FROM holds h WHERE h.close_posting_id = p.id))
        """
    ):
        problems.append(f"{r['kind']} posting {r['id']} has no matching hold")

    return problems

"""Settings read from the environment."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_pool_min: int
    db_pool_max: int


def load() -> Settings:
    return Settings(
        database_url=os.environ.get(
            "LLMGW_DATABASE_URL", "postgresql://llmgw:llmgw@localhost:5432/llmgw"
        ),
        db_pool_min=int(os.environ.get("LLMGW_DB_POOL_MIN", "1")),
        db_pool_max=int(os.environ.get("LLMGW_DB_POOL_MAX", "10")),
    )

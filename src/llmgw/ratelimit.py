"""Token-bucket rate limits in Redis: per team, and the shared provider budget per model.

Buckets:
- team TPM and RPM, one pair per team (same limits for every team for now);
- model TPM and RPM, shared by every team: the provider's budget.

A bucket's capacity is its per-minute limit and it refills at limit / 60 s, so
a full bucket allows one minute's worth in a burst. Levels are floats: slow
buckets (RPM = 60 is 0.001 per ms) must not lose fractional refill to rounding.

`acquire` runs one Lua script over every bucket the request touches. It is
all-or-nothing: if any bucket is short, nothing is debited, and the caller gets
the blocking buckets and how long until the request would fit. TPM buckets are
charged the request's worst case (prompt estimate + max_tokens, the same number
that sizes the hold); `refund` returns the unused part once the real usage is
known. RPM debits are never refunded.

Redis is not the source of truth. If it is unreachable or slow, `acquire`
allows the request (`debited=False`) and logs; money stays correct because the
ledger lives in Postgres. A request whose acquire didn't debit is never refunded.

The script reads the clock with Redis TIME, so every gateway process shares
one clock (needs Redis >= 5 for effects replication; we pin redis:7). Tests
pass an explicit `now_ms` instead. All keys must live on one Redis node: a
team key and a model key can't share a cluster slot.
"""

import asyncio
from dataclasses import dataclass, field

import orjson
import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError

log = structlog.get_logger("llmgw.ratelimit")

WINDOW_MS = 60_000
# Every bucket refills from empty in WINDOW_MS, so an idle key can expire after
# that without losing anything: a missing key reads as a full bucket.
KEY_TTL_MS = 2 * WINDOW_MS

# KEYS: bucket keys. ARGV[1]: now in ms, or "" to use Redis TIME.
# Then per bucket: capacity, rate per ms, cost, ttl ms.
# Returns {1, 0} when admitted, else {0, wait_ms, i, j, ...} (1-based blocking indexes).
_ACQUIRE = """
local now
if ARGV[1] ~= '' then
  now = tonumber(ARGV[1])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end
local levels, stamps, blocked, wait = {}, {}, {}, 0
for i = 1, #KEYS do
  local b = 1 + (i - 1) * 4
  local cap, rate, cost = tonumber(ARGV[b + 1]), tonumber(ARGV[b + 2]), tonumber(ARGV[b + 3])
  local h = redis.call('HMGET', KEYS[i], 'level', 'ts')
  local level, ts = tonumber(h[1]), tonumber(h[2])
  if level == nil or ts == nil then level, ts = cap, now end
  if now > ts then
    level = math.min(cap, level + (now - ts) * rate)
    ts = now
  end
  levels[i], stamps[i] = level, ts
  if level < cost then
    blocked[#blocked + 1] = i
    local w = math.ceil((cost - level) / rate)
    if w > wait then wait = w end
  end
end
if #blocked > 0 then
  local out = {0, wait}
  for _, i in ipairs(blocked) do out[#out + 1] = i end
  return out
end
for i = 1, #KEYS do
  local b = 1 + (i - 1) * 4
  local cost, ttl = tonumber(ARGV[b + 3]), tonumber(ARGV[b + 4])
  redis.call('HSET', KEYS[i], 'level', string.format('%.17g', levels[i] - cost),
             'ts', string.format('%.17g', stamps[i]))
  redis.call('PEXPIRE', KEYS[i], ttl)
end
return {1, 0}
"""

# Same layout as _ACQUIRE, with the per-bucket cost being the amount to give back.
# A missing key is a full bucket, so there is nothing to refund into it.
_REFUND = """
local now
if ARGV[1] ~= '' then
  now = tonumber(ARGV[1])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end
for i = 1, #KEYS do
  local b = 1 + (i - 1) * 4
  local cap, rate, amount, ttl = tonumber(ARGV[b + 1]), tonumber(ARGV[b + 2]),
                                 tonumber(ARGV[b + 3]), tonumber(ARGV[b + 4])
  local h = redis.call('HMGET', KEYS[i], 'level', 'ts')
  local level, ts = tonumber(h[1]), tonumber(h[2])
  if level ~= nil and ts ~= nil then
    if now > ts then
      level = level + (now - ts) * rate
      ts = now
    end
    level = math.min(cap, level + amount)
    redis.call('HSET', KEYS[i], 'level', string.format('%.17g', level),
               'ts', string.format('%.17g', ts))
    redis.call('PEXPIRE', KEYS[i], ttl)
  end
end
return 1
"""


@dataclass(frozen=True)
class ModelLimit:
    tpm: int | None = None
    rpm: int | None = None


@dataclass(frozen=True)
class RateLimits:
    """Per-minute limits. None means no bucket (unlimited)."""

    team_tpm: int | None = None
    team_rpm: int | None = None
    provider: dict[str, ModelLimit] = field(default_factory=dict)

    def __post_init__(self) -> None:
        limits = [self.team_tpm, self.team_rpm]
        limits += [x for m in self.provider.values() for x in (m.tpm, m.rpm)]
        if any(x is not None and x <= 0 for x in limits):
            raise ValueError("rate limits must be positive")


def parse_provider_limits(raw: str | None) -> dict[str, ModelLimit]:
    """LLMGW_PROVIDER_LIMITS: {"model": {"tpm": n, "rpm": n}, ...}. Unset → none."""
    if not raw:
        return {}
    table = orjson.loads(raw)
    return {
        model: ModelLimit(
            tpm=int(v["tpm"]) if v.get("tpm") is not None else None,
            rpm=int(v["rpm"]) if v.get("rpm") is not None else None,
        )
        for model, v in table.items()
    }


@dataclass(frozen=True)
class Bucket:
    scope: str  # "team" or "model": which limit blocked, for the 429 code
    key: str
    capacity: int

    @property
    def rate(self) -> float:
        return self.capacity / WINDOW_MS


@dataclass(frozen=True)
class Decision:
    allowed: bool
    # True only when the script actually debited the buckets. A request let
    # through because Redis was unavailable has debited=False and is never refunded.
    debited: bool
    blocked: tuple[str, ...] = ()  # scopes of the buckets that were short
    wait_ms: int = 0


def team_key(team_id: int, unit: str) -> str:
    return f"llmgw:rl:team:{team_id}:{unit}"


def model_key(model: str, unit: str) -> str:
    return f"llmgw:rl:model:{model}:{unit}"


class RateLimiter:
    def __init__(self, client: aioredis.Redis, limits: RateLimits, *, timeout_s: float) -> None:
        self.client = client
        self.limits = limits
        self.timeout_s = timeout_s
        self._acquire = client.register_script(_ACQUIRE)
        self._refund = client.register_script(_REFUND)

    @classmethod
    def from_url(cls, url: str, limits: RateLimits, *, timeout_s: float) -> "RateLimiter":
        client = aioredis.from_url(url, socket_timeout=timeout_s, socket_connect_timeout=timeout_s)
        return cls(client, limits, timeout_s=timeout_s)

    async def aclose(self) -> None:
        await self.client.aclose()

    def token_buckets(self, team_id: int, model: str) -> list[Bucket]:
        out = []
        if self.limits.team_tpm is not None:
            out.append(Bucket("team", team_key(team_id, "tpm"), self.limits.team_tpm))
        m = self.limits.provider.get(model)
        if m is not None and m.tpm is not None:
            out.append(Bucket("model", model_key(model, "tpm"), m.tpm))
        return out

    def request_buckets(self, team_id: int, model: str) -> list[Bucket]:
        out = []
        if self.limits.team_rpm is not None:
            out.append(Bucket("team", team_key(team_id, "rpm"), self.limits.team_rpm))
        m = self.limits.provider.get(model)
        if m is not None and m.rpm is not None:
            out.append(Bucket("model", model_key(model, "rpm"), m.rpm))
        return out

    def oversized(self, team_id: int, model: str, tokens: int) -> bool:
        """True if `tokens` exceeds a TPM bucket's whole capacity: it could never be admitted."""
        return any(tokens > b.capacity for b in self.token_buckets(team_id, model))

    async def acquire(self, team_id: int, model: str, tokens: int, *, now_ms: float | None = None) -> Decision:
        """Debit `tokens` from the TPM buckets and 1 from the RPM buckets, all or nothing."""
        charges = [(b, tokens) for b in self.token_buckets(team_id, model)]
        charges += [(b, 1) for b in self.request_buckets(team_id, model)]
        if not charges:
            return Decision(allowed=True, debited=False)
        keys, args = _layout(charges, now_ms)
        try:
            async with asyncio.timeout(self.timeout_s):
                result = await self._acquire(keys=keys, args=args)
        except (RedisError, OSError, TimeoutError) as e:
            log.warning("ratelimit_unavailable", op="acquire", error=repr(e))
            return Decision(allowed=True, debited=False)
        if int(result[0]) == 1:
            return Decision(allowed=True, debited=True)
        blocked = tuple(dict.fromkeys(charges[int(i) - 1][0].scope for i in result[2:]))
        return Decision(allowed=False, debited=False, blocked=blocked, wait_ms=int(result[1]))

    async def refund(self, team_id: int, model: str, tokens: int, *, now_ms: float | None = None) -> None:
        """Give `tokens` back to the TPM buckets, capped at capacity. Best effort; never raises."""
        if tokens <= 0:
            return
        charges = [(b, tokens) for b in self.token_buckets(team_id, model)]
        if not charges:
            return
        keys, args = _layout(charges, now_ms)
        try:
            async with asyncio.timeout(self.timeout_s):
                await self._refund(keys=keys, args=args)
        except (RedisError, OSError, TimeoutError) as e:
            log.warning("ratelimit_unavailable", op="refund", error=repr(e))

    async def level(self, key: str) -> float | None:
        """A bucket's stored level (before refill); None if the key is absent (full). For tests."""
        raw = await self.client.hget(key, "level")
        return None if raw is None else float(raw)


def _layout(charges: list[tuple[Bucket, int]], now_ms: float | None) -> tuple[list[str], list[str]]:
    keys = [b.key for b, _ in charges]
    args = ["" if now_ms is None else repr(float(now_ms))]
    for b, amount in charges:
        args += [str(b.capacity), repr(b.rate), str(amount), str(KEY_TTL_MS)]
    return keys, args

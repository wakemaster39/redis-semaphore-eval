import uuid
from contextlib import contextmanager
from typing import Generator, Optional, cast

from redis import Redis
from redis.client import Script


class FailedToAcquireLock(Exception):
    pass


class InvalidExpiry(Exception):
    pass


# ARGV[1] = id  # noqa: E800
# ARGV[2] = semaphore limit # noqa: E800
# ARGV[3] = expiry in seconds # noqa: E800
ACQUIRE_LOCK_SCRIPT = Script(
    None,
    b"""
redis.replicate_commands()
local expire_score = tonumber(redis.call("TIME")[1])
local purged_key_count = redis.call("zremrangebyscore", KEYS[1], '-inf', expire_score)

redis.call("del", KEYS[2])
for i=1,purged_key_count do redis.call("lpush", KEYS[2], 1) end
redis.call("pexpire", KEYS[2], 1000)

if redis.call("zcount", KEYS[1], '-inf', 'inf') < tonumber(ARGV[2]) then
    redis.call("zadd", KEYS[1], expire_score + tonumber(ARGV[3]), ARGV[1])
    return 1
else
    return 0
end
""",
)

# ARGV[1] = id # noqa: E800
# ARGV[2] = expiry in seconds # noqa: E800
EXTEND_LOCK_SCRIPT = Script(
    None,
    b"""
redis.replicate_commands()
local expire_score = tonumber(redis.call("TIME")[1])

if redis.call("zrank", KEYS[1], ARGV[1]) then
    redis.call("zadd", KEYS[1], expire_score + tonumber(ARGV[2]), ARGV[1])
    return 1
else
    return 0
end
""",
)

CONSUMED_LOCKS_SCRIPT = Script(
    None,
    b"""
redis.replicate_commands()
local expire_score = tonumber(redis.call("TIME")[1])
redis.call("zremrangebyscore", KEYS[1], '-inf', expire_score)
return redis.call("zcount", KEYS[1], '-inf', 'inf')
""",
)


def acquire_lock(
    redis: Redis, key: str, limit: int, expire_in: int = 60, signal_key: Optional[str] = None
) -> Optional[str]:
    if signal_key is None:
        signal_key = f"signal_key:{key}"
    if expire_in < 0:
        raise InvalidExpiry
    lock_id = str(uuid.uuid4())
    result = ACQUIRE_LOCK_SCRIPT(keys=[key, signal_key], args=[lock_id, limit, expire_in], client=redis)
    return lock_id if bool(result) else None


def extend_lock(redis: Redis, key: str, lock_id: str, expire_in: int = 60) -> bool:
    if expire_in < 0:
        raise InvalidExpiry
    result = EXTEND_LOCK_SCRIPT(keys=[key], args=[lock_id, expire_in], client=redis)
    return bool(result)


def clear_lock(
    redis: Redis,
    key: str,
    lock_id: str,
) -> None:
    redis.zrem(key, lock_id)


def consumed_locks(
    redis: Redis,
    key: str,
) -> int:
    return cast(int, CONSUMED_LOCKS_SCRIPT(keys=[key], args=[], client=redis))


@contextmanager
def semaphore(
    redis: Redis, key: str, limit: int, expire_in: int = 60, blocking: bool = True
) -> Generator[None, None, None]:
    lock_id = acquire_lock(redis, key, limit, expire_in)
    if lock_id is None and blocking:
        pass
    elif lock_id is None:
        raise FailedToAcquireLock
    try:
        yield
    finally:
        clear_lock(redis, key, lock_id)  # type: ignore

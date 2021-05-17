import threading
import weakref
from contextlib import contextmanager
from logging import getLogger
from typing import Generator, Optional, cast
from uuid import UUID, uuid4

from redis import Redis
from redis.client import Script


class FailedToAcquireLock(Exception):
    pass


class InvalidExpiry(Exception):
    pass


logger = getLogger()

# ARGV[1] = id  # noqa: E800
# ARGV[2] = semaphore limit # noqa: E800
# ARGV[3] = expiry in seconds # noqa: E800
# KEY[1] = lock key # noqa: E800
# KEY[2] = signal key # noqa: E800
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
# KEY[1] = lock key # noqa: E800
EXTEND_LOCK_SCRIPT = Script(
    None,
    b"""
redis.replicate_commands()
local expire_score = tonumber(redis.call("TIME")[1])

if redis.call("zscore", KEYS[1], ARGV[1]) then
    redis.call("zadd", KEYS[1], expire_score + tonumber(ARGV[2]), ARGV[1])
    return 1
else
    return 0
end
""",
)

# KEY[1] = lock key # noqa: E800
# KEY[2] = signal key # noqa: E800
CONSUMED_LOCKS_SCRIPT = Script(
    None,
    b"""
redis.replicate_commands()
local expire_score = tonumber(redis.call("TIME")[1])
local purged_key_count = redis.call("zremrangebyscore", KEYS[1], '-inf', expire_score)

redis.call("del", KEYS[2])
for i=1,purged_key_count do redis.call("lpush", KEYS[2], 1) end
redis.call("pexpire", KEYS[2], 1000)

return redis.call("zcount", KEYS[1], '-inf', 'inf')
""",
)

# ARGV[1] = id # noqa: E800
# KEY[1] = lock key # noqa: E800
# KEY[2] = signal key # noqa: E800
RELEASE_LOCK_SCRIPT = Script(
    None,
    b"""
if redis.call("zscore", KEYS[1], ARGV[1]) then
    redis.call("zrem", KEYS[1], ARGV[1])
    redis.call("lpush", KEYS[2], 1)
    redis.call("pexpire", KEYS[2], 1000)
    return 1
else
    return 0
end
""",
)


def acquire_lock(
    redis: Redis, key: str, limit: int, expire_in: int = 60, signal_key: Optional[str] = None
) -> Optional[UUID]:
    if signal_key is None:
        signal_key = f"signal_key:{key}"
    if expire_in < 0:
        raise InvalidExpiry
    lock_id = uuid4()
    result = ACQUIRE_LOCK_SCRIPT(keys=[key, signal_key], args=[str(lock_id), limit, expire_in], client=redis)
    return lock_id if bool(result) else None


def extend_lock(redis: Redis, key: str, lock_id: UUID, expire_in: int = 60) -> bool:
    if expire_in < 0:
        raise InvalidExpiry
    result = EXTEND_LOCK_SCRIPT(keys=[key], args=[str(lock_id), expire_in], client=redis)
    return bool(result)


def clear_lock(redis: Redis, key: str, lock_id: UUID, signal_key: Optional[str] = None) -> None:
    if signal_key is None:
        signal_key = f"signal_key:{key}"
    RELEASE_LOCK_SCRIPT(keys=[key, signal_key], args=[str(lock_id)], client=redis)


def consumed_locks(redis: Redis, key: str, signal_key: Optional[str] = None) -> int:
    if signal_key is None:
        signal_key = f"signal_key:{key}"
    return cast(int, CONSUMED_LOCKS_SCRIPT(keys=[key, signal_key], args=[], client=redis))


@contextmanager
def semaphore(
    redis: Redis,
    key: str,
    limit: int,
    expire_in: int = 60,
    blocking: bool = True,
    timeout: int = 0,
    signal_key: Optional[str] = None,
) -> Generator[UUID, None, None]:
    if timeout <= 0 and blocking:
        raise ValueError(f"Timeout {timeout} cannot be less than or equal to 0")

    if signal_key is None:
        signal_key = f"signal_key:{key}"
    lock_id = acquire_lock(redis, key=key, signal_key=signal_key, limit=limit, expire_in=expire_in)

    if lock_id is None and blocking:
        if redis.blpop(signal_key, timeout):
            lock_id = acquire_lock(redis, key=key, signal_key=signal_key, limit=limit, expire_in=expire_in)

    if lock_id is None:
        raise FailedToAcquireLock

    try:
        yield lock_id
    finally:
        clear_lock(redis, key=key, lock_id=lock_id, signal_key=signal_key)


@contextmanager
def auto_renewing_semaphore(
    redis: Redis,
    key: str,
    limit: int,
    auto_renewal_interval: int,
    expire_in: int = 60,
    blocking: bool = True,
    timeout: int = 0,
    signal_key: Optional[str] = None,
) -> Generator[UUID, None, None]:
    _lock_renewal_stop = threading.Event()
    _lock_renewal_thread = None
    try:
        with semaphore(
            redis=redis,
            key=key,
            limit=limit,
            expire_in=expire_in,
            blocking=blocking,
            timeout=timeout,
            signal_key=signal_key,
        ) as lock_id:
            _lock_renewal_thread = threading.Thread(
                group=None,
                target=__lock_renewer,
                kwargs={
                    "lockref": weakref.ref(lock_id),
                    "redisref": weakref.ref(redis),
                    "key": key,
                    "interval": auto_renewal_interval,
                    "expires_in": expire_in,
                    "stop": _lock_renewal_stop,
                },
            )
            _lock_renewal_thread.setDaemon(True)
            _lock_renewal_thread.start()
            yield lock_id
    finally:
        _lock_renewal_stop.set()
        if _lock_renewal_thread:
            _lock_renewal_thread.join()


def __lock_renewer(
    lockref: weakref.ReferenceType,
    redisref: weakref.ReferenceType,
    key: str,
    expires_in: int,
    interval: int,
    stop: threading.Event,
) -> None:
    while not stop.wait(timeout=interval):
        logger.debug("Refreshing lock")
        lock = lockref()
        redis = redisref()
        if lock is None:
            logger.debug("The lock no longer exists, stopping lock refreshing")
            break
        if redis is None:
            logger.debug("Redis no longer exists, stopping lock refreshing")
            break
        extend_lock(redis, key, lock, expire_in=expires_in)
        del lock
    logger.debug("Exit requested, stopping lock refreshing")

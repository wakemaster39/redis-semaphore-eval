import pytest
from redis import Redis

from redis_semaphore_eval import acquire_lock, extend_lock, clear_lock, consumed_locks, InvalidExpiry


@pytest.fixture()
def key() -> str:
    return "key"


@pytest.fixture()
def signal_key() -> str:
    return "signal_key"


@pytest.fixture()
def redis(key: str, signal_key: str) -> Redis:
    r = Redis(host="localhost", port=6379, db=0)
    yield r
    r.delete(key)
    r.delete(signal_key)


class TestAcquireLock:
    def test_returns_id_if_lock_acquired(self, redis: Redis, key: str, signal_key: str) -> None:
        assert acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=5)

    def test_returns_none_if_unable_to_acquire_lock(self, redis: Redis, key: str, signal_key: str) -> None:
        assert acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=10)
        assert acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=10)
        assert acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=10) is None

    def test_raises_if_invalid_expiry(self, redis: Redis, key: str, signal_key: str) -> None:
        with pytest.raises(InvalidExpiry):
            acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=-1)

    def test_purges_expired_keys(self, redis: Redis, key: str, signal_key: str) -> None:
        redis.zadd(
            key,
            {"qq": 0, "qq2": 0},
        )
        lock = acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=5)

        assert redis.zrangebyscore(key, "-inf", "inf") == [lock.encode("utf-8")]

    def test_pushes_to_signal_key_for_each_expired_key(self, redis: Redis, key: str, signal_key: str) -> None:
        redis.zadd(
            key,
            {"qq": 0, "qq2": 0},
        )
        acquire_lock(redis, key=key, signal_key=signal_key, limit=2, expire_in=5)
        assert redis.llen(signal_key) == 2


class TestExtendLock:
    def test_returns_true_if_extended_lock(self, redis: Redis, key: str) -> None:
        lock_id = acquire_lock(redis, key=key, limit=2, expire_in=10)
        assert extend_lock(redis, lock_id=lock_id, key=key, expire_in=10)

    def test_returns_false_if_lock_was_expired(self, redis: Redis, key: str) -> None:
        assert extend_lock(redis, lock_id="bad_id", key=key, expire_in=10) is False

    def test_raises_if_invalid_expiry(self, redis: Redis, key: str) -> None:
        with pytest.raises(InvalidExpiry):
            acquire_lock(redis, key=key, limit=2, expire_in=-1)


class TestClearLock:
    def test_does_not_error_if_lock_has_been_expired(self, redis: Redis, key: str) -> None:
        clear_lock(redis, key, "qq")

    def test_removes_lock(self, redis: Redis, key: str) -> None:
        lock_id = acquire_lock(redis, key=key, limit=2, expire_in=10)
        clear_lock(redis, key, lock_id)
        assert redis.zrank(key, lock_id) is None


class TestConsumedLocks:
    def test_returns_consumed_locks(self, redis: Redis, key: str) -> None:
        acquire_lock(redis, key=key, limit=2, expire_in=5)
        assert consumed_locks(redis, key=key) == 1
        lock_id = acquire_lock(redis, key=key, limit=2, expire_in=5)
        assert consumed_locks(redis, key=key) == 2
        clear_lock(redis, key, lock_id)
        assert consumed_locks(redis, key=key) == 1

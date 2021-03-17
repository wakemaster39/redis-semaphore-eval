# Redis Semaphore Eval

[![codecov](https://codecov.io/gh/wakemaster39/redis-semaphore-eval/branch/master/graph/badge.svg?token=BHTUPI4A0A)](https://codecov.io/gh/wakemaster39/redis-semaphore-eval)
[![Actions Status](https://github.com/wakemaster39/redis-semaphore-eval/workflows/Tests/badge.svg)](https://github.comwakemaster39/redis-semaphore-eval/actions)
[![Version](https://img.shields.io/pypi/v/redis-semaphore-eval)](https://pypi.org/project/redis-semaphore-eval/)
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/redis-semaphore-eval.svg)](https://pypi.org/project/redis-semaphore-eval/)
[![Pyversions](https://img.shields.io/pypi/pyversions/redis-semaphore-eval.svg)](https://pypi.org/project/redis-semaphore-eval/)

https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-3-counting-semaphores/

## Usage
To acquire a lock:
```python
from redis import Redis
from redis_semaphore_eval import semaphore

redis = Redis(host="localhost", port=6379, db=0)
key = "unique_lock_key"
with semaphore(redis, key=key, limit=2, expire_in=5, timeout=1) as lock_id:
    ...
```

To acquire a lock but continuously renew it in a background thread:
```python
from redis import Redis
from redis_semaphore_eval import auto_renewing_semaphore

redis = Redis(host="localhost", port=6379, db=0)
key = "unique_lock_key"
with auto_renewing_semaphore(
    redis,
    key=key,
    limit=2,
    expire_in=5,
    timeout=1,
    auto_renewal_interval=4
) as lock_id:
    ...
```



## Contributing

```bash
poetry run pre-commit install -t pre-commit -t commit-msg && poetry run pre-commit run --all
docker-compose up -d
poetry run python -m pytest
docker-compose down
```

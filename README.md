# Redis Semaphore Eval

[![codecov](https://codecov.io/gh/wakemaster39/redis-semaphore-eval/branch/master/graph/badge.svg?token=BHTUPI4A0A)](https://codecov.io/gh/wakemaster39/redis-semaphore-eval)
[![Actions Status](https://github.com/wakemaster39/redis-semaphore-eval/workflows/Tests/badge.svg)](https://github.comwakemaster39/redis-semaphore-eval/actions)
[![Version](https://img.shields.io/pypi/v/redis-semaphore-eval)](https://pypi.org/project/redis-semaphore-eval/)
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/redis-semaphore-eval.svg)](https://pypi.org/project/redis-semaphore-eval/)
[![Pyversions](https://img.shields.io/pypi/pyversions/redis-semaphore-eval.svg)](https://pypi.org/project/redis-semaphore-eval/)

https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-3-counting-semaphores/

## Contributing

```
poetry run pre-commit install -t pre-commit -t commit-msg && poetry run pre-commit run --all
docker-compose up -d
poetry run python -m pytest
docker-compose down
```

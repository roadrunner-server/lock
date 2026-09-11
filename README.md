# Lock plugin

The lock plugin provides exclusive and shared locks through the [RoadRunner lock RPC API](https://docs.roadrunner.dev/docs/plugins/locks.md).

## Backends

Omit the `lock` section to use in-memory locks. Each RoadRunner instance then has its own lock state.

The memory backend gives the same `ForceRelease` result as Redis: `Ok: true` only if the call removed at least one lock. It removes a released lock a moment after the `Release` reply. Until then `Exists` and `ForceRelease` still report that lock.

Both backends refuse a second read lock with the same ID on the same resource. Use `UpdateTTL` to extend a held lock.

Configure Redis to share locks between RoadRunner instances:

```yaml
lock:
  driver: redis
  config:
    addrs: ["127.0.0.1:6379"]
    username: ""
    password: ""
    db: 0
```

The `lock` section requires `driver: redis`. Invalid configuration and connection failures stop plugin initialization.

Redis requires version 7 or later. The backend uses `go-redis/v9`. One address selects a standalone client. Multiple addresses select a cluster client. The default address is `127.0.0.1:6379`. Authentication is optional. The default database is `0`.

Optional `dial_timeout`, `read_timeout`, and `write_timeout` settings accept Go durations, such as `5s`. Omitted timeouts use the Redis client defaults.

## Redis lock behavior

- `Lock` acquires exclusive access. It can promote a read lock when that caller holds the only read lock. An existing write lock also blocks another acquisition with the same ID.
- `LockRead` permits multiple readers while the resource has no writer. An existing read lock blocks another read acquisition with the same ID. A positive wait does not renew that read lock.
- `Release` removes the lock with the supplied ID.
- `ForceRelease` removes all locks on the resource. It accepts an empty ID. It returns `Ok: true` only if it removed at least one lock.
- `Exists` checks the supplied ID. The ID `"*"` checks for any lock on the resource.
- `UpdateTTL` replaces the supplied lock's TTL from the current time. An expired lock cannot be renewed.

RPC TTLs and wait times use microseconds. Each reader has its own TTL. A zero TTL creates a persistent lock. Redis server time controls expiration. The backend stores lock state in one sorted set per resource under the `rr:lock:` prefix. Lua scripts check ownership and change lock state atomically.

A zero wait makes one acquisition attempt. Redis network timeouts still apply. A positive wait bounds acquisition. Waiting calls use Redis Pub/Sub notifications and expiry timers. Lock contention and wait expiry return `Ok: false`. Redis command failures return RPC errors.

Stopping the plugin cancels waiting calls and closes its Redis client. Stored locks remain available to other RoadRunner instances until release or expiry.

## Tests

Start a test Redis instance:

```sh
docker run --rm -d --name rr-lock-redis -p 127.0.0.1:16379:6379 redis:7-alpine redis-server --save "" --appendonly no
```

Run both Go modules from the repository root:

```sh
go test -race -timeout 20m ./... ./tests/...
```

Set `RR_LOCK_REDIS_ADDR` to use another test Redis address. Redis tests require a dedicated server because the connection tests pause commands and disconnect Pub/Sub clients.

Stop the test server after the tests:

```sh
docker stop rr-lock-redis
```

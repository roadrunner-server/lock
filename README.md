# Lock plugin

The lock plugin provides exclusive and shared locks through the [RoadRunner lock RPC API](https://docs.roadrunner.dev/docs/plugins/locks.md).

## Backends

Omit the `lock` section or set `driver: memory` to use in-memory locks. Each RoadRunner instance then has its own lock state.

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
    pool_size: 0
```

The `lock` section requires `driver: memory` or `driver: redis`. Invalid configuration and connection failures stop plugin initialization.

Redis requires version 7 or later. The backend uses `go-redis/v9`. A set `master_name` selects a failover client for any number of addresses. Without `master_name`, one address selects a standalone client and two or more addresses select a cluster client. The default address is `127.0.0.1:6379`. Authentication is optional. The default database is `0`. The `db` setting applies to a standalone client and to a failover client. A cluster client uses database 0 only. Without `master_name`, a non-zero `db` with more than one address is rejected at startup.

Set `master_name` to use Redis Sentinel. The `addrs` list then holds the Sentinel addresses. Set `sentinel_password` when the Sentinel nodes need their own password.

```yaml
lock:
  driver: redis
  config:
    addrs: ["127.0.0.1:26379"]
    master_name: "mymaster"
    sentinel_password: ""
```

Set `pool_size` to size the connection pool for one Redis node. The client opens more connections when the pool is busy. The value `0` selects the `go-redis` default.

Optional `dial_timeout`, `read_timeout`, and `write_timeout` settings accept Go durations, such as `5s`. Omitted timeouts use the Redis client defaults. Negative timeouts are rejected at startup. This includes the values `-1` and `-2`, which the Redis client reads as no timeout. Plugin initialization tests the connection with one `PING` command. The client dials each address up to five times. The `dial_timeout` bounds each attempt and the `read_timeout` bounds the reply.

Add the `tls` block to connect with TLS:

```yaml
lock:
  driver: redis
  config:
    addrs: ["cache.example.com:6379"]
    tls:
      root_ca: ""
```

Set `root_ca` to the PEM file of a private certificate authority. An empty `root_ca` selects the system root certificates. Set `cert` and `key` together to send a client certificate. The backend reads the pair for each handshake, so a renewed certificate needs no restart. The minimum protocol version is TLS 1.2.

Write at least one key in the `tls` block. The configuration reader drops a block that has no keys, and the connection then stays plaintext. Write `root_ca: ""` for a server with a public certificate authority.

The `max_retries` key of the RoadRunner Redis plugin is absent. The backend keeps retries off, because a retry after a lost reply reports contention for a lock that the caller now holds. The `route_by_latency`, `route_randomly`, and `read_only` keys are absent, because the lock script writes and must run on the master. The `min_retry_backoff`, `max_retry_backoff`, `min_idle_conns`, `max_conn_age`, `pool_timeout`, `idle_timeout`, and `idle_check_freq` keys are absent as well.

Both backends accept a `ttl` and a `wait` from 0 to 9223372036854775 microseconds. That limit is the largest microsecond count which fits a Go duration. `Lock`, `LockRead`, and `UpdateTTL` reject a `ttl` outside this range. All methods reject a `wait` outside this range. A rejected request returns an RPC error and changes no lock state.

## Redis lock behavior

- `Lock` acquires exclusive access. It can promote a read lock when that caller holds the only read lock. An existing write lock also blocks another acquisition with the same ID.
- `LockRead` permits multiple readers while the resource has no writer. An existing read lock blocks another read acquisition with the same ID. A positive wait does not renew that read lock.
- `Release` removes the lock with the supplied ID.
- `ForceRelease` removes all locks on the resource. It accepts an empty ID. It returns `Ok: true` only if it removed at least one lock.
- `Exists` checks the supplied ID. The ID `"*"` checks for any lock on the resource.
- `UpdateTTL` replaces the supplied lock's TTL from the current time. An expired lock cannot be renewed.

RPC TTLs and wait times use microseconds. Each reader has its own TTL. A zero TTL creates a persistent lock. Redis server time controls expiration. The backend stores lock state in one sorted set per resource under the `rr:lock:` prefix. The prefix is fixed. The resource name is the namespace. Give resources unique names when different applications share one Redis server. Lua scripts check ownership and change lock state atomically.

A zero wait makes one acquisition attempt. Redis network timeouts still apply. A positive wait bounds acquisition. Waiting calls use Redis Pub/Sub notifications and expiry timers. Lock contention returns `Ok: false`. Wait expiry while the call waits for a notification also returns `Ok: false`. A Redis command that fails or exceeds its deadline returns an RPC error. The lock state is then unknown. Call `Exists` or `Release` to find the state of the lock.

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

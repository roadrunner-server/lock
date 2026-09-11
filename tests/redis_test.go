package lock

import (
	"fmt"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisLockCompatibility(t *testing.T) {
	holder, contender, admin := redisRPCClients(t)
	resource := t.Name()

	for _, tt := range []struct {
		name      string
		held      string
		requested string
		id        string
		wantOK    bool
	}{
		{name: "writer excludes writer", held: "lock.Lock", requested: "lock.Lock", id: "other", wantOK: false},
		{name: "writer excludes reader", held: "lock.Lock", requested: "lock.LockRead", id: "other", wantOK: false},
		{name: "writer cannot reacquire", held: "lock.Lock", requested: "lock.Lock", id: "owner", wantOK: false},
		{name: "writer cannot downgrade", held: "lock.Lock", requested: "lock.LockRead", id: "owner", wantOK: false},
		{name: "reader excludes writer", held: "lock.LockRead", requested: "lock.Lock", id: "other", wantOK: false},
		{name: "readers share resource", held: "lock.LockRead", requested: "lock.LockRead", id: "other", wantOK: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, admin.Del(t.Context(), "rr:lock:"+resource).Err())
			var acquired lockV1.Response
			require.NoError(t, holder.Call(tt.held, &lockV1.Request{
				Resource: resource, Id: "owner",
			}, &acquired))
			require.True(t, acquired.GetOk())

			var response lockV1.Response
			require.NoError(t, contender.Call(tt.requested, &lockV1.Request{
				Resource: resource, Id: tt.id,
			}, &response))
			assert.Equal(t, tt.wantOK, response.GetOk())
		})
	}
}

func TestRedisReadLockPromotion(t *testing.T) {
	first, second, _ := redisRPCClients(t)
	resource := t.Name()
	for i, client := range []*rpc.Client{first, second} {
		var acquired lockV1.Response
		require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{
			Resource: resource, Id: fmt.Sprintf("reader-%d", i),
		}, &acquired))
		require.True(t, acquired.GetOk())
	}

	var blocked lockV1.Response
	require.NoError(t, second.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "reader-0"}, &blocked))
	require.False(t, blocked.GetOk(), "promotion requires the caller to be the only reader")

	var released lockV1.Response
	require.NoError(t, first.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "reader-1"}, &released))
	require.True(t, released.GetOk())

	var promoted lockV1.Response
	require.NoError(t, second.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "reader-0"}, &promoted))
	require.True(t, promoted.GetOk())

	var reader lockV1.Response
	require.NoError(t, first.Call("lock.LockRead", &lockV1.Request{Resource: resource, Id: "reader-2"}, &reader))
	assert.False(t, reader.GetOk(), "the promoted lock must exclude readers")
}

// Both backends must refuse a second read lock with the same ID, so that one
// release frees the resource.
func TestReadLockReacquireIsRefused(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cfg   *config.Plugin
		redis bool
	}{
		{name: "memory", cfg: &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}}},
		{name: "redis", cfg: &config.Plugin{Path: "configs/.rr-lock-redis.yaml"}, redis: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.redis {
				redisAdmin(t, 0)
			}
			client, _ := lockRPCClient(t, tt.cfg)
			resource := t.Name()

			var acquired lockV1.Response
			require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{Resource: resource, Id: "reader"}, &acquired))
			require.True(t, acquired.GetOk())

			var again lockV1.Response
			require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{Resource: resource, Id: "reader"}, &again))
			assert.False(t, again.GetOk(), "an existing read lock blocks another read acquisition with the same ID")

			var released lockV1.Response
			require.NoError(t, client.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "reader"}, &released))
			require.True(t, released.GetOk())

			var writer lockV1.Response
			require.NoError(t, client.Call("lock.Lock", &lockV1.Request{
				Resource: resource, Id: "writer", Wait: new(int64(1_000_000)),
			}, &writer))
			require.True(t, writer.GetOk(), "one release frees the resource")

			var writerReleased lockV1.Response
			require.NoError(t, client.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "writer"}, &writerReleased))
			require.True(t, writerReleased.GetOk())

			// A wait must not renew the caller's own read lock.
			var short lockV1.Response
			require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{
				Resource: resource, Id: "waiter", Ttl: new(int64(300_000)),
			}, &short))
			require.True(t, short.GetOk())

			var waited lockV1.Response
			require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{
				Resource: resource, Id: "waiter", Ttl: new(int64(300_000)), Wait: new(int64(1_000_000)),
			}, &waited))
			assert.False(t, waited.GetOk(), "a waiting caller must not reacquire at its own expiry")
		})
	}
}

func TestRedisReadLockReacquireAfterExpiryNotificationIsRefused(t *testing.T) {
	owner, other, admin := redisRPCClients(t)
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, owner.Call("lock.LockRead", &lockV1.Request{
		Resource: resource, Id: "owner", Ttl: new(int64(300_000)),
	}, &held))
	require.True(t, held.GetOk())

	var response lockV1.Response
	pending := owner.Go("lock.LockRead", &lockV1.Request{
		Resource: resource, Id: "owner", Ttl: new(int64(300_000)), Wait: new(int64(1_000_000)),
	}, &response, nil)

	// The duplicate can finish before it needs a subscription.
	var result *rpc.Call
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		select {
		case result = <-pending.Done:
			return
		default:
		}
		counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+resource).Result()
		require.NoError(c, err)
		require.EqualValues(c, 1, counts["rr:lock:"+resource])
	}, time.Second, time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var exists lockV1.Response
		require.NoError(c, other.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &exists))
		require.False(c, exists.GetOk())
	}, time.Second, 5*time.Millisecond)

	// A new reader publishes after the owner's member expires.
	var reader lockV1.Response
	require.NoError(t, other.Call("lock.LockRead", &lockV1.Request{Resource: resource, Id: "other"}, &reader))
	require.True(t, reader.GetOk())

	if result == nil {
		select {
		case result = <-pending.Done:
		case <-time.After(time.Second):
			t.Fatal("duplicate read acquisition did not complete")
		}
	}
	require.NoError(t, result.Error)
	assert.False(t, response.GetOk(), "a notification after expiry must not revive a refused read acquisition")

	var exists lockV1.Response
	require.NoError(t, other.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &exists))
	assert.False(t, exists.GetOk(), "a refused read acquisition must not recreate the expired member")
}

func TestRedisConcurrentWriters(t *testing.T) {
	first, second, _ := redisRPCClients(t)
	clients := []*rpc.Client{first, second}
	const writers = 20
	responses := make([]lockV1.Response, writers)
	errs := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range writers {
		wg.Go(func() {
			<-start
			errs[i] = clients[i%len(clients)].Call("lock.Lock", &lockV1.Request{
				Resource: t.Name(), Id: fmt.Sprintf("writer-%d", i),
			}, &responses[i])
		})
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := range writers {
		require.NoError(t, errs[i], "writer %d", i)
		if responses[i].GetOk() {
			winners++
		}
	}
	assert.Equal(t, 1, winners, "one writer must acquire the shared resource")
}

func TestRedisReleaseOwnership(t *testing.T) {
	holder, releaser, admin := redisRPCClients(t)
	resource := t.Name()
	for _, tt := range []struct {
		name         string
		acquire      string
		id           string
		wantReleased bool
		wantHeld     bool
	}{
		{name: "writer owner", acquire: "lock.Lock", id: "owner", wantReleased: true, wantHeld: false},
		{name: "writer foreign id", acquire: "lock.Lock", id: "other", wantReleased: false, wantHeld: true},
		{name: "reader owner", acquire: "lock.LockRead", id: "owner", wantReleased: true, wantHeld: false},
		{name: "reader foreign id", acquire: "lock.LockRead", id: "other", wantReleased: false, wantHeld: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, admin.Del(t.Context(), "rr:lock:"+resource).Err())
			var acquired lockV1.Response
			require.NoError(t, holder.Call(tt.acquire, &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
			require.True(t, acquired.GetOk())

			var released lockV1.Response
			require.NoError(t, releaser.Call("lock.Release", &lockV1.Request{Resource: resource, Id: tt.id}, &released))
			assert.Equal(t, tt.wantReleased, released.GetOk())

			var exists lockV1.Response
			require.NoError(t, releaser.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &exists))
			assert.Equal(t, tt.wantHeld, exists.GetOk())
		})
	}
}

func TestRedisExists(t *testing.T) {
	holder, observer, _ := redisRPCClients(t)
	resource := t.Name()
	var acquired lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
	require.True(t, acquired.GetOk())

	for _, tt := range []struct {
		name     string
		resource string
		id       string
		wantOK   bool
	}{
		{name: "owner", resource: resource, id: "owner", wantOK: true},
		{name: "foreign id", resource: resource, id: "other", wantOK: false},
		{name: "wildcard", resource: resource, id: "*", wantOK: true},
		{name: "missing resource", resource: resource + "/missing", id: "*", wantOK: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var response lockV1.Response
			require.NoError(t, observer.Call("lock.Exists", &lockV1.Request{Resource: tt.resource, Id: tt.id}, &response))
			assert.Equal(t, tt.wantOK, response.GetOk())
		})
	}
}

func TestRedisForceRelease(t *testing.T) {
	first, second, _ := redisRPCClients(t)
	resource := t.Name()
	for i, client := range []*rpc.Client{first, second} {
		var acquired lockV1.Response
		require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{
			Resource: resource, Id: fmt.Sprintf("reader-%d", i),
		}, &acquired))
		require.True(t, acquired.GetOk())
	}

	var released lockV1.Response
	require.NoError(t, second.Call("lock.ForceRelease", &lockV1.Request{Resource: resource}, &released))
	require.True(t, released.GetOk())

	var writer lockV1.Response
	require.NoError(t, first.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "writer"}, &writer))
	assert.True(t, writer.GetOk(), "force release must remove every reader")
}

func TestRedisUpdateTTL(t *testing.T) {
	holder, updater, admin := redisRPCClients(t)
	resource := t.Name()
	for _, tt := range []struct {
		name      string
		initialUS int64
		updatedUS int64
		wantMS    int64
	}{
		{name: "persistent to expiring", initialUS: 0, updatedUS: 10_000_000, wantMS: 10_000},
		{name: "expiring to persistent", initialUS: 30_000_000, updatedUS: 0, wantMS: -1},
		{name: "shorten", initialUS: 30_000_000, updatedUS: 10_000_000, wantMS: 10_000},
		{name: "extend", initialUS: 10_000_000, updatedUS: 30_000_000, wantMS: 30_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, admin.Del(t.Context(), "rr:lock:"+resource).Err())
			var acquired lockV1.Response
			require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{
				Resource: resource, Id: "owner", Ttl: new(tt.initialUS),
			}, &acquired))
			require.True(t, acquired.GetOk())

			var updated lockV1.Response
			require.NoError(t, updater.Call("lock.UpdateTTL", &lockV1.Request{
				Resource: resource, Id: "owner", Ttl: new(tt.updatedUS),
			}, &updated))
			require.True(t, updated.GetOk())

			remaining, err := admin.Do(t.Context(), "PTTL", "rr:lock:"+resource).Int64()
			require.NoError(t, err)
			if tt.wantMS == -1 {
				assert.EqualValues(t, -1, remaining)
			} else {
				assert.InDelta(t, tt.wantMS, remaining, 1_000)
			}
		})
	}
}

func TestRedisReaderTTLsAreIndependent(t *testing.T) {
	first, second, _ := redisRPCClients(t)
	resource := t.Name()
	for i, client := range []*rpc.Client{first, second} {
		var acquired lockV1.Response
		require.NoError(t, client.Call("lock.LockRead", &lockV1.Request{
			Resource: resource, Id: fmt.Sprintf("reader-%d", i),
		}, &acquired))
		require.True(t, acquired.GetOk())
	}

	var updated lockV1.Response
	require.NoError(t, second.Call("lock.UpdateTTL", &lockV1.Request{
		Resource: resource, Id: "reader-0", Ttl: new(int64(50_000)),
	}, &updated))
	require.True(t, updated.GetOk())

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var expired lockV1.Response
		require.NoError(c, second.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "reader-0"}, &expired))
		require.False(c, expired.GetOk())
	}, time.Second, 5*time.Millisecond)

	var persistent lockV1.Response
	require.NoError(t, first.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "reader-1"}, &persistent))
	assert.True(t, persistent.GetOk())

	var writer lockV1.Response
	require.NoError(t, first.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "writer"}, &writer))
	assert.False(t, writer.GetOk(), "the remaining reader must still exclude writers")
}

func TestRedisExpiredOwnerCannotChangeReplacement(t *testing.T) {
	first, second, _ := redisRPCClients(t)
	resource := t.Name()
	var acquired lockV1.Response
	require.NoError(t, first.Call("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "expired", Ttl: new(int64(100)),
	}, &acquired))
	require.True(t, acquired.GetOk())

	var replacement lockV1.Response
	require.NoError(t, second.Call("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "replacement", Wait: new(int64(1_000_000)),
	}, &replacement))
	require.True(t, replacement.GetOk(), "a sub-millisecond TTL must expire")

	for _, method := range []string{"lock.UpdateTTL", "lock.Release"} {
		t.Run(method, func(t *testing.T) {
			var response lockV1.Response
			require.NoError(t, first.Call(method, &lockV1.Request{Resource: resource, Id: "expired"}, &response))
			assert.False(t, response.GetOk())
		})
	}
	var exists lockV1.Response
	require.NoError(t, first.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "replacement"}, &exists))
	assert.True(t, exists.GetOk())
}

func TestRedisWait(t *testing.T) {
	for _, tt := range []struct {
		name    string
		acquire string
		release string
		ttlUS   int64
	}{
		{name: "writer wakes on release", acquire: "lock.Lock", release: "lock.Release"},
		{name: "reader wakes on release", acquire: "lock.LockRead", release: "lock.Release"},
		{name: "force release", acquire: "lock.Lock", release: "lock.ForceRelease"},
		{name: "expiry after ttl change", acquire: "lock.Lock", release: "lock.UpdateTTL", ttlUS: 50_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			holder, waiter, admin := redisRPCClients(t)
			resource := t.Name()
			var held lockV1.Response
			require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
			require.True(t, held.GetOk())

			var acquired lockV1.Response
			pending := waiter.Go(tt.acquire, &lockV1.Request{
				Resource: resource, Id: "waiter", Wait: new(int64(2_000_000)),
			}, &acquired, nil)
			waitForSubscriber(t, admin, resource)
			select {
			case <-pending.Done:
				t.Fatal("acquisition completed while the resource was held")
			default:
			}

			var released lockV1.Response
			require.NoError(t, holder.Call(tt.release, &lockV1.Request{
				Resource: resource, Id: "holder", Ttl: new(tt.ttlUS),
			}, &released))
			require.True(t, released.GetOk())

			select {
			case result := <-pending.Done:
				require.NoError(t, result.Error)
			case <-time.After(time.Second):
				t.Fatal("acquisition did not complete after release or expiry")
			}
			assert.True(t, acquired.GetOk())
		})
	}
}

func TestRedisWaitersShareOneSubscription(t *testing.T) {
	admin := redisAdmin(t, 0)
	client, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	resource := t.Name()
	channel := "rr:lock:" + resource

	var held lockV1.Response
	require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())

	const waiters = 5
	responses := make([]lockV1.Response, waiters)
	done := make(chan *rpc.Call, waiters)
	for i := range responses {
		client.Go("lock.Lock", &lockV1.Request{
			Resource: resource, Id: fmt.Sprintf("waiter-%d", i), Wait: new(int64(3_000_000)),
		}, &responses[i], done)
	}

	subscribers := func() (int64, error) {
		counts, err := admin.PubSubNumSub(t.Context(), channel).Result()
		if err != nil {
			return 0, err
		}
		return counts[channel], nil
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		count, err := subscribers()
		require.NoError(c, err)
		require.EqualValues(c, 1, count, "the resource channel needs one subscriber")
	}, time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		count, err := subscribers()
		return err != nil || count != 1
	}, 200*time.Millisecond, 20*time.Millisecond, "a waiter opened its own subscription")

	var released lockV1.Response
	require.NoError(t, client.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())

	for range responses {
		select {
		case call := <-done:
			require.NoError(t, call.Error)
			require.True(t, call.Reply.(*lockV1.Response).GetOk(), "each waiter acquires the released resource")
			var next lockV1.Response
			require.NoError(t, client.Call("lock.Release", &lockV1.Request{
				Resource: resource, Id: call.Args.(*lockV1.Request).GetId(),
			}, &next))
			require.True(t, next.GetOk())
		case <-time.After(2 * time.Second):
			t.Fatal("a waiter did not acquire the released resource")
		}
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		count, err := subscribers()
		require.NoError(c, err)
		require.EqualValues(c, 0, count, "the last waiter leaves the resource channel")
	}, time.Second, 5*time.Millisecond)
}

func TestRedisWaitTimeout(t *testing.T) {
	holder, waiter, _ := redisRPCClients(t)
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())

	start := time.Now()
	var response lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(50_000)),
	}, &response, nil)
	select {
	case result := <-pending.Done:
		require.NoError(t, result.Error)
	case <-time.After(time.Second):
		t.Fatal("acquisition exceeded its wait timeout")
	}
	assert.False(t, response.GetOk())
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)

	var exists lockV1.Response
	require.NoError(t, waiter.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "waiter"}, &exists))
	assert.False(t, exists.GetOk())
}

func TestRedisAcquireDeadlineDuringCommand(t *testing.T) {
	redisAdmin(t, 0)
	proxy := slowRedis(t, 300*time.Millisecond)
	slow, _ := lockRPCClient(t, &config.Plugin{
		Type:      "yaml",
		ReadInCfg: fmt.Appendf(nil, "version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	observer, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	resource := t.Name()

	// Load the Lua script before the timed call.
	var warm lockV1.Response
	require.NoError(t, slow.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &warm))
	require.False(t, warm.GetOk())

	var response lockV1.Response
	err := slow.Call("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "owner", Wait: new(int64(100_000)),
	}, &response)
	require.Error(t, err, "a deadline during a Redis command must reach the caller")
	require.False(t, response.GetOk())

	var granted lockV1.Response
	require.NoError(t, observer.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &granted))
	assert.True(t, granted.GetOk(), "Redis grants the lock after the caller stops waiting")

	var released lockV1.Response
	require.NoError(t, observer.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "owner"}, &released))
	assert.True(t, released.GetOk())
}

func TestRedisWaitReconnect(t *testing.T) {
	holder, waiter, admin := redisRPCClients(t)
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())

	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(2_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)

	// Remove the lock while the subscriber is disconnected.
	_, err := admin.TxPipelined(t.Context(), func(pipe redis.Pipeliner) error {
		pipe.ClientKillByFilter(t.Context(), "TYPE", "pubsub")
		pipe.Del(t.Context(), "rr:lock:"+resource)
		return nil
	})
	require.NoError(t, err)

	select {
	case result := <-pending.Done:
		require.NoError(t, result.Error)
	case <-time.After(time.Second):
		t.Fatal("acquisition did not check lock state after reconnecting")
	}
	assert.True(t, acquired.GetOk())
}

func TestRedisZeroWaitAllowsNetworkLatency(t *testing.T) {
	client, _, admin := redisRPCClients(t)
	require.NoError(t, admin.ClientPause(t.Context(), 30*time.Millisecond).Err())

	var acquired lockV1.Response
	require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner"}, &acquired))
	assert.True(t, acquired.GetOk(), "zero wait must allow one Redis operation to complete")
}

func TestRedisStopCancelsWait(t *testing.T) {
	admin := redisAdmin(t, 0)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, stop := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())

	var response lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(2_000_000)),
	}, &response, nil)
	waitForSubscriber(t, admin, resource)
	require.NoError(t, stop())

	select {
	case result := <-pending.Done:
		require.Error(t, result.Error)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the waiting RPC")
	}
	assert.False(t, response.GetOk())

	var exists lockV1.Response
	require.NoError(t, holder.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "holder"}, &exists))
	assert.True(t, exists.GetOk())
}

func TestRedisLocksSurviveStop(t *testing.T) {
	redisAdmin(t, 0)
	holder, stop := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	resource := t.Name()
	var acquired lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
	require.True(t, acquired.GetOk())
	require.NoError(t, stop())

	replacement, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	var exists lockV1.Response
	require.NoError(t, replacement.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &exists))
	require.True(t, exists.GetOk())

	var released lockV1.Response
	require.NoError(t, replacement.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "owner"}, &released))
	assert.True(t, released.GetOk())
}

func TestRedisCommandError(t *testing.T) {
	client, _, admin := redisRPCClients(t)
	require.NoError(t, admin.Set(t.Context(), "rr:lock:"+t.Name(), "wrong type", 0).Err())
	var response lockV1.Response
	err := client.Call("lock.Exists", &lockV1.Request{Resource: t.Name(), Id: "*"}, &response)
	require.ErrorContains(t, err, "WRONGTYPE")
}

func TestRedisEmptyID(t *testing.T) {
	client, _, _ := redisRPCClients(t)
	for _, method := range []string{"lock.Lock", "lock.LockRead", "lock.Release", "lock.Exists", "lock.UpdateTTL"} {
		t.Run(method, func(t *testing.T) {
			var response lockV1.Response
			err := client.Call(method, &lockV1.Request{Resource: t.Name()}, &response)
			require.EqualError(t, err, "empty ID is not allowed")
		})
	}
}

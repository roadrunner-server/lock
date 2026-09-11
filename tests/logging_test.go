package lock

import (
	"log/slog"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryBackendLogsDriver(t *testing.T) {
	cont, _, logs := observedLockContainer(t, &config.Plugin{Path: "configs/.rr-lock-init.yaml"})
	require.NoError(t, cont.Init())
	t.Cleanup(func() { assert.NoError(t, cont.Stop()) })

	entries := logs.FilterMessage("lock backend initialized").All()
	require.Len(t, entries, 1)
	assert.Equal(t, slog.LevelInfo, entries[0].Level)
	assert.Equal(t, "memory", entries[0].Attrs["driver"])
}

func TestRedisBackendLogsDriver(t *testing.T) {
	cont, _, logs := observedLockContainer(t, &config.Plugin{Path: "configs/.rr-lock-redis-db.yaml"})
	require.NoError(t, cont.Init())
	t.Cleanup(func() { assert.NoError(t, cont.Stop()) })

	entries := logs.FilterMessage("lock backend initialized").All()
	require.Len(t, entries, 1)
	assert.Equal(t, slog.LevelInfo, entries[0].Level)
	assert.Equal(t, "redis", entries[0].Attrs["driver"])
	assert.Equal(t, []string{redisAddr()}, entries[0].Attrs["addrs"])
	assert.EqualValues(t, 3, entries[0].Attrs["db"])
}

func TestRedisBackendLogsScriptFailure(t *testing.T) {
	admin := redisAdmin(t, 0)
	cont, plugin, logs := observedLockContainer(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	client, _ := serveLockRPC(t, cont, plugin)
	require.NoError(t, admin.Set(t.Context(), "rr:lock:"+t.Name(), "wrong type", 0).Err())

	var response lockV1.Response
	require.ErrorContains(t, client.Call("lock.Exists", &lockV1.Request{Resource: t.Name(), Id: "*"}, &response), "WRONGTYPE")

	entries := logs.FilterMessage("redis lock script failed").All()
	require.Len(t, entries, 1)
	assert.Equal(t, slog.LevelError, entries[0].Level)
	assert.Equal(t, "exists", entries[0].Attrs["op"])
	assert.Equal(t, "rr:lock:"+t.Name(), entries[0].Attrs["key"])
	assert.Equal(t, "*", entries[0].Attrs["id"])
	failure, ok := entries[0].Attrs["error"].(error)
	require.True(t, ok)
	assert.ErrorContains(t, failure, "WRONGTYPE")
}

func TestRedisWaitExpiryLogsNoError(t *testing.T) {
	admin := redisAdmin(t, 0)
	cont, plugin, logs := observedLockContainer(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	client, _ := serveLockRPC(t, cont, plugin)
	require.NoError(t, admin.ClientPause(t.Context(), 300*time.Millisecond).Err())

	// The wait bound expires during the paused command. The call result depends on the wait contract.
	var acquired lockV1.Response
	_ = client.Call("lock.Lock", &lockV1.Request{
		Resource: t.Name(), Id: "waiter", Wait: new(int64(100_000)),
	}, &acquired)
	require.False(t, acquired.GetOk())

	assert.Empty(t, logs.FilterMessage("redis lock script failed").All(), "an expired wait bound is not a Redis failure")
}

func TestRedisBackendLogsResubscribe(t *testing.T) {
	admin := redisAdmin(t, 0)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	cont, plugin, logs := observedLockContainer(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := serveLockRPC(t, cont, plugin)
	resource := t.Name()

	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())

	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(3_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)

	// The holder keeps the lock, so the shared connection must subscribe again after the disconnect.
	require.NoError(t, admin.ClientKillByFilter(t.Context(), "TYPE", "pubsub").Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NotEmpty(c, logs.FilterMessage("redis lock subscription reconnected").All())
	}, 2*time.Second, time.Millisecond)

	entries := logs.FilterMessage("redis lock subscription reconnected").All()
	assert.Equal(t, slog.LevelWarn, entries[0].Level)
	assert.Equal(t, "rr:lock:"+resource, entries[0].Attrs["channel"])

	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())

	select {
	case result := <-pending.Done:
		require.NoError(t, result.Error)
	case <-time.After(2 * time.Second):
		t.Fatal("acquisition did not continue after the subscription reconnected")
	}
	assert.True(t, acquired.GetOk())
}

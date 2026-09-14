package lock

import (
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ForceRelease returns Ok: true only when it removed at least one live lock.
func TestForceReleaseReportsRemovedLocks(t *testing.T) {
	for _, backend := range []struct {
		name string
		cfg  *config.Plugin
		// prepare removes the leftover backend state for the resource.
		prepare func(t *testing.T)
	}{
		{
			name:    "memory",
			cfg:     &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}},
			prepare: func(*testing.T) {},
		},
		{
			name:    "redis",
			cfg:     &config.Plugin{Path: "configs/.rr-lock-redis.yaml"},
			prepare: func(t *testing.T) { redisAdmin(t, 0) },
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			client, _ := lockRPCClient(t, backend.cfg)

			// waitForRemoval waits until the backend reports no lock on the resource.
			waitForRemoval := func(t *testing.T, resource string) {
				t.Helper()
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					var live lockV1.Response
					require.NoError(c, client.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "*"}, &live))
					require.False(c, live.GetOk())
				}, 2*time.Second, 5*time.Millisecond)
			}

			t.Run("never locked", func(t *testing.T) {
				backend.prepare(t)

				var forced lockV1.Response
				require.NoError(t, client.Call("lock.ForceRelease", &lockV1.Request{Resource: t.Name()}, &forced))
				assert.False(t, forced.GetOk(), "a resource without locks has nothing to release")
			})

			t.Run("released lock", func(t *testing.T) {
				backend.prepare(t)
				resource := t.Name()

				var acquired lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
				require.True(t, acquired.GetOk())

				var released lockV1.Response
				require.NoError(t, client.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "owner"}, &released))
				require.True(t, released.GetOk())

				// The memory backend removes the lock after the release reply.
				waitForRemoval(t, resource)

				var forced lockV1.Response
				require.NoError(t, client.Call("lock.ForceRelease", &lockV1.Request{Resource: resource}, &forced))
				assert.False(t, forced.GetOk(), "the released lock is not a live lock")
			})

			t.Run("expired lock", func(t *testing.T) {
				backend.prepare(t)
				resource := t.Name()

				var acquired lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{
					Resource: resource, Id: "owner", Ttl: new(int64(100_000)),
				}, &acquired))
				require.True(t, acquired.GetOk())

				waitForRemoval(t, resource)

				var forced lockV1.Response
				require.NoError(t, client.Call("lock.ForceRelease", &lockV1.Request{Resource: resource}, &forced))
				assert.False(t, forced.GetOk(), "the expired lock is not a live lock")
			})

			t.Run("live lock", func(t *testing.T) {
				backend.prepare(t)
				resource := t.Name()

				var acquired lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &acquired))
				require.True(t, acquired.GetOk())

				var forced lockV1.Response
				require.NoError(t, client.Call("lock.ForceRelease", &lockV1.Request{Resource: resource}, &forced))
				assert.True(t, forced.GetOk(), "the live lock must be removed")

				var next lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{
					Resource: resource, Id: "next", Wait: new(int64(1_000_000)),
				}, &next))
				assert.True(t, next.GetOk(), "force release must free the resource")
			})
		})
	}
}

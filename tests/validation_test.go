package lock

import (
	"context"
	"math"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The largest microsecond count which fits a Go duration.
const maxAcceptedMicroseconds = 9223372036854775

const (
	ttlOutOfRangeMessage  = "ttl must be between 0 and 9223372036854775 microseconds"
	waitOutOfRangeMessage = "wait must be between 0 and 9223372036854775 microseconds"
)

type rejectedRequest struct {
	name    string
	method  string
	ttl     int64
	wait    int64
	hold    bool
	message string
}

func rejectedRequests() []rejectedRequest {
	return []rejectedRequest{
		{name: "negative ttl on lock", method: "lock.Lock", ttl: -1000000, message: ttlOutOfRangeMessage},
		{name: "negative ttl on read lock", method: "lock.LockRead", ttl: -1000000, message: ttlOutOfRangeMessage},
		{name: "negative ttl on update ttl", method: "lock.UpdateTTL", ttl: -1000000, hold: true, message: ttlOutOfRangeMessage},
		{name: "ttl above the limit", method: "lock.Lock", ttl: maxAcceptedMicroseconds + 1, message: ttlOutOfRangeMessage},
		{name: "overflowing ttl on lock", method: "lock.Lock", ttl: math.MaxInt64, message: ttlOutOfRangeMessage},
		{name: "negative wait on lock", method: "lock.Lock", wait: -1000000, message: waitOutOfRangeMessage},
		{name: "overflowing wait on lock", method: "lock.Lock", wait: math.MaxInt64, message: waitOutOfRangeMessage},
		{name: "negative wait on release", method: "lock.Release", wait: -1000000, message: waitOutOfRangeMessage},
		{name: "negative wait on exists", method: "lock.Exists", wait: -1000000, message: waitOutOfRangeMessage},
		{name: "negative wait on force release", method: "lock.ForceRelease", wait: -1000000, message: waitOutOfRangeMessage},
	}
}

func TestMemoryRejectsOutOfRangeRequest(t *testing.T) {
	client, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}})

	for _, tt := range rejectedRequests() {
		t.Run(tt.name, func(t *testing.T) {
			resource := t.Name()
			if tt.hold {
				var held lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner", Ttl: new(int64(60000000))}, &held))
				require.True(t, held.GetOk())
			}

			var rejected lockV1.Response
			require.EqualError(t, client.Call(tt.method, &lockV1.Request{
				Resource: resource, Id: "owner", Ttl: new(tt.ttl), Wait: new(tt.wait),
			}, &rejected), tt.message)

			var after lockV1.Response
			if tt.hold {
				require.NoError(t, client.Call("lock.Exists", &lockV1.Request{Resource: resource, Id: "owner"}, &after))
				assert.True(t, after.GetOk(), "a rejected request must keep the held lock")
				return
			}
			require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner"}, &after))
			assert.True(t, after.GetOk(), "a rejected request must leave the resource free")
		})
	}
}

func TestMemoryAcceptsRangeLimit(t *testing.T) {
	client, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-init.yaml", Flags: []string{"logs.level=error"}})

	var acquired lockV1.Response
	require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner", Ttl: new(int64(maxAcceptedMicroseconds)), Wait: new(int64(maxAcceptedMicroseconds))}, &acquired))
	assert.True(t, acquired.GetOk(), "the range limit must be an accepted value")
}

func TestRedisRejectsOutOfRangeRequest(t *testing.T) {
	admin := redisAdmin(t, 0)
	client, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})

	for _, tt := range rejectedRequests() {
		t.Run(tt.name, func(t *testing.T) {
			resource := t.Name()
			key := "rr:lock:" + resource
			require.NoError(t, admin.Del(t.Context(), key).Err())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, admin.Del(ctx, key).Err())
			})

			var expected int64
			if tt.hold {
				var held lockV1.Response
				require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "owner", Ttl: new(int64(60000000))}, &held))
				require.True(t, held.GetOk())
				expected = 1
			}

			var rejected lockV1.Response
			require.EqualError(t, client.Call(tt.method, &lockV1.Request{
				Resource: resource, Id: "owner", Ttl: new(tt.ttl), Wait: new(tt.wait),
			}, &rejected), tt.message)

			count, err := admin.Exists(t.Context(), key).Result()
			require.NoError(t, err)
			assert.Equal(t, expected, count, "a rejected request must not change Redis state")
		})
	}
}

func TestRedisAcceptsRangeLimit(t *testing.T) {
	admin := redisAdmin(t, 0)
	client, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})

	var acquired lockV1.Response
	require.NoError(t, client.Call("lock.Lock", &lockV1.Request{Resource: t.Name(), Id: "owner", Ttl: new(int64(maxAcceptedMicroseconds)), Wait: new(int64(maxAcceptedMicroseconds))}, &acquired))
	assert.True(t, acquired.GetOk(), "the range limit must be an accepted value")

	count, err := admin.Exists(t.Context(), "rr:lock:"+t.Name()).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)
}

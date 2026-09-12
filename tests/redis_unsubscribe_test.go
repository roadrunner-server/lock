package lock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisLostUnsubscribeReply(t *testing.T) {
	for _, tt := range []struct {
		name        string
		survivor    bool
		replacement bool
	}{
		{name: "new resource", survivor: true},
		{name: "replacement group", survivor: true, replacement: true},
		{name: "last channel", replacement: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			admin := redisAdmin(t, 0)
			a, b, c := t.Name(), t.Name()+"/B", t.Name()+"/C"
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, admin.Del(ctx, "rr:lock:"+b, "rr:lock:"+c).Err())
			})
			proxy, disconnected := redisUnsubscribeProxy(t, "rr:lock:"+a)
			holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
			waiter, _ := lockRPCClient(t, &config.Plugin{
				Type: "yaml", ReadInCfg: fmt.Appendf(nil,
					"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
			})
			for _, resource := range []string{a, b, c} {
				var held lockV1.Response
				require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
				require.True(t, held.GetOk())
			}
			before := redisScriptCalls(t, admin)
			var expired, surviving lockV1.Response
			short := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: a, Id: "expiring", Wait: new(int64(800_000)),
			}, &expired, nil)
			var long *rpc.Call
			checks := uint64(2)
			if tt.survivor {
				long = waiter.Go("lock.Lock", &lockV1.Request{
					Resource: b, Id: "survivor", Wait: new(int64(20_000_000)),
				}, &surviving, nil)
				checks = 4
			}
			require.Eventually(t, func() bool { return redisScriptCalls(t, admin) >= before+checks }, 500*time.Millisecond, time.Millisecond)
			clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
			require.NoError(t, err)
			fields := strings.Fields(clients)
			require.NotEmpty(t, fields)
			oldConnection := fields[0]
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("the proxy did not discard A's unsubscribe acknowledgment")
			}
			droppedAt := time.Now()
			require.NoError(t, (<-short.Done).Error)
			require.False(t, expired.GetOk())
			if tt.survivor {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
					require.NoError(c, err)
					require.NotContains(c, clients, oldConnection+" ")
					counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+b).Result()
					require.NoError(c, err)
					require.EqualValues(c, 1, counts["rr:lock:"+b], "the physical replacement must restore B")
				}, time.Second, time.Millisecond)
			}

			resource := c
			if tt.replacement {
				resource = a
			}
			before = redisScriptCalls(t, admin)
			var acquired lockV1.Response
			queued := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: resource, Id: "queued", Wait: new(int64(1_000_000)),
			}, &acquired, nil)
			require.Eventually(t, func() bool { return redisScriptCalls(t, admin) > before }, 300*time.Millisecond, time.Millisecond)
			var released lockV1.Response
			require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
			require.True(t, released.GetOk())
			select {
			case result := <-queued.Done:
				assert.NoError(t, result.Error)
				assert.True(t, acquired.GetOk(), "a lost unsubscribe reply must not stall a new or replacement waiter")
			case <-time.After(1500 * time.Millisecond):
				t.Fatal("the queued acquisition exceeded its wait")
			}
			if !tt.survivor {
				return
			}
			select {
			case result := <-long.Done:
				t.Fatalf("the restored unrelated waiter failed before its own deadline: %v", result.Error)
			case <-time.After(time.Until(droppedAt.Add(7 * time.Second))):
			}
			require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: b, Id: "holder"}, &released))
			require.True(t, released.GetOk())
			select {
			case result := <-long.Done:
				require.NoError(t, result.Error)
				require.True(t, surviving.GetOk(), "the surviving subscription must still deliver releases")
			case <-time.After(time.Second):
				t.Fatal("the restored unrelated waiter did not wake")
			}
		})
	}
}

// redisUnsubscribeProxy drops one completed UNSUBSCRIBE reply and closes its TCP connection.
func redisUnsubscribeProxy(t *testing.T, channel string) (string, <-chan struct{}) {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	disconnected := make(chan struct{})
	var dropped atomic.Bool
	var workers sync.WaitGroup
	ctx := t.Context()
	match := fmt.Appendf(nil, "$11\r\nunsubscribe\r\n$%d\r\n%s\r\n", len(channel), channel)
	workers.Go(func() {
		for {
			caller, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer func() { _ = caller.Close() }()
				var dialer net.Dialer
				server, err := dialer.DialContext(ctx, "tcp", redisAddr())
				if err != nil {
					return
				}
				defer func() { _ = server.Close() }()
				stop := context.AfterFunc(ctx, func() {
					_ = caller.Close()
					_ = server.Close()
				})
				defer stop()
				workers.Go(func() {
					_, _ = io.Copy(server, caller)
					_ = server.Close()
				})
				reader := bufio.NewReader(server)
				for {
					frame, err := readRedisFrame(reader)
					if err != nil {
						return
					}
					if bytes.Contains(frame, match) && dropped.CompareAndSwap(false, true) {
						_ = caller.Close()
						_ = server.Close()
						close(disconnected)
						return
					}
					if _, err = caller.Write(frame); err != nil {
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		workers.Wait()
	})
	return listener.Addr().String(), disconnected
}

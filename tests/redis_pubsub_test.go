package lock

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	lockV1 "github.com/roadrunner-server/api-go/v6/lock/v1"
	"github.com/roadrunner-server/config/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisSubscribeError(t *testing.T) {
	for _, tt := range []struct {
		name        string
		restriction string
		reconnect   bool
	}{
		{name: "command denied", restriction: "-subscribe"},
		{name: "channel denied", restriction: "resetchannels"},
		{name: "reconnect denied", restriction: "-subscribe", reconnect: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			admin := redisAdmin(t, 0)
			username := fmt.Sprintf("rr-lock-subscribe-%d", time.Now().UnixNano())
			require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username,
				"reset", "on", ">"+username, "+@all", "~*", "&*").Err())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				assert.NoError(t, admin.Do(ctx, "ACL", "DELUSER", username).Err())
			})

			holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
			waiter, _ := lockRPCClient(t, &config.Plugin{
				Type: "yaml",
				ReadInCfg: fmt.Appendf(nil,
					"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q], username: %q, password: %q}}",
					redisAddr(), username, username),
			})
			clients, err := admin.ClientList(t.Context()).Result()
			require.NoError(t, err)
			require.Contains(t, clients, "user="+username+" ", "the RPC backend must authenticate as the restricted user")
			resource := t.Name()
			var held lockV1.Response
			require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
			require.True(t, held.GetOk())

			if !tt.reconnect {
				require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, tt.restriction).Err())
			}
			const waiters = 5
			responses := make([]lockV1.Response, waiters)
			done := make(chan *rpc.Call, waiters)
			for i := range responses {
				waiter.Go("lock.Lock", &lockV1.Request{
					Resource: resource, Id: fmt.Sprintf("waiter-%d", i), Wait: new(int64(2_000_000)),
				}, &responses[i], done)
			}
			if tt.reconnect {
				waitForSubscriber(t, admin, resource)
				require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, tt.restriction).Err())
				require.NoError(t, admin.ClientKillByFilter(t.Context(), "TYPE", "pubsub").Err())
			}
			for range responses {
				select {
				case call := <-done:
					require.ErrorContains(t, call.Error, "NOPERM", "subscription rejection must reach every waiting RPC")
					require.False(t, call.Reply.(*lockV1.Response).GetOk())
				case <-time.After(time.Second):
					t.Fatal("subscription rejection did not reach the waiting RPC")
				}
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				counts, err := admin.PubSubNumSub(t.Context(), "rr:lock:"+resource).Result()
				require.NoError(c, err)
				require.Zero(c, counts["rr:lock:"+resource])
			}, time.Second, 5*time.Millisecond)

			// The same backend must accept new waiters after permissions recover.
			require.NoError(t, admin.Do(t.Context(), "ACL", "SETUSER", username, "+subscribe", "&*").Err())
			var acquired lockV1.Response
			pending := waiter.Go("lock.Lock", &lockV1.Request{
				Resource: resource, Id: "recovered", Wait: new(int64(2_000_000)),
			}, &acquired, nil)
			waitForSubscriber(t, admin, resource)
			var released lockV1.Response
			require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
			require.True(t, released.GetOk())
			select {
			case call := <-pending.Done:
				require.NoError(t, call.Error)
				require.True(t, acquired.GetOk())
			case <-time.After(time.Second):
				t.Fatal("the shared subscription did not recover after rejection")
			}
		})
	}
}

func TestRedisWaitIdleSubscription(t *testing.T) {
	holder, waiter, admin := redisRPCClients(t)
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(10_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
	require.NoError(t, err)
	fields := strings.Fields(clients)
	require.NotEmpty(t, fields)
	connection := fields[0]

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
		require.NoError(c, err)
		require.Contains(c, clients, connection+" ", "an idle healthy subscription keeps its connection")
		require.Contains(c, clients, "cmd=ping", "the shared connection checks its health while idle")
	}, 5*time.Second, 20*time.Millisecond)

	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk())
	case <-time.After(time.Second):
		t.Fatal("the idle subscription did not wake after release")
	}
}

func TestRedisWaitReconnectAfterLostPubSubReplies(t *testing.T) {
	admin := redisAdmin(t, 0)
	proxy, dropReplies := redisPubSubProxy(t, 0)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := lockRPCClient(t, &config.Plugin{
		Type: "yaml",
		ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	before := redisScriptCalls(t, admin)
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(12_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	// Both acquisition checks must observe the held lock before replies are lost.
	require.Eventually(t, func() bool { return redisScriptCalls(t, admin) >= before+2 }, time.Second, 5*time.Millisecond)
	dropReplies()
	// The old TCP connection stays open while its notifications and PONGs are lost.
	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk(), "reconnection must recheck the missed release")
	case <-time.After(9 * time.Second):
		t.Fatal("the subscriber did not recover after losing replies")
	}
}

func TestRedisWaitFragmentedNotification(t *testing.T) {
	admin := redisAdmin(t, 0)
	proxy, _ := redisPubSubProxy(t, 3500*time.Millisecond)
	holder, _ := lockRPCClient(t, &config.Plugin{Path: "configs/.rr-lock-redis.yaml"})
	waiter, _ := lockRPCClient(t, &config.Plugin{
		Type: "yaml",
		ReadInCfg: fmt.Appendf(nil,
			"version: '3'\nlogs: {level: error}\nlock: {driver: redis, config: {addrs: [%q]}}", proxy),
	})
	resource := t.Name()
	var held lockV1.Response
	require.NoError(t, holder.Call("lock.Lock", &lockV1.Request{Resource: resource, Id: "holder"}, &held))
	require.True(t, held.GetOk())
	before := redisScriptCalls(t, admin)
	var acquired lockV1.Response
	pending := waiter.Go("lock.Lock", &lockV1.Request{
		Resource: resource, Id: "waiter", Wait: new(int64(8_000_000)),
	}, &acquired, nil)
	waitForSubscriber(t, admin, resource)
	// The notification must arrive after both acquisition checks observe contention.
	require.Eventually(t, func() bool { return redisScriptCalls(t, admin) >= before+2 }, time.Second, 5*time.Millisecond)
	clients, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
	require.NoError(t, err)
	fields := strings.Fields(clients)
	require.NotEmpty(t, fields)
	connection := fields[0]

	var released lockV1.Response
	require.NoError(t, holder.Call("lock.Release", &lockV1.Request{Resource: resource, Id: "holder"}, &released))
	require.True(t, released.GetOk())
	select {
	case call := <-pending.Done:
		require.NoError(t, call.Error)
		require.True(t, acquired.GetOk(), "a fragmented release notification must wake the waiting RPC")
	case <-time.After(9 * time.Second):
		t.Fatal("the fragmented release notification did not complete the waiting RPC")
	}
	clients, err = admin.ClientList(t.Context()).Result()
	require.NoError(t, err)
	require.Contains(t, clients, connection+" ", "the healthy subscription must survive a fragmented notification")
}

func redisScriptCalls(t *testing.T, admin *redis.Client) uint64 {
	t.Helper()
	stats, err := admin.Info(t.Context(), "commandstats").Result()
	require.NoError(t, err)
	for line := range strings.SplitSeq(stats, "\r\n") {
		if stats, found := strings.CutPrefix(line, "cmdstat_evalsha:calls="); found {
			calls, _, _ := strings.Cut(stats, ",")
			count, err := strconv.ParseUint(calls, 10, 64)
			require.NoError(t, err)
			return count
		}
	}
	t.Fatal("Redis did not report the acquisition script calls")
	return 0
}

// redisPubSubProxy can drop replies or pause notifications after the message field.
func redisPubSubProxy(t *testing.T, fragmentDelay time.Duration) (string, func()) {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	type connection struct {
		pubsub  atomic.Bool
		blocked atomic.Bool
	}
	var mu sync.Mutex
	connections := make(map[*connection]struct{})
	var workers sync.WaitGroup
	ctx := t.Context()
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
				current := &connection{}
				mu.Lock()
				connections[current] = struct{}{}
				mu.Unlock()
				defer func() {
					mu.Lock()
					delete(connections, current)
					mu.Unlock()
				}()
				workers.Go(func() {
					_, _ = io.Copy(server, caller)
					_ = server.Close()
				})
				reader := bufio.NewReader(server)
				for {
					line, err := reader.ReadBytes('\n')
					if strings.EqualFold(string(line), "subscribe\r\n") {
						current.pubsub.Store(true)
					}
					if len(line) > 0 && !current.blocked.Load() {
						if _, writeErr := caller.Write(line); writeErr != nil {
							return
						}
						if fragmentDelay > 0 && current.pubsub.Load() && string(line) == "message\r\n" {
							timer := time.NewTimer(fragmentDelay)
							select {
							case <-ctx.Done():
								timer.Stop()
								return
							case <-timer.C:
							}
						}
					}
					if err != nil {
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
	return listener.Addr().String(), func() {
		t.Helper()
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			for current := range connections {
				if current.pubsub.Load() {
					current.blocked.Store(true)
					return true
				}
			}
			return false
		}, time.Second, time.Millisecond)
	}
}
